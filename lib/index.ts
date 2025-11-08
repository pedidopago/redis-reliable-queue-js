import { createClient, RedisClientType } from "redis";
import { luaScript } from "./lua-script";
import { setTimeout } from "timers/promises";
import { ReliableQueueCluster } from "./worker";
import { ReliableQueueListener } from "./listener";
import { logger } from "./logger";
import {
  CreateReliableQueueDTO,
  ListenParamsDTO,
  MetricsDTO,
  PopMessageResponseDTO,
  PushMessageParamsDTO,
} from "./types";

export class ReliableQueue {
  #redisCli!: RedisClientType<any, any, any>;
  #ackSuffix = "-ack";
  #listeners: Map<string, ReliableQueueListener<any>> = new Map<
    string,
    ReliableQueueListener<any>
  >();
  #listExpirationInMinutes: number;

  private async redisCliRef(): Promise<RedisClientType<any, any, any>> {
    if (this.#redisCli) return this.#redisCli;

    const cli = createClient({
      url: this.config.url,
      password: this.config.password,
      socket: {
        connectTimeout: 1000,
        reconnectStrategy(retries, cause) {
          logger("Redis reconnecting", { retries, cause });
          return Math.min(retries * 50, 10000);
        },
      },
    });

    cli.on("connect", () => {
      logger("Redis connecting");
    });

    cli.on("error", (err) => {
      logger("Redis error", { err });
    });

    cli.on("ready", () => {
      logger("Redis connected");
    });

    cli.on("end", () => {
      logger("Redis disconnected");
    });

    cli.on("reconnecting", () => {
      logger("Redis reconnecting");
    });

    cli.on("close", () => {
      logger("Redis closed");
    });

    await cli.connect();

    this.#redisCli = cli;

    return cli;
  }

  constructor(private readonly config: CreateReliableQueueDTO) {
    this.#listExpirationInMinutes = Number(
      (config.listExpirationInMinutes * 60).toFixed()
    );
  }

  private async moveAckToPrimaryQueue(queueName: string) {
    const cli = await this.redisCliRef();
    const ackList = queueName + this.#ackSuffix;

    const length = await cli.lLen(ackList);
    const charToSplit = "|";

    for (let i = length - 1; i !== -1; i--) {
      const cliForItem = await this.redisCliRef();
      const item = await cliForItem.lIndex(ackList, i);

      if (!item) break;

      const splitIndex = item.indexOf(charToSplit);
      const message = item.slice(splitIndex + 1);
      const pushCommand = this.config.leftPush ? "rPush" : "lPush";

      await cli[pushCommand](queueName, message);
      await cli.lRem(ackList, 1, item);
    }
  }

  private async *popMessage(
    params: ListenParamsDTO<any>
  ): AsyncGenerator<PopMessageResponseDTO> {
    const { queueName } = params;

    const nowInMilliseconds = new Date().getTime();
    const expireTimeMs = nowInMilliseconds + params.messageTimeoutMilliseconds;
    const expireTime = expireTimeMs.toFixed();
    const queueListenDebounceMilliseconds =
      params.queueListenDebounceMilliseconds || 10;
    const ackList = params.queueName + this.#ackSuffix;

    while (true) {
      const cli = await this.redisCliRef();
      const popCommand = this.config.leftPush ? "rpop" : "lpop";
      const ackListExpireTime = this.#listExpirationInMinutes.toString();

      const result = await cli.eval(luaScript, {
        keys: [
          queueName,
          ackList,
          nowInMilliseconds.toString(),
          expireTime,
          ackListExpireTime,
          popCommand,
          "1000",
        ],
      });

      if (result) {
        yield {
          message: String(result),
          ack: async () => {
            const arkMessage = expireTime + "|" + String(result);
            await cli.lRem(ackList, 1, arkMessage);
          },
          isEmpty: false,
        };
        await new Promise((r) => setImmediate(r));
        continue;
      }

      yield {
        isEmpty: true,
        message: "",
        ack: async () => {},
      };
    }
  }

  async metrics(): Promise<MetricsDTO> {
    const cli = await this.redisCliRef();
    const queues = this.#listeners;

    const metrics: MetricsDTO = {
      redis: {
        connected: this.#redisCli.isOpen,
      },
      queues: [],
    };

    for (const [queueName, listener] of queues) {
      const size = await cli.lLen(queueName);
      const ackList = queueName + this.#ackSuffix;
      const waitingAck = await cli.lLen(ackList);
      const workers = listener.cluster.toJSON().workers;
      const runningWorkers = workers.filter((w) => w.isRunning);

      metrics.queues.push({
        name: queueName,
        size,
        workers: runningWorkers,
        workersRunning: runningWorkers.length,
        workersLength: workers.length,
        waitingAck,
      });
    }

    return metrics;
  }

  async getRedisCLI() {
    return this.redisCliRef();
  }

  async pushMessage(params: PushMessageParamsDTO) {
    const { queueName, message } = params;
    const cli = await this.redisCliRef();

    if (this.config.leftPush) {
      await cli.lPush(queueName, message);
    } else {
      await cli.rPush(queueName, message);
    }
  }

  listen<MessageType>(params: ListenParamsDTO<MessageType>): void {
    if (this.#listeners.has(params.queueName)) {
      throw new Error(`Already listening ${params.queueName}`);
    }

    logger(`Listening ${params.queueName}`);

    const cluster = new ReliableQueueCluster({
      clusterId: params.queueName,
      maxWorkers: params.workers,
    });

    logger(`Starting ${params.workers} workers for ${params.queueName}`);
    logger(`Starting listener for ${params.queueName}`);

    const listener = new ReliableQueueListener<MessageType>({
      cluster,
      config: params,
      messagePopper: this.popMessage(params),
      onInit: async () => {
        logger(`Listener initialized for ${params.queueName}`);
        await this.moveAckToPrimaryQueue(params.queueName);
      },
    });

    logger(`Listener started for ${params.queueName}`);
    logger(`Workers started for ${params.queueName}`);

    logger(`Adding listener to HashMap for ${params.queueName}`);
    this.#listeners.set(params.queueName, listener);
    logger(`Listener added to HashMap for ${params.queueName}`);

    logger(`Listening ${params.queueName}`);

    listener.listen();
  }

  async getLock(params: { key: string; expireTimeSeconds: number }) {
    const cli = await this.redisCliRef();

    while (true) {
      const getLock = await cli.set(params.key, "1", {
        EX: params.expireTimeSeconds,
        NX: true,
      });

      if (getLock) break;

      await setTimeout(100);
    }

    return true;
  }

  async releaseLock(params: { key: string }) {
    const cli = await this.redisCliRef();
    await cli.del(params.key);
  }

  async close() {
    const cli = await this.redisCliRef();
    await cli.disconnect();
  }
}
