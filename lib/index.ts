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
  #redisCli: RedisClientType<any, any, any>;
  #ackSuffix = "-ack";
  #listeners: Map<string, ReliableQueueListener<any>> = new Map<
    string,
    ReliableQueueListener<any>
  >();
  #listExpirationInMinutes: number;

  private async redisCli(): Promise<RedisClientType<any, any, any>> {
    try {
      if (this.#redisCli.isOpen) {
        logger("Redis is already connected");
        return this.#redisCli;
      }

      logger("Connecting to Redis");
      await this.#redisCli.connect();

      if (!this.#redisCli.isOpen) {
        throw new Error("Could not connect to Redis");
      }

      logger("Connected to Redis");
      return this.#redisCli;
    } catch (e) {
      logger("Could not connect to Redis");
      logger(e);
      logger("Creating new Redis client");
      this.#redisCli = this.createRedisClient();
      logger("Created new Redis client");
      logger("Retrying to connect to Redis");
      return this.redisCli();
    }
  }

  private createRedisClient(): RedisClientType<any, any, any> {
    return createClient({
      url: this.config.url,
      password: this.config.password,
    });
  }

  constructor(private readonly config: CreateReliableQueueDTO) {
    this.#redisCli = this.createRedisClient();
    this.#listExpirationInMinutes = Number(
      (config.listExpirationInMinutes * 60).toFixed()
    );
  }

  private async getMessageByPrimaryQueue(
    params: ListenParamsDTO<any> & { expireTime: string }
  ) {
    const { queueName } = params;
    const cli = await this.redisCli();
    const popCommand = this.config.leftPush ? "rpop" : "lpop";
    const ackList = queueName + this.#ackSuffix;
    const expireTime = params.expireTime;
    const ackListExpireTime = this.#listExpirationInMinutes.toString();

    return cli.eval(luaScript, {
      keys: [
        queueName,
        ackList,
        expireTime,
        ackListExpireTime,
        popCommand,
        "lpush",
      ],
    });
  }

  private async moveAckToPrimaryQueue(queueName: string, forced = false) {
    const cli = await this.redisCli();
    const ackList = queueName + this.#ackSuffix;

    const length = await cli.lLen(ackList);

    for (let i = 0; i < length; i++) {
      const item = await cli.lIndex(ackList, 0);

      if (!item) break;

      const expireTime = Number(item.split("|")[0]);

      if (expireTime < new Date().getTime() && !forced) {
        //Push message back to queue

        const message = item.split("|")[1];

        const pushCommand = this.config.leftPush ? "rPush" : "lPush";

        await cli[pushCommand](queueName, message);
        await cli.lRem(ackList, 1, item);
        continue;
      }

      break;
    }
  }

  private async getMessage(
    params: ListenParamsDTO<any> & {
      expireTime: string;
    }
  ) {
    const { queueName } = params;

    await this.moveAckToPrimaryQueue(queueName);
    return this.getMessageByPrimaryQueue(params);
  }

  private async *popMessage(
    params: ListenParamsDTO<any>
  ): AsyncGenerator<PopMessageResponseDTO> {
    const cli = await this.redisCli();
    const nowInMilliseconds = new Date().getTime();
    const expireTimeMs = nowInMilliseconds + params.messageTimeoutMilliseconds;
    const expireTime = expireTimeMs.toFixed();
    const queueListenDebounceMilliseconds =
      params.queueListenDebounceMilliseconds || 10;
    const ackList = params.queueName + this.#ackSuffix;

    while (true) {
      const result = await this.getMessage({
        ...params,
        expireTime,
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
        await setTimeout(queueListenDebounceMilliseconds);
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
    const cli = await this.redisCli();
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
    return this.redisCli();
  }

  async pushMessage(params: PushMessageParamsDTO) {
    const { queueName, message } = params;
    const cli = await this.redisCli();

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
        await this.moveAckToPrimaryQueue(params.queueName, true);
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
    const cli = await this.redisCli();

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
    const cli = await this.redisCli();
    await cli.del(params.key);
  }

  async close() {
    const cli = await this.redisCli();
    await cli.disconnect();
  }
}
