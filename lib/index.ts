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
  #listExpirationSeconds: number;
  #messageTimeoutSeconds: number;
  #queueListenDebounceMilliseconds: number;
  #listeners: Map<string, ReliableQueueListener<any>> = new Map<
    string,
    ReliableQueueListener<any>
  >();

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
    this.#listExpirationSeconds = config.listExpirationSeconds || 3600;
    this.#messageTimeoutSeconds = config.messageTimeoutSeconds || 60 * 20;
    this.#queueListenDebounceMilliseconds =
      config.queueListenDebounceMilliseconds || 100;
  }

  private async getMessageByPrimaryQueue(params: {
    queueName: string;
    expireTime: string;
    ackList: string;
  }) {
    const { queueName, expireTime, ackList } = params;
    const cli = await this.redisCli();
    const popCommand = this.config.leftPush ? "rpop" : "lpop";

    return cli.eval(luaScript, {
      keys: [
        queueName,
        ackList,
        expireTime.toString(),
        this.#listExpirationSeconds.toString(),
        popCommand,
        "rpush",
      ],
    });
  }

  private async getMessageByAckQueue(queueName: string) {
    const cli = await this.redisCli();
    const ackList = queueName + this.#ackSuffix;

    while (true) {
      const item = await cli.lPop(ackList);

      if (!item) break;

      const expireTime = Number(item.split("|")[0]);

      if (expireTime < new Date().getTime()) {
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

  private async getMessage(params: {
    queueName: string;
    expireTime: string;
    ackList: string;
  }) {
    const { queueName } = params;

    await this.getMessageByAckQueue(queueName);
    return this.getMessageByPrimaryQueue(params);
  }

  private async *popMessage(
    queueName: string
  ): AsyncGenerator<PopMessageResponseDTO> {
    const cli = await this.redisCli();
    const expireTime = new Date(
      new Date().getTime() / 1000 + this.#messageTimeoutSeconds
    ).getTime();
    const ackList = queueName + this.#ackSuffix;

    const result = await this.getMessage({
      queueName,
      expireTime: expireTime.toString(),
      ackList,
    });

    while (true) {
      if (result) {
        yield {
          message: String(result),
          ack: async () => {
            const arkMessage = expireTime.toString() + "|" + String(result);
            await cli.lRem(ackList, 1, arkMessage);
          },
          isEmpty: false,
        };
        await setTimeout(this.#queueListenDebounceMilliseconds);
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

      metrics.queues.push({
        name: queueName,
        size,
        workers,
        waitingAck,
      });
    }

    return metrics;
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
      messagePopper: this.popMessage(params.queueName),
    });

    logger(`Listener started for ${params.queueName}`);
    logger(`Workers started for ${params.queueName}`);

    logger(`Adding listener to HashMap for ${params.queueName}`);
    this.#listeners.set(params.queueName, listener);
    logger(`Listener added to HashMap for ${params.queueName}`);

    logger(`Listening ${params.queueName}`);

    listener.listen();
  }

  async close() {
    const cli = await this.redisCli();
    await cli.disconnect();
  }
}
