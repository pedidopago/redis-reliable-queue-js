import { createClient, RedisClientType } from "redis";
import { luaScript } from "./lua-script";
import { setTimeout } from "timers/promises";
import { ReliableQueueCluster } from "./worker";
import {
  CreateReliableQueueDTO,
  ListenParamsDTO,
  MetricsDTO,
  PopMessageResponseDTO,
  PushMessageParamsDTO,
} from "./types";
import { ReliableQueueListener } from "./listener";

export class ReliableQueue {
  #redisCli: RedisClientType<any, any, any>;
  #ackSuffix = "-ack";
  #listExpirationSeconds: number;
  #messageTimeoutSeconds: number;
  #queueListenDebounceMilliseconds: number;
  #emptyQueueTimeoutSeconds: number;
  #listeners: Map<string, ReliableQueueListener<any>> = new Map<
    string,
    ReliableQueueListener<any>
  >();

  private async redisCli(): Promise<RedisClientType<any, any, any>> {
    try {
      if (this.#redisCli.isOpen) {
        return this.#redisCli;
      }

      await this.#redisCli.connect();

      if (!this.#redisCli.isOpen) {
        throw new Error("Could not connect to Redis");
      }

      return this.#redisCli;
    } catch (e) {
      this.#redisCli = this.createRedisClient();
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
    this.#emptyQueueTimeoutSeconds = config.emptyQueueTimeoutSeconds || 60;
    this.#queueListenDebounceMilliseconds =
      config.queueListenDebounceMilliseconds || 100;
  }

  private async *popMessage(
    queueName: string
  ): AsyncGenerator<PopMessageResponseDTO> {
    while (true) {
      const cli = await this.redisCli();
      const ackList = queueName + this.#ackSuffix;
      const popCommand = this.config.lifo ? "rpop" : "lpop";
      const expireTime = new Date(
        new Date().getTime() / 1000 + this.#messageTimeoutSeconds
      ).getTime();

      const result = await cli.eval(luaScript, {
        keys: [
          queueName,
          ackList,
          expireTime.toString(),
          this.#listExpirationSeconds.toString(),
          popCommand,
          "rpush",
        ],
      });

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
      await setTimeout(this.#emptyQueueTimeoutSeconds * 1000);
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

    if (this.config.lifo) {
      await cli.lPush(queueName, message);
    } else {
      await cli.rPush(queueName, message);
    }
  }

  listen<MessageType>(params: ListenParamsDTO<MessageType>): void {
    if (this.#listeners.has(params.queueName)) {
      throw new Error(`Already listening ${params.queueName}`);
    }

    const cluster = new ReliableQueueCluster({
      clusterId: params.queueName,
      maxWorkers: params.workers,
    });

    const listener = new ReliableQueueListener<MessageType>({
      cluster,
      config: params,
      messagePopper: this.popMessage(params.queueName),
    });

    this.#listeners.set(params.queueName, listener);

    listener.listen();
  }

  async close() {
    const cli = await this.redisCli();
    await cli.disconnect();
  }
}
