import { createClient, RedisClientType } from "redis";
import { luaScript } from "./lua-script";
import { setTimeout } from "timers/promises";
import { ReliableQueueCluster } from "./worker";
import {
  CreateReliableQueueDTO,
  ListenParamsDTO,
  PopMessageResponseDTO,
  PushMessageParamsDTO,
} from "./types";

export class ReliableQueue {
  #redisCli: RedisClientType<any, any, any>;
  #ackSuffix: string;
  #listExpirationSeconds: number;
  #messageTimeoutSeconds: number;
  #emptyQueueTimeoutSeconds: number;
  #listeners: string[] = [];
  #workers: ReliableQueueCluster[] = [];

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
    this.#ackSuffix = config.ackSuffix || "-ack";
    this.#listExpirationSeconds = config.listExpirationSeconds || 3600;
    this.#messageTimeoutSeconds = config.messageTimeoutSeconds || 60 * 20;
    this.#emptyQueueTimeoutSeconds = config.emptyQueueTimeoutSeconds || 60;
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

  async popMessage(
    queueName: string
  ): Promise<PopMessageResponseDTO | undefined> {
    const cli = await this.redisCli();
    const ackList = queueName + this.#ackSuffix;
    const popCommand = this.config.lifo ? "rpop" : "lpop";
    const expireTime =
      new Date().getTime() / 1000 + this.#messageTimeoutSeconds;

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
      return [
        String(result),
        async () => {
          const arkMessage = expireTime + "|" + String(result);
          await cli.lRem(ackList, 1, arkMessage);
        },
      ];
    }
  }

  listen<MessageType>(params: ListenParamsDTO<MessageType>): void {
    if (this.#listeners.includes(params.queueName)) {
      throw new Error(`Already listening ${params.queueName}`);
    }

    new Promise(async () => {
      let message: string | undefined;
      let ack: Function | undefined;

      while (true) {
        try {
          const value = await this.popMessage(params.queueName);

          if (!value) {
            if (params.queueEmptyHandler) await params.queueEmptyHandler();
            await setTimeout(this.#emptyQueueTimeoutSeconds * 1000);
            continue;
          }

          message = value[0];
          ack = value[1];

          const transformedMessage = await params.transform(message);

          const validated = params.validate
            ? await params.validate(transformedMessage)
            : true;

          if (!validated) {
            await params.errorHandler(
              new Error("Message validation failed"),
              message
            );
            await ack();
            continue;
          }

          const mutexKey = this.getListenMutex(
            transformedMessage,
            params.mutexPath
          );

          const job = async () => {
            try {
              await params.job({
                message: transformedMessage,
              });
              // @ts-ignore
              await ack();
            } catch (e) {
              const error = e as Error;
              // @ts-ignore
              await params.errorHandler(error, message);
              // @ts-ignore
              await ack();
            }
          };

          let worker;

          const workerExists = this.#workers.find(
            (worker) => worker.clusterId === params.queueName
          );

          if (workerExists) {
            worker = workerExists;
          } else {
            worker = new ReliableQueueCluster({
              maxWorkers: params.workers,
              clusterId: params.queueName,
            });
            this.#workers.push(worker);
          }

          await worker.addJob({
            job,
            mutexKey,
          });
        } catch (e) {
          const error = e as Error;
          if (message && ack) {
            await params.errorHandler(error, message);
            await ack();
          }
        }
      }
    });
  }

  private getListenMutex<MessageType>(
    message: MessageType,
    mutexPath?: string
  ): string | undefined {
    if (mutexPath) {
      if (typeof message !== "object" || Array.isArray(message) || !message) {
        throw new Error("Mutex path is only available for object messages");
      }

      const path = mutexPath.split(".");

      let mutex = message;

      for (const key of path) {
        //@ts-ignore
        if (mutex[key]) {
          //@ts-ignore
          mutex = mutex[key];
        } else {
          return undefined;
        }
      }

      if (typeof mutex !== "string") {
        throw new Error("Mutex path must be a string");
      }

      return mutex;
    }

    return undefined;
  }

  async close() {
    const cli = await this.redisCli();
    await cli.disconnect();
  }
}
