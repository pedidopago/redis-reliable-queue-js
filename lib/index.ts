import { createClient, RedisClientType } from "redis";
import { luaScript } from "./lua-script";
import {
  CreateReliableQueueDTO,
  ListenParamsDTO,
  PopMessageResponseDTO,
  PushMessageParamsDTO,
} from "./types";
import { setTimeout } from "timers/promises";

export class ReliableQueue {
  #redisCli: RedisClientType<any, any, any>;
  #ackSuffix: string;
  #listExpirationSeconds: number;
  #messageTimeoutSeconds: number;
  #emptyQueueTimeoutSeconds: number;

  private async redisCli() {
    if (this.#redisCli.isOpen) {
      return this.#redisCli;
    }

    await this.#redisCli.connect();

    if (!this.#redisCli.isOpen) {
      throw new Error("Could not connect to Redis");
    }

    return this.#redisCli;
  }

  constructor(private readonly config: CreateReliableQueueDTO) {
    this.#redisCli = createClient({
      url: config.url,
    });
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

  async listen(params: ListenParamsDTO): Promise<void> {
    while (true) {
      const value = await this.popMessage(params.queueName);
      if (value) {
        const [message, ackFunction] = value;
        const ack = await params.callback(message);
        if (ack) await ackFunction();
        continue;
      }

      await setTimeout(this.#emptyQueueTimeoutSeconds * 1000);
    }
  }

  async close() {
    const cli = await this.redisCli();
    await cli.disconnect();
  }
}
