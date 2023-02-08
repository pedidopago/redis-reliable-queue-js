import { createNodeRedisClient, WrappedNodeRedisClient } from "handy-redis";
import { ClientOpts } from "redis";

type ConnectionConfig = {
  port: number;
  host: string;
  options?: ClientOpts;
}

class RedisCli {
  private redisCli: WrappedNodeRedisClient | undefined;

  constructor(
    private readonly connectionConfig: ConnectionConfig,
  ) { }

  private async newConnection(): Promise<WrappedNodeRedisClient> {
    const { port, host, options } = this.connectionConfig;
    const redisCli = createNodeRedisClient(port, host, options);

    if (options && options.password) {
      // auth may be fullfilled or rejected
      await redisCli.auth(options.password);
    }

    return redisCli;
  }

  private async getRedisConnection(): Promise<WrappedNodeRedisClient> {
    if (this.redisCli) {
      return this.redisCli;
    }

    const redisCli = await this.newConnection();
    this.redisCli = redisCli;

    return redisCli;
  }

  private async assertRedisConnection(): Promise<void> {
    const redisCli = await this.getRedisConnection();

    try {
      await redisCli.ping();
    } catch (e) {
      try {
        this.redisCli = await this.newConnection();
      } catch (e) {
        throw new Error("Could not connect to redis server");
      }
    }
  }

  async getCli(): Promise<WrappedNodeRedisClient> {
    await this.assertRedisConnection();
    return await this.getRedisConnection();
  }

  async close(): Promise<void> {
    const redisCli = await this.getRedisConnection();
    await redisCli.quit();
  }
}

export class ReliableQueue<T> {
  private redisCli: RedisCli

  constructor(
    private readonly name: string,
    private readonly connectionConfig: ConnectionConfig,
  ) {
    this.redisCli = new RedisCli(connectionConfig);
  }

  static newWithRedisOpts<T2>(
    queueName: string,
    port: number,
    host: string,
    options?: ClientOpts
  ): ReliableQueue<T2> {
    const rq = new ReliableQueue<T2>(queueName, {
      host,
      port,
      options
    });

    return rq;
  }

  async listen(workerID: string): Promise<Listener<T>> {
    const processingQueue = this.name + "-processing-" + workerID;
    await this.queuePastEvents(processingQueue);
    return new Listener<T>(this.name, processingQueue, this.redisCli);
  }

  async pushMessage(topic: string, content: T): Promise<number> {
    const redisCli = await this.redisCli.getCli()

    const n = await redisCli.lpush(
      this.name,
      JSON.stringify({ topic, content })
    );
    return n;
  }

  async pushRawMessage(content: T): Promise<number> {
    const redisCli = await this.redisCli.getCli()

    const n = await redisCli.lpush(this.name, JSON.stringify(content));
    return n;
  }

  async close(): Promise<void> {
    const redisCli = await this.redisCli.getCli();
    await redisCli.quit();
  }

  private async queuePastEvents(processingQueue: string): Promise<void> {
    const redisCli = await this.redisCli.getCli();
    let lastv = "init";
    while (lastv) {
      lastv = await redisCli.rpoplpush(processingQueue, this.name);
    }
  }
}

export class Listener<T> {
  constructor(
    private readonly name: string,
    private readonly processingQueue: string,
    private readonly redisCli: RedisCli,
  ) { }

  async waitForMessage(
    timeout: number = 10
  ): Promise<[Message<T> | null, Function]> {
    const redisCli = await this.redisCli.getCli();

    const msg = await redisCli.brpoplpush(
      this.name,
      this.processingQueue,
      timeout
    );

    if (msg === null) {
      return [null, async () => { }];
    }
    //TODO: check if v is valid (?)
    const v = JSON.parse(msg) as Message<T>;
    const afn = async () => {
      await redisCli.lrem(this.processingQueue, 1, msg);
    };
    return [v, afn];
  }
}

export interface Message<T> {
  topic: string;
  content: T;
}
