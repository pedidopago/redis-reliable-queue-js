import { createNodeRedisClient, WrappedNodeRedisClient } from "handy-redis";
import { ClientOpts } from "redis";

type ConnectionConfig = {
  port: number;
  host: string;
  options?: ClientOpts;
}
export class ReliableQueue<T> {
  private redisCli: WrappedNodeRedisClient | undefined;

  constructor(
    private readonly name: string,
    private readonly connectionConfig: ConnectionConfig,
  ) {}

  async getRedisConnection(): Promise<WrappedNodeRedisClient> {
    if(this.redisCli) {
      return this.redisCli;
    }
    const {port, host, options} = this.connectionConfig;
    
    const redisCli = createNodeRedisClient(port, host, options);
    
    if(options && options.password) {
      const auth = await redisCli.auth(options.password)

      if (auth === "OK") {
        this.redisCli = redisCli;
        return redisCli;
      }
    }

    throw new Error("Could not connect to redis server because password is not set");
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
    const redisCli = await this.getRedisConnection();
    const processingQueue = this.name + "-processing-" + workerID;
    await this.queuePastEvents(processingQueue);
    return new Listener<T>(this.name, processingQueue, this.connectionConfig);
  }

  async pushMessage(topic: string, content: T): Promise<number> {
    const redisCli = await this.getRedisConnection();

    const n = await redisCli.lpush(
      this.name,
      JSON.stringify({ topic, content })
    );
    return n;
  }

  async pushRawMessage(content: T): Promise<number> {
    const redisCli = await this.getRedisConnection();

    const n = await redisCli.lpush(this.name, JSON.stringify(content));
    return n;
  }

  async close(): Promise<void> {
    const redisCli = await this.getRedisConnection();
    await redisCli.quit();
  }

  private async queuePastEvents(processingQueue: string): Promise<void> {
    const redisCli = await this.getRedisConnection();

    let lastv = "init";
    while (lastv) {
      lastv = await redisCli.rpoplpush(processingQueue, this.name);
    }
  }
}

export class Listener<T> {
  private redisCli: WrappedNodeRedisClient | undefined;

  constructor(
    private readonly name: string,
    private readonly processingQueue: string,
    private readonly connectionConfig: ConnectionConfig,
  ) {}

  async waitForMessage(
    timeout: number = 10
  ): Promise<[Message<T> | null, Function]> {
    await this.assertRedisConnection();
    const redisCli = await this.getRedisConnection();
    
    const msg = await redisCli.brpoplpush(
      this.name,
      this.processingQueue,
      timeout
    );

    if (msg === null) {
      return [null, async () => {}];
    }
    //TODO: check if v is valid (?)
    const v = JSON.parse(msg) as Message<T>;
    const afn = async () => {
      await redisCli.lrem(this.processingQueue, 1, msg);
    };
    return [v, afn];
  }

  private async getRedisConnection(): Promise<WrappedNodeRedisClient> {
    if(this.redisCli) {
      return this.redisCli;
    }
    
    this.redisCli = await this.newConnection();
    return this.redisCli;
  }

  private async newConnection(): Promise<WrappedNodeRedisClient> {
    const {port, host, options} = this.connectionConfig;
    const redisCli = createNodeRedisClient(port, host, options);
    
    if(options && options.password) {
      // auth may be fullfilled or rejected
      const auth = await redisCli.auth(options.password);

      if(auth === "OK") {
        return redisCli;
      }
    }
    
    throw new Error("Could not connect to redis server because password is not set");
  }

  private async assertRedisConnection(): Promise<void> {
    const redisCli = await this.getRedisConnection();

    try{
      await redisCli.ping();
    }catch(e) {
      try{
        this.redisCli = await this.newConnection();
      }catch(e){
        throw new Error("Could not connect to redis server");
      }
    }
  }
}

export interface Message<T> {
  topic: string;
  content: T;
}
