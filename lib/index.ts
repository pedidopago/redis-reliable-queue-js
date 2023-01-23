import { createNodeRedisClient, WrappedNodeRedisClient } from "handy-redis";
import { ClientOpts } from "redis";

type ConnectionConfig = {
  port: number;
  host: string;
  options?: ClientOpts;
}
export class ReliableQueue<T> {
  private redisCli: WrappedNodeRedisClient;

  constructor(
    private readonly name: string,
    private readonly connectionConfig: ConnectionConfig,
  ) {
    const {port, host, options} = connectionConfig;

    const redisCli = createNodeRedisClient(port, host, options);
    if(options && options.password) {
      // auth may be fullfilled or rejected
      redisCli.auth(options.password).then((ok) => {
        //console.log("redis auth ok!", ok);
      }, (e) => {
        console.error("redis auth error!", e);
      });
    }

    this.redisCli = redisCli;
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
    return new Listener<T>(this.name, processingQueue, this.connectionConfig, this.redisCli);
  }

  async pushMessage(topic: string, content: T): Promise<number> {
    const n = await this.redisCli.lpush(
      this.name,
      JSON.stringify({ topic, content })
    );
    return n;
  }

  async pushRawMessage(content: T): Promise<number> {
    const n = await this.redisCli.lpush(this.name, JSON.stringify(content));
    return n;
  }

  async close(): Promise<void> {
    await this.redisCli.quit();
  }

  private async queuePastEvents(processingQueue: string): Promise<void> {
    let lastv = "init";
    while (lastv) {
      lastv = await this.redisCli.rpoplpush(processingQueue, this.name);
    }
  }
}

export class Listener<T> {

  constructor(
    private readonly name: string,
    private readonly processingQueue: string,
    private readonly connectionConfig: ConnectionConfig,
    private redisCli: WrappedNodeRedisClient
  ) {}

  async waitForMessage(
    timeout: number = 10
  ): Promise<[Message<T> | null, Function]> {
    await this.assertRedisConnection();
    
    const msg = await this.redisCli.brpoplpush(
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
      await this.redisCli.lrem(this.processingQueue, 1, msg);
    };
    return [v, afn];
  }

  private async newConnection(): Promise<WrappedNodeRedisClient> {
    const {port, host, options} = this.connectionConfig;
    const rediscl = createNodeRedisClient(port, host, options);
    
    if(options && options.password) {
      // auth may be fullfilled or rejected
      rediscl.auth(options.password).then((ok) => {
        //console.log("redis auth ok!", ok);
      }, (e) => {
        console.error("redis auth error!", e);
      });
    }
    return rediscl;
  }

  async assertRedisConnection(): Promise<void> {
    try{
      await this.redisCli.ping();
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
