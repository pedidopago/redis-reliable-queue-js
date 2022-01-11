import { createNodeRedisClient, WrappedNodeRedisClient } from "handy-redis";
import { ClientOpts } from "redis";

export class ReliableQueue<T> {
  private rediscl: WrappedNodeRedisClient;
  private name: string;

  constructor(queuename: string, rediscl: WrappedNodeRedisClient) {
    this.name = queuename;
    this.rediscl = rediscl;
  }

  static newWithRedisOpts<T2>(
    queuename: string,
    port_arg: number,
    host_arg?: string,
    options?: ClientOpts
  ): ReliableQueue<T2> {
    const rediscl = createNodeRedisClient(port_arg, host_arg, options);
    const rq = new ReliableQueue<T2>(queuename, rediscl);
    return rq;
  }

  async listen(workerID: string): Promise<Listener<T>> {
    const processingQueue = this.name + "-processing-" + workerID;
    await this.queuePastEvents(processingQueue);
    return new Listener<T>(this.name, processingQueue, this.rediscl);
  }

  async pushMessage(topic: string, content: T): Promise<number> {
    const n = await this.rediscl.lpush(
      this.name,
      JSON.stringify({ topic, content })
    );
    return n;
  }

  async close(): Promise<void> {
    await this.rediscl.quit();
  }

  private async queuePastEvents(processingQueue: string): Promise<void> {
    let lastv = "init";
    while (lastv) {
      lastv = await this.rediscl.rpoplpush(processingQueue, this.name);
    }
  }
}

export class Listener<T> {
  private rediscl: WrappedNodeRedisClient;
  private name: string;
  private processingQueue: string;

  constructor(
    queuename: string,
    processingQueue: string,
    rediscl: WrappedNodeRedisClient
  ) {
    this.name = queuename;
    this.processingQueue = processingQueue;
    this.rediscl = rediscl;
  }

  async waitForMessage(
    timeout: number = 10
  ): Promise<[Message<T> | null, Function]> {
    const msg = await this.rediscl.brpoplpush(
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
      await this.rediscl.lrem(this.processingQueue, 1, msg);
    };
    return [v, afn];
  }
}

export interface Message<T> {
  topic: string;
  content: T;
}
