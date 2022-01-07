import { createNodeRedisClient, WrappedNodeRedisClient } from 'handy-redis';
import { ClientOpts } from 'redis';

export class ReliableQueue<T> {
    private rediscl: WrappedNodeRedisClient;
    private name: string;

    constructor(queuename: string, rediscl: WrappedNodeRedisClient) {
        this.name = queuename;
        this.rediscl = rediscl;
    };

    static newWithRedisOpts(queuename: string, port_arg: number, host_arg?: string, options?: ClientOpts): ReliableQueue {
        const rediscl = createNodeRedisClient(port_arg, host_arg, options);
        const rq = new ReliableQueue(queuename, rediscl);
        return rq;
    };

    async listen(workerID: string) {
        const processingQueue = this.name + "-processing-" + workerID
        await this.queuePastEvents(processingQueue);
    };
    private async queuePastEvents(processingQueue: string): Promise<void> {
        let lastv = "init";
        while (lastv !== "") {
            lastv = await this.rediscl.rpoplpush(processingQueue, this.name);
        }
    };
}

export class Listener<T> {

    private rediscl: WrappedNodeRedisClient;
    private name: string;
    private processingQueue: string;


    constructor(queuename: string, processingQueue: string, rediscl: WrappedNodeRedisClient) {
        this.name = queuename;
        this.processingQueue = processingQueue;
        this.rediscl = rediscl;
    }

    async waitForMessage(timeout: number = 0): Promise<Message<T> | null> {
        const msg = await this.rediscl.brpoplpush(this.name, this.processingQueue, timeout);
        if (msg === null) {
            return null;
        }
        //TODO: continue
        return msg;
    }
}

export interface Message<T> {
    topic: string;
    content: T;
}