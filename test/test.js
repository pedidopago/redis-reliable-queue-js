'use strict';
var expect = require('chai').expect;
var index = require('../dist/index');

const tqueue = process.env.TEST_QUEUE || 'test-queue';
const redishost = process.env.REDIS_HOST || 'localhost';
const redisport = process.env.REDIS_PORT || '6379';

describe('test reliable queue push pop', () => {
    const rq = index.ReliableQueue.newWithRedisOpts(tqueue, parseInt(redisport), redishost);
    it('should return >0', async () => {
        const result = await rq.pushMessage('topic1', 'hello');
        expect(result).to.greaterThan(0);
    });
    it('should return topic1:hello', async () => {
        const listener = await rq.listen('testworker');
        const [msg, didprocess] = await listener.waitForMessage(1);
        await didprocess();
        expect(msg.topic).to.equal('topic1');
        expect(msg.content).to.equal('hello');
    })
})