# redis-reliable-queue
<a href="https://github.com/pedidopago/redis-reliable-queue-go">![go](https://img.shields.io/badge/go-1.18-blue)</a>
<a href="https://github.com/pedidopago/redis-reliable-queue-js">![node-ts](https://img.shields.io/badge/node-14%2B-yellow)</a>

A Node.js module that implements a reliable queue that uses Redis for the backend.
It uses the RPOPLPUSH pattern:
https://redis.io/commands/rpoplpush#pattern-reliable-queue

References:
https://blog.tuleap.org/how-we-replaced-rabbitmq-redis

## Installation 
```sh
# with npm:
npm install @pedidopago/redis-reliable-queue --save

# with yarn:
yarn add @pedidopago/redis-reliable-queue
```

## Usage
```typescript
import { ReliableQueue, Listener, Message } from '@pedidopago/redis-reliable-queue';
// import { ClientOpts } from 'redis';
// ...
type MyMessagePayload interface {
    order_id : string;
}
// create a queue with exis
const rq = ReliableQueue.newWithRedisOpts<MyMessagePayload>("queuename", 6379,"redis-host.pedidopago.com.br");
// send a message:
await rq.pushMessage("topic", {order_id: "FFABE9"});
// listen for messages:
const listener = await rq.listen("worker-id");
// the worker ID is used to retrieve messages if the service crashes while reading messages.

// to wait until a message is received:
const [message, finalizemsg] = await listener.waitForMessage();
// the message is of type Message<MyMessagePayload> in this case
console.log(message.topic); // string -> "topic"
console.log(message.content); // MyMessagePayload {order_id: "FFABE9"}
await finalizemsg(); // you must run this after you did something with msg successfully
// failing to run "finalizemsg" will persist the message, so it will be received
// again once you instantiate rq.listen("id") after a restart
```

## Test

Setup the ".env" (optional):
```sh
#.env
REDIS_HOST=192.168.X.Y
REDIS_PORT=6379
TEST_QUEUE=sometestqueue
```

Run:
```sh
npm run test
```
