# redis-reliable-queue
A Node.js module that implements a reliable queue that uses Redis for the backend.
It uses the RPOPLPUSH pattern:
https://redis.io/commands/rpoplpush#pattern-reliable-queue

References:
https://blog.tuleap.org/how-we-replaced-rabbitmq-redis

## Installation 
```sh
npm install @pedidopago/redis-reliable-queue --save
npm install redis@3 --save
npm install @types/redis@2 --save

# you can also use yarn:
yarn add @pedidopago/redis-reliable-queue
yarn add redis@3 --save
yarn add @types/redis@2 --save
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
const rq = ReliableQueue.newWithRedisOpts<MyMessagePayload>(6379,"redis-host.pedidopago.com.br");
// send a message:
await rq.pushMessage("topic", {order_id: "FFABE9"});
// listen for messages:
const listener = await rq.listen("worker-id");
// the worker ID is used to retrieve messages if the service crashes while reading messages.

// to wait until a message is received:
const message = listener.waitForMessage();
// the message is of type Message<MyMessagePayload> in this case
console.log(message.topic); // string
console.log(message.content); // MyMessagePayload
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