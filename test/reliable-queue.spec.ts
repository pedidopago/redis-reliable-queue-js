import { ReliableQueue } from "../lib";
import { setTimeout as setTimeoutAsync } from "timers/promises";

jest.setTimeout(90000);

const log = (...message: any[]) => {
  console.log(message);
};

describe("ReliableQueue", () => {
  let rq: ReliableQueue;

  beforeAll(async () => {
    rq = new ReliableQueue({
      url: "redis://localhost:6379",
      password: "password",
      emptyQueueTimeoutSeconds: 5,
    });
  });

  afterAll(async () => {
    await rq.close();
  });

  // it("should push message", async () => {
  //   const addMutex = async (mutexKey: string, length = 10) => {
  //     const promises = Array.from({ length }).map(async (_, index) => {
  //       await rq.pushMessage({
  //         queueName: "test",
  //         message: JSON.stringify({
  //           id: index,
  //           mutexTest: mutexKey,
  //         }),
  //       });
  //     });

  //     await Promise.all(promises);
  //   };

  //   await addMutex("a");
  //   await addMutex("b");
  //   await addMutex("c", 2);

  //   await new Promise(async (resolve, reject) => {
  //     let failTimeout: NodeJS.Timeout;
  //     let successTimeout: NodeJS.Timeout;

  //     const resetTimeouts = () => {
  //       const timeLimit = 6000;

  //       clearTimeout(failTimeout);
  //       clearTimeout(successTimeout);

  //       failTimeout = setTimeout(() => {
  //         reject(new Error("Timeout"));
  //       }, timeLimit + 2000);

  //       successTimeout = setTimeout(() => {
  //         resolve(true);
  //       }, timeLimit);
  //     };

  //     resetTimeouts();

  //     rq.listen({
  //       queueName: "test",
  //       workers: 5,
  //       errorHandler(error, message) {
  //         log(error, message);
  //         return Promise.resolve();
  //       },
  //       queueEmptyHandler: async () => {
  //         log("Queue empty");
  //         return Promise.resolve();
  //       },
  //       transform: async (message) => JSON.parse(message),
  //       job: async (params) => {
  //         resetTimeouts();

  //         if (params.message.mutexTest === "a") {
  //           await setTimeoutAsync(5000);
  //           log("Sou a");
  //           log({ message: params.message }, new Date().toISOString());
  //           return;
  //         }

  //         if (params.message.mutexTest === "c") {
  //           throw new Error("Error, I'm c");
  //         }

  //         await setTimeoutAsync(1000);
  //         log({ message: params.message }, new Date().toISOString());
  //       },
  //       mutexPath: "mutexTest",
  //     });

  //     await setTimeoutAsync(20000);

  //     log("Adding more messages");
  //     await addMutex("b");
  //   });

  //   log("Done");

  //   expect(true).toBe(true);
  // });

  it("should listen more than one queue", async () => {
    const addMutex = async (mutexKey: string, length = 10) => {
      const promises = Array.from({ length }).map(async (_, index) => {
        await rq.pushMessage({
          queueName: "test",
          message: JSON.stringify({
            id: index,
            mutexTest: mutexKey,
          }),
        });

        await rq.pushMessage({
          queueName: "test2",
          message: JSON.stringify({
            id: index,
            mutexTest: mutexKey,
          }),
        });
      });

      await Promise.all(promises);
    };

    await addMutex("a");
    await addMutex("b");
    await addMutex("c", 2);

    await new Promise(async (resolve, reject) => {
      let failTimeout: NodeJS.Timeout;
      let successTimeout: NodeJS.Timeout;

      const resetTimeouts = () => {
        const timeLimit = 6000;

        clearTimeout(failTimeout);
        clearTimeout(successTimeout);

        failTimeout = setTimeout(() => {
          reject(new Error("Timeout"));
        }, timeLimit + 2000);

        successTimeout = setTimeout(() => {
          resolve(true);
        }, timeLimit);
      };

      resetTimeouts();

      rq.listen({
        queueName: "test",
        workers: 1,
        errorHandler(error, message) {
          log(error, message);
          return Promise.resolve();
        },
        queueEmptyHandler: async () => {
          log("Queue empty");
          return Promise.resolve();
        },
        transform: async (message) => JSON.parse(message),
        job: async (params) => {
          resetTimeouts();
          await setTimeoutAsync(1000);
          log({ message: params.message, queue: 1 }, new Date().toISOString());

          throw new Error("Error, I'm c");
        },
      });

      rq.listen({
        queueName: "test2",
        workers: 1,
        errorHandler(error, message) {
          log(error, message);
          return Promise.resolve();
        },
        queueEmptyHandler: async () => {
          log("Queue empty");
          return Promise.resolve();
        },
        transform: async (message) => JSON.parse(message),
        job: async (params) => {
          resetTimeouts();
          await setTimeoutAsync(1000);
          log({ message: params.message, queue: 2 }, new Date().toISOString());
        },
      });
    });

    log("Done");

    expect(true).toBe(true);
  });
});
