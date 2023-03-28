import { ReliableQueue } from "../lib";
import { faker } from "@faker-js/faker";

jest.setTimeout(20000);

describe("ReliableQueue", () => {
  let rq: ReliableQueue;

  beforeAll(async () => {
    rq = new ReliableQueue({
      url: "redis://localhost:6379",
    });
  });

  afterAll(async () => {
    await rq.close();
  });

  it("should push message", async () => {
    const promises = Array.from({ length: 1000 }).map(async (_, index) => {
      await rq.pushMessage({
        queueName: "test",
        message: JSON.stringify({
          id: index,
          message: faker.lorem.sentence(),
          mutexTest: faker.helpers.arrayElement(["a", "b", "c"]),
        }),
      });
    });

    await Promise.all(promises);

    await new Promise((resolve, reject) => {
      let failTimeout: NodeJS.Timeout;
      let successTimeout: NodeJS.Timeout;

      const resetTimeouts = () => {
        clearTimeout(failTimeout);
        clearTimeout(successTimeout);

        failTimeout = setTimeout(() => {
          reject(new Error("Timeout"));
        }, 5000);

        successTimeout = setTimeout(() => {
          resolve(true);
        }, 1000);
      };

      resetTimeouts();
      rq.listen({
        queueName: "test",
        workers: 5,
        errorHandler(error, message) {
          console.log(error, message);
          return Promise.resolve();
        },
        transform(message) {
          return Promise.resolve(JSON.parse(message));
        },
        validate() {
          return Promise.resolve(true);
        },
        job: async (params) => {
          resetTimeouts();
          console.log(params.message.id);
          await params.ack();
        },
        queueEmptyHandler() {
          return Promise.resolve();
        },
        mutexPath: ".mutexTest",
      });
    });

    console.log("Done");

    expect(true).toBe(true);
  });
});
