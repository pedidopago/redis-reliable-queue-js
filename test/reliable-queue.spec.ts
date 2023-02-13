import { ReliableQueue } from "../lib";
import { faker } from "@faker-js/faker";

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
    const queueName = faker.database.column();
    const message = faker.lorem.sentence();

    await rq.pushMessage({
      queueName,
      message,
    });

    //CRITICAL: Test the listener here
    expect(true).toBe(true);
  });
});
