import { ReliableQueue } from "./lib";

const main = async () => {
  const rq = new ReliableQueue({
    url: "redis://localhost:6379",
  });

  await Promise.all(
    Array.from({ length: 10 }).map(async (_, i) => {
      await rq.pushMessage({
        message: JSON.stringify({ test: `test ${i}` }),
        queueName: "test",
      });
    })
  );

  rq.listen({
    queueName: "test",
    callback: async (message) => {
      console.log(message);

      if (Math.random() > 0.5) {
        console.log("Returning false");
        return false;
      }

      console.log("Returning true");
      return true;
    },
  });

  console.log("Done");
};

main();
