import { logger } from "./logger";
import { ListenParamsDTO, PopMessageResponseDTO } from "./types";
import { ReliableQueueCluster } from "./worker";
import { setTimeout } from "timers/promises";

type ReliableQueueListenerParamsDTO<MessageType> = {
  config: ListenParamsDTO<MessageType>;
  cluster: ReliableQueueCluster;
  messagePopper: AsyncGenerator<PopMessageResponseDTO>;
  onInit: () => Promise<void>;
};

export class ReliableQueueListener<MessageType> {
  constructor(
    private readonly params: ReliableQueueListenerParamsDTO<MessageType>
  ) {}

  get cluster() {
    return this.params.cluster;
  }

  async listen(): Promise<void> {
    await this.params.onInit();

    for await (const { ack, isEmpty, message } of this.params.messagePopper) {
      logger("Message received from queue", {
        queueName: this.params.config.queueName,
        message,
      });

      try {
        if (isEmpty) {
          logger("Queue empty", {
            queueName: this.params.config.queueName,
          });
          if (this.params.config.queueEmptyHandler)
            await this.params.config.queueEmptyHandler();
          await setTimeout(this.params.config.emptyQueueTimeoutMilliseconds);
          continue;
        }

        const transformedMessage = this.params.config.transform
          ? await this.params.config.transform(message)
          : JSON.parse(message);

        const validated = this.params.config.validate
          ? await this.params.config.validate(transformedMessage)
          : true;

        if (!validated) {
          await this.params.config.errorHandler(
            new Error("Message validation failed"),
            message
          );
          await ack();
          continue;
        }

        let mutexKey: string | undefined;

        if (this.params.config.getMutex) {
          mutexKey = await this.params.config.getMutex(transformedMessage);
        }

        const job = async () => {
          try {
            logger("Job started", {
              queueName: this.params.config.queueName,
              message: transformedMessage,
            });
            await this.params.config.job({
              message: transformedMessage,
            });
            await ack();
          } catch (e) {
            logger("Job failed", {
              queueName: this.params.config.queueName,
              message: transformedMessage,
              error: e,
            });
            const error = e as Error;
            await this.params.config.errorHandler(error, message);
            await ack();
          }
        };

        logger("Adding job to cluster", {
          queueName: this.params.config.queueName,
          message: transformedMessage,
        });

        await this.cluster.addJob({
          job,
          mutexKey,
        });
      } catch (e) {
        const error = e as Error;
        await this.params.config.errorHandler(error, message);
        await ack();
      }
    }
  }
}
