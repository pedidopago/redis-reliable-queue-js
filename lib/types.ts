import { ReliableQueueWorkerData } from "./worker";

export type CreateReliableQueueDTO = {
  url: string;
  password?: string;
  leftPush?: boolean;
  listExpirationSeconds?: number;
  messageTimeoutSeconds?: number;
  emptyQueueTimeoutSeconds?: number;
  queueListenDebounceMilliseconds?: number;
};

export type PushMessageParamsDTO = {
  queueName: string;
  message: string;
};

export type PopMessageResponseDTO = {
  message: string;
  ack: () => Promise<void>;
  isEmpty: boolean;
};

type ListenJobParamsDTO<MessageType> = {
  message: MessageType;
};

export type ListenParamsDTO<MessageType> = {
  mutexPath?: string;
  queueName: string;
  workers: number;
  emptyQueueTimeoutMilliseconds: number;
  job: (params: ListenJobParamsDTO<MessageType>) => Promise<void>;
  errorHandler: (error: Error, message: string) => Promise<void>;
  transform?: (message: string) => Promise<MessageType>;
  validate?: (message: MessageType) => Promise<boolean>;
  queueEmptyHandler?: () => Promise<void>;
};

export type MetricsQueueDTO = {
  name: string;
  size: number;
  workers: ReliableQueueWorkerData[];
  waitingAck: number;
};

export type MetricsDTO = {
  redis: {
    connected: boolean;
  };
  queues: MetricsQueueDTO[];
};
