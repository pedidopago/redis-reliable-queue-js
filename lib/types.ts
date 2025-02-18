import { ReliableQueueWorkerData } from "./worker";

export type CreateReliableQueueDTO = {
  url: string;
  leftPush: boolean;
  password?: string;
  listExpirationInMinutes: number;
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
  queueName: string;
  workers: number;
  emptyQueueTimeoutMilliseconds: number;
  queueListenDebounceMilliseconds?: number;
  messageTimeoutMilliseconds: number;
  job: (params: ListenJobParamsDTO<MessageType>) => Promise<void>;
  errorHandler: (error: Error, message: string) => Promise<void>;
  transform?: (message: string) => Promise<MessageType>;
  validate?: (message: MessageType) => Promise<boolean>;
  queueEmptyHandler?: () => Promise<void>;
  getMutex?: (message: MessageType) => string | Promise<string>;
};

export type MetricsQueueDTO = {
  name: string;
  size: number;
  workers: ReliableQueueWorkerData[];
  workersRunning: number;
  workersLength: number;
  waitingAck: number;
};

export type MetricsDTO = {
  redis: {
    connected: boolean;
  };
  queues: MetricsQueueDTO[];
};
