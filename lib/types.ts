export type CreateReliableQueueDTO = {
  url: string;
  password?: string;
  lifo?: boolean;
  ackSuffix?: string;
  listExpirationSeconds?: number;
  messageTimeoutSeconds?: number;
  emptyQueueTimeoutSeconds?: number;
};

export type PushMessageParamsDTO = {
  queueName: string;
  message: string;
};

export type PopMessageResponseDTO = [string, Function];

type ListenJobParamsDTO<MessageType> = {
  message: MessageType;
};

export type ListenParamsDTO<MessageType> = {
  mutexPath?: string;
  queueName: string;
  workers: number;
  job: (params: ListenJobParamsDTO<MessageType>) => Promise<void>;
  transform: (message: string) => Promise<MessageType>;
  errorHandler: (error: Error, message: string) => Promise<void>;
  validate?: (message: MessageType) => Promise<boolean>;
  queueEmptyHandler?: () => Promise<void>;
};

export type MetricsQueueDTO = {
  name: string;
  size: number;
  workers: number;
  waitingAck: number;
};

export type MetricsDTO = {
  queues: MetricsQueueDTO[];
};
