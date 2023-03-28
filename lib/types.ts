export type CreateReliableQueueDTO = {
  url: string;
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
  ack: Function;
};

export type ListenParamsDTO<MessageType> = {
  mutexPath?: string;
  queueName: string;
  workers: number;
  job: (params: ListenJobParamsDTO<MessageType>) => Promise<void>;
  validate: (message: string) => Promise<boolean>;
  transform: (message: string) => Promise<MessageType>;
  errorHandler: (error: Error, message: string) => Promise<void>;
  queueEmptyHandler: () => Promise<void>;
};
