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

export type ListenParamsDTO = {
  queueName: string;
  callback: (message: string) => Promise<boolean>;
};
