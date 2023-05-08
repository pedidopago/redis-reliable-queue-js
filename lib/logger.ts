export const logger = (...args: any[]) => {
  const newArgs = args.map((arg) => {
    if (typeof arg === "object") {
      return JSON.stringify(arg);
    }
    if (arg instanceof Error) {
      return arg.message;
    }
    return arg;
  });

  if (process.env.RELIABLE_QUEUE_DEBUG) {
    console.log("[RELIABLE_QUEUE_JS] - ", ...newArgs);
  }
};
