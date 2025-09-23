const debug = process.env.RELIABLE_QUEUE_DEBUG;
export const logger = (...args: any[]) => {
  if (!debug) return;
  const newArgs = args.map((arg) => {
    if (typeof arg === "object") {
      return JSON.stringify(arg);
    }
    if (arg instanceof Error) {
      return arg.message;
    }
    return arg;
  });
  console.log("[RELIABLE_QUEUE_JS] - ", ...newArgs);
};
