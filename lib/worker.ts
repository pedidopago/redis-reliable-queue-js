type ReliableQueueWorkerParamsDTO = {
  id: string;
  mutexKey?: string;
};

class ReliableQueueWorker {
  #id: string;
  #jobs: Function[] = [];
  #isRunning = false;
  #mutexKey?: string;

  constructor(params: ReliableQueueWorkerParamsDTO) {
    this.#id = params.id;
    this.#mutexKey = params.mutexKey;
  }

  getJob(): Function | undefined {
    const job = this.#jobs.shift();
    if (job) return job;
    return undefined;
  }

  run() {
    new Promise(async (resolve) => {
      this.#isRunning = true;
      while (true) {
        const job = this.getJob();
        if (!job) break;
        await job();
      }

      this.#isRunning = false;
      this.#mutexKey = undefined;
      resolve(void 0);
    });
  }

  get isRunning() {
    return this.#isRunning;
  }

  get mutexKey() {
    return this.#mutexKey;
  }

  set mutexKey(value: string | undefined) {
    this.#mutexKey = value;
  }

  addJob(job: Function) {
    this.#jobs.push(job);
  }
}

type ReliableQueueClusterParamsDTO = {
  clusterId: string;
  maxWorkers: number;
};

type FindAvailableWorkerParamsDTO = {
  mutexKey?: string;
};

type AddJobParamsDTO = {
  job: Function;
  mutexKey?: string;
};

export class ReliableQueueCluster {
  #clusterId: string;
  #workers: ReliableQueueWorker[] = [];
  #findAvailableWorkerDebounce = 100;

  constructor(params: ReliableQueueClusterParamsDTO) {
    this.#clusterId = params.clusterId;

    for (let i = 0; i < params.maxWorkers; i++) {
      this.#workers.push(new ReliableQueueWorker({ id: i.toString() }));
    }
  }

  private async findAvailableWorker(
    params: FindAvailableWorkerParamsDTO
  ): Promise<ReliableQueueWorker> {
    while (true) {
      const workerWithMutex = this.#workers.find(
        (worker) => worker.mutexKey === params.mutexKey
      );

      if (workerWithMutex) {
        return workerWithMutex;
      }

      const availableWorker = this.#workers.find((worker) => !worker.isRunning);

      if (availableWorker) {
        return availableWorker;
      }

      await new Promise((resolve) =>
        setTimeout(resolve, this.#findAvailableWorkerDebounce)
      );
    }
  }

  async addJob(params: AddJobParamsDTO) {
    const worker = await this.findAvailableWorker({
      mutexKey: params.mutexKey,
    });

    worker.mutexKey = params.mutexKey;
    worker.addJob(params.job);
    if (!worker.isRunning) {
      worker.run();
    }
  }

  get clusterId() {
    return this.#clusterId;
  }
}
