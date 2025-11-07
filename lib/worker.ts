import { setTimeout } from "timers/promises";
import { logger } from "./logger";

type ReliableQueueWorkerParamsDTO = {
  id: string;
  mutexKey?: string;
};

export type ReliableQueueWorkerData = {
  id: string;
  isRunning: boolean;
  jobsLength: number;
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

  private getJob(): Function | undefined {
    const job = this.#jobs.shift();
    if (job) return job;
    return undefined;
  }

  run() {
    new Promise(async (resolve, reject) => {
      try {
        this.#isRunning = true;
        while (true) {
          const job = this.getJob();
          if (!job) break;
          logger("Running job", {
            workerId: this.#id,
            mutexKey: this.#mutexKey,
          });
          await job();
          await setTimeout(0);
        }

        this.#isRunning = false;
        this.#mutexKey = undefined;
        resolve(void 0);
      } catch (e) {
        this.#isRunning = false;
        this.#mutexKey = undefined;
        reject(e);
      }
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

  toJSON(): ReliableQueueWorkerData {
    return {
      id: this.#id,
      isRunning: this.#isRunning,
      jobsLength: this.#jobs.length,
    };
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

type ReliableQueueClusterData = {
  clusterId: string;
  workers: ReliableQueueWorkerData[];
};
export class ReliableQueueCluster {
  #clusterId: string;
  #workers: ReliableQueueWorker[] = [];
  #findAvailableWorkerDebounce = 1;

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

      await setTimeout(this.#findAvailableWorkerDebounce);
    }
  }

  async addJob(params: AddJobParamsDTO) {
    logger("Adding job to cluster", {
      clusterId: this.#clusterId,
      mutexKey: params.mutexKey,
    });

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

  toJSON(): ReliableQueueClusterData {
    return {
      clusterId: this.#clusterId,
      workers: this.#workers.map((worker) => worker.toJSON()),
    };
  }
}
