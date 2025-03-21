import { randomUUID } from 'node:crypto'
import { Database } from './database'
import { JobProcessor } from './jobProcessor'
import { Logger } from './logger'
import { Job, Queue, WorkerCallback } from './types'

export function WorkersFactory(logger: Logger, database: Database) {
  const workers: Worker[] = []

  function create(callback: WorkerCallback, pollingIntervalMs = 500, batchSize = 1, queue: Queue) {
    const worker = Worker(callback, pollingIntervalMs, batchSize, logger, database, queue)
    workers.push(worker)
    return worker
  }

  function stopAll() {
    return Promise.all(workers.map((worker) => worker.stop()))
  }

  return { create, stopAll }
}

export type Worker = ReturnType<typeof Worker>

export function Worker(
  callback: WorkerCallback,
  pollingIntervalMs = 500,
  batchSize = 1,
  logger: Logger,
  database: Database,
  queue: Queue,
) {
  const workerId = randomUUID()
  const wLogger = logger.child({ workerId: workerId })

  const jobProcessor = JobProcessor(database, wLogger, queue, callback)

  const controller = new AbortController()
  const { signal } = controller

  let stopPromiseResolve: (() => void) | null = null
  const stopPromise = new Promise<void>((resolve) => {
    stopPromiseResolve = resolve
  })

  async function start() {
    wLogger.info({ pollingIntervalMs, batchSize }, `worker.starting`)

    while (!signal.aborted) {
      await jobProcessor.processBatch(batchSize, signal)
      await sleep(pollingIntervalMs)
    }
    stopPromiseResolve?.()
    wLogger.debug(`worker.aborted`)
  }

  async function stop() {
    wLogger.debug(`worker.stopping`)
    controller.abort()
    await stopPromise
    wLogger.info(`worker.stopped`)
  }

  async function process(jobId: string) {
    const job = (await database.getJobById(jobId)) as Job

    await jobProcessor.process(job, signal)
  }

  return { start, stop, process }
}

async function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}
