import { PoolConnection } from 'mysql2/promise'
import { Database } from './database'
import { Logger } from './logger'
import { Job, Queue, WorkerCallback } from './types'

export function JobProcessor(database: Database, logger: Logger, queue: Queue, callback: WorkerCallback) {
  async function process(job: Job, workerAbortSignal: AbortSignal) {
    await database.withConnection(async (connection) => {
      await connection.beginTransaction()
      try {
        try {
          await executeCallbackWithTimeout(connection)
        } catch (error: unknown) {
          await handleCallbackError(error, connection)
        }
        await connection.commit()
      } catch (error: unknown) {
        await connection.rollback()
        const typedError = error as Error
        logger.error({ error: errorToJson(typedError) }, `jobProcessor.process.error`)
      }
    })

    async function executeCallbackWithTimeout(connection: PoolConnection) {
      const callbackAbortController = new AbortController()
      let timeoutId: NodeJS.Timeout
      workerAbortSignal.addEventListener('abort', () => {
        callbackAbortController.abort()
        clearTimeout(timeoutId)
        logger.debug({ jobId: job.id }, `jobProcessor.process.abortedDueWorkerAbort`)
      })

      const callbackPromise = callback(job, callbackAbortController.signal, connection)
      const timeoutPromise = new Promise((_, reject) => {
        timeoutId = setTimeout(() => {
          callbackAbortController.abort()
          reject(new Error(`Job execution exceed the timeout of ${queue.maxDurationMs}`))
          logger.debug({ jobId: job.id }, `jobProcessor.process.abortedDueTimeout`)
        }, queue.maxDurationMs)
      })

      await Promise.race([callbackPromise, timeoutPromise]).finally(() => clearTimeout(timeoutId))
      await database.markJobAsCompleted(connection, job.id, job.attempts)

      logger.info({ jobId: job.id }, `jobProcessor.process.completed`)
    }

    async function handleCallbackError(error: unknown, connection: PoolConnection) {
      if (workerAbortSignal.aborted) return
      const typedError = error as Error
      if (job.attempts < queue.maxRetries - 1) {
        const now = Date.now()
        const startAfter = queue.backoffMultiplier
          ? new Date(now + queue.minDelayMs * Math.pow(queue.backoffMultiplier, job.attempts))
          : new Date(now + queue.minDelayMs)
        await database.incrementJobAttempts(connection, job.id, typedError.message, job.attempts, startAfter)
        logger.warn(
          {
            jobId: job.id,
            error: errorToJson(typedError),
            startAfter,
            retryInSeconds: Math.floor((startAfter.getTime() - now) / 1000),
          },
          `jobProcessor.process.failed`,
        )
      } else {
        await database.markJobAsFailed(connection, job.id, typedError.message, job.attempts)
        logger.error({ jobId: job.id, error: errorToJson(typedError) }, `jobProcessor.process.failedAfterAttempts`)
      }
    }
  }

  async function processBatch(batchSize = 1, workerAbortSignal: AbortSignal) {
    if (workerAbortSignal?.aborted) {
      logger.warn('jobProcessor.processBatch.abortedBeforeFetching')
      return
    }

    try {
      const jobs = (await database.getPendingJobs(queue.id, batchSize)) as Job[]

      if (jobs.length === 0) {
        return
      }
      logger.debug({ jobsCount: jobs.length }, `jobProcessor.processBatch.jobsFound`)

      await Promise.all(
        jobs.map(async (job) => {
          await process(job, workerAbortSignal)
        }),
      )
    } catch (error: unknown) {
      const typedError = error as Error
      logger.error({ error: errorToJson(typedError) }, `jobProcessor.processBatch.error`)
    }
  }

  return {
    processBatch,
    process,
  }
}

function errorToJson(error: Error) {
  return {
    name: error.name,
    message: error.message,
    stack: error.stack,
  }
}
