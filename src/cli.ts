import { Command } from 'commander'
import pino from 'pino'
import { MysqlQueue } from './'
import { cancellablePromiseFactory } from './cancellablePromise'
import { Job, WorkerCallback } from './types'
import { sleep } from './utils'

const SERENIS_DB_URI = 'mysql://root:password@localhost:3306/serenis'

const logger = pino({
  transport: {
    target: 'pino-pretty',
    options: {
      colorize: true,
      messageFormat: '[cli] - {msg}',
    },
  },
})

const program = new Command()
program.name('mysql-queue').description('MysqlQueue simple CLI').version('0.8.0')

const jobs = program.command('jobs').description('Manage jobs')
jobs
  .command('add')
  .description('Add a job to a queue')
  .requiredOption('-q, --queue <queue>', 'Queue name')
  .requiredOption('-n, --name <name>', 'Job name')
  .requiredOption('-p, --payload <json>', 'Job payload as JSON')
  .option('-s, --start-after <seconds>', 'Start after seconds')
  .action(async (options) => {
    let payload
    try {
      payload = JSON.parse(options.payload)
    } catch (error: unknown) {
      const typedError = error as Error
      console.error('Invalid JSON payload:', typedError.message)
      process.exit(1)
    }

    const instance = MysqlQueue({ dbUri: SERENIS_DB_URI, loggingLevel: 'silent' })
    await instance.initialize()
    await instance.upsertQueue(options.queue, {})
    await instance.enqueue(options.queue, {
      name: options.name,
      payload,
      startAfter: options.startAfter ? new Date(Date.now() + Number(options.startAfter) * 1000) : undefined,
    })
    await instance.dispose()
    logger.info('🟩 job added to the queue')
  })

const queues = program.command('queues').description('Manage queues')
queues
  .command('add')
  .description('Add a new queue')
  .requiredOption('-q, --queue <queue>', 'Queue name')
  .action(async (options) => {
    const instance = MysqlQueue({ dbUri: SERENIS_DB_URI, loggingLevel: 'silent' })
    await instance.initialize()
    await instance.upsertQueue(options.queue, {})
    await instance.dispose()
    logger.info('🟩 queue created')
  })
queues
  .command('consume')
  .description('Consume jobs from a queue')
  .option('-s, --batch-size <size>', 'Number of jobs to process in parallel', '1')
  .option('-p, --polling-interval <interval>', 'Polling interval in ms', '500')
  .option('-b, --behavior <timeout | success | error | slowSuccess>', 'Behavior on job processing')
  .requiredOption('-q, --queue <queue>', 'Queue name')
  .action(async (options) => {
    const instance = MysqlQueue({ dbUri: SERENIS_DB_URI, loggingPrettyPrint: true })
    await instance.initialize()
    await instance.upsertQueue(options.queue, { maxRetries: 10 })

    let cb: WorkerCallback = callbacks['success']

    if (options.behavior) {
      cb = callbacks[options.behavior as keyof typeof callbacks]
      if (!cb) {
        throw new Error('Invalid fail-for option')
      }
    }

    const worker = await instance.work(
      options.queue,
      cb,
      options.pollingInterval ? Number(options.pollingInterval) : undefined,
      options.batchSize ? Number(options.batchSize) : undefined,
    )
    void worker.start()
    logger.info('🟩 worker started')

    function gracefulShutdown() {
      logger.warn('Received termination signal. Shutting down gracefully...')

      worker.stop().then(() => {
        logger.info('Worker stopped 🟩')
        process.exit(0)
      })
    }

    process.on('SIGINT', gracefulShutdown)
    process.on('SIGTERM', gracefulShutdown)
  })

program.parse(process.argv)

const callbacks: Record<'timeout' | 'success' | 'error' | 'slowSuccess', WorkerCallback> = {
  timeout: async (job: Job, signal: AbortSignal) => {
    const { promise } = cancellablePromiseFactory(async () => {
      signal.addEventListener('abort', () => {
        logger.info(`job ${job.id} aborted 🟥`)
      })

      await sleep(10000)
      logger.info(`Job ${job.id} processed 🟩`)
    }, signal)

    logger.info({ name: job.name, payload: job.payload }, `processing job ${job.id} 🟨`)
    await promise
  },
  success: (job: Job) => {
    logger.info({ ...job }, `Job ${job.id} processed 🟩`)
  },
  error: (job: Job) => {
    logger.error({ ...job }, `Job ${job.id} failed 🟥`)
    throw new Error('Generic error')
  },
  slowSuccess: async (job: Job, signal) => {
    if (signal.aborted) throw new Error('Job aborted')

    return new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => {
        logger.info(`Job ${job.id} processed 🟩`)
        resolve()
      }, 5000)

      signal.addEventListener(
        'abort',
        () => {
          clearTimeout(timeout)
          logger.info(`job ${job.id} aborted 🟥`)
          reject(new Error('Job aborted'))
        },
        { once: true },
      )

      const { name, payload } = job
      logger.info({ name, payload }, `processing job ${job.id} 🟨`)
    })
  },
}
