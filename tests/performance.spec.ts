import { afterEach, describe, expect, it, jest } from '@jest/globals'
import { MysqlQueue } from '../src'
import { waitInvoked } from './utils/waitInvoked'

describe('Performance', () => {
  const WORKER_COUNT = 10
  const JOB_COUNT = 1000

  const WorkerMocks = {
    handle: jest.fn<(j: unknown) => void>(),
  }
  const mysqlQueue = MysqlQueue({ dbUri: 'mysql://root:password@localhost:3306/serenis', loggingLevel: 'fatal' })

  afterEach(async () => {
    await mysqlQueue.destroy()
    await mysqlQueue.dispose()
  })

  it('should handle 1000 jobs in less than 2 seconds (with 10 workers)', async () => {
    await mysqlQueue.initialize()
    const queue = 'performance'
    await mysqlQueue.upsertQueue(queue)

    const workers = await Promise.all(
      Array.from({ length: WORKER_COUNT }).map((_) => {
        return mysqlQueue.work(queue, (j) => WorkerMocks.handle(j), undefined, 50)
      }),
    )

    void Promise.all(workers.map((w) => w.start()))

    const jobs = Array.from({ length: JOB_COUNT }).map((_, i) => ({ name: 'perf-job', payload: { i } }))

    const start = performance.now()
    await mysqlQueue.enqueue(queue, jobs)

    await waitInvoked(WorkerMocks, 'handle', JOB_COUNT)
    const end = performance.now()

    expect(end - start).toBeLessThan(2000)
    await Promise.all(workers.map((w) => w.stop()))
  })
})
