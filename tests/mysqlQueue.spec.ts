import { afterAll, afterEach, describe, expect, it, jest } from '@jest/globals'
import { ResultSetHeader, RowDataPacket } from 'mysql2/promise'
import { MysqlQueue } from '../src'
import { TABLE_JOBS, TABLE_MIGRATIONS, TABLE_QUEUES, TABLES_NAME_PREFIX } from '../src/database'
import { QueryDatabase } from './utils/queryDatabase'
import { waitInvoked } from './utils/waitInvoked'
import { randomUUID } from 'node:crypto'

describe('mysqlQueue', () => {
  const mysqlQueue = MysqlQueue({ dbUri: 'mysql://root:password@localhost:3306/serenis', loggingLevel: 'fatal' })
  const queryDatabase = QueryDatabase({ uri: 'mysql://root:password@localhost:3306/serenis' })

  afterAll(async () => {
    await mysqlQueue.destroy()
    await mysqlQueue.dispose()
    await queryDatabase.dispose()
  })

  afterEach(() => {
    jest.resetAllMocks()
  })

  const WorkerCbMock = {
    handle: jest.fn<(job: unknown) => Promise<void>>(),
  }

  it('initialize', async () => {
    await mysqlQueue.initialize()

    const migrationsTable = await queryDatabase.query<RowDataPacket[]>(`SELECT * FROM ${TABLE_MIGRATIONS}`)
    expect(migrationsTable).toHaveLength(2)
    expect(migrationsTable[0]).toMatchObject({
      id: 1,
      name: 'create-queues-table',
      applied_at: expect.any(Date),
    })
    expect(migrationsTable[1]).toMatchObject({
      id: 2,
      name: 'create-jobs-table',
      applied_at: expect.any(Date),
    })
    const tables = await queryDatabase.query<RowDataPacket[]>(`SELECT table_name 
      FROM information_schema.tables 
      WHERE table_name LIKE '${TABLES_NAME_PREFIX}_%';
    `)
    expect(tables).toHaveLength(3)
  })

  it('destroy', async () => {
    await mysqlQueue.initialize()

    await mysqlQueue.destroy()
    const tablesAfterDestroy = await queryDatabase.query<RowDataPacket[]>(`SELECT table_name
          FROM information_schema.tables
          WHERE table_schema = DATABASE()
          AND table_name LIKE '${TABLES_NAME_PREFIX}_%';
    `)
    expect(tablesAfterDestroy).toHaveLength(0)
  })

  it('upsertQueue', async () => {
    await mysqlQueue.initialize()
    const queueName = randomUUID()
    const queue = await mysqlQueue.upsertQueue(queueName)
    expect(queue.id).toMatch(/^[0-9A-F]{8}-[0-9A-F]{4}-[4][0-9A-F]{3}-[89AB][0-9A-F]{3}-[0-9A-F]{12}$/i)
    expect(queue).toMatchObject({ name: queueName })
    expect(await queryDatabase.query<RowDataPacket[]>(`SELECT * FROM ${TABLE_QUEUES}`)).toHaveLength(1)
    expect(await mysqlQueue.retrieveQueue({ name: queueName })).toMatchObject({
      id: queue.id,
      name: queueName,
      backoffMultiplier: 2,
      maxRetries: 3,
      minDelayMs: 1000,
      maxDurationMs: 5000,
    })

    const sameQueue = await mysqlQueue.upsertQueue(queueName, {
      backoffMultiplier: 3,
      maxRetries: 4,
      minDelayMs: 1001,
      maxDurationMs: 5001,
    })
    expect(sameQueue).toEqual({
      ...queue,
      maxRetries: 4,
      backoffMultiplier: 3,
      minDelayMs: 1001,
      maxDurationMs: 5001,
    })
    const queues = await queryDatabase.query<RowDataPacket[]>(`SELECT * FROM ${TABLE_QUEUES}`)
    expect(queues).toHaveLength(1)
    expect(queues[0]).toMatchObject({
      ...queue,
      maxRetries: 4,
      backoffMultiplier: 3,
      minDelayMs: 1001,
      maxDurationMs: 5001,
    })

    await mysqlQueue.destroy()
  })

  it('add', async () => {
    await mysqlQueue.initialize()
    const queueName = randomUUID()
    await mysqlQueue.upsertQueue(queueName)

    const { jobIds } = await mysqlQueue.enqueue(queueName, { name: 'test-job', payload: { foo: 'bar' } })
    expect(jobIds[0]).toMatch(/^[0-9A-F]{8}-[0-9A-F]{4}-[4][0-9A-F]{3}-[89AB][0-9A-F]{3}-[0-9A-F]{12}$/i)
    expect(await queryDatabase.query<RowDataPacket[]>(`SELECT * FROM ${TABLE_JOBS}`)).toHaveLength(1)

    await mysqlQueue.destroy()
  })

  it('addMany', async () => {
    await mysqlQueue.initialize()
    const queueName = randomUUID()
    await mysqlQueue.upsertQueue(queueName)

    const { jobIds } = await mysqlQueue.enqueue(queueName, [
      { name: 'test-job', payload: { foo: 'bar' } },
      {
        name: 'test-job',
        payload: { foo2: 'bar2' },
      },
    ])
    expect(jobIds).toHaveLength(2)
    expect(await queryDatabase.query<RowDataPacket[]>(`SELECT * FROM ${TABLE_JOBS}`)).toHaveLength(2)

    await mysqlQueue.destroy()
  })

  it('addMany with external connection', async () => {
    await mysqlQueue.initialize()
    const queueName = randomUUID()

    await mysqlQueue.upsertQueue(queueName)

    const connection = await queryDatabase.pool.getConnection()
    const { jobIds } = await mysqlQueue.enqueue(
      queueName,
      [
        { name: 'test-job', payload: { foo: 'bar' } },
        {
          name: 'test-job',
          payload: { foo2: 'bar2' },
        },
      ],
      connection,
    )
    connection.release()
    expect(jobIds).toHaveLength(2)
    expect(await queryDatabase.query<RowDataPacket[]>(`SELECT * FROM ${TABLE_JOBS}`)).toHaveLength(2)

    await mysqlQueue.destroy()
  })

  it('work, successful execution case', async () => {
    await mysqlQueue.initialize()
    const queueName = randomUUID()
    const queue = await mysqlQueue.upsertQueue(queueName, { minDelayMs: 0 })

    const worker = await mysqlQueue.work(queueName, (j) => WorkerCbMock.handle(j), 50)

    void worker.start()
    const { jobIds } = await mysqlQueue.enqueue(queueName, { name: 'test-job', payload: { foo: 'bar' } })
    await waitInvoked(WorkerCbMock, 'handle', 1)

    expect(WorkerCbMock.handle).toHaveBeenCalledWith(
      expect.objectContaining({
        id: jobIds[0],
      }),
    )

    await sleep(100) // wait for the last attempt to be processed
    const jobs = await queryDatabase.query<RowDataPacket[]>(`SELECT * FROM ${TABLE_JOBS}`)
    expect(jobs).toHaveLength(1)
    expect(jobs[0]).toMatchObject({
      attempts: 1,
      completedAt: expect.any(Date),
      createdAt: expect.any(Date),
      failedAt: null,
      id: jobIds[0],
      latestFailureReason: null,
      name: 'test-job',
      payload: {
        foo: 'bar',
      },
      queueId: queue.id,
      status: 'completed',
    })

    await mysqlQueue.destroy()
  })

  it('work, retried execution case', async () => {
    WorkerCbMock.handle
      .mockRejectedValueOnce(new Error('Attempt 1 failed'))
      .mockRejectedValueOnce(new Error('Attempt 2 failed'))
      .mockResolvedValueOnce()

    await mysqlQueue.initialize()
    const queueName = randomUUID()
    const queue = await mysqlQueue.upsertQueue(queueName, {
      minDelayMs: 0,
      maxRetries: 3,
      backoffMultiplier: null,
    })

    const worker = await mysqlQueue.work(queueName, (j) => WorkerCbMock.handle(j), 50)

    void worker.start()
    const { jobIds } = await mysqlQueue.enqueue(queueName, { name: 'test-job', payload: { foo: 'bar' } })

    await waitInvoked(WorkerCbMock, 'handle', 3)
    await sleep(100) // wait for the last attempt to be processed
    expect(WorkerCbMock.handle).toHaveBeenCalledWith(
      expect.objectContaining({
        id: jobIds[0],
      }),
    )

    const jobs = await queryDatabase.query<RowDataPacket[]>(`SELECT * FROM ${TABLE_JOBS}`)
    expect(jobs).toHaveLength(1)
    expect(jobs[0]).toMatchObject({
      attempts: 3,
      completedAt: expect.any(Date),
      createdAt: expect.any(Date),
      failedAt: null,
      id: jobIds[0],
      latestFailureReason: 'Attempt 2 failed',
      name: 'test-job',
      payload: {
        foo: 'bar',
      },
      queueId: queue.id,
      status: 'completed',
    })
  })

  it('addRawSql', async () => {
    await mysqlQueue.initialize()
    const queueName = randomUUID()
    await mysqlQueue.upsertQueue(queueName)

    const sql = await mysqlQueue.getEnqueueRawSql(queueName, { name: 'test-job', payload: { foo: 'bar' } })

    const sqlWithoutUuids = sql.replace(/([a-f0-9-]{36})/g, '<<uuidv4>>')
    expect(sqlWithoutUuids).toBe(
      `INSERT INTO mysql_queue_jobs (id, name, payload, status, priority, startAfter, queueId) SELECT j.id, j.name, j.payload, j.status, j.priority, j.startAfter, q.id FROM (SELECT '<<uuidv4>>', 'test-job', '{"foo":"bar"}', 'pending', '0', NULL) AS j(id, name, payload, status, priority, startAfter) JOIN mysql_queue_queues q ON q.name = '<<uuidv4>>'`,
    )

    const insertResult = await queryDatabase.query<ResultSetHeader>(sql)
    expect(insertResult).toMatchObject({ affectedRows: 1 })
    await mysqlQueue.destroy()
  })

  it('addManyRawSql', async () => {
    await mysqlQueue.initialize()
    const queueName = randomUUID()
    await mysqlQueue.upsertQueue(queueName)

    const sql = await mysqlQueue.getEnqueueRawSql(queueName, [
      { name: 'test-job', payload: { foo: 'bar' }, priority: 1, startAfter: new Date('2025-03-05T14:30:00.000Z') },
      {
        name: 'test-job-2',
        payload: { foo: 'bar-2' },
      },
    ])

    const sqlWithoutUuids = sql.replace(/([a-f0-9-]{36})/g, '<<uuidv4>>')
    expect(sqlWithoutUuids).toEqual(
      `INSERT INTO mysql_queue_jobs (id, name, payload, status, priority, startAfter, queueId) SELECT j.id, j.name, j.payload, j.status, j.priority, j.startAfter, q.id FROM (SELECT '<<uuidv4>>', 'test-job', '{"foo":"bar"}', 'pending', '1', '2025-03-05 14:30:00' UNION ALL SELECT '<<uuidv4>>', 'test-job-2', '{"foo":"bar-2"}', 'pending', '0', NULL) AS j(id, name, payload, status, priority, startAfter) JOIN mysql_queue_queues q ON q.name = '<<uuidv4>>'`,
    )

    const insertResult = await queryDatabase.query<ResultSetHeader>(sql)
    expect(insertResult).toMatchObject({ affectedRows: 2 })
  })

  it('addJobs with priority', async () => {
    await mysqlQueue.initialize()
    const queueName = randomUUID()
    await mysqlQueue.upsertQueue(queueName)

    const { jobIds } = await mysqlQueue.enqueue(queueName, [
      {
        name: 'test-job',
        payload: { foo2: 'bar2' },
        priority: 1,
      },
      { name: 'test-job', payload: { foo: 'bar' } },
    ])
    expect(jobIds).toHaveLength(2)

    const worker = await mysqlQueue.work(queueName, (j) => WorkerCbMock.handle(j), 50)

    void worker.start()
    await waitInvoked(WorkerCbMock, 'handle', 2)

    expect(WorkerCbMock.handle).toHaveBeenCalledTimes(2)
    expect(WorkerCbMock.handle.mock.calls[0][0]).toMatchObject({ id: jobIds[1] })
    expect(WorkerCbMock.handle.mock.calls[1][0]).toMatchObject({ id: jobIds[0] })
  })

  it('consume timeout', async () => {
    WorkerCbMock.handle.mockImplementation(async () => {
      await sleep(200)
    })

    await mysqlQueue.initialize()
    const queueName = randomUUID()
    const queue = await mysqlQueue.upsertQueue(queueName, { maxDurationMs: 100, backoffMultiplier: 0 })

    await mysqlQueue.enqueue(queueName, { name: 'test-job', payload: { foo: 'bar' } })

    const worker = await mysqlQueue.work(queueName, (j) => WorkerCbMock.handle(j), 50)
    void worker.start()
    await waitInvoked(WorkerCbMock, 'handle', 3)

    const jobs = await queryDatabase.query<RowDataPacket[]>(`SELECT * FROM ${TABLE_JOBS} WHERE queueId = '${queue.id}'`)
    expect(jobs).toHaveLength(1)
    expect(jobs[0]).toMatchObject({
      latestFailureReason: 'Job execution exceed the timeout of 100',
      status: 'failed',
      attempts: 3,
    })
  })
})

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}
