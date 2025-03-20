import mysql, { QueryResult } from 'mysql2/promise'

export type Database = ReturnType<typeof QueryDatabase>

export function QueryDatabase(params: { uri: string }) {
  const pool = mysql.createPool({ uri: params.uri, waitForConnections: true })

  async function query<T extends QueryResult>(sql: string) {
    const connection = await pool.getConnection()
    try {
      const [rows] = await connection.query<T>(sql)
      return rows
    } finally {
      connection.release()
    }
  }

  async function dispose() {
    await pool.end()
  }

  return { query, dispose, pool }
}
