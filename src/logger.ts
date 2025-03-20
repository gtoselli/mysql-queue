import pino, { LevelWithSilentOrString } from 'pino'

export type Logger = ReturnType<typeof Logger>

export function Logger(options: { level?: LevelWithSilentOrString; prettyPrint?: boolean }) {
  return pino({
    level: options.level || 'debug',
    transport: options.prettyPrint
      ? {
          target: 'pino-pretty',
          options: {
            colorize: true,
          },
        }
      : undefined,
  })
}
