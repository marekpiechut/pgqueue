import { Broadcaster } from '@pgqueue/broadcast'
import { logger, psql } from '@pgqueue/core'
import { PGQueue } from '@pgqueue/jobs'
import pg from 'pg'

// const log = logger.create('quickstart')

export type { Broadcaster, PGQueue }
export type Quickstart = {
	broadcaster: Broadcaster
	queue: PGQueue
	start: () => Promise<void>
	stop: () => Promise<void>
}

export type Config = {
	logLevel?: logger.Level
	schema?: string
	pool?: pg.Pool
}

const configureLogger = (config?: Config): void => {
	logger.setLevel(
		config?.logLevel || (process.env.PGQUEUE_LOG_LEVEL as logger.Level)
	)
}

export const withPool = async (
	pool: pg.Pool,
	config?: Config
): Promise<Quickstart> => {
	configureLogger(config)

	const persistentConnection = new psql.SharedPersistentConnection(pool)
	const connectionFactory = psql.poolConnectionFactory(pool)
	const broadcaster = new Broadcaster(persistentConnection)
	const queue = new PGQueue(connectionFactory, persistentConnection, config)

	const start = async (): Promise<void> => {
		await Promise.all([queue.start(), broadcaster.start()])
	}

	const stop = async (): Promise<void> => {
		await Promise.all([queue.stop(), broadcaster.stop()])
	}

	return {
		broadcaster,
		queue,
		start,
		stop,
	}
}

// //TODO: add global instances for super simple usage
// //make sure users can setup all listeners before starting
// //right now it's not possible, as all the constructors
// //require a connection to be passed in
// export const
// export const start = async (
// 	config?: Config & pg.ClientConfig
// ): Promise<Quickstart> => {
// 	configureLogger(config)
// 	if (global) {
// 		log.warn('Global instance already started, restarting to reconfigure')
// 		await global.stop()
// 	}

// 	const pool = config?.pool || new pg.Pool(config)
// 	global = await withPool(pool, config)

// 	await global.start()
// 	return global
// }

// export const stop = async (): Promise<void> => {
// 	if (!global) return
// 	await global.stop()
// }
