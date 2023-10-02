import pg from 'pg'
import { logger } from '@pgqueue/core'
import * as broadcast from '@pgqueue/broadcast'
import * as jobs from '@pgqueue/jobs'

export type Quickstart = {
	broadcaster: broadcast.Broadcaster
	processors: jobs.Processors
	queue: (client: pg.ClientBase) => jobs.Queue
	evolutions: jobs.Evolutions
}

export type Config = {
	logLevel?: logger.Level
	schema?: string
}

export const withPool = async (
	pool: pg.Pool,
	config?: Config
): Promise<Quickstart> => {
	if (config?.logLevel) {
		logger.setLevel(
			config.logLevel || (process.env.PGQUEUE_LOG_LEVEL as logger.Level)
		)
	}

	const [broadcaster, processors, queue, evolutions] = await Promise.all([
		broadcast.fromPool(pool).then(b => {
			b.start()
			return b
		}),
		jobs.processors.fromPool(pool, config).then(j => {
			j.start()
			return j
		}),
		jobs.queues.create(config),
		jobs.evolutions,
	])

	return {
		evolutions,
		broadcaster,
		processors,
		queue,
	}
}
