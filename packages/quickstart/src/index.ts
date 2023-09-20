import pg from 'pg'
import { logger } from '@pgqueue/core'
import broadcast, { Broadcaster } from '@pgqueue/broadcast'

export type Quickstart = {
	broadcast: Broadcaster
}

export type Config = {
	logLevel?: logger.Level
	dbPrefix?: string
}

export const startWithPool = async (
	pool: pg.Pool,
	config?: Config
): Promise<Quickstart> => {
	if (config?.logLevel) {
		logger.setLevel(
			config.logLevel || (process.env.PGQUEUE_LOG_LEVEL as logger.Level)
		)
	}

	const [broadcaster] = await Promise.all([
		broadcast.fromPool(pool).then(b => {
			b.start()
			return b
		}),
	])

	return {
		broadcast: broadcaster,
	}
}
