import pg from 'pg'
import { LogLevel, setLevel } from '~/utils/logger'
import { Config } from './models'
import broadcast, { Broadcaster } from './broadcast'

export type PGQueue = {
	broadcaster: Broadcaster
}

export const startWithPool = async (
	pool: pg.Pool,
	config?: Config
): Promise<PGQueue> => {
	if (config?.logLevel) {
		setLevel(config.logLevel || (process.env.PGQUEUE_LOG_LEVEL as LogLevel))
	}

	const [broadcaster] = await Promise.all([
		broadcast.fromPool(pool).then(b => {
			b.start()
			return b
		}),
	])

	return {
		broadcaster,
	}
}
