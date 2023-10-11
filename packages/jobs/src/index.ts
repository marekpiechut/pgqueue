import { psql } from '@pgqueue/core'
import pg from 'pg'
import { PGQueue } from './queue/index.js'
import * as schema from './schema/index.js'

const DEFAULT_SCHEMA = 'pgqueue'
export const DEFAULT_CONFIG = {
	schema: DEFAULT_SCHEMA,
	pollInterval: 30000,
	runMaintenance: true,
}
export type Config = Partial<typeof DEFAULT_CONFIG>

export const evolutions = {
	apply: (client: pg.ClientBase, config?: schema.Config): Promise<void> => {
		return schema.applyEvolutions({ schema: DEFAULT_SCHEMA, ...config }, client)
	},
}

export const quickstart = async (
	poolOrConfig: pg.PoolConfig | pg.Pool,
	config?: Config
): Promise<PGQueue> => {
	const pool =
		poolOrConfig instanceof pg.Pool ? poolOrConfig : new pg.Pool(poolOrConfig)
	const persistentConnection = new psql.SharedPersistentConnection(pool)
	const connectionFactory = psql.poolConnectionFactory(pool)

	return new PGQueue(connectionFactory, persistentConnection, config)
}

export { DEFAULT_SCHEMA, PGQueue }
export default {
	DEFAULT_SCHEMA,
	PGQueue,
	quickstart,
	evolutions,
}
