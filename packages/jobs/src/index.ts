import pg from 'pg'
import { PGQueue } from './queue/index.js'
import * as schema from './schema/index.js'

const DEFAULT_SCHEMA = 'pgqueue'
export const DEFAULT_CONFIG = {
	schema: DEFAULT_SCHEMA,
	pollInterval: 30000,
	runMaintenance: true,
}

export const evolutions = {
	apply: (
		client: pg.ClientBase,
		config?: Partial<schema.Config>
	): Promise<void> => {
		return schema.applyEvolutions({ ...DEFAULT_CONFIG, ...config }, client)
	},
}

export { DEFAULT_SCHEMA, PGQueue }
export default {
	DEFAULT_SCHEMA,
	PGQueue,
	evolutions,
}
