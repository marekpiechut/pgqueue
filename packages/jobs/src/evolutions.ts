import pg from 'pg'
import { DEFAULT_CONFIG } from './core/index.js'
import { applyEvolutions } from './schema/index.js'

export type Evolutions = {
	apply: (
		client: pg.ClientBase,
		evoConfig: {
			schema?: string
			destroy_my_data_AllowDownMigration?: boolean
		}
	) => Promise<void>
}
export const evolutions: Evolutions = {
	apply: (client, evoConfig) => {
		return applyEvolutions({ ...DEFAULT_CONFIG, ...evoConfig }, client)
	},
}

export default evolutions
