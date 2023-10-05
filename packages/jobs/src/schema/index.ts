import * as evolutions from '@pgqueue/evolutions'
import pg from 'pg'

/***********************************
 * ALL EVOLUTIONS                  *
 * NEVER REMOVE OR CHANGE ORDER!!! *
 ***********************************/
const EVOLUTIONS = ['./versions/v1.sql.js']
/***********************************
 * ALL EVOLUTIONS END              *
 ***********************************/

export type Config = evolutions.Config & {
	schema: string
}

export const applyEvolutions = async (
	config: Config,
	client: pg.ClientBase
): Promise<void> => {
	validateConfig(config)
	const sqls = await Promise.all(EVOLUTIONS.map(evo => import(evo))).then(
		evos => evos.map(evo => evo.default(config))
	)
	return evolutions.apply(sqls, client, config)
}

const validateConfig = (config: Config): void => {
	if (!config.schema) {
		throw Error('Database schema is required')
	}
}
