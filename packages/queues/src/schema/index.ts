import * as evolutions from '@pgqueue/evolutions'
import pg from 'pg'

/***********************************
 * ALL EVOLUTIONS                  *
 * NEVER REMOVE OR CHANGE ORDER!!! *
 ***********************************/
const EVOLUTIONS = ['./versions/v1.sql']
/***********************************
 * ALL EVOLUTIONS END              *
 ***********************************/

export type Config = evolutions.Config & {
	typeSize: number
	eventBase: string
}
export const DEFAULT_CONFIG: Config = {
	baseName: 'events',
	typeSize: 16,
	eventBase: 'pgevents:queue',
}

export const applyEvolutions = async (
	config: Partial<Config>,
	client: pg.ClientBase
): Promise<void> => {
	const mergedConfig = { ...DEFAULT_CONFIG, ...config }
	validateConfig(mergedConfig)
	const sqls = await Promise.all(EVOLUTIONS.map(evo => import(evo))).then(
		evos => evos.map(evo => evo.default(mergedConfig))
	)
	return evolutions.apply(sqls, client, mergedConfig)
}

const validateConfig = (config: Config): void => {
	if (config.typeSize < 16) {
		throw Error('Type size must be at least 16 characters')
	}
}
