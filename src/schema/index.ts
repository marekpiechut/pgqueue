import * as evolutions from './evolutions'
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
}

export const applyEvolutions = async (
	config: Config,
	client: pg.Client
): Promise<void> => {
	validateConfig(config)
	return evolutions.apply(EVOLUTIONS, client, config)
}

const validateConfig = (config: Config): void => {
	if (config.typeSize < 16) {
		throw Error('Type size must be at least 16 characters')
	}
}
