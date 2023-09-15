import pg from 'pg'
import config from './test-config'

export const client = (): pg.Client => {
	return new pg.Client(config.postgres)
}
