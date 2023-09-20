import pg from 'pg'

export type ClientFactory = {
	acquire: () => Promise<pg.Client | pg.PoolClient>
	release: (client: pg.Client | pg.PoolClient) => Promise<void>
}
