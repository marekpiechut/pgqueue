import pg from 'pg'
import * as logger from './logger.js'

export type ClientFactory = {
	acquire: () => Promise<pg.ClientBase>
	persistent: (config?: {
		onConnect: (newClient: pg.ClientBase) => unknown
	}) => Promise<pg.ClientBase>
	release: (client: pg.ClientBase) => Promise<void>
	withClient: (
		fn: (client: pg.ClientBase) => Promise<unknown>
	) => Promise<unknown>
}

const log = logger.create('core:psql')

export const poolConnectionFactory = (pool: pg.Pool): ClientFactory => {
	let persistent: pg.PoolClient | undefined
	let persistentConnections: number = 0

	return {
		acquire: async () => {
			return pool.connect()
		},
		release: async client => {
			if (client === persistent && --persistentConnections <= 0) {
				log.info('Releasing persistent client')
				persistent.release()
				persistent = undefined
			} else if ((client as pg.PoolClient).release) {
				return (client as pg.PoolClient).release()
			} else {
				throw new Error('Cannot release client not from the pool')
			}
		},
		withClient(fn) {
			return pool.connect().then(client => {
				return fn(client).finally(() => client.release())
			})
		},
		persistent: async config => {
			if (!persistent) {
				persistent = await pool.connect()
				config?.onConnect?.(persistent)
				persistent.on('error', async err => {
					log.error(err, 'Error from persistent client, restarting connection')
					persistent?.release()
					persistent = await pool.connect()
					config?.onConnect?.(persistent)
				})
			}
			persistentConnections++
			return persistent
		},
	}
}

export const singleConnectionFactory = (client: pg.Client): ClientFactory => ({
	acquire: async () => {
		return client
	},
	release: async () => {
		return client.end()
	},
	withClient: fn => {
		return fn(client)
	},
	persistent: async () => {
		//Just return the same client, there's nothing we can do to make it persistent
		return client
	},
})

export type QueryArgs = ReadonlyArray<
	| boolean
	| number
	| string
	| Date
	| null
	| typeof undefined
	| ReadonlyArray<boolean | number | string | Date | null | typeof undefined>
>

type SQLTemplate = {
	readonly statement: string
	readonly values: QueryArgs
}

export const SQL = (strings: string[], ...args: QueryArgs): SQLTemplate => {
	let idx = 1
	let statement = strings[0]
	const values = []
	for (const arg of args) {
		if (Array.isArray(arg)) {
			for (const val of arg) {
				statement += `$${idx++},`
				values.push(val)
			}
		} else {
			statement += `$${idx++}`
			values.push(arg)
		}
	}

	return { statement, values }
}

export const withTx = <R>(
	client: pg.ClientBase,
	fn: () => Promise<R>
): Promise<R> => {
	return client
		.query('BEGIN')
		.then(fn)
		.then(res => {
			client.query('COMMIT')
			return res
		})
		.catch(err => {
			client.query('ROLLBACK')
			throw err
		})
}

export const withClient = <R>(
	pool: pg.Pool,
	fn: (client: pg.ClientBase) => Promise<R>
): Promise<R> => {
	return pool.connect().then(client => {
		return fn(client).finally(() => client.release())
	})
}
