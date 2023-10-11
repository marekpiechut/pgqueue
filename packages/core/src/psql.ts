import pg from 'pg'
import { formatDuration, seconds } from './duration.js'
import * as logger from './logger.js'
import { Delay, nextRun } from './retry.js'

export type ClientFactory = {
	acquire: () => Promise<pg.ClientBase>
	release: (client: pg.ClientBase) => Promise<void>
	withClient: <R>(fn: (client: pg.ClientBase) => Promise<R>) => Promise<R>
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
		withClient: fn => {
			return pool.connect().then(client => {
				return fn(client).finally(() => client.release())
			})
		},
	}
}

const DEFAULT_RETRY = {
	type: 'exponential',
	delay: 200,
	max: seconds(30),
} as Delay
type ConnectionListener = (client: pg.ClientBase) => unknown
export class SharedPersistentConnection {
	private pool: pg.Pool
	private client: pg.PoolClient | undefined
	private listeners: ConnectionListener[] = []
	private connections = 0
	private connecting: Promise<pg.ClientBase> | undefined = undefined
	private retryDelay: Delay
	private maxRetries: number

	constructor(
		pool: pg.Pool,
		config?: {
			retryDelay?: Delay
			maxRetries?: number
		}
	) {
		this.pool = pool
		this.maxRetries = config?.maxRetries ?? Number.MAX_SAFE_INTEGER
		this.retryDelay = config?.retryDelay ?? DEFAULT_RETRY
	}

	public async getClient(): Promise<pg.ClientBase> {
		return this.client ?? this.connecting ?? this.connect()
	}
	public async acquire(listener: ConnectionListener): Promise<void> {
		this.listeners.push(listener)
		if (!this.client) {
			await this.connect()
		} else {
			listener(this.client)
		}
	}

	public async release(listener: ConnectionListener): Promise<void> {
		const idx = this.listeners.indexOf(listener)
		if (idx >= 0) {
			this.listeners.splice(idx, 1)
			if (this.listeners.length === 0) {
				await this.client?.release()
			}
		}
	}

	private onError = ((err: Error): void => {
		log.error(err, 'Persistent connection error')
		this.connecting = this.connect()
	}).bind(this)

	private async connect(): Promise<pg.ClientBase> {
		if (this.connecting) return this.connecting
		this.connecting = new Promise((resolve, reject) => {
			let attempt = 0
			const tryConnecting = (): void => {
				this.client?.release()
				this.client = undefined
				this.pool.connect((err, client) => {
					if (err) {
						log.error(err, 'Failed to connect to database')
						if (attempt > this.maxRetries) {
							log.error('Max connect attempts reached, giving up')
							return reject(err)
						} else {
							const delay = nextRun(this.retryDelay, attempt++)
							log.info(`reconnecting in ${formatDuration(delay)}`)
							setTimeout(tryConnecting, delay)
						}
					} else {
						log.debug('Database connected')
						client.on('error', this.onError)
						this.client = client
						this.listeners.forEach(listener => listener(client))
						this.connecting = undefined
						resolve(client)
					}
				})
			}
			tryConnecting()
		})

		return this.connecting
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
		statement += `$${idx++}`
		values.push(arg)
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
