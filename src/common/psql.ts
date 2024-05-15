import pg, { QueryConfig, QueryResult, QueryResultRow } from 'pg'
import * as duration from './duration'
import * as logger from './logger'
import { RetryPolicy, nextRunDelay } from './retry'

const log = logger.logger('core:psql')

export type DBConnectionSpec = DBConnection | pg.Pool | string

const DEFAULT_RETRY = {
	type: 'exponential',
	delay: 200,
	max: duration.seconds(30),
} as RetryPolicy
type ConnectionListener = (client: pg.ClientBase) => unknown
export class DBConnection {
	public pool: pg.Pool
	private client: pg.PoolClient | undefined
	private listeners: ConnectionListener[] = []
	private connections = 0
	private connecting: Promise<pg.ClientBase> | undefined = undefined
	private retryDelay: RetryPolicy
	private maxRetries: number

	private constructor(
		pool: pg.Pool,
		config?: {
			retryDelay?: RetryPolicy
			maxRetries?: number
		}
	) {
		this.pool = pool
		this.maxRetries = config?.maxRetries ?? Number.MAX_SAFE_INTEGER
		this.retryDelay = config?.retryDelay ?? DEFAULT_RETRY
	}

	public static create = (
		spec: DBConnection | pg.Pool | string
	): DBConnection => {
		if (typeof spec === 'string') {
			console.log('Creating new connection', spec)
			spec = new pg.Pool({ connectionString: spec })
			return new DBConnection(spec)
		} else if (spec instanceof DBConnection) {
			return spec
		} else {
			return new DBConnection(spec)
		}
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
					if (err || !client) {
						log.error(err, 'Failed to connect to database')
						const delay = nextRunDelay(this.retryDelay, attempt++)
						if (delay == null) {
							log.error('Max connect attempts reached, giving up')
							return reject(err)
						} else {
							log.info(`reconnecting in ${duration.format(delay)}`)
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

export const withTx = async <R>(
	clientOrPool: pg.ClientBase | pg.Pool,
	fn: (tx: pg.ClientBase) => Promise<R>
): Promise<R> => {
	let client: pg.ClientBase
	if (clientOrPool instanceof pg.Pool) {
		client = await clientOrPool.connect()
	} else {
		client = clientOrPool
	}
	try {
		await client.query('BEGIN')
		try {
			const res = await fn(client)
			await client.query('COMMIT')
			return res
		} catch (err) {
			await client.query('ROLLBACK')
			throw err
		}
	} finally {
		if (clientOrPool instanceof pg.Pool) {
			;(client as pg.PoolClient).release()
		}
	}
}

export const withClient = <R>(
	pool: pg.Pool,
	fn: (client: pg.ClientBase) => Promise<R>
): Promise<R> => {
	return pool.connect().then(client => {
		return fn(client).finally(() => client.release())
	})
}

export type Managed = { __managed: true }
export type MaybeManaged = { __managed?: boolean }
export type TenantId = string
export abstract class Repository {
	protected pgPool: pg.Pool
	protected tx?: pg.ClientBase
	protected tenantId?: TenantId

	constructor(pgPool: pg.Pool) {
		this.pgPool = pgPool
	}

	/**
	 * Create a new instance of the repository with the same configuration.
	 * Will be used for withTx and withTenant methods.
	 */
	protected abstract clone(): this

	public withTenant(tenantId: TenantId): this {
		const repo = this.clone()
		repo.tenantId = tenantId
		return repo as this
	}
	public withTx(tx: pg.ClientBase): this {
		const repo = this.clone()
		repo.tx = tx
		return repo as this
	}

	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	protected async execute<T extends QueryResultRow = any>(
		query: string,
		...args: unknown[]
	): Promise<QueryResult<T>>
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	protected async execute<T extends QueryResultRow = any>(
		config: QueryConfig
	): Promise<QueryResult<T>>
	protected async execute<T>(
		fn: (client: pg.ClientBase) => Promise<T>
	): Promise<T>
	protected async execute<T>(
		fnOrQuery: ((client: pg.ClientBase) => Promise<T>) | string | QueryConfig,
		...args: unknown[]
	): Promise<T> {
		const releaseClient = !this.tx
		const client = this.tx || (await this.pgPool.connect())

		try {
			if (this.tenantId)
				await client.query(
					`set "pgqueue.current_tenant"=${client.escapeLiteral(this.tenantId)}`
				)
			else {
				await client.query('reset "pgqueue.current_tenant"')
			}

			if (typeof fnOrQuery === 'string') {
				return (await client.query(fnOrQuery, args)) as T
			} else if (typeof fnOrQuery === 'object') {
				return (await client.query(fnOrQuery)) as T
			} else {
				return await fnOrQuery(client)
			}
		} finally {
			try {
				await client.query('reset "pgqueue.current_tenant"')
			} finally {
				if (releaseClient) {
					await (client as pg.PoolClient).release()
				}
			}
		}
	}
}

export type SortOrder = 'ASC' | 'DESC'
//Make sure we don't get an SQL injection when passing sort from API
export const sanitySort: (
	dir?: SortOrder | null | undefined
) => SortOrder = dir => {
	return dir?.toUpperCase() === 'DESC' ? 'DESC' : 'ASC'
}

export type QueryArg = unknown
export const countArguments = (sql: string): number => {
	const matches = sql.match(/\$[0-9]+/g)
	return matches ? matches.length : 0
}
