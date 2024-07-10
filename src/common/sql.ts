import * as pg from 'pg'
import { TenantId } from './psql'

// biome-ignore lint/suspicious/noExplicitAny: Expected any type
type SqlArg = any
export type QueryConfig<R, M = R, O = M[]> = {
	name?: string
	mapper: (row: R) => M
	extractor: (res: M[]) => O
}
export type Query<R, M = R, O = M[], V = SqlArg[]> = QueryConfig<R, M, O> & {
	text: string
	values: pg.QueryConfigValues<V>
}

export const identity = <T>(a: T): T => a
export const noopMapper = <T extends pg.QueryResultRow>(a: T): T => a
export const first = <T>(a: T[]): T | undefined => a[0]
export const returnNothing = (): void => {}
export const firstRequired = <T>(a: T[]): T => {
	const first = a[0]
	if (!first) throw new Error('Result is empty')
	return first
}

export const query = <
	R extends pg.QueryResultRow,
	M = R,
	O = M[],
	V = SqlArg[],
>(
	client: pg.ClientBase,
	sql: Query<R, M, O, V>
): Promise<O> => {
	return client.query<R>(sql).then(res => {
		const mapped = res.rows.map(sql.mapper)
		return sql.extractor(mapped)
	})
}

type SQL<R, M = R, O = M[], V = SqlArg[]> = (
	strings: TemplateStringsArray,
	...args: SqlArg[]
) => Query<R, M, O, V>

type Mapper<R, M> = (row: R) => M
type Extractor<M, O> = (mapped: M[]) => O

export function sql<R, V = SqlArg[]>(schema: string): SQL<R, R, R[], V>
export function sql<R extends pg.QueryResultRow, M, O = M[], V = SqlArg[]>(
	schema: string,
	mapper: Mapper<R, M> | QueryConfig<R, M, O>
): SQL<R, M, M[], V>
export function sql<R extends pg.QueryResultRow, M = R, O = M[], V = SqlArg[]>(
	schema: string,
	mapper: Mapper<R, M>,
	extractor: Extractor<M, O>,
	name?: string
): SQL<R, M, O, V>
export function sql<R extends pg.QueryResultRow, M, O, V = SqlArg[]>(
	schema: string,
	mapper?: Mapper<R, M> | QueryConfig<R, M, O>,
	extractor?: Extractor<M, O>,
	name?: string
): SQL<R, M, O, V> {
	return (strings, ...values): Query<R, M, O, V> => {
		if (typeof mapper === 'object') {
			extractor = extractor ?? mapper.extractor
			name = name ?? mapper.name
			mapper = mapper.mapper
		}

		const text = strings.reduce((acc, part, i) => {
			acc += part.replaceAll('{{schema}}', schema)
			if (values.length > i) {
				acc += `$${i + 1}`
			}
			return acc
		}, '')

		return {
			text,
			values: values as pg.QueryConfigValues<V>,
			mapper: (mapper || identity) as Mapper<R, M>,
			extractor: extractor || (identity as Extractor<M, O>),
		}
	}
}

export type DBConnectionSpec = pg.Pool | string | DB
export class DB {
	protected pgPool: pg.Pool
	protected tx?: pg.ClientBase
	protected tenantId?: TenantId

	constructor(pool: pg.Pool) {
		this.pgPool = pool
	}

	public static create(spec: DBConnectionSpec): DB {
		if (spec instanceof DB) {
			return spec
		} else if (typeof spec === 'string') {
			return new DB(new pg.Pool({ connectionString: spec }))
		} else {
			return new DB(spec)
		}
	}

	public withTenant(tenantId: TenantId): DB {
		const copy = new DB(this.pgPool)
		copy.tenantId = tenantId
		copy.tx = this.tx

		return copy
	}

	public withTx(tx?: pg.ClientBase): DB {
		const copy = new DB(this.pgPool)
		copy.tenantId = this.tenantId
		copy.tx = tx
		return copy
	}

	public async transactional<T>(body: (db: DB) => Promise<T>): Promise<T> {
		if (this.tx) {
			return body(this)
		} else {
			return startTx(this.pgPool, async tx => {
				return body(this.withTx(tx))
			})
		}
	}

	public async execute<R, M = R, O = M[], V = SqlArg[]>(
		query: Query<R, M, O, V> | ((client: pg.ClientBase) => O)
	): Promise<O> {
		const releaseClient = !this.tx
		const client = this.tx || (await this.pgPool.connect())

		try {
			this.setTenant(client)
			if (typeof query === 'function') {
				return await query(client)
			} else {
				const res = await client.query(query.text, query.values)
				const mapped = res.rows.map(query.mapper)
				return query.extractor(mapped)
			}
		} finally {
			try {
				this.clearTenant(client)
			} finally {
				if (releaseClient) {
					await (client as pg.PoolClient).release()
				}
			}
		}
	}

	private async setTenant(client: pg.ClientBase): Promise<void> {
		if (this.tenantId) {
			await client.query(
				`set "pgqueue.current_tenant"=${client.escapeLiteral(this.tenantId)}`
			)
		} else {
			await this.clearTenant(client)
		}
	}

	private async clearTenant(client: pg.ClientBase): Promise<void> {
		await client.query('reset "pgqueue.current_tenant"')
	}
}

export const startTx = async <R>(
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
