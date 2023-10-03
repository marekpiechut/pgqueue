import pg from 'pg'

export type ClientFactory = {
	acquire: () => Promise<pg.Client | pg.PoolClient>
	release: (client: pg.Client | pg.PoolClient) => Promise<void>
}

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
