import pg from 'pg'
import { Job, JobId } from '../models.js'

type JsonSerializable = unknown
type JobRow = {
	id: string
	type: string
	created: Date
	state: string
	payload: JsonSerializable
}
const toRow = <P>(job: Job<P>): JobRow => job
const toJob = <P>(row: JobRow): Job<P> => row as Job<P>

type DBConfig = {
	schema: string
}
export class JobRepository {
	constructor(
		private client: pg.ClientBase,
		private config: DBConfig
	) {}

	public async push<P>(job: Job<P>): Promise<Job<P>> {
		const { client, config } = this
		const { schema } = config
		const row = toRow(job)
		await client.query(
			`INSERT INTO ${schema}.QUEUE (id, type, created, state, payload) VALUES ($1, $2, $3, $4, $5)`,
			[row.id, row.type, row.created, row.state, row.payload]
		)
		return job
	}
	public async pop<P>(types: string[]): Promise<Job<P> | undefined> {
		const { client, config } = this
		const { schema } = config
		const { rows } = await client.query<JobRow>(
			`SELECT * FROM ${schema}.QUEUE 
				 WHERE type = ANY($1) ORDER BY created, id ASC LIMIT 1
				 FOR UPDATE SKIP LOCKED
			`,
			[types]
		)
		if (rows.length === 0) return undefined
		const row = rows[0]
		return toJob(row)
	}
	public async peek<P>(
		types: string[],
		limit?: number,
		before?: JobId
	): Promise<ReadonlyArray<Job<P>>> {
		const { client, config } = this
		const { schema } = config
		const { rows } = await client.query<JobRow>(
			`WITH last(created, id) AS (
				SELECT created, id FROM ${schema}.QUEUE WHERE id = $2
			)
			SELECT * FROM ${schema}.QUEUE 
				 WHERE type = ANY($1) AND
				 (created, id) < (last.created, $2)
				 ORDER BY created, id ASC LIMIT $3
			`,
			[types, before, limit]
		)

		return rows.map(toJob) as ReadonlyArray<Job<P>>
	}
}
