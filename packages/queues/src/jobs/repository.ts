import pg from 'pg'
import { JobId } from '../models.js'
import { ArchivalJob, Job, PendingJob } from './models.js'

type JsonSerializable = unknown
type JobRow = {
	id: string
	type: string
	created: Date
	updated?: Date
	state: string
	payload: JsonSerializable
}
const toRow = <P, R>(job: Job<P, R>): JobRow => job
// eslint-disable-next-line @typescript-eslint/no-explicit-any
const toJob = <J extends Job<any, any>>(row: JobRow): J => row as J

type ArchivalJobRow = JobRow & {
	result?: JsonSerializable | null
	error?: string | null
}
const toArchivalRow = <P, R>(job: ArchivalJob<P, R>): ArchivalJobRow => {
	if (job.state === 'COMPLETED') {
		return job
	} else {
		return {
			...job,
			error: job.error?.toString() ?? null,
		}
	}
}
const toArchivalJob = <J extends ArchivalJob<unknown, unknown>>(
	row: ArchivalJobRow
): J => {
	if (row.state === 'COMPLETED') {
		return row as J
	} else {
		return {
			...row,
			error: row.error ? new Error(row.error) : undefined,
		} as J
	}
}

type DBConfig = {
	schema: string
}
export class JobRepository {
	constructor(
		private client: pg.ClientBase,
		private config: DBConfig
	) {}

	public async create<P>(job: PendingJob<P>): Promise<PendingJob<P>> {
		const { client, config } = this
		const { schema } = config
		const row = toRow(job)
		await client.query(
			`INSERT INTO ${schema}.QUEUE (id, type, created, state, payload) VALUES ($1, $2, $3, $4, $5)`,
			[row.id, row.type, row.created, row.state, row.payload]
		)
		return job
	}

	public async update<J extends Job<unknown, unknown>>(job: J): Promise<J> {
		const { client, config } = this
		const { schema } = config
		const row = toRow(job)
		const res = await client.query(
			`UPDATE ${schema}.QUEUE set 
				state=$2, updated=$3
				WHERE id=$1
			`,
			[row.id, row.state, row.updated]
		)
		if (res.rowCount !== 1) {
			throw new Error(`Failed to find job for update: ${job.id}`)
		}
		return job
	}

	public async delete(id: JobId): Promise<number> {
		const { client, config } = this
		const { schema } = config
		const res = await client.query(`DELETE FROM ${schema}.QUEUE WHERE id=$1`, [
			id,
		])
		return res.rowCount
	}

	public async pop<P>(types: string[]): Promise<PendingJob<P> | undefined> {
		const { client, config } = this
		const { schema } = config
		const { rows } = await client.query<JobRow>(
			`SELECT * FROM ${schema}.QUEUE 
					WHERE type=ANY($1) AND state='PENDING' 
					ORDER BY created, id ASC LIMIT 1
					FOR UPDATE SKIP LOCKED
				`,
			[types]
		)
		if (rows.length === 0) return undefined
		const row = rows[0]
		return toJob(row) as PendingJob<P>
	}

	public async archive<J extends ArchivalJob<unknown, unknown>>(
		job: J
	): Promise<J> {
		const { client, config } = this
		const { schema } = config
		const row = toArchivalRow(job)
		await client.query(
			`INSERT INTO ${schema}.QUEUE_HISTORY
				(id, type, created, state, payload, result, error)
				VALUES
				($1, $2, $3, $4, $5, $6, $7)`,
			[
				row.id,
				row.type,
				row.created,
				row.state,
				row.payload,
				row.result,
				row.error?.toString(),
			]
		)
		return job
	}

	public async fetchArchive(
		id: JobId
	): Promise<ArchivalJob<unknown, unknown> | undefined> {
		const { client, config } = this
		const { schema } = config
		const { rows } = await client.query<ArchivalJobRow>(
			`SELECT * FROM ${schema}.QUEUE_HISTORY WHERE id=$1`,
			[id]
		)
		if (rows.length === 0) return undefined
		return toArchivalJob(rows[0])
	}
}
