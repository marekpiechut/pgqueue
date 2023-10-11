import pg from 'pg'
import {
	ActiveJob,
	ArchivalJob,
	Job,
	JobId,
	PendingJob,
	RunningJob,
} from './models.js'

type JsonSerializable = unknown
type JobRow = {
	id: string
	type: string
	created: Date
	updated?: Date
	state: string
	priority?: number
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

type Config = {
	schema: string
	nodeId: string
}
export class JobRepository {
	constructor(
		private client: pg.ClientBase,
		private config: Config
	) {}

	public async create<P>(job: PendingJob<P>): Promise<PendingJob<P>> {
		const { client, config } = this
		const { schema } = config
		const row = toRow(job)
		await client.query(
			`INSERT INTO ${schema}.QUEUE (id, type, created, state, priority, payload) VALUES ($1, $2, $3, $4, $5, $6)`,
			[row.id, row.type, row.created, row.state, row.priority, row.payload]
		)
		return job
	}

	public async update<J extends ActiveJob<unknown>>(job: J): Promise<J> {
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

	public async poll<P>(
		types: string[],
		batchSize: number = 1
	): Promise<RunningJob<P>[]> {
		const { client, config } = this
		const { schema, nodeId } = config

		//TODO: this should handle priority by type, not globally
		const { rows } = await client.query<JobRow>(
			`WITH next AS (
				SELECT *
				FROM ${client.escapeIdentifier(schema)}.QUEUE
				WHERE type = ANY($1)
					AND state = 'PENDING'
					AND (
						lock_key IS NULL
						OR lock_timeout < now()
					)
				ORDER BY priority NULLS LAST, created, id ASC
				LIMIT $3 FOR
				UPDATE SKIP LOCKED
			)
			UPDATE ${client.escapeIdentifier(schema)}.QUEUE as updated
			SET lock_key = $2,
				state = 'RUNNING',
				version = next.version + 1,
				tries = next.tries + 1,
				started = now(),
				updated = now(),
				lock_timeout = now() + INTERVAL '15 minutes'
			FROM next
			WHERE updated.id = next.id
			RETURNING updated.*
			`,
			[types, nodeId, batchSize]
		)
		return rows.map(toJob) as RunningJob<P>[]
	}

	public async restartAll(): Promise<number> {
		const { client, config } = this
		const { schema } = config
		const res = await client.query(
			`UPDATE ${schema}.QUEUE SET
			 lock_key=NULL,
			 lock_timeout=NULL,
			 version=version+1,
			 tries=tries+1,
			 state='PENDING',
			 updated=now(),
			 started=NULL,
			 error='Restarted on node restart'
			 WHERE state='RUNNING'
			 AND lock_key=$1`,
			[config.nodeId]
		)
		return res.rowCount
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
