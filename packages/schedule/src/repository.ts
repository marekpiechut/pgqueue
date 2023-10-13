import pg from 'pg'
import cron from './cron.js'
import { ScheduleId, ScheduledJob } from './models.js'

type Config = {
	schema: string
	nodeId: string
}
type JsonSerializable = unknown
type ScheduledJobRow = {
	id: string
	type: string
	created: Date
	updated?: Date
	schedule: string
	timezone?: string
	next_run?: string
	payload: JsonSerializable
}
const toRow = <P>(job: ScheduledJob<P>): ScheduledJobRow => ({
	...job,
	schedule: cron.serialize(job.schedule),
})
export const toJob = <P>(row: ScheduledJobRow): ScheduledJob<P> => ({
	...row,
	payload: row.payload as P,
	schedule: cron.deserialize(row.schedule),
	nextRun: row.next_run ? new Date(row.next_run) : undefined,
})

export class ScheduledJobRepository {
	constructor(
		private client: pg.ClientBase,
		private config: Config
	) {}

	public async create<P>(job: ScheduledJob<P>): Promise<ScheduledJob<P>> {
		const { client, config } = this
		const { schema } = config
		const row = toRow(job)
		await client.query(
			`INSERT INTO ${schema}.SCHEDULE 
			(id, type, created, schedule, timezone, payload)
			VALUES
			($1, $2, $3, $4, $5, $6)`,
			[row.id, row.type, row.created, row.schedule, row.timezone, row.payload]
		)
		return job
	}

	public async delete(id: ScheduleId): Promise<number> {
		const { client, config } = this
		const { schema } = config
		const res = await client.query(
			`DELETE FROM ${schema}.SCHEDULE WHERE id=$1`,
			[id]
		)
		return res.rowCount
	}

	public async fetch<P>(id: ScheduleId): Promise<ScheduledJob<P> | undefined> {
		const { client, config } = this
		const { schema } = config
		const res = await client.query(
			`SELECT * FROM ${schema}.SCHEDULE WHERE id=$1`,
			[id]
		)
		if (res.rowCount === 0) return undefined
		return toJob(res.rows[0])
	}

	// public async poll<P>(batchSize: number = 1): Promise<ScheduledJob<P>[]> {
	// 	const { client, config } = this
	// 	const { schema, nodeId } = config

	// 	//TODO: this should handle priority by type, not globally
	// 	const { rows } = await client.query<JobRow>(
	// 		`WITH next AS (
	// 			SELECT *
	// 			FROM ${client.escapeIdentifier(schema)}.QUEUE
	// 			WHERE type = ANY($1)
	// 				AND state = 'PENDING'
	// 				AND (
	// 					lock_key IS NULL
	// 					OR lock_timeout < now()
	// 				)
	// 			ORDER BY priority NULLS LAST, created, id ASC
	// 			LIMIT $3 FOR
	// 			UPDATE SKIP LOCKED
	// 		)
	// 		UPDATE ${client.escapeIdentifier(schema)}.QUEUE as updated
	// 		SET lock_key = $2,
	// 			state = 'RUNNING',
	// 			tries = next.tries + 1,
	// 			started = now(),
	// 			updated = now(),
	// 			lock_timeout = now() + INTERVAL '15 minutes'
	// 		FROM next
	// 		WHERE updated.id = next.id
	// 		RETURNING updated.*
	// 		`,
	// 		[types, nodeId, batchSize]
	// 	)
	// 	return rows.map(toJob) as ScheduledJob<P>[]
	// }
}
