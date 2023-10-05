import pg from 'pg'
import { JobId } from '../core/index.js'
import cron from './cron.js'
import { ScheduledJob } from './models.js'

type Config = {
	schema: string
}
type JsonSerializable = unknown
type ScheduledJobRow = {
	id: string
	type: string
	created: Date
	updated?: Date
	schedule: string
	timezone?: string
	payload: JsonSerializable
}
const toRow = <P>(job: ScheduledJob<P>): ScheduledJobRow => ({
	...job,
	schedule: cron.serialize(job.schedule),
})
const toJob = <P>(row: ScheduledJobRow): ScheduledJob<P> => ({
	...row,
	payload: row.payload as P,
	schedule: cron.deserialize(row.schedule),
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

	public async delete(id: JobId): Promise<number> {
		const { client, config } = this
		const { schema } = config
		const res = await client.query(
			`DELETE FROM ${schema}.SCHEDULE WHERE id=$1`,
			[id]
		)
		return res.rowCount
	}

	public async fetch<P>(id: JobId): Promise<ScheduledJob<P> | undefined> {
		const { client, config } = this
		const { schema } = config
		const res = await client.query(
			`SELECT * FROM ${schema}.SCHEDULE WHERE id=$1`,
			[id]
		)
		if (res.rowCount === 0) return undefined
		return toJob(res.rows[0])
	}
}
