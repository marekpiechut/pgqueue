import pg from 'pg'
import { CompletedJob, FailedJob } from '../models.js'

type JsonSerializable = unknown
type JobRow = {
	id: string
	type: string
	created: Date
	updated?: Date
	state: string
	payload: JsonSerializable
	result?: JsonSerializable | null
	error?: Error | string
}
const toRow = <P, R>(job: ArchivalJob<P, R>): JobRow => job
// eslint-disable-next-line @typescript-eslint/no-explicit-any
// const toJob = <J extends ArchivalJob<any, any>>(row: JobRow): J => row as J

type ArchivalJob<P, R> = CompletedJob<P, R> | FailedJob<P>

type DBConfig = {
	schema: string
}
export class JobHistoryRepository {
	constructor(
		private client: pg.ClientBase,
		private config: DBConfig
	) {}

	public async create<J extends ArchivalJob<unknown, unknown>>(
		job: J
	): Promise<J> {
		const { client, config } = this
		const { schema } = config
		const row = toRow(job)
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

	public async update<J extends ArchivalJob<unknown, unknown>>(
		job: J
	): Promise<J> {
		const { client, config } = this
		const { schema } = config
		const row = toRow(job)
		const res = await client.query(
			`UPDATE ${schema}.QUEUE set 
				state=$2, result=$3, updated=$4
				WHERE id=$1
			`,
			[row.id, row.state, row.result, row.updated]
		)
		if (res.rowCount !== 1) {
			throw new Error(`Failed to find job for update: ${job.id}`)
		}
		return job
	}
}
