import pg from 'pg'
import { Job } from '../models'

type JsonSerializable = unknown
type JobRow = {
	id: string
	type: string
	created: Date
	state: string
	payload: JsonSerializable
}
const toRow = <P>(job: Job<P>): JobRow => job

type DBConfig = {
	baseName: string
}
export class JobRepository {
	constructor(
		private client: pg.ClientBase,
		private config: DBConfig
	) {}

	public async push<P>(job: Job<P>): Promise<Job<P>> {
		const { client, config } = this
		const { baseName } = config
		const row = toRow(job)
		await client.query(
			`INSERT INTO ${baseName}_QUEUE (id, type, created, state, payload) VALUES ($1, $2, $3, $4, $5)`,
			[row.id, row.type, row.created, row.state, row.payload]
		)
		return job
	}
}
