import pg from 'pg'
import cron from './cron.js'
import {
	CompletedScheduledJob,
	FailedScheduledJob,
	ScheduleId,
	ScheduleUpdatedEvent,
	ScheduledJob,
	ScheduledJobRun,
} from './models.js'

type Config = {
	schema: string
	nodeId: string
}
type JsonSerializable = unknown
type ScheduledJobRow = {
	id: string
	type: string
	created: Date
	state: string
	updated?: Date
	started?: Date
	schedule: string
	timezone?: string
	next_run?: Date
	result?: JsonSerializable | null
	error?: string | null
	payload: JsonSerializable
}
const toRow = (job: ScheduledJob<unknown>): ScheduledJobRow => ({
	...job,
	next_run: job.nextRun,
	schedule: cron.serialize(job.schedule),
})
const toJob = <P>(row: ScheduledJobRow): ScheduledJob<P> => ({
	...row,
	state: row.state as ScheduledJob<P>['state'],
	payload: row.payload as P,
	schedule: cron.deserialize(row.schedule),
	nextRun: row.next_run,
})

/**
 * We need to parse all dates when deserializing row returned as JSON
 */
const toJobFromJsonRow = <P>(row: ScheduledJobRow): ScheduledJob<P> => ({
	...toJob(row),
	created: new Date(row.created),
	updated: row.updated ? new Date(row.updated) : undefined,
	started: row.started ? new Date(row.started) : undefined,
	nextRun: row.next_run ? new Date(row.next_run) : undefined,
})

type ScheduledJobRunRow = {
	id: string
	schedule_id: ScheduleId
	ran_at: Date
	state: string
	type: string
	result?: JsonSerializable | null
	error?: string | null
}
const runToRow = <R>(run: ScheduledJobRun<R>): ScheduledJobRunRow => ({
	...run,
	ran_at: run.ranAt,
	schedule_id: run.scheduleId,
	result: (run as CompletedScheduledJob<R>).result ?? null,
	error: (run as FailedScheduledJob).error?.toString() ?? null,
})
const runFromRow = <R>(row: ScheduledJobRunRow): ScheduledJobRun<R> => {
	if (row.state === 'COMPLETED') {
		return {
			...row,
			state: 'COMPLETED',
			ranAt: row.ran_at,
			scheduleId: row.schedule_id,
			result: row.result as R,
		}
	} else {
		return {
			...row,
			state: 'FAILED',
			ranAt: row.ran_at,
			scheduleId: row.schedule_id,
			error: new Error(row.error || 'Unknown error'),
		}
	}
}

export const parseNotification = (
	event: pg.Notification
): ScheduleUpdatedEvent => {
	if (event.channel === 'pgqueue_schedule_updated' && event.payload) {
		const payload = JSON.parse(event.payload)
		const job = toJobFromJsonRow(payload)
		return {
			type: 'schedule:updated',
			job: job,
		}
	}

	throw new Error(`Invalid notification ${event.channel}`)
}

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
			(id, type, state, created, schedule, next_run, timezone, payload)
			VALUES
			($1, $2, $3, $4, $5, $6, $7, $8)`,
			[
				row.id,
				row.type,
				row.state,
				row.created,
				row.schedule,
				row.next_run,
				row.timezone,
				row.payload,
			]
		)
		return job
	}

	public async update<P>(job: ScheduledJob<P>): Promise<ScheduledJob<P>> {
		const { client, config } = this
		const { schema } = config
		const row = toRow(job)
		const res = await client.query(
			`UPDATE ${schema}.SCHEDULE set 
				state=$2, updated=$3, next_run=$4
				WHERE id=$1
			`,
			[row.id, row.state, row.updated, row.next_run]
		)

		if (res.rowCount === 0) {
			throw new Error(`Schedule not found for update: ${job.id}`)
		}
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

	public async fetchNext(): Promise<ScheduledJob<unknown> | undefined> {
		const { client, config } = this
		const { schema } = config
		const res = await client.query(
			`SELECT * FROM ${schema}.SCHEDULE WHERE state = 'WAITING' ORDER BY next_run LIMIT 1`
		)
		if (res.rowCount === 0) return undefined
		return toJob(res.rows[0])
	}

	public async poll(
		types: string[],
		batchSize: number = 1
	): Promise<ScheduledJob<unknown>[]> {
		const { client, config } = this
		const { schema, nodeId } = config

		//TODO: make lock timeout configurable
		const { rows } = await client.query(
			`WITH next AS (
				SELECT *
				FROM ${client.escapeIdentifier(schema)}.SCHEDULE
				WHERE type = ANY($1)
					AND state = 'WAITING'
					OR state = 'RUNNING' AND (
						lock_key IS NULL
						OR lock_timeout < now()
					)
				ORDER BY next_run 
				LIMIT $3 FOR
				UPDATE SKIP LOCKED
			)
			UPDATE ${client.escapeIdentifier(schema)}.SCHEDULE as updated
			SET lock_key = $2,
				state = 'RUNNING',
				started = now(),
				updated = now(),
				lock_timeout = now() + INTERVAL '15 minutes'
			FROM next
			WHERE updated.id = next.id
			RETURNING updated.*
			`,
			[types, nodeId, batchSize]
		)
		return rows.map(toJob) as ScheduledJob<unknown>[]
	}

	public async saveRun<R, Run extends ScheduledJobRun<R>>(
		run: Run
	): Promise<Run> {
		const { client, config } = this
		const { schema } = config

		const row = runToRow(run)
		await client.query(
			`INSERT INTO ${schema}.SCHEDULE_RUNS 
			(id, schedule_id, ran_at, state, type, result, error)
			VALUES
			($1, $2, $3, $4, $5, $6, $7)`,
			[
				row.id,
				row.schedule_id,
				row.ran_at,
				row.state,
				row.type,
				row.result,
				row.error,
			]
		)
		return run
	}
	public async fetchRuns(
		job: ScheduledJob<unknown>
	): Promise<ScheduledJobRun<unknown>[]> {
		const { client, config } = this
		const { schema } = config

		const res = await client.query(
			`SELECT * FROM ${schema}.SCHEDULE_RUNS
			 WHERE schedule_id=$1 ORDER BY ran_at`,
			[job.id]
		)

		return res.rows.map(runFromRow)
	}

	public async restartAll(): Promise<number> {
		const { client, config } = this
		const { schema } = config
		const res = await client.query(
			`UPDATE ${schema}.SCHEDULE SET
			 lock_key=NULL,
			 lock_timeout=NULL,
			 state='WAITING',
			 updated=now(),
			 started=NULL
			 WHERE state='RUNNING'
			 AND lock_key=$1`,
			[config.nodeId]
		)
		return res.rowCount
	}
}
