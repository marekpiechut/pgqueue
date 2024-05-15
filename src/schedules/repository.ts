import pg from 'pg'
import cron from '~/common/cron'
import { UUID } from '~/common/models'
import { Repository } from '~/common/psql'
import { RetryPolicy } from '~/common/retry'
import { ScheduleRow } from '~/db'
import { Schedule } from './models'

export class ScheduleRepository extends Repository {
	constructor(
		private schema: string,
		pool: pg.Pool
	) {
		super(pool)
	}

	protected clone(): this {
		return new ScheduleRepository(this.schema, this.pgPool) as this
	}

	async fetchAll(): Promise<Schedule<unknown>[]> {
		const res = await this.execute<ScheduleRow>(
			`SELECT * FROM ${this.schema}.schedules`
		)
		return res.rows.map(scheduleFromRow)
	}

	async fetchAndLockRunnable(batchSize: number): Promise<Schedule<unknown>[]> {
		const res = await this.execute<ScheduleRow>(
			`
			SELECT * FROM ${this.schema}.schedules
			WHERE schedule <= now() AND NOT paused
			LIMIT $1
			ORDER BY schedule ASC
			FOR UPDATE SKIP LOCKED
		`,
			[batchSize]
		)
		return res.rows.map(scheduleFromRow)
	}

	async fetchItem<T>(id: UUID): Promise<Schedule<T> | null> {
		const res = await this.execute<ScheduleRow>(
			`SELECT * FROM ${this.schema}.schedules WHERE id = $1`,
			id
		)

		return res.rows[0] ? scheduleFromRow(res.rows[0]) : null
	}

	async insert<T>(schedule: Schedule<T>): Promise<Schedule<T>> {
		const res = await this.execute<ScheduleRow>(
			`
			INSERT INTO ${this.schema}.schedules
			(id, tenant_id, name, type, queue, paused, retry_policy, schedule, payload_type, payload, target, timezone, created, updated)
			VALUES
			($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, now(), now())
			RETURNING *
		`,
			schedule.id,
			schedule.tenantId,
			schedule.name,
			schedule.type,
			schedule.queue,
			schedule.paused,
			schedule.retryPolicy,
			cron.serialize(schedule.schedule),
			schedule.payloadType,
			schedule.payload,
			schedule.target,
			schedule.timezone
		)

		if (res.rowCount !== 1) {
			throw new Error('Failed to insert schedule')
		}
		return scheduleFromRow(res.rows[0])
	}
}

export const scheduleFromRow = <T>(row: ScheduleRow): Schedule<T> => ({
	id: row.id,
	tenantId: row.tenant_id,
	name: row.name,
	type: row.type,
	queue: row.queue,
	paused: row.paused || false,
	version: row.version,
	tries: row.tries,
	retryPolicy: row.retry as RetryPolicy,
	created: row.created,
	updated: row.updated,
	schedule: cron.deserialize(row.schedule),
	payload: row.payload,
	payloadType: row.payload_type,
	target: row.target as T,
	timezone: row.timezone,
})
