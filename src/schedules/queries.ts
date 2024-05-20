import { RetryPolicy } from '~/common/retry'
import { first, firstRequired, sql } from '~/common/sql'
import { ScheduleRow } from '~/db'
import cron from './cron'
import { Schedule } from './models'

export type Queries = ReturnType<typeof withSchema>
export const withSchema = (schema: string) =>
	({
		fetchAll: () => sql(schema, rowToSchedule)`
			SELECT * FROM {{schema}}.schedules
		`,
		fetch: <T>(id: Schedule<unknown>['id']) => sql(
			schema,
			rowToSchedule<T>,
			first
		)`
			SELECT * FROM {{schema}}.schedules WHERE id = ${id}
		`,
		//TODO: skip already scheduled!!!
		fetchAndLockRunnable: (batchSize: number) => sql(schema, rowToSchedule)`
			SELECT * FROM {{schema}}.schedules
			WHERE next_run IS NOT NULL AND next_run <= now() AND paused IS NOT TRUE
			ORDER BY schedule ASC
			LIMIT ${batchSize}
			FOR UPDATE SKIP LOCKED
		`,
		insert: <T>(schedule: Schedule<T>) => sql(
			schema,
			rowToSchedule<T>,
			firstRequired
		)`
			INSERT INTO {{schema}}.schedules
			(id, tenant_id, name, type, queue, paused, retry_policy, schedule, payload_type, payload, target, timezone, next_run, created)
			VALUES
			(
				${schedule.id}, 
				${schedule.tenantId}, 
				${schedule.name}, 
				${schedule.type}, 
				${schedule.queue}, 
				${schedule.paused}, 
				${schedule.retryPolicy}, 
				${cron.serialize(schedule.schedule)}, 
				${schedule.payloadType}, 
				${schedule.payload}, 
				${schedule.target}, 
				${schedule.timezone}, 
				${schedule.nextRun}, 
				now()
			) RETURNING *`,
		update: <T>(schedule: Schedule<T>) => sql(
			schema,
			rowToSchedule<T>,
			firstRequired
		)`
			UPDATE {{schema}}.schedules
			SET
				name = ${schedule.name},
				type = ${schedule.type},
				queue = ${schedule.queue},
				paused = ${schedule.paused},
				retry_policy = ${schedule.retryPolicy},
				schedule = ${cron.serialize(schedule.schedule)},
				next_run = ${schedule.nextRun},
				last_run = ${schedule.lastRun},
				payload_type = ${schedule.payloadType},
				payload = ${schedule.payload},
				target = ${schedule.target},
				timezone = ${schedule.timezone},
				updated = now()
			WHERE id = ${schedule.id}
			RETURNING *`,
	}) as const

export const rowToSchedule = <T>(row: ScheduleRow): Schedule<T> => ({
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
	nextRun: row.next_run,
	lastRun: row.last_run,
	timezone: row.timezone,
})
