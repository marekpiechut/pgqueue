import { uuidv7 } from 'uuidv7'
import cron, { ScheduleConfig } from './cron'
import { MimeType, TenantId, UUID } from '~/common/models'
import { RetryPolicy } from '~/common/retry'
import { NewQueueItem } from '~/queues'

export const DEFAULT_TIMEZONE = 'UTC'

export type NewSchedule<T> = {
	name: string
	queue: string
	schedule: ScheduleConfig
	paused?: boolean
	retryPolicy?: RetryPolicy
	payloadType?: MimeType
	payload?: Buffer
	type: string
	target?: T
	timezone?: string
}
export type Schedule<T> = NewSchedule<T> & {
	id: UUID
	tenantId: TenantId
	version: number
	tries: number
	created: Date
	updated?: Date
	nextRun?: Date
	lastRun?: Date
	timezone: string
}
export type AnySchedule = Schedule<unknown>
export type ScheduleUpdate<T> = Partial<
	Pick<Schedule<T>, 'name' | 'schedule' | 'paused'>
>

export const newSchedule = <T>(
	tenant: TenantId,
	input: NewSchedule<T>
): Schedule<T> => ({
	...input,
	id: uuidv7(),
	tenantId: tenant,
	version: 0,
	tries: 0,
	created: new Date(),
	nextRun: cron.nextRun(input.schedule, { tz: input.timezone }),
	timezone: input.timezone || DEFAULT_TIMEZONE,
})

export const executeSchedule = <T>(schedule: Schedule<T>): NewQueueItem<T> => ({
	queue: schedule.queue,
	type: schedule.type,
	scheduleId: schedule.id,
	target: schedule.target,
	payload: schedule.payload,
	payloadType: schedule.payloadType,
	retryPolicy: schedule.retryPolicy,
})

export const executedSuccessfully = <T>(
	schedule: Schedule<T>
): Schedule<T> => ({
	...schedule,
	nextRun: cron.nextRun(schedule.schedule, { tz: schedule.timezone }),
	lastRun: new Date(),
})

export const updateSchedule = <T>(
	current: Schedule<T>,
	update: ScheduleUpdate<T>
): Schedule<T> => {
	const updated = {
		...current,
		name: firstDefined(update.name, current.name),
		paused: firstDefined(update.paused, current.paused, false),
		schedule: firstDefined(update.schedule, current.schedule),
	}

	if (!updated.paused && current.paused) {
		updated.nextRun = cron.nextRun(updated.schedule, { tz: updated.timezone })
	}

	return updated
}

const firstDefined = <T>(...values: Array<T | undefined>): T =>
	values.find(v => v !== undefined)!
