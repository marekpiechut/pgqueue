import { uuidv7 } from 'uuidv7'
import cron, { ScheduleConfig } from '~/common/cron'
import { MimeType, TenantId, UUID } from '~/common/models'
import { RetryPolicy } from '~/common/retry'
import { NewQueueItem } from '~/queues'

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
}

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
})

export const executeSchedule = <T>(schedule: Schedule<T>): NewQueueItem<T> => ({
	queue: schedule.queue,
	type: schedule.type,
	scheduleId: schedule.id,
	payload: schedule.payload,
	payloadType: schedule.payloadType,
	retryPolicy: schedule.retryPolicy,
})

export const executedSuccessfully = <T>(
	schedule: Schedule<T>
): Schedule<T> => ({
	...schedule,
	nextRun: cron.nextRun(schedule.schedule, { tz: schedule.timezone }),
})
