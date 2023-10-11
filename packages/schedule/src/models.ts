import { ids } from '@pgqueue/core'
import { Schedule, isFuture } from './cron.js'

export type ScheduleId = string
export type ScheduledJob<P> = {
	id: ScheduleId
	type: string
	payload: P
	created: Date
	schedule: Schedule
	timezone?: string
	updated?: Date
}

export type ScheduledJobOptions = {
	limit?: number
	timezone?: string
}

export const newSchedule = <P>(
	name: string,
	schedule: Schedule,
	payload: P,
	_options?: ScheduledJobOptions
): ScheduledJob<P> => {
	const job = {
		id: ids.uuid(),
		type: name,
		payload: payload,
		schedule: schedule,
		timezone: _options?.timezone,
		created: new Date(),
	}
	validate.isFuture(job)
	return job
}

const validate = {
	isFuture: <P>(job: ScheduledJob<P>): void => {
		if (!isFuture(job.schedule)) {
			throw new Error(`Schedule is in the past: ${job.schedule}`)
		}
	},
}
