import { v4 as uuid } from 'uuid'
import type { JobId } from '../models.js'
import { Schedule } from './cron.js'

export type ScheduledJob<P> = {
	id: JobId
	type: string
	payload: P
	created: Date
	schedule: Schedule
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
): ScheduledJob<P> => ({
	id: uuid(),
	type: name,
	payload: payload,
	schedule: schedule,
	created: new Date(),
})
