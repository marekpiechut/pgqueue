import { ids } from '@pgqueue/core'
import { Schedule, isFuture, nextRun } from './cron.js'

export type ScheduleId = string
//TODO: split types
export type ScheduledJob<P> = {
	id: ScheduleId
	type: string
	payload: P
	created: Date
	//This should go into separate type
	started?: Date
	state: 'WAITING' | 'RUNNING' | 'PAUSED' | 'COMPLETED' | 'FAILED'
	schedule: Schedule
	timezone?: string
	nextRun?: Date
	updated?: Date
}
export type FailedScheduledJob = {
	id: string
	scheduleId: ScheduleId
	state: 'FAILED'
	type: string
	ranAt: Date
	error: Error
}
export type CompletedScheduledJob<R> = {
	id: string
	scheduleId: ScheduleId
	state: 'COMPLETED'
	type: string
	ranAt: Date
	result: R
}
export type ScheduledJobRun<R> = FailedScheduledJob | CompletedScheduledJob<R>

export type ScheduleUpdatedEvent = {
	type: 'schedule:updated'
	job: ScheduledJob<unknown>
}

export type ScheduledJobOptions = {
	limit?: number
	timezone?: string
}

export type ScheduledJobContext = null
export type ScheduleHandler<P, R> = (
	job: ScheduledJob<P>,
	context: ScheduledJobContext
) => Promise<R>

export const newSchedule = <P>(
	name: string,
	schedule: Schedule,
	payload: P,
	options?: ScheduledJobOptions
): ScheduledJob<P> => {
	const job: ScheduledJob<P> = {
		id: ids.uuid(),
		type: name,
		state: 'WAITING',
		payload: payload,
		schedule: schedule,
		timezone: options?.timezone,
		nextRun: nextRun(schedule, { tz: options?.timezone }),
		created: new Date(),
	}
	validate.isFuture(job)
	return job
}

export const runPerformed = <P, R>(
	job: ScheduledJob<P>,
	result: R
): [ScheduledJob<P>, CompletedScheduledJob<R>] => {
	if (job.state !== 'RUNNING') {
		throw new Error(
			`Cannot update schedule in non RUNNING state(${job.state}): ${job.id}`
		)
	}
	const scheduleAt = nextRun(job.schedule, { tz: job.timezone })
	const isComplete = !isFuture(scheduleAt)

	const updatedJob: ScheduledJob<P> = {
		...job,
		updated: new Date(),
		started: undefined,
		state: isComplete ? 'COMPLETED' : 'WAITING',
		nextRun: isComplete ? undefined : scheduleAt,
	}
	const run = jobCompleted(job, result)

	return [updatedJob, run]
}
export const runFailed = <P>(
	job: ScheduledJob<P>,
	error: Error
): [ScheduledJob<P>, FailedScheduledJob] => {
	if (job.state !== 'RUNNING') {
		throw new Error(
			`Cannot update schedule in non RUNNING state(${job.state}): ${job.id}`
		)
	}

	const scheduleAt = nextRun(job.schedule, { tz: job.timezone })
	const isComplete = !isFuture(scheduleAt)

	const updatedJob: ScheduledJob<P> = {
		...job,
		updated: new Date(),
		started: undefined,
		state: isComplete ? 'FAILED' : 'WAITING',
		nextRun: isComplete ? undefined : scheduleAt,
	}
	const run = jobFailed(job, error)

	return [updatedJob, run]
}

const jobCompleted = <P, R>(
	job: ScheduledJob<P>,
	result: R
): CompletedScheduledJob<R> => ({
	id: ids.uuid(),
	scheduleId: job.id,
	state: 'COMPLETED',
	type: job.type,
	ranAt: new Date(),
	result: result,
})

const jobFailed = <P>(
	job: ScheduledJob<P>,
	error: Error
): FailedScheduledJob => ({
	id: ids.uuid(),
	scheduleId: job.id,
	state: 'FAILED',
	type: job.type,
	ranAt: new Date(),
	error: error,
})

const validate = {
	isFuture: <P>(job: ScheduledJob<P>): void => {
		if (!isFuture(job.schedule)) {
			throw new Error(`Schedule is in the past: ${job.schedule}`)
		}
	},
}
