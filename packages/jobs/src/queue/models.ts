import { Duration } from 'schedule/duration.js'
import { ids } from '@pgqueue/core'

export type JobId = string

export type JOB_STATE = Job<unknown, unknown>['state']
export type JobBase<P> = {
	id: JobId
	type: string
	payload: P
	priority?: number
	created: Date
}
export type PendingJob<P> = JobBase<P> & {
	state: 'PENDING'
}
export type RunningJob<P> = JobBase<P> & {
	state: 'RUNNING'
	updated: Date
}
export type CompletedJob<P, R> = JobBase<P> & {
	state: 'COMPLETED'
	result: R
	updated: Date
}
export type FailedJob<P> = JobBase<P> & {
	state: 'FAILED'
	error: Error
	updated: Date
}
export type ActiveJob<P> = PendingJob<P> | RunningJob<P> | FailedJob<P>
export type ArchivalJob<P, R> = CompletedJob<P, R> | FailedJob<P>
export type Job<P, R> = ActiveJob<P> | ArchivalJob<P, R>

export type JobWithOptions<P> = JobOptions & {
	type: string
	payload: P
}
export type JobOptions = {
	retries?: number
	priority?: number
	delay?: Duration
}

export type JobContext = {
	postpone: (delay: Duration) => Promise<void>
}
export type JobHandler<P, R> = (
	job: RunningJob<P>,
	context: JobContext
) => Promise<R>

export const newJob = <P>(
	type: string,
	payload: P,
	options?: JobOptions
): PendingJob<P> => ({
	id: ids.uuid(),
	type: type,
	state: 'PENDING',
	payload: payload,
	priority: options?.priority,
	created: new Date(),
})

export const startJob = <P>(job: PendingJob<P>): RunningJob<P> => {
	if (job.state !== 'PENDING') {
		throw new Error(`Cannot start job in ${job.state} state: ${job.id}`)
	}

	return {
		...job,
		state: 'RUNNING',
		updated: new Date(),
	}
}

export const completeJob = <P, R>(
	job: RunningJob<P>,
	result: R
): CompletedJob<P, R> => {
	if (job.state !== 'RUNNING') {
		throw new Error(`Cannot complete job in ${job.state} state: ${job.id}`)
	}
	return {
		...job,
		state: 'COMPLETED',
		result: result,
		updated: new Date(),
	}
}

export const failJob = <P>(job: RunningJob<P>, error: Error): FailedJob<P> => {
	if (job.state !== 'RUNNING') {
		throw new Error(`Cannot fail job in ${job.state} state: ${job.id}`)
	}
	return {
		...job,
		state: 'FAILED',
		error: error,
		updated: new Date(),
	}
}
