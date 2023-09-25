import { v4 as uuid } from 'uuid'
export type UUID = string
export type JobId = UUID

export type JOB_STATE = Job<unknown, unknown>['state']
export type JobBase<P> = {
	id: JobId
	type: string
	payload: P
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
export type Job<P, R> = PendingJob<P> | RunningJob<P> | CompletedJob<P, R>

export type JobOptions = {
	retries?: number
}

export const newJob = <P>(
	name: string,
	payload: P,
	_options?: JobOptions
): PendingJob<P> => ({
	id: uuid(),
	type: name,
	state: 'PENDING',
	payload: payload,
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
