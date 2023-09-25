import { v4 as uuid } from 'uuid'
export type UUID = string
export type JobId = UUID

export type JOB_STATE = 'PENDING' | 'RUNNING' | 'FAILED' | 'COMPLETED'
export type Job<P> = {
	id: JobId
	type: string
	state: JOB_STATE
	payload: P
	created: Date
	updated?: Date
}
export type JobOptions = {
	retries?: number
}

export const newJob = <P>(
	name: string,
	payload: P,
	_options?: JobOptions
): Job<P> => ({
	id: uuid(),
	type: name,
	state: 'PENDING',
	payload: payload,
	created: new Date(),
})
