import { Duration } from './duration.js'

export type UUID = string
export type JobId = UUID

export type JobInput<P> = {
	id: JobId
	type: string
	payload: P
}
export type JobContext<_P> = {
	postpone: (delay: Duration) => Promise<void>
}
export type JobHandler<P, R> = (
	job: JobInput<P>,
	context: JobContext<P>
) => Promise<R>

export type DBConfig = {
	schema: string
}
