import { ids } from '@pgqueue/core'
import { Duration } from '../schedule/duration.js'

const randomNodeId = (): string => {
	return ids.uuid()
}

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

export type Config = {
	/**
	 * Postgres schema name for queue tables.
	 * Defaults to `pgqueue`
	 */
	schema?: string
	/**
	 * Id of cluster node. Used to mark tasks as locked by given instance.
	 * It will also be used when node is restarted to unlock all tasks.
	 *
	 * Make sure it's **unique and stays the same between restarts**.
	 *
	 * Without this, tasks will not be automatically unlocked on restart
	 * and will have to wait until lock timeout expires.
	 */
	nodeId?: string
}

export type AppliedConfig = Config & typeof DEFAULT_CONFIG

export const DEFAULT_SCHEMA = 'pgqueues'
export const DEFAULT_CONFIG = {
	schema: DEFAULT_SCHEMA,
	nodeId: randomNodeId(),
}
