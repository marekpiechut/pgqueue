import { truncate } from 'lodash'
import { uuidv7 } from 'uuidv7'
import { minutes, seconds } from '~/common/duration'
import { MAX_ERROR_LEN, MimeType, TenantId, UUID } from '~/common/models'
import { RetryPolicy } from '~/common/retry'

export const DEFAULT_QUEUE_CONFIG = {
	retryPolicy: {
		type: 'exponential',
		delay: seconds(10),
		max: minutes(15),
		tries: 5,
	},
} as const

export type QueueConfig = {
	id: string
	displayName?: string
	tenantId: TenantId
	paused: boolean
	retryPolicy?: RetryPolicy
}

export type QueueItemState =
	| 'PENDING'
	| 'RUNNING'
	| 'RETRY'
	| 'FAILED'
	| 'COMPLETED'
export type AnyQueueItem = QueueItem<unknown>
export type QueueItem<T, R = unknown> = {
	id: UUID
	tenantId: TenantId
	key?: string
	type: string
	version: number
	tries: number
	queue: string
	created: Date
	updated?: Date
	started?: Date
	state: QueueItemState
	delay?: number
	runAfter?: Date
	retryPolicy?: RetryPolicy
	payloadType?: MimeType
	payload?: Buffer
	target?: T
	//These are here to store last error data for debugging on retry
	//They will be overwritten with actual results on final failure or completion
	error?: string
	result?: Buffer
	resultType?: MimeType
	workerData?: R
	//
}

export type NewQueueItem<T> = {
	key?: string
	type: string
	queue: string
	scheduleId?: UUID
	delay?: number
	payload?: Buffer
	payloadType?: MimeType
	retryPolicy?: RetryPolicy
	target?: T
}

export const newItem = <T>(
	tenant: TenantId,
	config: NewQueueItem<T>
): QueueItem<T> => ({
	...config,
	id: uuidv7(),
	tenantId: tenant,
	created: new Date(),
	state: 'PENDING',
	tries: 0,
	version: 0,
})

export const itemRunFailed = <T, R>(
	item: QueueItem<T>,
	nextRun: Date,
	result?: WorkResult<R> | null | undefined,
	error?: string
): QueueItem<T, R> => ({
	...item,
	state: 'RETRY',
	tries: item.tries + 1,
	started: undefined,
	updated: new Date(),
	resultType: result?.payloadType,
	result: result?.payload,
	error: truncate(error, { length: MAX_ERROR_LEN }),
	workerData: result?.data,
	runAfter: nextRun,
})

export const itemCompleted = <T, R>(
	item: QueueItem<T>,
	result: WorkResult<R>
): QueueHistory<T, R> => ({
	id: item.id,
	queue: item.queue,
	tenantId: item.tenantId,
	key: item.key,
	type: item.type,
	created: new Date(),
	started: item.started || new Date(),
	scheduled: item.created,
	state: 'COMPLETED',
	tries: item.tries + 1,
	target: item.target,
	payloadType: item.payloadType,
	payload: item.payload,
	resultType: result.payloadType,
	result: result.payload,
	workerData: result.data,
})

export const itemFailed = <T, R>(
	item: QueueItem<T>,
	result: WorkResult<R> | null | undefined,
	error: string
): QueueHistory<T, R> => ({
	id: item.id,
	queue: item.queue,
	tenantId: item.tenantId,
	key: item.key,
	type: item.type,
	created: new Date(),
	started: item.started || new Date(),
	scheduled: item.created,
	state: 'FAILED',
	tries: item.tries,
	target: item.target,
	payloadType: item.payloadType,
	payload: item.payload,
	resultType: result?.payloadType,
	result: result?.payload,
	workerData: result?.data,
	error: truncate(error, { length: MAX_ERROR_LEN }),
})

export const newConfig = (tenantId: TenantId, queue: string): QueueConfig => ({
	id: queue,
	tenantId: tenantId,
	paused: false,
})

export const updateConfig = (
	config: QueueConfig,
	options: Pick<QueueConfig, 'displayName' | 'paused'>
): QueueConfig => ({
	...config,
	paused: options.paused ?? config.paused,
	displayName:
		options.displayName === undefined
			? config.displayName
			: options.displayName,
})

export type QueueHistoryState = 'COMPLETED' | 'FAILED'
export type AnyHistory = QueueHistory<unknown, unknown>
export type QueueHistory<T, R> = {
	id: UUID
	queue: string
	tenantId: UUID
	key?: string
	type: string
	created: Date
	scheduled: Date
	started: Date
	state: QueueHistoryState
	delay?: number
	tries: number
	payload?: Buffer
	payloadType?: MimeType
	target?: T
	result?: Buffer
	resultType?: MimeType
	workerData?: R
	error?: string
}

export type NewWorkItem = {
	id: UUID
	tenantId: TenantId
	batchOrder: number
}
export type WorkItem = NewWorkItem & {
	created: Date
	updated?: Date
	version: number
	started?: Date
	lockKey?: string
	lockTimeout?: Date
}

export type AnyWorkResult = WorkResult<unknown>
export type WorkResult<R> = {
	data: R
	payloadType?: MimeType
	payload?: Buffer
}

export class HandlerError extends Error {
	result?: AnyWorkResult
	constructor(message: string, result?: AnyWorkResult) {
		super(message)
		this.result = result
	}
}

export const queueForWork = (
	item: AnyQueueItem,
	batchOrder: number
): NewWorkItem => ({
	id: item.id,
	tenantId: item.tenantId,
	batchOrder: batchOrder,
})
