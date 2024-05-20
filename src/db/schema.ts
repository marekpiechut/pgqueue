import * as evolutions from '@dayone-labs/evolutions'
import pg from 'pg'
import { logger } from '~/common/logger'

const log = logger('pgqueue:schema')

type UUID = string
type Json = unknown

export type QueueItemRow = {
	tenant_id: UUID
	id: UUID
	version: number
	created: Date
	updated?: Date
	started?: Date
	key?: string
	type: string
	schedule_id?: UUID
	tries: number
	queue: string
	state: string
	delay?: number
	run_after: Date
	payload?: Buffer
	payload_type?: string
	target?: Json
	retry_policy?: Json
	result?: Buffer
	result_type?: string
	error?: string
	worker_data?: Json
}

export type QueueConfigRow = {
	tenant_id: UUID
	queue: string
	created: Date
	updated?: Date
	version: number
	display_name?: string
	paused: boolean
	retryPolicy?: Json
}

export type QueueHistoryRow = {
	tenant_id: UUID
	id: UUID
	type: string
	key?: string
	queue: string
	schedule_id?: UUID
	created: Date
	scheduled: Date
	started: Date
	state: string
	delay?: number
	tries: number
	payload?: Buffer
	payload_type?: string
	result?: Buffer
	result_type?: string
	error?: string
	target?: Json
	worker_data?: Json
}

export type WorkItemRow = {
	id: UUID
	tenant_id: UUID
	version: number
	created: Date
	batch_order: number
	updated?: Date
	started?: Date
	lock_key?: string
	lock_timeout?: Date
}

export type ScheduleRow = {
	id: UUID
	tenant_id: UUID
	key?: string
	type: string
	queue: string
	paused?: boolean
	retry?: Json
	version: number
	tries: number
	created: Date
	updated?: Date
	next_run?: Date
	last_run?: Date
	schedule: string
	payload_type?: string
	payload?: Buffer
	target?: Json
	timezone: string
}

export const DEFAULT_SCHEMA = 'queues'

export const applyEvolutions = async (
	client: pg.ClientBase,
	config: evolutions.Config & { schema: string }
): Promise<void> => {
	const sqls = (await Promise.all([import('./v01.sql')])).map(m =>
		m.default(config.schema)
	)
	const evos = await evolutions.load(sqls, client, config)
	await evos.withLogger(log).apply()
}
