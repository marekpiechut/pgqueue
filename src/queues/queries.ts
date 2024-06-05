import { first } from 'lodash'
import { Duration, toSeconds } from '~/common/duration'
import { AnyObject } from '~/common/models'
import { RetryPolicy } from '~/common/retry'
import { firstRequired, noopMapper, returnNothing, sql } from '~/common/sql'
import {
	QueueConfigRow,
	QueueHistoryRow,
	QueueItemRow,
	WorkItemRow,
	WorkerMetadataRow,
	createPagedFetcher,
} from '../db'
import {
	AnyHistory,
	AnyQueueItem,
	NewWorkItem,
	QueueConfig,
	QueueHistory,
	QueueHistoryState,
	QueueItem,
	QueueItemState,
	WorkItem,
} from './models'

export type Queries = ReturnType<typeof withSchema>
export const withSchema = (schema: string) =>
	({
		fetchConfig: (queue: string) => sql(schema, rowToConfig, first)`
			SELECT * FROM {{schema}}.queue_config
			WHERE queue = ${queue}
		`,
		saveConfig: (config: QueueConfig) => sql(
			schema,
			rowToConfig,
			firstRequired
		)`
			INSERT INTO {{schema}}.queue_config
			(tenant_id, queue, paused, retry_policy)
			VALUES (${config.tenantId}, ${config.id}, ${config.paused}, ${config.retryPolicy})
			ON CONFLICT (tenant_id, queue) DO UPDATE
			SET paused=${config.paused}, retry_policy=${config.retryPolicy}, updated=now(), version=queue_config.version+1
			RETURNING *
		`,
		fetchQueues: () => sql(schema, rowToConfig)`
			WITH q AS (
				SELECT DISTINCT tenant_id, queue FROM {{schema}}.queue
				UNION
				SELECT DISTINCT tenant_id, queue FROM {{schema}}.queue_history
			)
			SELECT q.tenant_id, q.queue, COALESCE(c.paused, FALSE) as paused
			FROM q FULL JOIN {{schema}}.queue_config c
			ON q.tenant_id=c.tenant_id AND q.queue=c.queue
			ORDER BY q.tenant_id, q.queue
		`,
		fetchQueue: (name: string) => sql(schema, rowToConfig, first)`
			WITH q AS (
				SELECT DISTINCT tenant_id, queue FROM {{schema}}.queue
				UNION
				SELECT DISTINCT tenant_id, queue FROM {{schema}}.queue_history
			)
			SELECT q.tenant_id, q.queue, COALESCE(c.paused, FALSE) as paused
			FROM q LEFT JOIN {{schema}}.queue_config c
			ON q.tenant_id=c.tenant_id AND q.queue=c.queue
			WHERE q.queue = ${name}
		`,
		fetchAndLockDueItems: (limit: number = 100) => sql(schema, rowToItem)`
			SELECT *
			FROM {{schema}}.QUEUE
			WHERE (state='PENDING' OR state='RETRY') AND (run_after IS NULL OR run_after <= now())
			ORDER BY created, id ASC
			LIMIT ${limit} FOR UPDATE SKIP LOCKED
		`,
		markAs: (state: QueueItemState, items: AnyQueueItem['id'][]) => sql(schema)`
			UPDATE {{schema}}.queue
			SET state=${state}, version=version+1, updated=now()
			WHERE id = ANY(${items})
		`,
		fetchItem: (id: AnyQueueItem['id']) => sql(schema, rowToItem, first)`
			SELECT * FROM {{schema}}.queue WHERE id = ${id} 
		`,
		fetchItemByKey: (
			queue: AnyQueueItem['queue'],
			key: AnyQueueItem['key']
		) => sql(schema, rowToItem, first)`
			SELECT * FROM {{schema}}.queue WHERE queue = ${queue} AND key = ${key}
		`,
		deleteItem: (id: AnyQueueItem['id']) => sql(schema, rowToItem, first)`
			DELETE FROM {{schema}}.queue WHERE id = ${id} RETURNING *
		`,
		deleteItemByKey: (
			queue: AnyQueueItem['queue'],
			key: AnyQueueItem['key']
		) => sql(schema, rowToItem, first)`
			DELETE FROM {{schema}}.queue WHERE queue = ${queue} AND key = ${key} RETURNING *
		`,
		insert: <T>(item: QueueItem<T>) => sql(schema, rowToItem<T>, firstRequired)`
			INSERT INTO {{schema}}.queue (
				id, tenant_id, key, type, schedule_id, queue, created, state, delay, run_after, payload, payload_type, target, result, result_type, error, worker_data
			) values (
				${item.id},
				${item.tenantId},
				${item.key},
				${item.type},
				${item.scheduleId},
				${item.queue},
				${item.created},
				${item.state},
				${item.delay},
				${item.runAfter},
				${item.payload},
				${item.payloadType},
				${item.target},
				${item.result},
				${item.resultType},
				${item.error},
				${item.workerData}
			) 
			ON CONFLICT (tenant_id, queue, key) DO UPDATE SET
				version=queue.version+1,
				updated = now(),
				created = ${item.created},
				payload = ${item.payload},
				payload_type = ${item.payloadType},
				key = ${item.key},
				delay = ${item.delay},
				target = ${item.target},
				type = ${item.type},
				state = ${item.state},
				tries = 0,
				run_after = ${item.runAfter},
				retry_policy = ${item.retryPolicy}	
			RETURNING *
		`,
		updateItem: <T>(item: QueueItem<T>) => sql(
			schema,
			rowToItem<T>,
			firstRequired
		)`
			UPDATE {{schema}}.queue SET
			version = version + 1,
				updated = now(),
				payload = ${item.payload},
				payload_type = ${item.payloadType},
				state = ${item.state},
				key = ${item.key},
				delay = ${item.delay},
				target = ${item.target},
				type = ${item.type},
				tries = ${item.tries},
				run_after = ${item.runAfter},
				result = ${item.result},
				result_type = ${item.resultType},
				error = ${item.error},
				worker_data	= ${item.workerData},
				retry_policy = ${item.retryPolicy}
			WHERE
				id = ${item.id} AND version = ${item.version}
			RETURNING *
		`,
		fetchItemsPage: createPagedFetcher(
			`${schema}.queue`,
			'queue=$1',
			rowToItem
		),
		fetchScheduleRunsPage: createPagedFetcher(
			`${schema}.queue_history`,
			'schedule_id=$1',
			rowToHistory
		),
		fetchHistory: <T, R>(id: AnyHistory['id']) => sql(
			schema,
			rowToHistory<T, R>,
			first
		)`
			SELECT * FROM {{schema}}.queue_history WHERE id = ${id} 
		`,
		fetchHistoryByKey: <T, R>(
			queue: AnyHistory['key'],
			key: AnyHistory['key']
		) => sql(schema, rowToHistory<T, R>, first)`
			SELECT * FROM {{schema}}.queue_history WHERE queue = ${queue} AND key = ${key}
		`,
		fetchHistoryPage: createPagedFetcher(
			`${schema}.queue_history`,
			'queue=$1',
			rowToHistory
		),
		insertHistory: <T, R>(history: QueueHistory<T, R>) => sql(
			schema,
			rowToHistory,
			firstRequired
		)`
		INSERT INTO {{schema}}.queue_history (
			id,
			tenant_id,
			key,
			type,
			schedule_id,
			queue,
			created,
			scheduled,
			started,
			state,
			delay,
			tries,
			payload,
			payload_type,
			result,
			result_type,
			target,
			worker_data,
			error
		) VALUES (
			${history.id},
			${history.tenantId},
			${history.key},
			${history.type},
			${history.scheduleId},
			${history.queue},
			${history.created},
			${history.scheduled},
			${history.started},
			${history.state},
			${history.delay},
			${history.tries},
			${history.payload},
			${history.payloadType},
			${history.result},
			${history.resultType},
			${history.target},
			${history.workerData},
			${history.error}
		) RETURNING *
		`,
		pollWorkQueue: (
			nodeId: string,
			batchSize: number,
			lockTimeout: Duration
		) => sql(schema, rowToWorkItem)`
		WITH next AS (
				SELECT *
				FROM {{schema}}.WORK_QUEUE
				WHERE lock_key IS NULL OR lock_timeout < now()
				ORDER BY created, batch_order ASC
				LIMIT ${batchSize} FOR UPDATE SKIP LOCKED
		)
		UPDATE {{schema}}.WORK_QUEUE as updated
		SET lock_key = ${nodeId},
				started = now(),
				version = updated.version + 1,
				lock_timeout = (now() + (${toSeconds(lockTimeout)} || ' seconds')::INTERVAL)
		FROM next
		WHERE updated.id = next.id
		RETURNING updated.*
		`,
		updateWorkQueue: (item: WorkItem) => sql(
			schema,
			rowToWorkItem,
			firstRequired
		)`
		UPDATE {{schema}}.WORK_QUEUE
			SET
				updated = now(),
				started = ${item.started},
				lock_timeout = ${item.lockTimeout},
				version = version + 1
			WHERE id = ${item.id} AND version = ${item.version}
			RETURNING *
		`,
		insertWorkItems: (items: NewWorkItem[]) => sql(
			schema,
			noopMapper,
			returnNothing
		)`
			INSERT INTO {{schema}}.WORK_QUEUE (id, tenant_id, batch_order)
			SELECT * FROM UNNEST(
				${items.map(i => i.id)}::uuid[],
			 	${items.map(i => i.tenantId)}::varchar[],
				${items.map(i => i.batchOrder)}::integer[]
			) ON CONFLICT DO NOTHING`,
		deleteWorkItem: (id: WorkItem['id']) => sql(schema, rowToWorkItem, first)`
			DELETE FROM {{schema}}.WORK_QUEUE WHERE id = ${id} RETURNING *
		`,
		unlockAllWorkItems: (nodeId: string) => sql(schema)`
			UPDATE {{schema}}.WORK_QUEUE
			SET lock_key = NULL,
					lock_timeout = NULL,
					started = NULL
			WHERE lock_key = ${nodeId}
		`,
		setMetadata: <T extends AnyObject>(key: string, value: T) => sql(
			schema,
			rowToWorkerMetadata<T>,
			firstRequired
		)`
			INSERT INTO {{schema}}.worker_metadata (key, value)
			VALUES (${key}, ${value})
			ON CONFLICT (tenant_id, key) DO UPDATE
			SET value=${value}, updated=now(), version=worker_metadata.version+1
			RETURNING *
		`,
		getMetadata: <T extends AnyObject>(key: string) => sql(
			schema,
			rowToWorkerMetadata<T>,
			first
		)`
			SELECT * FROM {{schema}}.worker_metadata WHERE key = ${key}
		`,
		deleteMetadata: <T extends AnyObject>(key: string) => sql(
			schema,
			rowToWorkerMetadata<T>,
			first
		)`
			DELETE FROM {{schema}}.worker_metadata WHERE key = ${key} RETURNING *
		`,
	}) as const

const rowToItem = <T>(row: QueueItemRow): QueueItem<T> => {
	return {
		id: row.id,
		tenantId: row.tenant_id,
		key: row.key,
		type: row.type,
		scheduleId: row.schedule_id,
		version: row.version,
		tries: row.tries,
		queue: row.queue,
		created: row.created,
		updated: row.updated,
		started: row.started,
		state: row.state as QueueItemState,
		delay: row.delay,
		payload: row.payload,
		payloadType: row.payload_type,
		target: row.target as T,
		runAfter: row.run_after,
		retryPolicy: row.retry_policy as RetryPolicy,
		result: row.result,
		resultType: row.result_type,
		error: row.error,
		workerData: row.worker_data,
	}
}

const rowToConfig = (row: QueueConfigRow): QueueConfig => ({
	id: row.queue,
	tenantId: row.tenant_id,
	paused: row.paused || false,
	retryPolicy: row.retryPolicy as RetryPolicy,
})

const rowToHistory = <T, R>(row: QueueHistoryRow): QueueHistory<T, R> => ({
	id: row.id,
	tenantId: row.tenant_id,
	key: row.key,
	type: row.type,
	scheduleId: row.schedule_id,
	started: row.started,
	queue: row.queue,
	created: row.created,
	scheduled: row.scheduled,
	state: row.state as QueueHistoryState,
	delay: row.delay,
	tries: row.tries,
	payload: row.payload,
	payloadType: row.payload_type,
	result: row.result,
	resultType: row.result_type,
	target: row.target as T,
	workerData: row.worker_data as R,
	error: row.error,
})

const rowToWorkItem = (row: WorkItemRow): WorkItem => ({
	id: row.id,
	version: row.version,
	tenantId: row.tenant_id,
	created: row.created,
	batchOrder: row.batch_order,
	updated: row.updated,
	started: row.started,
	lockKey: row.lock_key,
	lockTimeout: row.lock_timeout,
})

const rowToWorkerMetadata = <T>(row: WorkerMetadataRow): T => row.value as T
