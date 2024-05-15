// import { first } from 'lodash'
// import { RetryPolicy } from '~/common/retry'
// import { QueueConfigRow, QueueItemRow, createPagedFetcher } from '../db'
// import { AnyQueueItem, QueueConfig, QueueItem, QueueItemState } from './models'
// import { firstRequired, sql } from '~/common/sql'

// export type Queries = ReturnType<typeof queries>
// export const queries = (schema: string) =>
// 	({
// 		fetchConfig: (queue: string) => sql(schema, deserializeConfig, first)`
// 			SELECT * FROM {{schema}}.queue_config
// 			WHERE queue = ${queue}
// 		`,
// 		saveConfig: (config: QueueConfig) => sql(
// 			schema,
// 			deserializeConfig,
// 			firstRequired
// 		)`
// 			INSERT INTO {{schema}}.queue_config
// 			(tenant_id, queue, paused, retry_policy)
// 			VALUES (${config.tenantId}, ${config.id}, ${config.paused}, ${config.retryPolicy})
// 			ON CONFLICT (tenant_id, queue) DO UPDATE
// 			SET paused=${config.paused}, retry_policy=${config.retryPolicy}, updated=now(), version=queue_config.version+1
// 			RETURNING *
// 		`,
// 		fetchQueues: () => sql(schema, deserializeConfig)`
// 			WITH q AS (
// 				SELECT DISTINCT tenant_id, queue FROM {{schema}}.queue
// 				UNION
// 				SELECT DISTINCT tenant_id, queue FROM {{schema}}.queue_history
// 			)
// 			SELECT q.tenant_id, q.queue, COALESCE(c.paused, FALSE) as paused
// 			FROM q FULL JOIN {{schema}}.queue_config c
// 			ON q.tenant_id=c.tenant_id AND q.queue=c.queue
// 			ORDER BY q.tenant_id, q.queue
// 		`,
// 		fetchQueue: (name: string) => sql(schema, deserializeConfig, first)`
// 			WITH q AS (
// 				SELECT DISTINCT tenant_id, queue FROM {{schema}}.queue
// 				UNION
// 				SELECT DISTINCT tenant_id, queue FROM {{schema}}.queue_history
// 			)
// 			SELECT q.tenant_id, q.queue, COALESCE(c.paused, FALSE) as paused
// 			FROM q LEFT JOIN {{schema}}.queue_config c
// 			ON q.tenant_id=c.tenant_id AND q.queue=c.queue
// 			WHERE q.queue = ${name}
// 		`,
// 		fetchAndLockDueItems: (limit: number = 100) => sql(schema, deserializeItem)`
// 			SELECT *
// 			FROM {{schema}}.QUEUE
// 			WHERE state='PENDING' or state='RETRY' AND (run_after IS NULL OR run_after <= now())
// 			ORDER BY created, id ASC
// 			LIMIT ${limit} FOR UPDATE SKIP LOCKED
// 		`,
// 		markAs: (state: QueueItemState, items: AnyQueueItem['id'][]) => sql(schema)`
// 			UPDATE {{schema}}.queue
// 			SET state=$${state}, version=version+1, updated=now()
// 			WHERE id = ANY(${items})
// 		`,
// 		fetchItem: (id: AnyQueueItem['id']) => sql(schema, deserializeItem, first)`
// 			SELECT * FROM {{schema}}.queue WHERE id = ${id}
// 		`,
// 		deleteItem: (id: AnyQueueItem['id']) => sql(
// 			schema,
// 			deserializeItem,
// 			firstRequired
// 		)`
// 			DELETE FROM {{schema}}.queue WHERE id = ${id} RETURNING *
// 		`,
// 		insert: <T>(item: QueueItem<T>) => sql(
// 			schema,
// 			deserializeItem<T>,
// 			firstRequired
// 		)`
// 			INSERT INTO {{schema}}.queue (
// 				id, tenant_id, key, type, queue, created, state, delay, run_after, payload, payload_type, target, result, result_type, error, worker_data
// 			) values (
// 				${item.id},
// 				${item.tenantId},
// 				${item.key},
// 				${item.type},
// 				${item.queue},
// 				${item.created},
// 				${item.state},
// 				${item.delay},
// 				${item.runAfter},
// 				${item.payload},
// 				${item.payloadType},
// 				${item.target},
// 				${item.result},
// 				${item.resultType},
// 				${item.error},
// 				${item.workerData}
// 			) RETURNING *
// 		`,
// 		updateItem: <T>(item: QueueItem<T>) => sql(
// 			schema,
// 			deserializeItem<T>,
// 			firstRequired
// 		)`
// 			UPDATE {{schema}}.queue SET
// 			version = version + 1,
// 				updated = now(),
// 				payload = ${item.payload},
// 				payload_type = ${item.payloadType},
// 				state = ${item.state},
// 				key = ${item.key},
// 				delay = ${item.delay},
// 				target = ${item.target},
// 				type = ${item.type},
// 				tries = ${item.tries},
// 				run_after = ${item.runAfter},
// 				result = ${item.result},
// 				result_type = ${item.resultType},
// 				error = ${item.error},
// 				worker_data	= ${item.workerData},
// 				retry_policy = ${item.retryPolicy}
// 			WHERE
// 				id = ${item.id} AND version = ${item.version}
// 		`,
// 		fetchItems: createPagedFetcher(`${schema}.queue`, 'queue=$1'),
// 	}) as const

// const deserializeItem = <T>(row: QueueItemRow): QueueItem<T> => {
// 	return {
// 		id: row.id,
// 		tenantId: row.tenant_id,
// 		key: row.key,
// 		type: row.type,
// 		version: row.version,
// 		tries: row.tries,
// 		queue: row.queue,
// 		created: row.created,
// 		updated: row.updated,
// 		started: row.started,
// 		state: row.state as QueueItemState,
// 		delay: row.delay,
// 		payload: row.payload,
// 		payloadType: row.payload_type,
// 		target: row.target as T,
// 		runAfter: row.run_after,
// 		retryPolicy: row.retry_policy as RetryPolicy,
// 		result: row.result,
// 		resultType: row.result_type,
// 		error: row.error,
// 		workerData: row.worker_data,
// 	}
// }

// const deserializeConfig = (row: QueueConfigRow): QueueConfig => ({
// 	id: row.queue,
// 	tenantId: row.tenant_id,
// 	paused: row.paused || false,
// 	retryPolicy: row.retryPolicy as RetryPolicy,
// })
