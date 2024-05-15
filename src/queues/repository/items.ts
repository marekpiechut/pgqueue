import pg from 'pg'
import { PagedResult, UUID } from '~/common/models'
import { Managed, MaybeManaged, Repository, SortOrder } from '~/common/psql'
import { RetryPolicy } from '~/common/retry'
import { PagedFetcher, createPagedFetcher } from '~/db/paged'
import { QueueConfigRow, QueueItemRow } from '~/db/schema'
import { AnyQueueItem, QueueConfig, QueueItem, QueueItemState } from '../models'

export class QueueRepository extends Repository {
	private fetchPage: PagedFetcher<QueueItemRow>
	constructor(
		private schema: string,
		pool: pg.Pool,
		fetchPage?: PagedFetcher<QueueItemRow>
	) {
		super(pool)
		this.fetchPage =
			fetchPage ||
			createPagedFetcher<QueueItemRow>(`${schema}.queue`, 'queue=$1')
	}

	protected clone(): this {
		return new QueueRepository(this.schema, this.pgPool, this.fetchPage) as this
	}

	public async getConfig(queue: string): Promise<QueueConfig | null> {
		const res = await this.execute<QueueConfigRow>(
			`
			SELECT * FROM ${this.schema}.queue_config
			WHERE queue = $1
		`,
			queue
		)
		const row = res.rows[0]
		if (row) {
			return deserializeConfig(row)
		} else {
			return null
		}
	}

	public async saveConfig(config: QueueConfig): Promise<QueueConfig> {
		const res = await this.execute<QueueConfigRow>(
			`
			INSERT INTO ${this.schema}.queue_config
			(tenant_id, queue, paused, retry_policy)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT (tenant_id, queue) DO UPDATE
			SET paused=$3, retry_policy=$4 updated=now(), version=queue_config.version+1
			RETURNING *
		`,
			config.tenantId,
			config.id,
			config.paused,
			config.retryPolicy
		)
		if (res.rowCount !== 1) {
			throw new Error('Failed to save config')
		}
		return deserializeConfig(res.rows[0])
	}

	async fetchQueues(): Promise<QueueConfig[]> {
		const { schema } = this
		const res = await this.execute<QueueConfigRow>(`
			WITH q AS (
				SELECT DISTINCT tenant_id, queue FROM ${schema}.queue 
				UNION 
				SELECT DISTINCT tenant_id, queue FROM ${schema}.queue_history
			)
			SELECT q.tenant_id, q.queue, COALESCE(c.paused, FALSE) as paused
			FROM q FULL JOIN ${schema}.queue_config c
			ON q.tenant_id=c.tenant_id AND q.queue=c.queue
			ORDER BY q.tenant_id, q.queue
		`)
		return res.rows.map(deserializeConfig)
	}

	async fetchQueue(name: string): Promise<QueueConfig | null> {
		const { schema } = this
		const res = await this.execute<QueueConfigRow>(
			`
			WITH q AS (
				SELECT DISTINCT tenant_id, queue FROM ${schema}.queue 
				UNION 
				SELECT DISTINCT tenant_id, queue FROM ${schema}.queue_history
			)
			SELECT q.tenant_id, q.queue, COALESCE(c.paused, FALSE) as paused
			FROM q LEFT JOIN ${schema}.queue_config c
			ON q.tenant_id=c.tenant_id AND q.queue=c.queue
			WHERE q.queue = $1
		`,
			name
		)
		if (res.rows[0]) {
			return deserializeConfig(res.rows[0])
		} else {
			return null
		}
	}

	async fetchAndLockDueItems(limit: number = 100): Promise<AnyQueueItem[]> {
		const res = await this.execute<QueueItemRow>(
			`
				SELECT *
				FROM ${this.schema}.QUEUE
				WHERE state='PENDING' or state='RETRY' AND (run_after IS NULL OR run_after <= now())
				ORDER BY created, id ASC
				LIMIT $1 FOR UPDATE SKIP LOCKED
			`,
			limit
		)

		return res.rows.map(deserializeItem)
	}

	async markAs(state: QueueItemState, ids: UUID[]): Promise<void> {
		await this.execute(
			`
			UPDATE ${this.schema}.queue
			SET state=$2, version=version+1, updated=now()
			WHERE id = ANY($1)
		`,
			ids,
			state
		)
	}

	async fetchItem(id: UUID): Promise<AnyQueueItem | null> {
		const res = await this.execute<QueueItemRow>(
			`SELECT * FROM ${this.schema}.queue WHERE id = $1`,
			id
		)

		return res.rows[0] ? deserializeItem(res.rows[0]) : null
	}

	async delete(
		idOrItem: AnyQueueItem['id'] | AnyQueueItem
	): Promise<AnyQueueItem> {
		const id = typeof idOrItem === 'string' ? idOrItem : idOrItem.id
		const res = await this.execute(
			`DELETE FROM ${this.schema}.queue WHERE id = $1 RETURNING *`,
			id
		)
		if (res.rowCount !== 1) {
			throw new Error(`Item not found: ${id}`)
		}

		return deserializeItem(res.rows[0])
	}

	async save<T>(item: QueueItem<T>): Promise<QueueItem<T>> {
		if ((item as MaybeManaged).__managed) {
			return this.update(item)
		} else {
			return this.insert(item)
		}
	}

	async insert<T>(item: QueueItem<T>): Promise<QueueItem<T>> {
		const res = await this.execute<QueueItemRow>(
			`
			insert into ${this.schema}.queue (
				id, tenant_id, key, type, queue, created, state, delay, run_after, payload, payload_type, target, result, result_type, error, worker_data, retry_policy
			) values (
				$1, $2, $3, $4, $5, now(), $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16
			) returning *
		`,
			item.id,
			item.tenantId,
			item.key,
			item.type,
			item.queue,
			item.state,
			item.delay,
			item.runAfter,
			item.payload,
			item.payloadType,
			item.target,
			item.result,
			item.resultType,
			item.error,
			item.workerData,
			item.retryPolicy
		)

		return deserializeItem(res.rows[0])
	}

	async update<T>(item: QueueItem<T>): Promise<QueueItem<T>> {
		const res = await this.execute<QueueItemRow>(
			`
			UPDATE ${this.schema}.queue
			set
				version = version + 1,
				updated = now(), 
				payload = $3,
				payload_type = $4,
				state = $5,
				key = $6,
				delay = $7,
				target = $8,
				type = $9,
				tries = $10,
				run_after = $11,
				result = $12,
				result_type = $13,
				error = $14,
				worker_data	= $15,
				retry_policy = $16
			WHERE
				id = $1 AND version = $2
			RETURNING *
		`,
			item.id,
			item.version,
			item.payload,
			item.payloadType,
			item.state,
			item.key,
			item.delay,
			item.target,
			item.type,
			item.tries,
			item.runAfter,
			item.result,
			item.resultType,
			item.error,
			item.workerData,
			item.retryPolicy
		)

		if (res.rowCount !== 1) {
			throw new Error(
				`Item not found or version mismatch: ${item.id} (v:${item.version})`
			)
		}

		return deserializeItem(res.rows[0])
	}

	async fetchItems(
		queue: string,
		limit: number = 100,
		after?: UUID | null | undefined | 'FIRST',
		before?: UUID | null | undefined | 'LAST',
		sort?: SortOrder
	): Promise<PagedResult<AnyQueueItem>> {
		return this.execute(async client => {
			const page = await this.fetchPage(
				client,
				[queue],
				limit,
				after,
				before,
				sort
			)

			return {
				...page,
				items: page.items.map(deserializeItem),
			}
		})
	}
}

const deserializeItem = <T>(
	row: QueueItemRow
): QueueItem<T> & { __managed: true } => {
	return {
		__managed: true,
		id: row.id,
		tenantId: row.tenant_id,
		key: row.key,
		type: row.type,
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

const deserializeConfig = (row: QueueConfigRow): QueueConfig & Managed => ({
	__managed: true,
	id: row.queue,
	tenantId: row.tenant_id,
	paused: row.paused || false,
	retryPolicy: row.retryPolicy as RetryPolicy,
})
