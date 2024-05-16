import pg from 'pg'
import logger from '~/common/logger'
import { PagedResult, TenantId, UUID } from '~/common/models'
import { SortOrder } from '~/common/psql'
import { nextRun } from '~/common/retry'
import { DB, DBConnectionSpec } from '~/common/sql'
import { DEFAULT_SCHEMA } from '~/db'
import {
	AnyHistory,
	AnyQueueItem,
	DEFAULT_QUEUE_CONFIG,
	NewQueueItem,
	QueueConfig,
	QueueItem,
	WorkResult,
	itemCompleted,
	itemFailed,
	itemRunFailed,
	newConfig,
	newItem,
	updateConfig,
} from './models'
import * as queries from './queries'

const log = logger('pgqueue:queues')

export type QueuesConfig = {
	schema?: string
}
const DEFAULT_CONFIG = {
	schema: DEFAULT_SCHEMA,
}

export interface QueueManager {
	fetchQueues(): Promise<QueueConfig[]>
	fetchQueue(name: string): Promise<QueueConfig | undefined>
	fetchItem(id: UUID): Promise<AnyQueueItem | AnyHistory | undefined>
	fetchItems(
		queue: string,
		limit?: number,
		after?: UUID | null | undefined,
		before?: UUID | null | undefined,
		order?: 'ASC' | 'DESC'
	): Promise<PagedResult<AnyQueueItem>>
	fetchHistory(
		queue: string,
		limit?: number,
		after?: UUID | null | undefined,
		before?: UUID | null | undefined,
		order?: 'ASC' | 'DESC'
	): Promise<PagedResult<AnyHistory>>
	delete(id: UUID): Promise<AnyQueueItem>
	withTenant(tenantId: TenantId): TenantQueueManager
	withTx(tx: pg.ClientBase | DB): this
}

export interface TenantQueueManager extends QueueManager {
	configure(
		queue: string,
		options: Partial<Pick<QueueConfig, 'displayName' | 'paused'>>
	): Promise<QueueConfig>
	push<T>(item: NewQueueItem<T>): Promise<QueueItem<T>>
	completed<T, R>(item: QueueItem<T>, result: WorkResult<R>): Promise<void>
	failed<T, R>(
		item: QueueItem<T>,
		result: WorkResult<R>,
		error: string
	): Promise<void>
}

export class Queues implements QueueManager, TenantQueueManager {
	private tenantId: TenantId | undefined

	private constructor(
		private db: DB,
		private config: QueuesConfig & typeof DEFAULT_CONFIG,
		private queries: queries.Queries
	) {}

	public static create(dbSpec: DBConnectionSpec, config: QueuesConfig): Queues {
		const db = DB.create(dbSpec)
		const mergedConfig = { ...DEFAULT_CONFIG, ...config }
		const sqls = queries.withSchema(mergedConfig.schema)
		return new Queues(db, mergedConfig, sqls)
	}

	public withTenant(tenantId: TenantId): TenantQueueManager {
		const copy = new Queues(
			this.db.withTenant(tenantId),
			this.config,
			this.queries
		)
		copy.tenantId = tenantId
		return copy
	}
	public withTx(tx: pg.ClientBase | DB): this {
		if (!(tx instanceof DB)) {
			tx = this.db.withTx(tx)
		}
		return new Queues(tx, this.config, this.queries) as this
	}

	public async configure(
		queue: string,
		options: Pick<QueueConfig, 'displayName' | 'paused'>
	): Promise<QueueConfig> {
		this.requireTenant()
		const { db, queries } = this
		let config = await db.execute(queries.fetchConfig(queue))
		if (!config) {
			config = newConfig(this.tenantId!, queue)
		}
		config = updateConfig(config, options)
		return db.execute(queries.saveConfig(config))
	}

	async fetchQueues(): Promise<QueueConfig[]> {
		const { db, queries } = this
		return db.execute(queries.fetchQueues())
	}

	async fetchQueue(name: string): Promise<QueueConfig | undefined> {
		const { db, queries } = this
		return db.execute(queries.fetchQueue(name))
	}

	async fetchItem(id: UUID): Promise<AnyQueueItem | AnyHistory | undefined> {
		const { db, queries } = this
		const [current, history] = await Promise.all([
			db.execute(queries.fetchItem(id)),
			db.execute(queries.fetchHistory(id)),
		])
		return current || history
	}

	async fetchItems(
		queue: string,
		limit: number = 100,
		after?: UUID | null | undefined | 'FIRST',
		before?: UUID | null | undefined | 'LAST',
		sort?: SortOrder
	): Promise<PagedResult<AnyQueueItem>> {
		const { db, queries } = this
		return db.execute(client => {
			return queries.fetchItemsPage(client, [queue], limit, after, before, sort)
		})
	}
	async fetchHistory(
		queue: string,
		limit: number = 100,
		after?: UUID | null | undefined | 'FIRST',
		before?: UUID | null | undefined | 'LAST',
		sort?: SortOrder
	): Promise<PagedResult<AnyHistory>> {
		const { db, queries } = this
		return db.execute(client => {
			return queries.fetchHistoryPage(
				client,
				[queue],
				limit,
				after,
				before,
				sort
			)
		})
	}

	async push<T>(item: NewQueueItem<T>): Promise<QueueItem<T>> {
		this.requireTenant()
		const { db, queries } = this

		const queueItem = newItem(this.tenantId!, item)
		log.debug('Pushing new item', queueItem)
		return db.execute(queries.insert(queueItem))
	}

	async delete(
		idOrItem: AnyQueueItem['id'] | AnyQueueItem
	): Promise<AnyQueueItem> {
		const { db, queries } = this

		if (typeof idOrItem !== 'string') {
			idOrItem = idOrItem.id
		}
		return db.execute(queries.deleteItem(idOrItem))
	}

	async completed<T, R>(
		item: QueueItem<T>,
		result: WorkResult<R>
	): Promise<void> {
		const { queries } = this
		return this.db.transactional(async db => {
			const history = itemCompleted(item, result)
			await db.execute(queries.insertHistory(history))
			await db.execute(queries.deleteItem(item.id))
		})
	}

	async failed<T, R>(
		item: QueueItem<T>,
		result: WorkResult<R> | null | undefined,
		error: string
	): Promise<void> {
		const { queries } = this
		const retryAt = await this.getNextRetry(item)
		if (retryAt) {
			const updated = itemRunFailed(item, retryAt, result, error)
			await this.db.execute(queries.updateItem(updated))
		} else {
			return this.db.transactional(async db => {
				const history = itemFailed(item, result, error)
				await db.execute(queries.insertHistory(history))
				await db.execute(queries.deleteItem(item.id))
			})
		}
	}

	private async getNextRetry(item: AnyQueueItem): Promise<Date | null> {
		const { db, queries } = this
		const config = await db.execute(queries.fetchConfig(item.queue))
		const delay = config?.retryPolicy || DEFAULT_QUEUE_CONFIG.retryPolicy

		return nextRun(delay, item.tries)
	}

	private requireTenant(message?: string): void {
		if (!this.tenantId) {
			throw new Error(message || 'TenantId is required')
		}
	}
}
