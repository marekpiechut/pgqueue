import logger from '~/common/logger'
import { PagedResult, TenantId, UUID } from '~/common/models'
import {
	DBConnection,
	DBConnectionSpec,
	SortOrder,
	withTx,
} from '~/common/psql'
import { nextRun } from '~/common/retry'
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
	newConfig,
	newItem,
	updateConfig,
} from './models'
import { QueueHistoryRepository } from './repository/history'
import { QueueRepository } from './repository/items'

const log = logger('pgqueue:queues')

export type QueuesConfig = {
	schema: string
	pollInterval?: number
	batchSize?: number
}

export type QueueManager = {
	fetchQueues(): Promise<QueueConfig[]>
	fetchQueue(name: string): Promise<QueueConfig | null>
	fetchItem(id: UUID): Promise<AnyQueueItem | AnyHistory | null>
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
}

export type TenantQueueManager = QueueManager & {
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
	private queueRepo: QueueRepository
	private historyRepo: QueueHistoryRepository

	private constructor(
		private connection: DBConnection,
		private config: QueuesConfig
	) {
		this.queueRepo = new QueueRepository(config.schema, connection.pool)
		this.historyRepo = new QueueHistoryRepository(
			config.schema,
			connection.pool
		)
	}

	public static create(
		connectionSpec: DBConnectionSpec,
		config: QueuesConfig
	): QueueManager {
		const connection = DBConnection.create(connectionSpec)
		return new Queues(connection, config)
	}

	public withTenant(tenantId: TenantId): TenantQueueManager {
		const copy = new Queues(this.connection, this.config) as this
		copy.tenantId = tenantId
		copy.queueRepo = this.queueRepo.withTenant(tenantId)
		copy.historyRepo = this.historyRepo.withTenant(tenantId)
		return copy
	}

	public async configure(
		queue: string,
		options: Pick<QueueConfig, 'displayName' | 'paused'>
	): Promise<QueueConfig> {
		let config = await this.queueRepo.getConfig(queue)
		if (!config) {
			config = newConfig(this.tenantId!, queue)
		}
		config = updateConfig(config, options)
		return this.queueRepo.saveConfig(config)
	}

	async fetchQueues(): Promise<QueueConfig[]> {
		return this.queueRepo.fetchQueues()
	}

	async fetchQueue(name: string): Promise<QueueConfig | null> {
		return this.queueRepo.fetchQueue(name)
	}

	async fetchItem(id: UUID): Promise<AnyQueueItem | AnyHistory | null> {
		const [current, history] = await Promise.all([
			this.queueRepo.fetchItem(id),
			this.historyRepo.fetchItem(id),
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
		return this.queueRepo.fetchItems(queue, limit, after, before, sort)
	}
	async fetchHistory(
		queue: string,
		limit: number = 100,
		after?: UUID | null | undefined | 'FIRST',
		before?: UUID | null | undefined | 'LAST',
		sort?: SortOrder
	): Promise<PagedResult<AnyHistory>> {
		return this.historyRepo.fetchItems(queue, limit, after, before, sort)
	}

	async push<T>(item: NewQueueItem<T>): Promise<QueueItem<T>> {
		if (!this.tenantId) {
			throw new Error('TenantId is required to push new items')
		}

		const queueItem = newItem(this.tenantId!, item)
		log.debug('Pushing new item', queueItem)
		return this.queueRepo.insert(queueItem)
	}

	async delete(
		idOrItem: AnyQueueItem['id'] | AnyQueueItem
	): Promise<AnyQueueItem> {
		return this.queueRepo.delete(idOrItem)
	}

	async completed<T, R>(
		item: QueueItem<T>,
		result: WorkResult<R>
	): Promise<void> {
		await withTx(this.connection.pool, async tx => {
			const history = itemCompleted(item, result)
			await this.historyRepo.withTx(tx).save(history)
			await this.queueRepo.withTx(tx).delete(item.id)
		})
	}

	async failed<T, R>(
		item: QueueItem<T>,
		result: WorkResult<R> | null | undefined,
		error: string
	): Promise<void> {
		const retryAt = await this.getNextRetry(item)
		if (retryAt) {
			await this.queueRepo.update({
				...item,
				runAfter: retryAt,
				state: 'PENDING',
			})
		} else {
			await withTx(this.connection.pool, async tx => {
				const history = itemFailed(item, result, error)
				await this.historyRepo.withTx(tx).save(history)
				await this.queueRepo.withTx(tx).delete(item.id)
			})
		}
	}

	private async getNextRetry(item: AnyQueueItem): Promise<Date | null> {
		const config = await this.queueRepo.getConfig(item.queue)
		const delay = config?.retryPolicy || DEFAULT_QUEUE_CONFIG.retryPolicy

		return nextRun(delay, item.tries)
	}
}
