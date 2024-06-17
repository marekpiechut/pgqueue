import { pull } from 'lodash'
import crypto from 'node:crypto'
import pg from 'pg'
import { minutes } from '~/common/duration'
import logger from '~/common/logger'
import { RerunImmediately, pollingLoop } from '~/common/polling'
import { TenantId } from '~/common/psql'
import { nextRun } from '~/common/retry'
import { DB, DBConnectionSpec } from '~/common/sql'
import { DEFAULT_SCHEMA } from '~/db'
import {
	AnyQueueItem,
	AnyWorkResult,
	DEFAULT_QUEUE_CONFIG,
	HandlerError,
	QueueItem,
	WorkResult,
	itemCompleted,
	itemFailed,
	itemRunFailed,
} from './models'
import { Queries, withSchema } from './queries'
import { AnyObject } from '~/common/models'
import { mergeConfig } from '~/common/config'

const log = logger('pgqueue:worker')

export type WorkerConfig = {
	nodeId: string
	schema?: string
	lockTimeout?: number
}

const DEFAULT_CONFIG = {
	schema: DEFAULT_SCHEMA,
	pollInterval: 1000,
	batchSize: 10,
	lockTimeout: minutes(2),
}

const startedWorkerNodes: string[] = []
export type WorkerHandler = (item: AnyQueueItem) => Promise<AnyWorkResult>
export class Worker {
	private abort: AbortController | undefined

	private constructor(
		private db: DB,
		private queries: Queries,
		private config: WorkerConfig & typeof DEFAULT_CONFIG,
		private handler: WorkerHandler
	) {}

	static create(
		db: DBConnectionSpec | string,
		config: WorkerConfig,
		handler: WorkerHandler
	): Worker {
		const connection = DB.create(db)
		const mergedConfig = mergeConfig(DEFAULT_CONFIG, config)
		const queries = withSchema(mergedConfig.schema)
		return new Worker(connection, queries, mergedConfig, handler)
	}

	async start(): Promise<void> {
		const { db, queries, config } = this
		log.info('Starting worker', config)
		if (startedWorkerNodes.includes(config.nodeId)) {
			throw new Error(`Worker for nodeId ${config.nodeId} already started`)
		}

		startedWorkerNodes.push(config.nodeId)
		await db.execute(queries.unlockAllWorkItems(config.nodeId))

		this.abort = new AbortController()
		pollingLoop(this.poll, config.pollInterval, this.abort.signal)
	}

	async stop(): Promise<void> {
		if (!this.abort) return
		log.info('Shutting down worker', this.config)
		this.abort.abort()
		this.abort = undefined
		pull(startedWorkerNodes, this.config.nodeId)
	}

	private poll = (async (): Promise<RerunImmediately> => {
		const { db, queries } = this
		const { nodeId, batchSize, lockTimeout } = this.config
		log.debug('Polling for work')
		const items = await db.execute(
			queries.pollWorkQueue(nodeId, batchSize, lockTimeout)
		)
		for (const item of items) {
			let job: QueueItem<unknown> | undefined
			try {
				job = await db.execute(queries.fetchItem(item.id))
				if (job) {
					const result = await this.handler(job)
					await this.completed(job, result)
					log.debug('Item completed %s', item.id)
				} else {
					log.error('Item not found, will not process item %s', item.id)
				}
			} catch (error) {
				if (job) {
					let message = 'Unknown error'
					let result: AnyWorkResult | null = null
					if (error instanceof HandlerError) {
						result = error.result || null
						message = error.message
					} else {
						const errorId = crypto.randomUUID()
						message = `Unknown error: ${errorId}`
						log.error(error, 'Server error (%s) %s - %s', errorId, item.id)
					}
					await this.failed(job, result, message)
				}
				log.debug('Item failed %s - %s', item.id, error)
			} finally {
				try {
					await db.execute(queries.deleteWorkItem(item.id))
				} catch (error) {
					log.error('Failed to delete work queue item %s', item.id)
				}
			}
		}

		return items.length >= batchSize
	}).bind(this)

	async completed<T, R>(
		item: QueueItem<T>,
		result: WorkResult<R>
	): Promise<void> {
		const { db, queries } = this
		await db.transactional(async withTx => {
			const history = itemCompleted(item, result)
			await withTx.execute(queries.insertHistory(history))
			await withTx.execute(queries.deleteItem(item.id))
		})
	}

	async failed<T, R>(
		item: QueueItem<T>,
		result: WorkResult<R> | null | undefined,
		error: string
	): Promise<void> {
		const { db, queries } = this
		const retryAt = await this.getNextRetry(item)
		if (retryAt) {
			log.debug('Item %s will retry at %s', item.id, retryAt)
			const updated = itemRunFailed(item, retryAt, result, error)
			await db.execute(queries.updateItem(updated))
		} else {
			log.debug('Item %s exceeded retry limit, fail', item.id)
			await db.transactional(async withTx => {
				const history = itemFailed(item, result, error)
				await withTx.execute(queries.insertHistory(history))
				await withTx.execute(queries.deleteItem(item.id))
			})
		}
	}

	private async getNextRetry(item: AnyQueueItem): Promise<Date | null> {
		const { db, queries } = this
		let retryPolicy = item.retryPolicy
		if (!retryPolicy) {
			const config = await db.execute(queries.fetchConfig(item.queue))
			retryPolicy = config?.retryPolicy
		}
		return nextRun(
			retryPolicy || DEFAULT_QUEUE_CONFIG.retryPolicy,
			item.tries + 1
		)
	}
}

export type WorkerMetadataStore = {
	withTenant: (tenantId: TenantId) => TenantWorkerMetadataStore
}
export type TenantWorkerMetadataStore = {
	set: <T extends AnyObject>(key: string, value: T) => Promise<T>
	get: <T extends AnyObject>(key: string) => Promise<T | undefined>
	remove: <T extends AnyObject>(key: string) => Promise<T | undefined>
}
export class WorkerMetadata
	implements WorkerMetadataStore, TenantWorkerMetadataStore
{
	private constructor(
		private db: DB,
		private queries: Queries,
		private tenantId: TenantId | undefined
	) {}

	static create(
		db: DBConnectionSpec | string,
		config?: { schema?: string }
	): WorkerMetadataStore {
		const connection = DB.create(db)
		const queries = withSchema(config?.schema || DEFAULT_SCHEMA)
		return new WorkerMetadata(connection, queries, undefined)
	}

	withTenant(tenantId: TenantId): TenantWorkerMetadataStore {
		return new WorkerMetadata(
			this.db.withTenant(tenantId),
			this.queries,
			tenantId
		)
	}

	public withTx(tx: pg.ClientBase | DB): this {
		if (!(tx instanceof DB)) {
			tx = this.db.withTx(tx)
		}
		return new WorkerMetadata(tx, this.queries, this.tenantId) as this
	}

	async set<T extends AnyObject>(key: string, value: T): Promise<T> {
		this.requireTenant()
		const { db, queries } = this
		return await db.execute(queries.setMetadata(key, value))
	}

	async get<T extends AnyObject>(key: string): Promise<T | undefined> {
		this.requireTenant()
		const { db, queries } = this
		return db.execute(queries.getMetadata(key))
	}

	async remove<T extends AnyObject>(key: string): Promise<T | undefined> {
		this.requireTenant()
		const { db, queries } = this
		return db.execute(queries.deleteMetadata(key))
	}

	private requireTenant(message?: string): void {
		if (!this.tenantId) {
			throw new Error(message || 'TenantId is required')
		}
	}
}
