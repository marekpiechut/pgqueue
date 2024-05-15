import { assignWith, pull } from 'lodash'
import crypto from 'node:crypto'
import { minutes } from '~/common/duration'
import logger from '~/common/logger'
import { DBConnection, DBConnectionSpec, withTx } from '~/common/psql'
import { nextRun } from '~/common/retry'
import {
	AnyQueueItem,
	AnyWorkResult,
	DEFAULT_QUEUE_CONFIG,
	QueueItem,
	WorkResult,
	itemCompleted,
	itemFailed,
	itemRunFailed,
} from './models'
import { QueueHistoryRepository } from './repository/history'
import { QueueRepository } from './repository/items'
import { WorkQueueRepository } from './repository/work'
import { RerunImmediately, pollingLoop } from '~/common/polling'

const log = logger('pgqueue:worker')

export type WorkerConfig = {
	schema: string
	nodeId: string
	lockTimeout?: number
}

const DEFAULT_CONFIG = {
	pollInterval: 1000,
	batchSize: 10,
	lockTimeout: minutes(2),
}

export class HandlerError extends Error {
	result?: AnyWorkResult
	constructor(message: string, result?: AnyWorkResult) {
		super(message)
		this.result = result
	}
}

const startedWorkerNodes: string[] = []
export type WorkerHandler = (item: AnyQueueItem) => Promise<AnyWorkResult>
export class Worker {
	private connection: DBConnection
	private config: WorkerConfig & typeof DEFAULT_CONFIG
	private handler: WorkerHandler
	private workQueueRepo: WorkQueueRepository
	private queueRepo: QueueRepository
	private historyRepo: QueueHistoryRepository
	private abort: AbortController | undefined

	private constructor(
		connection: DBConnection,
		config: WorkerConfig,
		handler: WorkerHandler
	) {
		this.handler = handler
		this.connection = connection
		this.workQueueRepo = new WorkQueueRepository(
			config.schema,
			this.connection.pool
		)
		this.queueRepo = new QueueRepository(config.schema, this.connection.pool)
		this.historyRepo = new QueueHistoryRepository(
			config.schema,
			this.connection.pool
		)

		this.config = assignWith(DEFAULT_CONFIG, config, (objValue, srcValue) => {
			return srcValue === undefined ? objValue : srcValue
		})
	}

	static create(
		db: DBConnectionSpec | string,
		config: WorkerConfig,
		handler: WorkerHandler
	): Worker {
		const connection = DBConnection.create(db)
		return new Worker(connection, config, handler)
	}

	async start(): Promise<void> {
		log.info('Starting worker', this.config)
		if (startedWorkerNodes.includes(this.config.nodeId)) {
			throw new Error(`Worker for nodeId ${this.config.nodeId} already started`)
		}

		startedWorkerNodes.push(this.config.nodeId)
		await this.workQueueRepo.unlockAll(this.config.nodeId)

		this.abort = new AbortController()
		pollingLoop(this.poll, this.config.pollInterval, this.abort.signal)
	}

	async stop(): Promise<void> {
		if (!this.abort) return
		log.info('Shutting down worker', this.config)
		this.abort.abort()
		this.abort = undefined
		pull(startedWorkerNodes, this.config.nodeId)
	}

	private poll = (async (): Promise<RerunImmediately> => {
		const { nodeId, batchSize, lockTimeout } = this.config
		const items = await this.workQueueRepo.poll(nodeId, batchSize, lockTimeout)
		for (const item of items) {
			let job: QueueItem<unknown> | null = null
			try {
				job = await this.queueRepo.fetchItem(item.id)
				if (job === null) {
					log.error('Item not found, will not process item %s', item.id)
				} else {
					const result = await this.handler(job)
					await this.completed(job, result)
					log.debug('Item completed %s', item.id)
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
					await this.workQueueRepo.delete(item)
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
			log.debug('Item %s will retry at %s', item.id, retryAt)
			const updated = itemRunFailed(item, retryAt, result, error)
			await this.queueRepo.update(updated)
		} else {
			log.debug('Item %s exceeded retry limit, fail', item.id)
			return withTx(this.connection.pool, async tx => {
				const history = itemFailed(item, result, error)
				await this.historyRepo.withTx(tx).save(history)
				await this.queueRepo.withTx(tx).delete(item.id)
			})
		}
	}

	private async getNextRetry(item: AnyQueueItem): Promise<Date | null> {
		const config = await this.queueRepo.getConfig(item.queue)
		const retry = config?.retryPolicy || DEFAULT_QUEUE_CONFIG.retryPolicy
		return nextRun(retry, item.tries + 1)
	}
}
