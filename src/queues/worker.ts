import { pull } from 'lodash'
import crypto from 'node:crypto'
import { minutes } from '~/common/duration'
import logger from '~/common/logger'
import { RerunImmediately, pollingLoop } from '~/common/polling'
import { nextRun } from '~/common/retry'
import { DB, DBConnectionSpec } from '~/common/sql'
import { DEFAULT_SCHEMA } from '~/db'
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
import { Queries, withSchema } from './queries'

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
		const mergedConfig = { ...DEFAULT_CONFIG, ...config }
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
