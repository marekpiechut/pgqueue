import { shuffleBy } from '~/common/collections'
import logger from '~/common/logger'
import { RerunImmediately, pollingLoop } from '~/common/polling'
import { DB, DBConnectionSpec } from '~/common/sql'
import { DEFAULT_SCHEMA } from '~/db'
import { AnyQueueItem, queueForWork } from './models'
import { Queries, withSchema } from './queries'
import { mergeConfig } from '~/common/config'

const log = logger('pgqueue:scheduler')

type Config = {
	schema?: string
	pollInterval?: number
	batchSize?: number
}

const DEFAULT_CONFIG = {
	schema: DEFAULT_SCHEMA,
	pollInterval: 1000,
	batchSize: 100,
}

export class Scheduler {
	private abort: AbortController | undefined

	private constructor(
		private db: DB,
		private queries: Queries,
		private config: Config & typeof DEFAULT_CONFIG
	) {
		this.config = mergeConfig(DEFAULT_CONFIG, config)
		this.db = db
		this.queries = queries
	}

	public static create(
		connectionSpec: DBConnectionSpec,
		config: Config
	): Scheduler {
		const mergedConfig = mergeConfig(DEFAULT_CONFIG, config)
		const db = DB.create(connectionSpec)
		const queries = withSchema(mergedConfig.schema)
		return new Scheduler(db, queries, mergedConfig)
	}

	async start(): Promise<void> {
		log.info('Starting scheduler', this.config)
		this.abort = new AbortController()
		pollingLoop(this.schedule, this.config.pollInterval, this.abort.signal)
	}

	async stop(): Promise<void> {
		if (!this.abort) return
		log.info('Shutting down scheduler', this.config)
		this.abort.abort()
		this.abort = undefined
	}

	//TODO: switch to batch size 1 and fail item that could not be scheduled
	//if there was an error with regular scheduling
	private schedule = (async (): Promise<RerunImmediately> => {
		const { db, queries } = this
		const { batchSize } = this.config
		return db.transactional(async withTx => {
			const items = await withTx.execute(
				queries.fetchAndLockDueItems(batchSize)
			)

			if (items.length === 0) return false

			//Distribute by tenant, so no one tenant can block the others
			const shuffled = shuffleBy(items, 'tenantId')

			const workItems = shuffled
				.filter(Boolean)
				.map((item, idx) => queueForWork(item as AnyQueueItem, idx))

			log.debug('Scheduling work', workItems)
			await withTx.execute(queries.insertWorkItems(workItems))
			await withTx.execute(
				queries.markAs(
					'RUNNING',
					shuffled.map(i => (i as AnyQueueItem).id)
				)
			)

			return items.length >= batchSize
		})
	}).bind(this)
}
