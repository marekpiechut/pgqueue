import { assignWith, groupBy } from 'lodash'
import logger from '~/common/logger'
import { DBConnection, DBConnectionSpec, withTx } from '~/common/psql'
import { DEFAULT_SCHEMA } from '~/db'
import { AnyQueueItem, queueForWork } from './models'
import { QueueRepository } from './repository/items'
import { WorkQueueRepository } from './repository/work'
import { RerunImmediately, pollingLoop } from '~/common/polling'

const log = logger('pgqueue:scheduler')

type Config = {
	schema: string
	pollInterval: number
	batchSize: number
}

const DEFAULT_CONFIG: Config = {
	schema: DEFAULT_SCHEMA,
	pollInterval: 1000,
	batchSize: 100,
}

export class Scheduler {
	private connection: DBConnection
	private config: Config
	private queueRepo: QueueRepository
	private workQueueRepo: WorkQueueRepository
	private abort: AbortController | undefined

	private constructor(connection: DBConnection, config?: Partial<Config>) {
		this.connection = connection
		this.config = { ...DEFAULT_CONFIG, ...config }

		const { schema } = this.config
		this.queueRepo = new QueueRepository(schema, this.connection.pool)
		this.workQueueRepo = new WorkQueueRepository(schema, this.connection.pool)

		this.config = assignWith(DEFAULT_CONFIG, config, (objValue, srcValue) => {
			return srcValue === undefined ? objValue : srcValue
		})
	}
	public static create(
		connectionSpec: DBConnectionSpec,
		config?: Partial<Config>
	): Scheduler {
		const connection = DBConnection.create(connectionSpec)
		return new Scheduler(connection, config)
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
		const { batchSize } = this.config
		return await withTx(this.connection.pool, async tx => {
			const items = await this.queueRepo
				.withTx(tx)
				.fetchAndLockDueItems(batchSize)

			if (items.length === 0) return false

			//Distribute by tenant, so no one tenant can block the others
			const byTenant = groupBy(items, 'tenantId')
			let done = false
			const shuffled = []
			do {
				done = true
				for (const tenantId in byTenant) {
					const tenantItems = byTenant[tenantId]
					if (tenantItems.length) {
						shuffled.push(tenantItems.shift())
						done = false
					}
				}
			} while (!done)
			const workItems = shuffled
				.filter(Boolean)
				.map((item, idx) => queueForWork(item as AnyQueueItem, idx))

			log.debug('Scheduling work', workItems)
			await this.workQueueRepo.withTx(tx).insertMany(workItems)
			await this.queueRepo.withTx(tx).markAs(
				'RUNNING',
				shuffled.filter(Boolean).map(i => (i as AnyQueueItem).id)
			)
			return items.length >= batchSize
		})
	}).bind(this)
}
