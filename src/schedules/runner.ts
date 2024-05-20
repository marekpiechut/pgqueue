import { shuffleBy } from '~/common/collections'
import logger from '~/common/logger'
import { pollingLoop } from '~/common/polling'
import { DB, DBConnectionSpec } from '~/common/sql'
import { DEFAULT_SCHEMA } from '~/db'
import Queues from '~/queues'
import { executeSchedule, executedSuccessfully } from './models'
import { Queries, withSchema } from './queries'

const log = logger('pgqueue:schedules:runner')

export type ScheduleRunnerConfig = {
	pollInterval?: number
	batchSize?: number
	schema?: string
}
const DEFAULT_CONFIG = {
	schema: DEFAULT_SCHEMA,
	batchSize: 100,
	pollInterval: 10000,
}

export class ScheduleRunner {
	private abort?: AbortController

	private constructor(
		private db: DB,
		private queries: Queries,
		private config: ScheduleRunnerConfig & typeof DEFAULT_CONFIG,
		private queues: Queues
	) {}

	public static create(
		connectionSpec: DBConnectionSpec,
		config: ScheduleRunnerConfig
	): ScheduleRunner {
		const connection = DB.create(connectionSpec)
		const mergedConfig = { ...DEFAULT_CONFIG, ...config }
		const queries = withSchema(mergedConfig.schema)
		const queues = Queues.create(connectionSpec, mergedConfig)
		return new ScheduleRunner(connection, queries, mergedConfig, queues)
	}

	async start(): Promise<void> {
		log.info('Starting scheduler', this.config)
		this.abort = new AbortController()
		pollingLoop(this.run, this.config.pollInterval, this.abort.signal)
	}

	async stop(): Promise<void> {
		if (!this.abort) return
		log.info('Shutting down scheduler', this.config)
		this.abort.abort()
		this.abort = undefined
	}

	private run = async (): Promise<boolean> => {
		const { db, queries, config } = this
		const schedules = await db.execute(
			queries.fetchAndLockRunnable(this.config.batchSize)
		)

		log.debug('Fetched schedules to run', { count: schedules.length })

		//Distribute by tenant, so no one tenant can block the others
		const shuffled = shuffleBy(schedules, 'tenantId')

		await Promise.all(
			shuffled.map(async schedule => {
				try {
					db.transactional(async withTx => {
						const queueItem = executeSchedule(schedule)
						await this.queues
							.withTx(withTx)
							.withTenant(schedule.tenantId)
							.push(queueItem)

						const nextRun = executedSuccessfully(schedule)
						await withTx.execute(queries.update(nextRun))
					})
				} catch (err) {
					log.error(err, 'Error pushing schedule to queue', {
						schedule: schedule.id,
					})
				}
			})
		)
		return schedules.length >= config.batchSize
	}
}
