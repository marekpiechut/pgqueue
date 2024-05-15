import logger from '~/common/logger'
import { DBConnection, DBConnectionSpec, withTx } from '~/common/psql'
import Queues from '~/queues'
import { ScheduleRepository } from './repository'
import { SchedulesConfig } from './schedules'
import { pollingLoop } from '~/common/polling'
import { groupBy } from 'lodash'
import { Schedule } from './models'

const log = logger('pgqueue:schedules:runner')

export type ScheduleRunnerConfig = {
	pollInterval?: number
	batchSize?: number
	schema: string
}
const DEFAULT_CONFIG = {
	batchSize: 100,
	pollInterval: 10000,
}

export class ScheduleRunner {
	private config: ScheduleRunnerConfig & typeof DEFAULT_CONFIG
	private abort?: AbortController
	private connection: DBConnection
	private repository: ScheduleRepository
	private queues: Queues

	private constructor(
		connection: DBConnection,
		queues: Queues,
		config: SchedulesConfig
	) {
		this.config = { ...DEFAULT_CONFIG, ...config }
		this.connection = connection
		this.repository = new ScheduleRepository(config.schema, connection.pool)
		this.queues = queues
	}

	public static create(
		connectionSpec: DBConnectionSpec,
		queues: Queues,
		config: ScheduleRunnerConfig
	): ScheduleRunner {
		const connection = DBConnection.create(connectionSpec)
		return new ScheduleRunner(connection, queues, config)
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
		const schedules = await this.repository.fetchAndLockRunnable(
			this.config.batchSize
		)

		//Distribute by tenant, so no one tenant can block the others
		const byTenant = groupBy(schedules, 'tenantId')
		let done = false
		const shuffled = []
		do {
			done = true
			for (const tenantId in byTenant) {
				const tenantItems = byTenant[tenantId]
				if (tenantItems.length) {
					const next = tenantItems.shift()
					if (next) {
						shuffled.push(next)
						done = false
					}
				}
			}
		} while (!done)

		await Promise.all(
			shuffled.map(async schedule => {
				try {
					withTx(this.connection.pool, async tx => {
						await this.queues.withTenant(schedule.tenantId).withTx(tx).push({
							queue: schedule.queue,
							type: schedule.type,
							scheduleId: schedule.id,
						})
					})
				} catch (err) {
					log.error(err, 'Error pushing schedule to queue', {
						schedule: schedule.id,
					})
				}
			})
		)
		return schedules.length >= this.config.batchSize
	}
}
