import { ids, logger, psql } from '@pgqueue/core'
import { Schedule } from 'cron.js'
import pg from 'pg'
import { ScheduledJob, ScheduledJobOptions, newSchedule } from './models.js'
import { ScheduledJobRepository } from './repository.js'
import * as schema from './schema/index.js'

const log = logger.create('schedule')
const DEFAULT_SCHEMA = 'schedules'
type Config = {
	schema?: string
	nodeId?: string
	pollInterval?: number
	runMaintenance?: boolean
}

const DEFAULT_CONFIG = {
	schema: DEFAULT_SCHEMA,
	nodeId: ids.uuid(),
	pollInterval: 30000,
	runMaintenance: true,
}
class Scheduler {
	private config: Config & typeof DEFAULT_CONFIG
	constructor(config?: Config) {
		this.config = { ...DEFAULT_CONFIG, ...config }
	}

	public async schedule<P>(
		client: pg.ClientBase,
		type: string,
		schedule: Schedule,
		payload: P,
		options?: ScheduledJobOptions
	): Promise<ScheduledJob<P>> {
		const job = newSchedule(type, schedule, payload, options)
		log.debug(`Scheduling job "${type}"`, job)
		const repository = new ScheduledJobRepository(client, this.config)
		const res = await repository.create(job)
		log.debug(`Job scheduled"${type}"`, res)
		return res
	}
}

const evolutions = {
	apply: (client: pg.ClientBase, config?: schema.Config): Promise<void> => {
		return schema.applyEvolutions({ schema: DEFAULT_SCHEMA, ...config }, client)
	},
}

const quickstart = async (
	poolOrConfig: pg.PoolConfig | pg.Pool,
	config?: Config
): Promise<Scheduler> => {
	const pool =
		poolOrConfig instanceof pg.Pool ? poolOrConfig : new pg.Pool(poolOrConfig)
	const _persistentConnection = new psql.SharedPersistentConnection(pool)
	const _connectionFactory = psql.poolConnectionFactory(pool)

	return new Scheduler(config)
}

export { DEFAULT_SCHEMA, Scheduler, evolutions, quickstart }
export default {
	DEFAULT_SCHEMA,
	Scheduler,
	quickstart,
	evolutions,
}
