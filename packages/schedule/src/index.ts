import { date, ids, logger, psql } from '@pgqueue/core'
import { Schedule } from 'cron.js'
import pg from 'pg'
import { ScheduledJob, ScheduledJobOptions, newSchedule } from './models.js'
import { ScheduledJobRepository, toJob } from './repository.js'
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
	private clientFactory: psql.ClientFactory
	private persistentConnection: psql.SharedPersistentConnection
	private nextWakeUp = new Date(0)
	private wakeUpTimeout: NodeJS.Timeout | undefined

	constructor(
		clientFactory: psql.ClientFactory,
		persistentConnection: psql.SharedPersistentConnection,
		config?: Config
	) {
		this.config = { ...DEFAULT_CONFIG, ...config }
		this.clientFactory = clientFactory
		this.persistentConnection = persistentConnection
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

	public async start(): Promise<void> {
		log.debug('Starting scheduler')
		this.persistentConnection.acquire(this.onConnection)
	}

	public async stop(): Promise<void> {
		log.debug('Stopping scheduler')
		const client = await this.persistentConnection.getClient()
		await client.query('UNLISTEN pgqueue:schedule:updated')
		await this.persistentConnection.release(this.onConnection)
	}

	private onEvent = (async (event: pg.Notification) => {
		if (event.channel !== 'pgqueue:schedule:updated') return
		if (!event.payload) {
			log.warn('Received empty payload for pgqueue:schedule:updated')
			return
		}

		//TODO: maybe this should go into repostiory
		const item = toJob(JSON.parse(event.payload))
		if (item.nextRun) {
			this.scheduleWakeUp(item.nextRun)
		}
	}).bind(this)

	private onConnection = (async (client: pg.ClientBase) => {
		client.on('notification', this.onEvent)
		await client.query('LISTEN pgqueue:schedule:updated')
	}).bind(this)

	private scheduleWakeUp(newDate: Date): void {
		if (date.isBefore(newDate, this.nextWakeUp)) {
			if (this.wakeUpTimeout) {
				clearTimeout(this.wakeUpTimeout)
			}
			this.wakeUpTimeout = setTimeout(() => this.poll(), newDate.getTime())
		}
	}

	private async poll(): Promise<void> {}
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
	const persistentConnection = new psql.SharedPersistentConnection(pool)
	const connectionFactory = psql.poolConnectionFactory(pool)

	return new Scheduler(connectionFactory, persistentConnection, config)
}

export { DEFAULT_SCHEMA, Scheduler, evolutions, quickstart }
export default {
	DEFAULT_SCHEMA,
	Scheduler,
	quickstart,
	evolutions,
}
