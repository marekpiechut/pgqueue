import {
	collections,
	date,
	errors,
	events,
	ids,
	logger,
	psql,
} from '@pgqueue/core'
import { Schedule } from 'cron.js'
import pg from 'pg'
import {
	ScheduleHandler,
	ScheduledJob,
	ScheduledJobContext,
	ScheduledJobOptions,
	ScheduledJobRun,
	newSchedule,
	runFailed,
	runPerformed,
} from './models.js'
import { ScheduledJobRepository, parseNotification } from './repository.js'
import * as schema from './schema/index.js'

const log = logger.create('schedule')
const DEFAULT_SCHEMA = 'schedules'
type Config = {
	schema?: string
	nodeId?: string
	runMaintenance?: boolean
}

const DEFAULT_CONFIG = {
	schema: DEFAULT_SCHEMA,
	nodeId: ids.uuid(),
	runMaintenance: true,
}
type Events = {
	started: () => void
	stopped: () => void
	failed: (
		job: ScheduledJob<unknown>,
		run: ScheduledJobRun<unknown>,
		err: Error
	) => void
	processed: (job: ScheduledJob<unknown>, run: ScheduledJobRun<unknown>) => void
	error: (err: Error) => void
}
class Scheduler extends events.TypedEventEmitter<Events> {
	private config: Config & typeof DEFAULT_CONFIG
	private clientFactory: psql.ClientFactory
	private persistentConnection: psql.SharedPersistentConnection
	private nextWakeUp: Date | undefined
	private wakeUpTimeout: NodeJS.Timeout | undefined
	private handlers = new collections.Multimap<
		string,
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		ScheduleHandler<any, any>
	>()

	constructor(
		clientFactory: psql.ClientFactory,
		persistentConnection: psql.SharedPersistentConnection,
		config?: Config
	) {
		super()
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
		await this.clientFactory.withTx(client =>
			schema.applyEvolutions(this.config, client)
		)
		await this.clientFactory.withTx(client =>
			schema.applyEvolutions(this.config, client)
		)
		this.persistentConnection.acquire(this.onConnection)

		this.poll()
		this.emit('started')
	}

	public async stop(): Promise<void> {
		log.debug('Stopping scheduler')
		const client = await this.persistentConnection.getClient()
		await client.query(`UNLISTEN pgqueue_schedule_updated"`)
		await this.persistentConnection.release(this.onConnection)
		this.nextWakeUp = undefined
		if (this.wakeUpTimeout) {
			clearTimeout(this.wakeUpTimeout)
		}
		this.emit('stopped')
	}

	public async addHandler<P, R>(
		type: string,
		handler: ScheduleHandler<P, R>
	): Promise<void> {
		this.handlers.set(type, handler)
	}

	public async removeHandler<P, R>(
		type: string,
		handler: ScheduleHandler<P, R>
	): Promise<void> {
		this.handlers.delete(type, handler)
	}

	private onEvent = (async (notification: pg.Notification) => {
		//TODO: this is executed also for jobs processed by this very node, maybe we should skip these?
		const event = parseNotification(notification)
		if (event.type === 'schedule:updated' && event.job.nextRun) {
			this.scheduleWakeUp(event.job.nextRun)
		}
	}).bind(this)

	private onConnection = (async (client: pg.ClientBase) => {
		client.on('notification', this.onEvent)
		await client.query(`LISTEN pgqueue_schedule_updated`)
	}).bind(this)

	//TODO: we should probably skip wake-ups for jobs that don't have handlers registered
	private scheduleWakeUp(newDate: Date): void {
		if (!this.nextWakeUp || date.isBefore(newDate, this.nextWakeUp)) {
			log.debug("Scheduler's next wake up", newDate)
			if (this.wakeUpTimeout) {
				clearTimeout(this.wakeUpTimeout)
			}
			const now = new Date()
			const delay = Math.max(0, newDate.getTime() - now.getTime())
			this.nextWakeUp = delay > 0 ? newDate : now
			//Add a small random delay so we don't wake up all nodes at the same time and bomb the database
			const random = Math.random() * 100
			const timeout = delay + random
			this.wakeUpTimeout = setTimeout(() => {
				this.nextWakeUp = undefined
				this.poll()
			}, timeout)
		}
	}

	private async poll(): Promise<void> {
		//TODO: make configurable
		const batchSize = 2
		const jobs = await this.clientFactory.withTx(async client => {
			const repository = new ScheduledJobRepository(client, this.config)
			const types = Array.from(this.handlers.keys())
			return repository.poll(types, batchSize)
		})

		log.debug(`Found ${jobs.length} scheduled jobs`, jobs)

		for (const job of jobs) {
			await this.processJob(job)
		}
		if (jobs.length >= batchSize) {
			await this.poll()
		} else {
			const next = await this.clientFactory.withClient(async client =>
				new ScheduledJobRepository(client, this.config).fetchNext()
			)
			if (next?.nextRun) {
				this.scheduleWakeUp(next?.nextRun)
			}
		}
	}

	private async processJob<P>(job: ScheduledJob<P>): Promise<void> {
		const handlers = this.handlers.get(job.type)

		if (handlers) {
			log.debug(`Found ${handlers.length} handlers for job type ${job.type}`)

			const context = this.createContext(job)
			try {
				const res = await Promise.all(handlers.map(h => h(job, context)))

				await this.clientFactory.withTx(async client => {
					const repository = new ScheduledJobRepository(client, this.config)
					const [completedJob, run] = runPerformed(
						job,
						res.length === 1 ? res[0] : res
					)
					const [savedJob, savedRun] = await Promise.all([
						repository.update(completedJob),
						repository.saveRun(run),
					])
					this.emit('processed', savedJob, savedRun)
					log.debug('Scheduled job processed', savedJob, savedRun)
				})
			} catch (err) {
				log.error(err, 'Error while processing scheduled job', job)
				await this.clientFactory.withTx(async client => {
					const repository = new ScheduledJobRepository(client, this.config)
					const error = errors.toError(err)
					const [failedJob, run] = runFailed(job, error)
					const [savedJob, savedRun] = await Promise.all([
						repository.update(failedJob),
						repository.saveRun(run),
					])
					this.emit('failed', savedJob, savedRun, error)
				})

				throw err
			}
		}
	}
	private createContext<P>(_job: ScheduledJob<P>): ScheduledJobContext {
		return null
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
	const persistentConnection = new psql.SharedPersistentConnection(pool)
	const connectionFactory = psql.poolConnectionFactory(pool)

	return new Scheduler(connectionFactory, persistentConnection, config)
}

export { DEFAULT_SCHEMA, Scheduler, evolutions, quickstart }
export type {
	ScheduleHandler,
	ScheduledJob,
	ScheduledJobContext,
	ScheduledJobOptions,
	ScheduledJobRun,
}
export default {
	DEFAULT_SCHEMA,
	Scheduler,
	quickstart,
	evolutions,
}
