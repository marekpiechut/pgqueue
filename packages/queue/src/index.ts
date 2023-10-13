import { collections, errors, events, ids, logger, psql } from '@pgqueue/core'
import pg from 'pg'
import {
	CompletedJob,
	FailedJob,
	JobContext,
	JobHandler,
	JobWithOptions,
	PendingJob,
	RunningJob,
	completeJob,
	failJob,
	newJob,
} from './models.js'
import { JobRepository } from './repository.js'
import * as schema from './schema/index.js'

const log = logger.create('jobs:queue')
const DEFAULT_SCHEMA = 'queues'
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
type QueueEvents = {
	started: () => void
	stopped: () => void
	failed: (job: FailedJob<unknown> | PendingJob<unknown>, err: Error) => void
	processed: (job: CompletedJob<unknown, unknown>) => void
	error: (err: Error) => void
}
class Queue extends events.TypedEventEmitter<QueueEvents> {
	private clientFactory: psql.ClientFactory
	private persistentConnection: psql.SharedPersistentConnection
	private config: Config & typeof DEFAULT_CONFIG
	private channelKey: string
	private started = false
	private run: Promise<unknown> | undefined
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	private handlers = new collections.Multimap<string, JobHandler<any, any>>()

	constructor(
		clientFactory: psql.ClientFactory,
		persistentConnection: psql.SharedPersistentConnection,
		config?: Config
	) {
		super()
		this.config = { ...DEFAULT_CONFIG, ...config }
		this.clientFactory = clientFactory
		this.persistentConnection = persistentConnection
		this.channelKey = `pgqueue:job:added:${this.config.nodeId}`
	}

	public async start(): Promise<void> {
		log.info('Starting queue with config', this.config)
		if (this.started) {
			throw new Error('Queue already started')
		}
		log.info('Starting queue listener')
		await this.clientFactory.withClient(client =>
			schema.applyEvolutions(this.config, client)
		)
		await this.clientFactory.withClient(client => {
			const repository = new JobRepository(client, this.config)
			return repository.restartAll()
		})
		await this.persistentConnection.acquire(this.onConnection)

		this.started = true
		this.emit('started')
	}

	public async stop(force?: boolean): Promise<void> {
		//Wait for current run to finish
		if (!force && this.run) {
			await this.run
		}

		try {
			await this.unsubscribe(...this.handlers.keys())
			const client = await this.persistentConnection.getClient()
			client.off('notification', this.onEvent)
		} catch (err) {
			this.emit('error', errors.toError(err))
			if (!force) throw err
		}
		await this.persistentConnection.release(this.onConnection)

		this.started = false
		this.emit('stopped')
	}

	public async push<P>(
		client: pg.ClientBase,
		item: JobWithOptions<P>
	): Promise<PendingJob<P>> {
		const job = newJob(item.type, item.payload, item)
		log.debug(`Pushing job "${job.type}"`, job)
		const repository = new JobRepository(client, this.config)
		const res = await repository.create(job)
		log.debug(`Job pushed "${job.type}"`, res)
		return res
	}

	public withTx(client: pg.ClientBase): {
		push: <P>(item: JobWithOptions<P>) => Promise<PendingJob<P>>
	} {
		return {
			push: <P>(item: JobWithOptions<P>) => this.push(client, item),
		}
	}

	public async addHandler<P, R>(
		type: string,
		handler: JobHandler<P, R>
	): Promise<void> {
		if (this.handlers.set(type, handler) && this.started) {
			await this.subscribe(type)
		}
	}

	public async removeHandler<P, R>(
		type: string,
		handler: JobHandler<P, R>
	): Promise<void> {
		if (this.handlers.delete(type, handler) && this.started) {
			await this.unsubscribe(type)
		}
	}

	private onConnection = (async (client: pg.ClientBase): Promise<void> => {
		client.on('notification', this.onEvent)
		this.subscribe(...this.handlers.keys())
		await this.run
		this.run = this.poll()
	}).bind(this)

	private onEvent = (async (event: pg.Notification): Promise<void> => {
		const { channel, payload } = event
		if (channel === this.channelKey) {
			const type = payload as string
			const listeners = this.handlers.get(type)
			if (listeners) {
				await this.run
				this.run = this.poll()
			}
		}
	}).bind(this)

	private async poll(count: number = 0): Promise<void> {
		log.debug('Polling for jobs')
		//TODO: configure batch size
		try {
			const batchSize = 2
			const types = this.handlers.keys()
			const jobs = await this.clientFactory.withTx(async client => {
				const repository = new JobRepository(client, this.config)
				return repository.poll(Array.from(types), batchSize)
			})
			if (jobs.length === 0) {
				log.debug('No jobs found, processed', count)
				return
			}

			log.debug(`Found ${jobs.length} jobs, batch size ${batchSize}`)
			for (const job of jobs) {
				await this.processJob(job)
			}
			if (jobs.length === batchSize) {
				await this.poll(count + jobs.length)
			} else {
				log.debug('No more jobs found, processed', count + jobs.length)
			}
		} catch (err) {
			log.error(err, 'Error while processing jobs')
			this.emit('error', errors.toError(err))
		}
	}

	private async processJob<P>(job: RunningJob<P>): Promise<void> {
		const handlers = this.handlers.get(job.type)

		if (handlers) {
			log.debug(`Found ${handlers.length} handlers for job type ${job.type}`)

			const context = this.createContext(job)

			try {
				const res = await Promise.all(handlers.map(h => h(job, context)))

				await this.clientFactory.withTx(async client => {
					const repository = new JobRepository(client, this.config)
					await repository.delete(job.id)
					const completedJob = await repository.archive(
						completeJob(job, res.length > 1 ? res : res[0])
					)
					this.emit('processed', completedJob)
					log.debug('Job processed', completedJob)
				})
			} catch (err) {
				log.error(err, 'Error while processing job', job)
				const failedJob = await this.clientFactory.withTx(async client => {
					const repository = new JobRepository(client, this.config)
					const error = errors.toError(err)
					//TODO: move to job history if attempts > max
					return repository.update(failJob(job, error))
				})
				this.emit('failed', failedJob, errors.toError(err))
				throw err
			}
		}
	}

	private createContext<P>(_job: RunningJob<P>): JobContext {
		return {
			postpone: async _ => {
				throw new Error('Not implemented')
			},
		}
	}

	private async subscribe(...types: string[]): Promise<void> {
		log.debug('Subscribing to job types', types)
		const client = await this.persistentConnection.getClient()
		const { schema, nodeId } = this.config
		await client.query(
			`SELECT ${client.escapeIdentifier(schema)}.SUBSCRIBE($1, $2)`,
			[nodeId, types]
		)
	}

	private async unsubscribe(...types: string[]): Promise<void> {
		log.debug('Unsubscribing from job types', types)
		const client = await this.persistentConnection.getClient()
		const { schema, nodeId } = this.config
		await client.query(
			`SELECT ${client.escapeIdentifier(schema)}.UNSUBSCRIBE($1, $2)`,
			[nodeId, types]
		)
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
): Promise<Queue> => {
	const pool =
		poolOrConfig instanceof pg.Pool ? poolOrConfig : new pg.Pool(poolOrConfig)
	const persistentConnection = new psql.SharedPersistentConnection(pool)
	const connectionFactory = psql.poolConnectionFactory(pool)

	return new Queue(connectionFactory, persistentConnection, config)
}

export { DEFAULT_SCHEMA, Queue, evolutions, quickstart }
export default {
	DEFAULT_SCHEMA,
	Queue,
	quickstart,
	evolutions,
}
