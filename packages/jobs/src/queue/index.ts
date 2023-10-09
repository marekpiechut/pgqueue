import { collections, errors, ids, logger, psql } from '@pgqueue/core'
import EventEmitter from 'events'
import pg from 'pg'
import schema from '../schema/index.js'
import {
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

const log = logger.create('jobs:queue')

type Config = {
	schema?: string
	nodeId?: string
	pollInterval?: number
	runMaintenance?: boolean
}

const DEFAULT_CONFIG = {
	schema: 'pgqueue',
	nodeId: ids.uuid(),
	pollInterval: 30000,
	runMaintenance: true,
}
export class PGQueue extends EventEmitter {
	private clientFactory: psql.ClientFactory
	private client: pg.ClientBase | undefined
	private config: Config & typeof DEFAULT_CONFIG
	private channelKey: string
	private run: Promise<unknown> | undefined
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	private handlers = new collections.Multimap<string, JobHandler<any, any>>()

	constructor(config: Config, clientFactory: psql.ClientFactory) {
		super()
		this.config = { ...DEFAULT_CONFIG, ...config }
		this.clientFactory = clientFactory
		this.channelKey = `pgqueue:job:added:${this.config.nodeId}`
	}

	public static fromPool(pool: pg.Pool, config: Config): PGQueue {
		return new PGQueue(config, psql.poolConnectionFactory(pool))
	}
	public static fromClient(client: pg.Client, config: Config): PGQueue {
		return new PGQueue(config, psql.singleConnectionFactory(client))
	}

	public async start(): Promise<void> {
		log.info('Starting queue listener')
		await this.clientFactory.withClient(client =>
			schema.applyEvolutions(this.config, client)
		)
		await this.clientFactory.withClient(client => {
			const repository = new JobRepository(client, this.config)
			return repository.restartAll()
		})
		await this.clientFactory.persistent({
			onConnect: client => {
				this.client = client
				this.client.on('notification', this.onEvent)
				this.subscribe(...this.handlers.keys())
				this.poll()
			},
		})
		this.emit('started')
	}

	public async stop(force?: boolean): Promise<void> {
		//Wait for current run to finish
		if (!force && this.run) {
			await this.run
		}

		if (this.client) {
			try {
				await this.unsubscribe(...this.handlers.keys())
				this.client.off('notification', this.onEvent)
			} catch (err) {
				this.emit('error', err)
				if (!force) throw err
			}
			await this.clientFactory.release(this.client)
			this.client = undefined
		}

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

	public withClient(client: pg.ClientBase): {
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
		if (this.handlers.set(type, handler) && this.client) {
			await this.subscribe(type)
		}
	}

	public async removeHandler<P, R>(
		type: string,
		handler: JobHandler<P, R>
	): Promise<void> {
		if (this.handlers.delete(type, handler) && this.client) {
			await this.unsubscribe(type)
		}
	}

	private onEvent = ((event: pg.Notification): void => {
		const { channel, payload } = event
		if (channel === this.channelKey) {
			const type = payload as string
			const listeners = this.handlers.get(type)
			if (listeners && !this.run) {
				this.poll()
			}
		}
	}).bind(this)

	private async poll(): Promise<void> {
		log.debug('Polling for jobs')
		//TODO: poll until there are unlocked jobs to process
		const types = this.handlers.keys()
		this.run = this.clientFactory
			.withClient(async client => {
				const repository = new JobRepository(client, this.config)
				const jobs = await psql.withTx(client, () =>
					repository.poll(Array.from(types))
				)
				if (!jobs.length) {
					log.debug('No jobs found')
				}
				for (const job of jobs) {
					await this.processJob(client, repository, job)
				}
			})
			.catch(err => {
				log.error(err, 'Error while processing jobs')
				this.emit('error', err)
			})
			.finally(() => (this.run = undefined))

		await this.run
	}

	private async processJob<P>(
		client: pg.ClientBase,
		repository: JobRepository,
		job: RunningJob<P>
	): Promise<void> {
		const handlers = this.handlers.get(job.type)

		if (handlers) {
			log.debug(`Found ${handlers.length} handlers for job type ${job.type}`)

			const context = this.createContext(job)

			try {
				const res = await Promise.all(handlers.map(h => h(job, context)))

				const completedJob = await psql.withTx(client, async () => {
					await repository.delete(job.id)
					return repository.archive(
						completeJob(job, res.length > 1 ? res : res[0])
					)
				})
				log.debug('Job processed', completedJob)
			} catch (err) {
				log.error(err, 'Error while processing job')
				await psql.withTx(client, async () => {
					const error = errors.toError(err)
					//TODO: move to job history if attempts > max
					return repository.update(failJob(job, error))
				})
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
		const client = await this.clientFactory.persistent()
		const { schema, nodeId } = this.config
		await client.query(
			`SELECT ${client.escapeIdentifier(schema)}.SUBSCRIBE($1, $2)`,
			[nodeId, types]
		)
	}

	private async unsubscribe(...types: string[]): Promise<void> {
		const client = await this.clientFactory.persistent()
		const { schema, nodeId } = this.config
		await client.query(
			`SELECT ${client.escapeIdentifier(schema)}.UNSUBSCRIBE($1, $2)`,
			[nodeId, types]
		)
	}
}
