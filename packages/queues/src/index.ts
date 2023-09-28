import { logger } from '@pgqueue/core'
import pg from 'pg'
import {
	JobOptions,
	PendingJob,
	RunningJob,
	completeJob,
	newJob,
	startJob,
} from './jobs/models.js'
import { JobRepository } from './jobs/repository.js'
import { applyEvolutions } from './schema/index.js'

export const DEFAULT_SCHEMA = 'pgqueues'
const log = logger.create('queues')

const dbConfig = { typeSize: 32, schema: DEFAULT_SCHEMA }
type JobContext<P> = {
	postpone: (options?: JobOptions) => Promise<PendingJob<P>>
}
type JobHandler<P, R> = (
	job: RunningJob<P>,
	context: JobContext<P>
) => Promise<R>

type Unsubscribe = () => Promise<void>
type Queues = {
	on<P, R>(name: string, handler: JobHandler<P, R>): Promise<Unsubscribe>
	off(name: string): Promise<void>
	start(): Promise<void>
}
const fromPool = async (pool: pg.Pool): Promise<Queues> => {
	log.info('Starting queues')
	const connection = await pool.connect()
	try {
		log.info('Checking database schema')
		await applyEvolutions(dbConfig, connection)
		log.info('Datase schema is up to date, starting queues')
	} finally {
		connection.release()
	}
	return queues(pool)
}

export const evolutions = {
	apply: (
		client: pg.ClientBase,
		config: { destroy_my_data_AllowDownMigration?: boolean }
	): Promise<void> => {
		return applyEvolutions({ ...dbConfig, ...config }, client)
	},
}

const queues = (pool: pg.Pool): Queues => {
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	const handlers: Record<string, JobHandler<any, any>> = {}

	const createContext = <P>(_job: RunningJob<P>): JobContext<P> => ({
		postpone: async _ => {
			throw new Error('Not implemented')
		},
	})

	const poll = async (): Promise<void> => {
		log.debug('Polling for jobs')
		const client = await pool.connect()
		try {
			const repository = new JobRepository(client, dbConfig)
			await client.query('BEGIN')
			const job = await repository.pop(Object.keys(handlers))
			if (job) {
				const handler = handlers[job.type]
				if (handler) {
					try {
						const startedJob = await repository.update(startJob(job))
						const context = createContext(startedJob)
						try {
							const res = await handler(startedJob, context)

							await repository.delete(startedJob.id)
							const completedJob = await repository.archive(
								completeJob(startedJob, res)
							)
							log.debug('Job processed', completedJob)
						} catch (err) {
							log.error(err, 'Error while processing job')
							//TODO: store error and update attempts
							//move to job history if attempts > max
							throw err
						}
					} catch (err) {
						log.error(err, 'Failed to start acquired job', job)
						throw err
					}
				}
			}
			await client.query('COMMIT')
		} catch (err) {
			await client.query('ROLLBACK')
		} finally {
			await client.release()
		}
	}

	const instance: Queues = {
		on: async (name, handler) => {
			log.debug('Registering handler for job type', name)
			const current = handlers[name]
			if (current != null) {
				log.warn('Overriding existing handler for job type', name)
			}

			handlers[name] = handler
			return () => {
				return instance.off(name)
			}
		},
		off: async name => {
			log.info('Removing handler for job type', name)
			delete handlers[name]
		},
		start: async () => {
			log.info('Starting queues with processors for', Object.keys(handlers))
			setInterval(poll, 1000)
		},
	}
	return instance
}

type Queue = {
	push<P>(
		name: string,
		payload: P,
		options?: JobOptions
	): Promise<PendingJob<P>>
}
export const queue = (client: pg.ClientBase): Queue => ({
	async push<P>(
		name: string,
		payload: P,
		options?: JobOptions
	): Promise<PendingJob<P>> {
		const repository = new JobRepository(client, dbConfig)
		const job = newJob(name, payload, options)
		log.debug(`Pushing job "${name}"`, job)
		const res = await repository.create(job)
		log.debug(`Job pushed "${name}"`, res)
		return res
	},
	// async schedule<P>(
	// 	name: string,
	// 	payload: P,
	// 	schedule: schedule.Schedule,
	// 	options?: ScheduledJobOptions
	// ): Promise<PendingJob<P>> {
	// 	const repository = new JobRepository(client, dbConfig)
	// 	const job = newJob(name, payload, options)
	// 	log.debug(`Scheduling job "${name}"`, job)
	// 	const res = await repository.create(job)
	// 	log.debug(`Job scheduled "${name}"`, res)
	// 	return res
	// },
})

export default {
	DEFAULT_SCHEMA,
	queue,
	evolutions,
	fromPool,
}
