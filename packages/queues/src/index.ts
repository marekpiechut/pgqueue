import { logger } from '@pgqueue/core'
import pg from 'pg'
import { Job, JobOptions, newJob } from './models.js'
import { JobRepository } from './persistence/job-repository.js'
import { applyEvolutions } from './schema/index.js'

const log = logger.create('queues')
const dbConfig = { typeSize: 32, schema: 'pgqueues' }
type JobContext<P> = {
	postpone: (options?: JobOptions) => Promise<Job<P>>
}
type JobHandler<P, R> = (job: Job<P>, context: JobContext<P>) => Promise<R>
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

const queues = (pool: pg.Pool): Queues => {
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	const handlers: Record<string, JobHandler<any, any>> = {}

	const poll = async (): Promise<void> => {
		log.debug('Polling for jobs')
		const client = await pool.connect()
		try {
			const repository = new JobRepository(client, dbConfig)
			await client.query('BEGIN')
			const next = await repository.pop(Object.keys(handlers))
			if (next) {
				const handler = handlers[next.type]
				if (handler) {
					try {
						const res = await handler(next, {
							postpone: async _ => {
								throw new Error('Not implemented')
							},
						})
						//TODO: handle result
						//TODO: consume job
						log.debug('Job processed', next.type, 'returned', res)
					} catch (err) {
						log.error(err, 'Error while processing job')
						throw err
					}
				}
			}
			client.query('COMMIT')
		} catch (err) {
			client.query('ROLLBACK')
		} finally {
			client.release()
		}
	}

	const instance: Queues = {
		//TODO: make sure we have only one handler for given job type
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
			log.debug('Removing handler for job type', name)
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
	push<P>(name: string, payload: P, options?: JobOptions): Promise<Job<P>>
}
export const queue = (client: pg.ClientBase): Queue => ({
	async push<P>(
		name: string,
		payload: P,
		options?: JobOptions
	): Promise<Job<P>> {
		const repository = new JobRepository(client, dbConfig)
		const job = newJob(name, payload, options)
		log.debug(`Pushing job "${name}"`, job)
		const res = await repository.push(job)
		log.debug(`Job pushed "${name}"`, res)
		return res
	},
})

export default {
	queue,
	fromPool,
}
