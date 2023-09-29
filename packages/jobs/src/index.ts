import { async, logger } from '@pgqueue/core'
import pg from 'pg'
import { QueueManager } from 'queue/manager.js'
import { JobsRunner } from 'runner.js'
import { JobHandler } from './models.js'
import { JobOptions, PendingJob } from './queue/models.js'
import { JobRepository } from './queue/repository.js'
import { applyEvolutions } from './schema/index.js'

export const DEFAULT_SCHEMA = 'pgqueues'
const log = logger.create('jobs')

const dbConfig = { schema: DEFAULT_SCHEMA }
type Queues = {
	on<P, R>(name: string, handler: JobHandler<P, R>): Promise<async.Unsubscribe>
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
	const runner = new JobsRunner(pool, dbConfig)

	return {
		on: async (name, handler) => {
			runner.addHander(name, handler)
			//TODO: This should wait for jobs to finish
			return async () => runner.removeHandler(name)
		},
		off: async name => {
			//TODO: This should wait for jobs to finish
			runner.removeHandler(name)
		},
		start: async () => {
			return runner.start()
		},
	}
}

type Queue = {
	push<P>(
		name: string,
		payload: P,
		options?: JobOptions
	): Promise<PendingJob<P>>
}
export const queue = (client: pg.ClientBase): Queue => {
	const queueRepository = new JobRepository(client, dbConfig)
	const queueManager = new QueueManager(queueRepository)
	return {
		async push<P>(
			name: string,
			payload: P,
			options?: JobOptions
		): Promise<PendingJob<P>> {
			return queueManager.push(name, payload, options)
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
	}
}

export default {
	DEFAULT_SCHEMA,
	queue,
	evolutions,
	fromPool,
}
