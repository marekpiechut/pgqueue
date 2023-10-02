import { async, logger } from '@pgqueue/core'
import pg from 'pg'
import { AppliedConfig, DEFAULT_SCHEMA, JobHandler } from './models.js'
import { QueueManager } from './queue/manager.js'
import { JobOptions, PendingJob } from './queue/models.js'
import { JobRepository } from './queue/repository.js'
import { JobsRunner } from './runner.js'
import { Schedule } from './schedule/cron.js'
import { ScheduledJob, ScheduledJobOptions } from './schedule/models.js'
import { ScheduledJobRepository } from './schedule/repository.js'
import { Scheduler } from './schedule/scheduler.js'
import { applyEvolutions } from './schema/index.js'

const log = logger.create('jobs')

//TODO: This should be configurable
const config: AppliedConfig = {
	schema: DEFAULT_SCHEMA,
	nodeId: 'node-1',
}
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
		await applyEvolutions(config, connection)
		log.info('Datase schema is up to date, starting queues')
	} finally {
		connection.release()
	}
	return queues(pool)
}

export const evolutions = {
	apply: (
		client: pg.ClientBase,
		evoConfig: { destroy_my_data_AllowDownMigration?: boolean }
	): Promise<void> => {
		return applyEvolutions({ ...config, ...evoConfig }, client)
	},
}

const queues = (pool: pg.Pool): Queues => {
	const runner = new JobsRunner(pool, config)

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
	schedule<P>(
		name: string,
		schedule: Schedule,
		payload: P,
		options?: ScheduledJobOptions
	): Promise<ScheduledJob<P>>
}
export const queue = (client: pg.ClientBase): Queue => {
	const queueRepository = new JobRepository(client, config)
	const queueManager = new QueueManager(queueRepository)
	const scheduleRepository = new ScheduledJobRepository(client, config)
	const scheduler = new Scheduler(scheduleRepository)

	return {
		async push<P>(
			name: string,
			payload: P,
			options?: JobOptions
		): Promise<PendingJob<P>> {
			return queueManager.push(name, payload, options)
		},
		async schedule<P>(
			name: string,
			schedule: Schedule,
			payload: P,
			options?: ScheduledJobOptions
		): Promise<ScheduledJob<P>> {
			return scheduler.schedule(name, schedule, payload, options)
		},
	}
}

export default {
	DEFAULT_SCHEMA,
	queue,
	evolutions,
	fromPool,
}
