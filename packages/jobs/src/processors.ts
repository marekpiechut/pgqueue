import { async, logger } from '@pgqueue/core'
import pg from 'pg'
import {
	AppliedConfig,
	Config,
	DEFAULT_CONFIG,
	JobHandler,
} from './core/index.js'
import { JobsRunner } from './runner/index.js'
import { applyEvolutions } from './schema/index.js'

const log = logger.create('jobs')

export type Processors = {
	on<P, R>(name: string, handler: JobHandler<P, R>): Promise<async.Unsubscribe>
	off(name: string): Promise<void>
	start(): Promise<void>
}
export const fromPool = async (
	pool: pg.Pool,
	config?: Config
): Promise<Processors> => {
	log.info('Starting queues')
	const appliedConfig = { ...DEFAULT_CONFIG, ...config }
	const connection = await pool.connect()
	try {
		log.info('Checking database schema')
		await applyEvolutions(appliedConfig, connection)
		log.info('Datase schema is up to date, starting queues')
	} finally {
		connection.release()
	}
	return createProcessors(pool, appliedConfig)
}

const createProcessors = (pool: pg.Pool, config: AppliedConfig): Processors => {
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

export default {
	fromPool,
}
