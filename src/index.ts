import pg from 'pg'
// import * as broadcast from './broadcast'
import * as logger from './common/logger'
import * as sql from './common/sql'
import { applyEvolutions } from './db/schema'
import * as queues from './queues'
import * as schedules from './schedules'
import * as stats from './stats'

export {
	MimeType,
	MimeTypes,
	MAX_ERROR_LEN,
	MAX_KEY_LEN,
	MAX_NAME_LEN,
} from './common/models'

export type Config = {
	schema: string
}
export type Quickstart = {
	start: () => Promise<void>
	stop: () => Promise<void>
	queues: queues.QueueManager
	// broadcaster: broadcast.Broadcaster
	stats: stats.Stats
	schedules: schedules.ScheduleManager
	scheduler: (pollInterval: number, batchSize: number) => queues.Scheduler
	worker: (
		handler: queues.WorkerHandler,
		config: Omit<queues.WorkerConfig, 'schema'>
	) => queues.Worker
	workerMetadata: queues.WorkerMetadata
}

export default {
	initialize: async (
		clientSpec: pg.ClientBase | string,
		config: {
			schema: string
			allowDown?: boolean
			logger?: logger.LoggerDelegate
		}
	): Promise<void> => {
		if (config.logger) {
			logger.setDelegate(config.logger)
		}

		let adminClient
		if (typeof clientSpec === 'string') {
			adminClient = new pg.Client(clientSpec)
			await adminClient.connect()
		} else {
			adminClient = clientSpec
		}
		try {
			await applyEvolutions(adminClient, config)
		} finally {
			if (typeof clientSpec === 'string') {
				await (adminClient as pg.Client).end()
			}
		}
	},
	quickstart: async (
		pool: pg.Pool,
		config: { schema: string }
	): Promise<Quickstart> => {
		validateConfig(config)
		const db = sql.DB.create(pool)

		// const broadcaster = broadcast.Broadcaster.create(db)
		const queueStats = stats.Stats.create(db, config)
		const queueManager = queues.Queues.create(db, config)
		const scheduleManager = schedules.Schedules.create(db, config)
		const workerMetadata = queues.WorkerMetadata.create(db, config)
		return {
			// broadcaster: broadcaster,
			queues: queueManager,
			stats: queueStats,
			schedules: scheduleManager,
			workerMetadata: workerMetadata,
			worker: (handler, workerConfig) =>
				queues.Worker.create(db, { ...config, ...workerConfig }, handler),
			scheduler: (pollInterval, batchSize) =>
				queues.Scheduler.create(db, { ...config, pollInterval, batchSize }),
			start: async () => {
				// await broadcaster.start()
			},
			stop: async () => {
				// await broadcaster.stop()
			},
		}
	},
}

const validateConfig = (config: Config): void => {
	if (!config.schema) {
		throw new Error('schema is required')
	}
}
