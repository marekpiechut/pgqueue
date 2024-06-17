import { addMonths, format, isBefore, startOfMonth } from 'date-fns'
import { hours } from '~/common/duration'
import logger from '~/common/logger'
import { RerunImmediately, pollingLoop } from '~/common/polling'
import { DB, DBConnectionSpec } from '~/common/sql'
import { DEFAULT_SCHEMA } from '~/db'
import { Queries, withSchema } from './queries'
import { mergeConfig } from '~/common/config'

const log = logger('pgqueue:maintenance')

const PARTITIONS_UPFRONT_MONTHS = 3

export type MaintenanceConfig = {
	schema?: string
	interval?: number
}

const DEFAULT_CONFIG = {
	schema: DEFAULT_SCHEMA,
	interval: hours(24),
}

export class Maintenance {
	private abort: AbortController | undefined

	private constructor(
		private db: DB,
		private queries: Queries,
		private config: MaintenanceConfig & typeof DEFAULT_CONFIG
	) {
		this.config = config
		this.db = db
		this.queries = queries
	}

	public static create(
		connectionSpec: DBConnectionSpec,
		config?: MaintenanceConfig
	): Maintenance {
		const mergedConfig = mergeConfig(DEFAULT_CONFIG, config)
		const db = DB.create(connectionSpec)
		const queries = withSchema(mergedConfig.schema)
		return new Maintenance(db, queries, mergedConfig)
	}

	async start(): Promise<void> {
		log.info('Starting maintenance worker', this.config)
		this.abort = new AbortController()
		pollingLoop(this.runTask, this.config.interval, this.abort.signal)
	}

	async stop(): Promise<void> {
		if (!this.abort) return
		log.info('Shutting down maintenance worker', this.config)
		this.abort.abort()
		this.abort = undefined
	}

	private runTask = (async (): Promise<RerunImmediately> => {
		log.info('Running maintenance task')
		const { db, queries } = this
		return db.transactional(async withTx => {
			const lastPartition = await withTx.execute(
				queries.fetchLastHistoryPartition()
			)

			let nextPartition = lastPartition
				? addMonths(lastPartition, 1)
				: startOfMonth(new Date())

			const requiredLastPartition = addMonths(
				new Date(),
				PARTITIONS_UPFRONT_MONTHS
			)

			while (isBefore(nextPartition, requiredLastPartition)) {
				log.info(
					'Creating queue history partition %s',
					format(nextPartition, 'yyyy_MM')
				)
				await withTx.execute(queries.createHistoryPartition(nextPartition))
				nextPartition = addMonths(nextPartition, 1)
			}

			return false
		})
	}).bind(this)
}
