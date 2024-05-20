import {
	addDays,
	isAfter,
	isBefore,
	isSameDay,
	startOfDay,
	subDays,
} from 'date-fns'
import { groupBy, times } from 'lodash'
import logger from '~/common/logger'
import { DB, DBConnectionSpec } from '~/common/sql'
import { DEFAULT_SCHEMA } from '~/db'
import { QueueItemState } from '~/queues/models'
import { BasicStats, StatsHistogram } from './models'
import { Queries, withSchema } from './queries'

const log = logger('pgqueue:stats')

type Config = {
	schema?: string
}
const DEFAULT_CONFIG = {
	schema: DEFAULT_SCHEMA,
}

export class Stats {
	private tenantId?: string

	private constructor(
		private db: DB,
		private queries: Queries,
		private config: Config & typeof DEFAULT_CONFIG
	) {}

	static create(connectionSpec: DBConnectionSpec, config: Config): Stats {
		const connection = DB.create(connectionSpec)
		const mergedConfig = { ...DEFAULT_CONFIG, ...config }
		const queries = withSchema(mergedConfig.schema)
		return new Stats(connection, queries, mergedConfig)
	}

	withTenant(tenantId: string): this {
		const copy = new Stats(
			this.db.withTenant(tenantId),
			this.queries,
			this.config
		)
		copy.tenantId = tenantId
		return copy as this
	}

	async itemStats(
		queues: string[],
		after?: Date
	): Promise<Record<string, { [K in QueueItemState]: number }>> {
		const { db, queries } = this
		const rows = await db.execute(queries.queueStats(queues, after))
		const result = queues.reduce(
			(acc, queue) => {
				acc[queue] = {
					PENDING: 0,
					RETRY: 0,
					FAILED: 0,
					COMPLETED: 0,
					RUNNING: 0,
				}
				return acc
			},
			{} as Record<string, { [K in QueueItemState]: number }>
		)

		rows.forEach(row => {
			const current = result[row.queue]
			current[row.state] = row.count
		})

		return result
	}

	async queueHistogram(
		queues: string[],
		days: number
	): Promise<Record<string, StatsHistogram[]>> {
		const { db, queries } = this
		const res = await db.execute(queries.queueHistogram(queues, days))

		const firstDay = subDays(startOfDay(new Date()), days - 1)
		const byQueue = groupBy(res, 'queue')

		return queues.reduce(
			(acc, queue) => {
				const fromDb = byQueue[queue]
				if (!fromDb) {
					acc[queue] = times(days, n => ({
						date: addDays(firstDay, n),
						completed: 0,
						failed: 0,
					}))
					return acc
				}

				const result = []
				let day = firstDay
				let dbDayData = fromDb.shift()
				for (let i = 0; i < days; i++) {
					if (dbDayData && isSameDay(dbDayData.date, day)) {
						result.push({
							date: day,
							completed: dbDayData.completed,
							failed: dbDayData.failed,
						})
						dbDayData = fromDb.shift()
					} else if (dbDayData && isAfter(day, dbDayData.date)) {
						log.warn('Skipping day! Something is wrong with the data')
						dbDayData = fromDb.shift()
					} else {
						result.push({ date: day, completed: 0, failed: 0 })
					}
					day = addDays(day, 1)
				}

				acc[queue] = result

				return acc
			},
			{} as Record<string, StatsHistogram[]>
		)
	}

	async todayStats(): Promise<BasicStats> {
		const { db, queries } = this
		return db.execute(queries.todayStats())
	}

	async globalHistogram(from: Date, to: Date): Promise<StatsHistogram[]> {
		const { db, queries } = this
		const res = await db.execute(queries.globalHistogram(from, to))

		//Now fill in the gaps with zeros
		let currentDate = startOfDay(from)
		let idx = 0
		const result: StatsHistogram[] = []
		while (currentDate.getTime() <= to.getTime()) {
			let fromDb = res[idx]
			while (fromDb && isBefore(fromDb.date, currentDate) && idx < res.length) {
				idx++
				fromDb = res[idx]
			}

			if (fromDb && isSameDay(fromDb.date, currentDate)) {
				result.push(fromDb)
			} else {
				result.push({ date: currentDate, completed: 0, failed: 0 })
			}
			currentDate = addDays(currentDate, 1)
		}
		return result
	}
}
