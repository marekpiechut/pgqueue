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
import { DBConnection, DBConnectionSpec, Repository } from '~/common/psql'
import { QueueItemState } from '~/queues/models'

const log = logger('pgqueue:stats')

export type BasicStats = {
	queues: number
	pending: number
	failedToday: number
	lastFailedAt?: Date
}

export type StatsHistogram = {
	date: Date
	completed: number
	failed: number
}

type Config = {
	schema: string
}
export class Stats extends Repository {
	public started: boolean = false

	private constructor(
		private connection: DBConnection,
		private config: Config
	) {
		super(connection.pool)
	}

	static create(connectionSpec: DBConnectionSpec, config: Config): Stats {
		const connection = DBConnection.create(connectionSpec)
		return new Stats(connection, config)
	}

	protected clone(): this {
		return new Stats(this.connection, this.config) as this
	}

	async itemStats(
		types: string[],
		after?: Date
	): Promise<Record<string, { [K in QueueItemState]: number }>> {
		let res
		if (after && after.getTime() > 0) {
			res = await this.execute<{
				queue: string
				state: QueueItemState
				count: number
			}>(
				`
				SELECT queue, state::VARCHAR, count(*) :: INTEGER as count
					FROM ${this.config.schema}.queue WHERE
					queue = ANY($1) AND created > $2
					GROUP BY queue, state
				UNION ALL
				SELECT queue, state::VARCHAR, count(*) :: INTEGER as count
					FROM ${this.config.schema}.queue_history WHERE
					queue = ANY($1) AND created > $2
					GROUP BY queue, state
		`,
				types,
				after
			)
		} else {
			res = await this.execute<{
				queue: string
				state: QueueItemState
				count: number
			}>(
				`
				SELECT queue, state::VARCHAR, count(*) :: INTEGER as count
					FROM ${this.config.schema}.queue WHERE
					queue = ANY($1) 
					GROUP BY queue, state
				UNION ALL
				SELECT queue, state::VARCHAR, count(*) :: INTEGER as count
					FROM ${this.config.schema}.queue_history WHERE
					queue = ANY($1)
					GROUP BY queue, state
		`,
				types
			)
		}

		return res.rows.reduce(
			(acc, row) => {
				const current = acc[row.queue] || {
					PENDING: 0,
					PROCESSING: 0,
					RETRY: 0,
					FAILED: 0,
					COMPLETED: 0,
					RUNNING: 0,
				}
				current[row.state] = row.count
				acc[row.queue] = current
				return acc
			},
			{} as Record<string, Record<QueueItemState, number>>
		)
	}

	async histogram(
		queues: string[],
		days: number
	): Promise<Record<string, StatsHistogram[]>> {
		const res = await this.execute<{
			queue: string
			date: Date
			completed: number
			failed: number
		}>(
			`
			SELECT
				queue,
				date_trunc('day', created) as date,
				SUM(CASE WHEN state = 'COMPLETED' THEN 1 ELSE 0 END) AS "completed",
				SUM(CASE WHEN state = 'FAILED' THEN 1 ELSE 0 END) AS "failed"
			FROM ${this.config.schema}.queue_history
			WHERE queue = ANY($1)
			AND created > (now() - ($2 || ' days')::INTERVAL)
			GROUP BY queue, date
			ORDER BY queue, date 
		`,
			queues,
			days
		)

		const firstDay = subDays(startOfDay(new Date()), days)
		const byQueue = groupBy(res.rows, 'queue')

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
		const { schema } = this.config
		//TODO: optimize this query
		//TODO: add cache
		//TODO: add timezone handling
		const res = await this.execute<{
			queues: number
			pending: number
			errors_today: number
			last_error: Date
		}>(`
			WITH pending AS (
				SELECT count(*) AS count FROM ${schema}.queue WHERE state = 'PENDING' OR state = 'RUNNING' OR state = 'RETRY'
			),
			queue_count AS (
				SELECT count(*) AS count FROM (SELECT DISTINCT tenant_id, queue FROM ${schema}.queue) AS q
			),
			errors_today AS (
				SELECT count(*) AS count FROM ${schema}.queue_history WHERE state='FAILED' AND created > (now() - interval '1 day')
			),
			last_error AS (
				SELECT max(created) AS created FROM ${schema}.queue_history WHERE state='FAILED' AND created > (now() - interval '1 day')
			)
			SELECT 
				pending.count as pending,	
				queue_count.count as queues,
				errors_today.count as errors_today,
				last_error.created as last_error
				FROM pending, queue_count, errors_today, last_error 
		`)

		const row = res.rows[0]
		return {
			queues: row.queues,
			pending: row.pending,
			failedToday: row.errors_today,
			lastFailedAt: row.last_error,
		}
	}

	async fetchHistogram(from: Date, to: Date): Promise<StatsHistogram[]> {
		//TODO: optimize this query
		//TODO: add timezone handling
		const res = await this.execute<StatsHistogram>(
			`
			SELECT
				date_trunc('day', created) as date,
				SUM(CASE WHEN state = 'COMPLETED' THEN 1 ELSE 0 END) AS "completed",
				SUM(CASE WHEN state = 'FAILED' THEN 1 ELSE 0 END) AS "failed"
			FROM ${this.config.schema}.queue_history
			WHERE created BETWEEN $1 AND $2
			GROUP BY date
			ORDER BY date
		`,
			from,
			to
		)

		let currentDate = startOfDay(from)
		let idx = 0
		const result: StatsHistogram[] = []
		while (currentDate.getTime() <= to.getTime()) {
			let fromDb = res.rows[idx]
			while (
				fromDb &&
				isBefore(fromDb.date, currentDate) &&
				idx < res.rows.length
			) {
				idx++
				fromDb = res.rows[idx]
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
