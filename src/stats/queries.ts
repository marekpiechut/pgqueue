import { Query, firstRequired, sql } from '~/common/sql'
import { QueueItemState } from '~/queues'
import { BasicStats, StatsHistogram } from './models'

export type Queries = ReturnType<typeof withSchema>
export const withSchema = (schema: string) =>
	({
		queueStats: (queues: string[], after?: Date): Query<QueueStateStats> => {
			if (after) {
				return sql<QueueStateStats>(schema)`
					SELECT queue, state::VARCHAR, count(*) :: INTEGER as count
						FROM {{schema}}.queue WHERE
						queue = ANY(${queues}) AND created > ${after}
						GROUP BY queue, state
					UNION ALL
					SELECT queue, state::VARCHAR, count(*) :: INTEGER as count
						FROM {{schema}}.queue_history WHERE
						queue = ANY(${queues}) AND created > ${after}
						GROUP BY queue, state
				`
			} else {
				return sql<QueueStateStats>(schema)`
					SELECT queue, state::VARCHAR, count(*) :: INTEGER as count
						FROM {{schema}}.queue WHERE
						queue = ANY(${queues}) 
						GROUP BY queue, state
					UNION ALL
					SELECT queue, state::VARCHAR, count(*) :: INTEGER as count
						FROM {{schema}}.queue_history WHERE
						queue = ANY(${queues})
						GROUP BY queue, state
					`
			}
		},
		queueHistogram: (queues: string[], days: number) => sql<QueueHistogram>(
			schema
		)`
				SELECT
					queue,
					date_trunc('day', created) as date,
					SUM(CASE WHEN state = 'COMPLETED' THEN 1 ELSE 0 END) AS "completed",
					SUM(CASE WHEN state = 'FAILED' THEN 1 ELSE 0 END) AS "failed"
				FROM {{schema}}.queue_history
				WHERE queue = ANY(${queues})
				AND created > (now() - (${days} || ' days')::INTERVAL)
				GROUP BY queue, date
				ORDER BY queue, date 
			`,
		//TODO: optimize this query
		//TODO: add cache
		//TODO: add timezone handling
		todayStats: () => sql(schema, todayMapper, firstRequired)`
				WITH pending AS (
					SELECT count(*) AS count FROM {{schema}}.queue WHERE state = 'PENDING' OR state = 'RUNNING' OR state = 'RETRY'
				),
				queue_count AS (
					SELECT count(*) AS count FROM (SELECT DISTINCT tenant_id, queue FROM {{schema}}.queue) AS q
				),
				errors_today AS (
					SELECT count(*) AS count FROM {{schema}}.queue_history WHERE state='FAILED' AND created > (now() - interval '1 day')
				),
				last_error AS (
					SELECT max(created) AS created FROM {{schema}}.queue_history WHERE state='FAILED' AND created > (now() - interval '1 day')
				)
				SELECT 
					pending.count as pending,	
					queue_count.count as queues,
					errors_today.count as errors_today,
					last_error.created as last_error
					FROM pending, queue_count, errors_today, last_error
			`,
		//TODO: optimize this query
		//TODO: add timezone handling
		globalHistogram: (from: Date, to: Date) => sql<StatsHistogram>(schema)`
			SELECT
				date_trunc('day', created) as date,
				SUM(CASE WHEN state = 'COMPLETED' THEN 1 ELSE 0 END) AS "completed",
				SUM(CASE WHEN state = 'FAILED' THEN 1 ELSE 0 END) AS "failed"
			FROM {{schema}}.queue_history
			WHERE created BETWEEN ${from} AND ${to}
			GROUP BY date
			ORDER BY date
		`,
	}) as const

type QueueStateStats = {
	queue: string
	state: QueueItemState
	count: number
}
type QueueHistogram = {
	queue: string
	date: Date
	completed: number
	failed: number
}

const todayMapper = (row: {
	queues: number
	pending: number
	errors_today: number
	last_error: Date
}): BasicStats => ({
	queues: row.queues,
	pending: row.pending,
	failedToday: row.errors_today,
	lastFailedAt: row.last_error,
})
