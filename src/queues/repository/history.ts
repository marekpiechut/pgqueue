import pg from 'pg'
import { PagedResult, UUID } from '~/common/models'
import { Repository, SortOrder } from '~/common/psql'
import { PagedFetcher, createPagedFetcher } from '~/db/paged'
import { QueueHistoryRow } from '~/db/schema'
import { AnyHistory, QueueHistory, QueueHistoryState } from '../models'

export class QueueHistoryRepository extends Repository {
	private fetchPage: PagedFetcher<QueueHistoryRow>
	constructor(
		private schema: string,
		pool: pg.Pool,
		fetchPage?: PagedFetcher<QueueHistoryRow>
	) {
		super(pool)
		this.fetchPage =
			fetchPage ||
			createPagedFetcher<QueueHistoryRow>(`${schema}.queue_history`, 'queue=$1')
	}

	protected clone(): this {
		return new QueueHistoryRepository(
			this.schema,
			this.pgPool,
			this.fetchPage
		) as this
	}

	public async fetchItem(id: AnyHistory['id']): Promise<AnyHistory | null> {
		const res = await this.execute<QueueHistoryRow>(
			`SELECT * FROM ${this.schema}.queue_history WHERE id = $1`,
			id
		)
		return res.rows.map(deserializeItem)[0] || null
	}

	public async fetchItems(
		queue: string,
		limit: number = 100,
		after?: UUID | null | undefined | 'FIRST',
		before?: UUID | null | undefined | 'LAST',
		sort?: SortOrder
	): Promise<PagedResult<AnyHistory>> {
		return this.execute(async client => {
			const page = await this.fetchPage(
				client,
				[queue],
				limit,
				after,
				before,
				sort
			)

			return {
				...page,
				items: page.items.map(deserializeItem),
			}
		})
	}

	public async save<T, R>(
		history: QueueHistory<T, R>
	): Promise<QueueHistory<T, R>> {
		const res = await this.execute<QueueHistoryRow>(
			`
			INSERT INTO ${this.schema}.queue_history (
				id,
				tenant_id,
				key,
				type,
				queue,
				created,
				scheduled,
				started,
				state,
				delay,
				tries,
				payload,
				payload_type,
				result,
				result_type,
				target,
				worker_data,
				error
			) VALUES (
				$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18
			)
			RETURNING *
		`,

			history.id,
			history.tenantId,
			history.key,
			history.type,
			history.queue,
			history.created,
			history.scheduled,
			history.started,
			history.state,
			history.delay,
			history.tries,
			history.payload,
			history.payloadType,
			history.result,
			history.resultType,
			history.target,
			history.workerData,
			history.error
		)

		if (res.rowCount !== 1) {
			throw new Error('Failed to save history')
		}
		return deserializeItem(res.rows[0])
	}
}

const deserializeItem = <T, R>(row: QueueHistoryRow): QueueHistory<T, R> => ({
	id: row.id,
	tenantId: row.tenant_id,
	key: row.key,
	type: row.type,
	started: row.started,
	queue: row.queue,
	created: row.created,
	scheduled: row.scheduled,
	state: row.state as QueueHistoryState,
	delay: row.delay,
	tries: row.tries,
	payload: row.payload,
	payloadType: row.payload_type,
	result: row.result,
	resultType: row.result_type,
	target: row.target as T,
	workerData: row.worker_data as R,
	error: row.error,
})
