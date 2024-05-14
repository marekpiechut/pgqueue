import pg from 'pg'
import { Duration, minutes } from '~/common/duration'
import { Managed, Repository } from '~/common/psql'
import { WorkItemRow } from '~/db/schema'
import { NewWorkItem, WorkItem } from '../models'

export class WorkQueueRepository extends Repository {
	constructor(
		private schema: string,
		pool: pg.Pool
	) {
		super(pool)
	}

	protected clone(): this {
		return new WorkQueueRepository(this.schema, this.pgPool) as this
	}

	public async poll(
		nodeId: string,
		batchSize: number,
		lockTimeout: Duration = minutes(2)
	): Promise<WorkItem[]> {
		const res = await this.execute<WorkItemRow>(
			`
				WITH next AS (
						SELECT *
						FROM ${this.schema}.WORK_QUEUE
						WHERE lock_key IS NULL OR lock_timeout < now()
						ORDER BY created, batch_order ASC
						LIMIT $1 FOR UPDATE SKIP LOCKED
				)
				UPDATE ${this.schema}.WORK_QUEUE as updated
				SET lock_key = $2,
						started = now(),
						version = updated.version + 1,
						lock_timeout = (now() + ($3 || ' seconds')::INTERVAL)
				FROM next
				WHERE updated.id = next.id
				RETURNING updated.*
				        `,
			batchSize,
			nodeId,
			lockTimeout * 1000
		)

		return res.rows.map(deserializeItem)
	}

	public async update(item: WorkItem): Promise<WorkItem> {
		const res = await this.execute<WorkItemRow>(
			`
			UPDATE ${this.schema}.WORK_QUEUE
			SET
				updated = now(),
				started = $2,
				lock_timeout = $4,
				version = version + 1
			WHERE id = $1 AND version = $5
			RETURNING *
		`,
			item.id,
			item.started,
			item.lockTimeout,
			item.version
		)

		if (res.rowCount != 1) {
			throw new Error('Version missmatch when updating work item: ' + item.id)
		}

		return deserializeItem(res.rows[0])
	}

	public async delete(item: WorkItem): Promise<void> {
		const res = await this.execute<WorkItemRow>(
			`
			DELETE FROM ${this.schema}.WORK_QUEUE
			WHERE id = $1 AND version = $2
		`,
			item.id,
			item.version
		)

		if (res.rowCount !== 1) {
			throw new Error('Failed to delete work item: ' + item.id)
		}
	}

	public async insertMany(items: NewWorkItem[]): Promise<void> {
		const res = await this.execute<WorkItemRow>(
			//We can safely ignore duplicates, as they will be processed anyway
			`
			INSERT INTO ${this.schema}.WORK_QUEUE (id, tenant_id, batch_order)
			SELECT * FROM UNNEST($1::uuid[], $2::varchar[], $3::integer[])
			ON CONFLICT DO NOTHING`,
			items.map(i => i.id),
			items.map(i => i.tenantId),
			items.map(i => i.batchOrder)
		)

		if (res.rowCount !== items.length) {
			throw new Error('Failed to insert work items')
		}
	}

	public async unlockAll(nodeId: string): Promise<void> {
		await this.execute<WorkItemRow>(
			`
			UPDATE ${this.schema}.WORK_QUEUE
			SET lock_key = NULL,
					lock_timeout = NULL,
					started = NULL
			WHERE lock_key = $1
		`,
			nodeId
		)
	}
}

const deserializeItem = (row: WorkItemRow): WorkItem & Managed => ({
	__managed: true,
	id: row.id,
	version: row.version,
	tenantId: row.tenant_id,
	created: row.created,
	batchOrder: row.batch_order,
	updated: row.updated,
	started: row.started,
	lockKey: row.lock_key,
	lockTimeout: row.lock_timeout,
})
