import * as pg from 'pg'
import { TenantId } from '~/common/models'
import { DB, DBConnectionSpec } from '~/common/sql'
import { DEFAULT_SCHEMA } from '~/db'
import { NewSchedule, Schedule, newSchedule } from './models'
import { Queries, withSchema } from './queries'

export type ScheduleManager = {
	withTenant(tenantId: TenantId): TenantScheduleManager
}
export type TenantScheduleManager = {
	fetchAll(): Promise<Schedule<unknown>[]>
	fetch<T>(id: Schedule<T>['id']): Promise<Schedule<T> | undefined>
	create<T>(schedule: NewSchedule<T>): Promise<Schedule<T>>
	// save<T>(schedule: Schedule<T> | NewSchedule<T>): Promise<Schedule<T>>
}

export type SchedulesConfig = {
	schema?: string
}

const DEFAULT_CONFIG = {
	schema: DEFAULT_SCHEMA,
}

export class Schedules implements ScheduleManager, TenantScheduleManager {
	private tenantId?: TenantId

	private constructor(
		private db: DB,
		private queries: Queries,
		private config: SchedulesConfig & typeof DEFAULT_CONFIG
	) {}

	public static create(
		connectionSpec: DBConnectionSpec,
		config: SchedulesConfig
	): ScheduleManager {
		const connection = DB.create(connectionSpec)
		const mergedConfig = { ...DEFAULT_CONFIG, ...config }
		const queries = withSchema(mergedConfig.schema)

		return new Schedules(connection, queries, mergedConfig)
	}

	withTenant(tenantId: string): TenantScheduleManager {
		const copy = new Schedules(
			this.db.withTenant(tenantId),
			this.queries,
			this.config
		)
		copy.tenantId = tenantId
		return copy
	}

	withTx(tx: pg.PoolClient): this {
		const copy = new Schedules(this.db.withTx(tx), this.queries, this.config)
		return copy as this
	}

	fetchAll(): Promise<Schedule<unknown>[]> {
		const { db, queries } = this
		return db.execute(queries.fetchAll())
	}
	fetch<T>(id: string): Promise<Schedule<T> | undefined> {
		const { db, queries } = this
		return db.execute(queries.fetch(id))
	}
	create<T>(input: NewSchedule<T>): Promise<Schedule<T>> {
		this.requireTenant()
		const { db, queries } = this
		const schedule = newSchedule(this.tenantId!, input)
		return db.execute(queries.insert(schedule))
	}
	private requireTenant(message?: string): void {
		if (!this.tenantId) {
			throw new Error(message || 'TenantId is required')
		}
	}
}
