import * as pg from 'pg'
import { TenantId } from '~/common/models'
import { DBConnection, DBConnectionSpec } from '~/common/psql'
import { NewSchedule, Schedule, newSchedule } from './models'
import { ScheduleRepository } from './repository'

export type ScheduleManager = {
	withTenant(tenantId: TenantId): TenantScheduleManager
}
export type TenantScheduleManager = {
	fetchAll(): Promise<Schedule<unknown>[]>
	fetch<T>(id: Schedule<T>['id']): Promise<Schedule<T> | null>
	create<T>(schedule: NewSchedule<T>): Promise<Schedule<T>>
	// save<T>(schedule: Schedule<T> | NewSchedule<T>): Promise<Schedule<T>>
}

export type SchedulesConfig = {
	schema: string
}

export class Schedules implements ScheduleManager, TenantScheduleManager {
	private repository: ScheduleRepository
	private tenantId?: TenantId

	private constructor(repository: ScheduleRepository) {
		this.repository = repository
	}

	public static create(
		connectionSpec: DBConnectionSpec,
		config: SchedulesConfig
	): ScheduleManager {
		const connection = DBConnection.create(connectionSpec)
		const repository = new ScheduleRepository(config.schema, connection.pool)
		return new Schedules(repository)
	}

	withTenant(tenantId: string): TenantScheduleManager {
		const copy = new Schedules(this.repository.withTenant(tenantId))
		return copy
	}

	withTx(tx: pg.PoolClient): this {
		const copy = new Schedules(this.repository.withTx(tx))
		return copy as this
	}

	fetchAll(): Promise<Schedule<unknown>[]> {
		return this.repository.fetchAll()
	}
	fetch<T>(id: string): Promise<Schedule<T> | null> {
		return this.repository.fetchItem(id)
	}
	create<T>(input: NewSchedule<T>): Promise<Schedule<T>> {
		if (!this.tenantId) {
			throw new Error('TenantId is required to push new items')
		}
		const schedule = newSchedule(this.tenantId, input)
		return this.repository.insert(schedule)
	}
}
