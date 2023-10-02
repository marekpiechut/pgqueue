import { logger } from '@pgqueue/core'
import { Schedule } from './cron'
import { ScheduledJob, ScheduledJobOptions, newSchedule } from './models'
import { ScheduledJobRepository } from './repository'

const log = logger.create('jobs:schedule')
export class Scheduler {
	constructor(private repository: ScheduledJobRepository) {}

	public async schedule<P>(
		type: string,
		schedule: Schedule,
		payload: P,
		options?: ScheduledJobOptions
	): Promise<ScheduledJob<P>> {
		const job = newSchedule(type, schedule, payload, options)
		log.debug(`Scheduling job "${type}"`, job)
		const res = await this.repository.create(job)
		log.debug(`Job scheduled"${type}"`, res)
		return res
	}
}
