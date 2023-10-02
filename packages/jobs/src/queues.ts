import pg from 'pg'
import { Config, DEFAULT_CONFIG } from './core/index.js'
import { QueueManager } from './queue/manager.js'
import { JobOptions, PendingJob } from './queue/models.js'
import { JobRepository } from './queue/repository.js'
import { Schedule } from './schedule/cron.js'
import { ScheduledJob, ScheduledJobOptions } from './schedule/models.js'
import { ScheduledJobRepository } from './schedule/repository.js'
import { Scheduler } from './schedule/scheduler.js'

export type Queue = {
	push<P>(
		name: string,
		payload: P,
		options?: JobOptions
	): Promise<PendingJob<P>>
	schedule<P>(
		name: string,
		schedule: Schedule,
		payload: P,
		options?: ScheduledJobOptions
	): Promise<ScheduledJob<P>>
}
export const create = (config?: Config): ((client: pg.ClientBase) => Queue) => {
	const appliedConfig = { ...DEFAULT_CONFIG, ...config }

	return client => {
		const queueRepository = new JobRepository(client, appliedConfig)
		const queueManager = new QueueManager(queueRepository)
		const scheduleRepository = new ScheduledJobRepository(client, appliedConfig)
		const scheduler = new Scheduler(scheduleRepository)

		return {
			async push<P>(
				name: string,
				payload: P,
				options?: JobOptions
			): Promise<PendingJob<P>> {
				return queueManager.push(name, payload, options)
			},
			async schedule<P>(
				name: string,
				schedule: Schedule,
				payload: P,
				options?: ScheduledJobOptions
			): Promise<ScheduledJob<P>> {
				return scheduler.schedule(name, schedule, payload, options)
			},
		}
	}
}

export default {
	create,
}
