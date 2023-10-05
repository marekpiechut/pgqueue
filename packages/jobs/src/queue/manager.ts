import { logger } from '@pgqueue/core'
import { JobOptions, PendingJob, newJob } from './models.js'
import { JobRepository } from './repository.js'

const log = logger.create('jobs:queue')
export class QueueManager {
	constructor(private repository: JobRepository) {}

	public async push<P>(
		type: string,
		payload: P,
		options?: JobOptions
	): Promise<PendingJob<P>> {
		const job = newJob(type, payload, options)
		log.debug(`Pushing job "${type}"`, job)
		const res = await this.repository.create(job)
		log.debug(`Job pushed "${type}"`, res)
		return res
	}
}
