import { logger } from '@pgqueue/core'
import pg from 'pg'
import { AppliedConfig, JobContext, JobHandler } from './models.js'
import { RunningJob, completeJob, startJob } from './queue/models.js'
import { JobRepository } from './queue/repository.js'

const log = logger.create('jobs:runner')

export class JobsRunner {
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	private handlers: Record<string, JobHandler<any, any>> = {}
	constructor(
		private pool: pg.Pool,
		private config: AppliedConfig
	) {}

	public addHander<P, R>(name: string, handler: JobHandler<P, R>): void {
		log.debug('Registering handler for job type', name)
		if (this.handlers[name]) {
			log.warn('Overwriting handler for job type', name)
		}
		this.handlers[name] = handler
	}

	public removeHandler(name: string): void {
		log.info('Removing handler for job type', name)
		delete this.handlers[name]
	}

	public async start(): Promise<void> {
		log.info('Starting PGQueue jobs runner')
	}

	async poll(): Promise<void> {
		log.debug('Polling for jobs')
		const pool = this.pool
		const client = await pool.connect()
		try {
			const repository = new JobRepository(client, this.config)
			await client.query('BEGIN')
			const job = await repository.pop(Object.keys(this.handlers))
			if (job) {
				const handler = this.handlers[job.type]
				if (handler) {
					try {
						const startedJob = await repository.update(startJob(job))
						const context = this.createContext(startedJob)
						try {
							const res = await handler(startedJob, context)

							await repository.delete(startedJob.id)
							const completedJob = await repository.archive(
								completeJob(startedJob, res)
							)
							log.debug('Job processed', completedJob)
						} catch (err) {
							log.error(err, 'Error while processing job')
							//TODO: store error and update attempts
							//move to job history if attempts > max
							throw err
						}
					} catch (err) {
						log.error(err, 'Failed to start acquired job', job)
						throw err
					}
				}
			}
			await client.query('COMMIT')
		} catch (err) {
			await client.query('ROLLBACK')
		} finally {
			await client.release()
		}
	}

	private createContext<P>(_job: RunningJob<P>): JobContext<P> {
		return {
			postpone: async _ => {
				throw new Error('Not implemented')
			},
		}
	}
}
