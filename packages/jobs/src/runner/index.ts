import { errors, logger, psql } from '@pgqueue/core'
import pg from 'pg'
import { AppliedConfig, JobContext, JobHandler } from '../core/index.js'
import { RunningJob, completeJob, failJob } from '../queue/models.js'
import { JobRepository } from '../queue/repository.js'

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
		setInterval(() => this.poll(), 1000)
	}

	async poll(): Promise<void> {
		log.debug('Polling for jobs')
		const pool = this.pool
		await psql.withClient(pool, async client => {
			const repository = new JobRepository(client, this.config)
			const job = await psql.withTx(client, async () =>
				repository.pop(Object.keys(this.handlers))
			)

			if (job) {
				const handler = this.handlers[job.type]
				if (handler) {
					const context = this.createContext(job)
					try {
						const res = await handler(job, context)

						const completedJob = await psql.withTx(client, async () => {
							await repository.delete(job.id)
							return repository.archive(completeJob(job, res))
						})
						log.debug('Job processed', completedJob)
					} catch (err) {
						log.error(err, 'Error while processing job')
						await psql.withTx(client, async () => {
							const error = errors.toError(err)
							//TODO: move to job history if attempts > max
							return repository.update(failJob(job, error))
						})
						throw err
					}
				}
			}
		})
	}

	private createContext<P>(_job: RunningJob<P>): JobContext<P> {
		return {
			postpone: async _ => {
				throw new Error('Not implemented')
			},
		}
	}
}
