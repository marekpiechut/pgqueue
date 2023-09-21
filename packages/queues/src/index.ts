import pg from 'pg'
import { logger } from '@pgqueue/core'
import { applyEvolutions } from 'schema'
import { JobRepository } from './persistence/job-repository'
import { Job, JobOptions, newJob } from 'models'

const log = logger.create('queues')
const dbConfig = { typeSize: 32, baseName: 'pgqueues' }
const fromPool = async (pool: pg.Pool): Promise<void> => {
	log.info('Starting queues')
	const connection = await pool.connect()
	try {
		log.info('Checking database schema')
		await applyEvolutions(dbConfig, connection)
		log.info('Datase schema is up to date, starting queues')
	} finally {
		connection.release()
	}
}

type Queue = {
	push<P>(name: string, payload: P, options?: JobOptions): Promise<Job<P>>
}
const queue = (client: pg.ClientBase): Queue => ({
	async push<P>(
		name: string,
		payload: P,
		options?: JobOptions
	): Promise<Job<P>> {
		const repository = new JobRepository(client, dbConfig)
		const job = newJob(name, payload, options)
		return repository.push(job)
	},
})

const pool = new pg.Pool({ user: 'postgres' })

const workWork = async (): Promise<void> => {
	await fromPool(pool)
	queue(await pool.connect()).push('test', { hello: 'world' })
}

workWork()
console.log('done')
