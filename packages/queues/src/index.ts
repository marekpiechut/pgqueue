import pg from 'pg'
import { logger } from '@pgqueue/core'
import { applyEvolutions } from 'schema'

const log = logger.create('queues')
const fromPool = async (pool: pg.Pool): Promise<void> => {
	log.info('Starting queues')
	const connection = await pool.connect()
	try {
		log.info('Checking database schema')
		await applyEvolutions({ typeSize: 32, baseName: 'pgqueues' }, connection)
		log.info('Datase schema is up to date, starting queues')
	} finally {
		connection.release()
	}
}

const pool = new pg.Pool({ user: 'postgres' })
const queues = fromPool(pool)
console.log('queues: ', queues)
