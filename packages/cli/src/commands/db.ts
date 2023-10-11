import queue from '@pgqueue/queue'
import schedule from '@pgqueue/schedule'
import { Command } from 'commander'
import pg from 'pg'
import { pgConfig } from './utils.js'

export const db = new Command('db').description(
	'Utilities to manage the database'
)

const evolutions = new Command('evolutions')
	.option(
		'--queue-schema <schema>',
		'Database schema for queues',
		queue.DEFAULT_SCHEMA
	)
	.option(
		'--schedule-schema <schema>',
		'Database schema for schedules',
		schedule.DEFAULT_SCHEMA
	)
	.option(
		'--destroy-my-data-apply-down',
		"Apply down evolutions. DON'T DO THIS IN PRODUCTION !!!"
	)
	.description('Run database evolutions')
	.action(async () => {
		const opts = evolutions.opts()
		console.log(
			`Running database evolutions on schemas: ${opts.queueSchema}, ${opts.scheduleSchema}`
		)

		const client = new pg.Client(pgConfig(evolutions.optsWithGlobals()))
		await client.connect()

		try {
			await queue.evolutions.apply(client, {
				destroy_my_data_AllowDownMigration: opts.destroyMyDataApplyDown,
				schema: opts.queueSchema,
			})
			await schedule.evolutions.apply(client, {
				destroy_my_data_AllowDownMigration: opts.destroyMyDataApplyDown,
				schema: opts.scheduleSchema,
			})
		} finally {
			await client.end()
		}
	})
db.addCommand(evolutions)
