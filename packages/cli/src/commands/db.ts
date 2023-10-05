import jobs from '@pgqueue/jobs'
import { Command } from 'commander'
import pg from 'pg'
import { pgConfig } from './utils.js'

export const db = new Command('db').description(
	'Utilities to manage the database'
)

const evolutions = new Command('evolutions')
	.option(
		'-s, --schema <schema>',
		'Schema to apply evolutions to',
		jobs.DEFAULT_SCHEMA
	)
	.option(
		'--destroy-my-data-apply-down',
		"Apply down evolutions. DON'T DO THIS IN PRODUCTION !!!"
	)
	.description('Run database evolutions')
	.action(async () => {
		const opts = evolutions.opts()
		console.log(`Running database evolutions on schema ${opts.schema}`)

		const client = new pg.Client(pgConfig(evolutions.optsWithGlobals()))
		await client.connect()

		try {
			const options = {
				destroy_my_data_AllowDownMigration: opts.destroyMyDataApplyDown,
				schema: opts.schema,
			}
			await jobs.evolutions.apply(client, options)
		} finally {
			await client.end()
		}
	})
db.addCommand(evolutions)
