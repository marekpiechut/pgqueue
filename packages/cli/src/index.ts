#!/usr/bin/env node
import { logger } from '@pgqueue/core'
import { program } from 'commander'
import { db } from 'commands/db.js'
import { emit, listen } from './commands/broadcast.js'
import { poll, push } from './commands/queue.js'
import { schedule } from 'commands/schedule.js'

program
	.name('pgqueue')
	.option('-p, --port <port>', 'Postgres port', '5432')
	.option('-h, --host <host>', 'Postgres host', 'localhost')
	.option('-u, --user <username>', 'Postgres user', 'postgres')
	.option('-w, --pass <password>', 'Postgres password', 'postgres')
	.option('-d, --db <database>', 'Postgres database', 'postgres')
	.option('-l --log-level <level>', 'Log level', 'warn')
	.hook('preAction', cmd => {
		const level = cmd.opts().logLevel
		if (level) {
			logger.setLevel(level)
		}
	})

program.addCommand(listen)
program.addCommand(emit)
program.addCommand(push)
program.addCommand(poll)
program.addCommand(db)
program.addCommand(schedule)

await program.parseAsync().catch(e => {
	console.error(e)
	process.exit(1)
})
