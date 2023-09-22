#!/usr/bin/env node
import { program } from 'commander'
import { emit, listen } from './commands/broadcast.js'
import { logger } from '@pgqueue/core'

logger.setLevel('error')

program
	.option('-p, --port <port>', 'Postgres port', '5432')
	.option('-h, --host <host>', 'Postgres host', 'localhost')
	.option('-u, --user <username>', 'Postgres user', 'postgres')
	.option('-w, --pass <password>', 'Postgres password', 'postgres')
	.option('-d, --db <database>', 'Postgres database', 'postgres')

program.addCommand(listen)
program.addCommand(emit)

program.parse(process.argv)
