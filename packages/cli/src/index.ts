#!/usr/bin/env node
import chalk from 'chalk'
import { program } from 'commander'
import pg from 'pg'
import broadcast from '@pgqueue/broadcast'

const parsePayload = (data: string[]): Record<string, string> => {
	if (data.length === 1 && data[0].trim().startsWith('{')) {
		return JSON.parse(data[0])
	} else {
		return data.reduce(
			(acc, curr) => {
				const [key, value] = curr.split('=')
				acc[key] = value
				return acc
			},
			{} as Record<string, string>
		)
	}
}

program
	.option('-p, --port <port>', 'Postgres port', '5432')
	.option('-h, --host <host>', 'Postgres host', 'localhost')
	.option('-u, --user <username>', 'Postgres user', 'postgres')
	.option('-w, --pass <password>', 'Postgres password', 'postgres')
	.option('-d, --db <database>', 'Postgres database', 'postgres')

program
	.command('listen <channel>')
	.description('Start listening to a channel')
	.action(async channel => {
		const opts = program.opts()
		const events = await broadcast.fromPool(
			new pg.Pool({
				port: opts.port,
				host: opts.host,
				user: opts.user,
				password: opts.pass,
				database: opts.db,
			})
		)
		let last = new Date(0)
		events.on(channel, payload => {
			if (last.getTime() < payload.created.getTime() - 1000 * 30) {
				console.log(chalk.gray('----------------------------------------'))
			}
			console.log(chalk.green(JSON.stringify(payload, null, 2)))
			last = payload.created
		})
		await events.start()
	})

program
	.command('emit <channel> <payload...>')
	.description('Broadcast a payload to a channel')
	.action(async (channel, data) => {
		const payload = parsePayload(data)
		const opts = program.opts()
		const client = new pg.Client({
			port: opts.port,
			host: opts.host,
			user: opts.user,
			password: opts.pass,
			database: opts.db,
		})

		await client.connect()
		try {
			const events = await broadcast.fromClient(client)
			await events.start()
			await events.emit(channel, payload)
			await events.shutdown()
		} finally {
			await client.end()
		}
	})

program.parse(process.argv)
