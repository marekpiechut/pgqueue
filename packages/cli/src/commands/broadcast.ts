#!/usr/bin/env node
import chalk from 'chalk'
import { Command } from 'commander'
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

export const listen = new Command('listen')
listen
	.description('Start listening to a channel')
	.argument('channel', 'name of the channel to listen to')
	.action(async channel => {
		const opts = listen.optsWithGlobals()
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
		console.log(`Listening to ${channel}...`)
		await events.start()
	})

export const emit = new Command('emit')
emit
	.argument('channel', 'emit to this channel')
	.argument(
		'payload...',
		'data to emit (key=value pairs or JSON object as string)'
	)
	.description('Broadcast a payload to a channel')
	.action(async (channel, data) => {
		const payload = parsePayload(data)
		const opts = emit.optsWithGlobals()
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
