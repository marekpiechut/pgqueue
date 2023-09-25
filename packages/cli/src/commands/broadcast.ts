import chalk from 'chalk'
import { Command } from 'commander'
import pg from 'pg'
import broadcast from '@pgqueue/broadcast'
import { PAYLOAD_FORMAT_HELP, parsePayload, pgConfig } from './utils.js'

export const listen = new Command('listen')
listen
	.description('Start listening to a channel')
	.argument('channel', 'name of the channel to listen to')
	.action(async channel => {
		const opts = listen.optsWithGlobals()
		const pool = new pg.Pool(pgConfig(opts))
		const events = await broadcast.fromPool(pool)
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
	.argument('payload...', `data to emit - ${PAYLOAD_FORMAT_HELP}`)
	.description('Broadcast a payload to a channel')
	.action(async (channel, data) => {
		const payload = parsePayload(data)
		const opts = emit.optsWithGlobals()
		const client = new pg.Client(pgConfig(opts))

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
