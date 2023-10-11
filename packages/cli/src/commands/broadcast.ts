import chalk from 'chalk'
import { Command, Option } from 'commander'
import pg from 'pg'
import broadcast from '@pgqueue/broadcast'
import { PAYLOAD_FORMAT_HELP, parsePayload, pgConfig } from './utils.js'

export const listen = new Command('listen')
listen
	.description('Start listening to a channel')
	.argument('channel', 'name of the channel to listen to')
	.addOption(
		new Option('--format <format>', 'Output format')
			.default('json')
			.choices(['short', 'json'])
	)
	.action(async channel => {
		const opts = listen.optsWithGlobals()
		const broadcaster = await broadcast.quickstart(pgConfig(opts))
		const formatter =
			EVENT_FORMATTERS[opts.format as keyof typeof EVENT_FORMATTERS] ||
			EVENT_FORMATTERS.json
		let count = 0
		let last = new Date(0)
		broadcaster.subscribe(channel, payload => {
			if (last.getTime() < payload.created.getTime() - 1000 * 30) {
				console.log(chalk.gray('----------------------------------------'))
			}
			console.log(formatter(++count, payload))
			last = payload.created
		})
		console.log(`Listening to ${channel}...`)
		await broadcaster.start()
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
			await broadcast.publish(client, channel, payload)
		} finally {
			await client.end()
		}
	})

const EVENT_FORMATTERS = {
	short: (count: number, event: { type: string }) =>
		`Event ${count}: ${chalk.green(event.type)}`,
	json: (_count: number, event: unknown) => JSON.stringify(event, null, 2),
}
