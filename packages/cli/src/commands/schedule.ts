import { quickstart } from '@pgqueue/schedule'
import { Command, Option } from 'commander'
import pg from 'pg'
import { PAYLOAD_FORMAT_HELP, parsePayload, pgConfig } from './utils.js'
import date from 'date.js-two'

export const schedule = new Command('schedule')
schedule
	.description('Schedule a delayed or recurring job')
	.argument('name', 'job name')
	.argument('payload...', `job payload - ${PAYLOAD_FORMAT_HELP}`)
	.addOption(
		new Option(
			'-t, --time <delay>',
			'Delay in natural language ex.: "tomorrow night at 9", check (date.js) for details'
		).conflicts('cron')
	)
	.addOption(
		new Option(
			'-c --cron <cron>',
			'Cron expression ex.: "0 0 12 * * *", check (cron-parser) for details'
		).conflicts('time')
	)
	.option('--timezone <tz>', 'Timezone to use for scheduling')
	.action(async (type, data) => {
		const delay = schedule.opts().time
		const cron = schedule.opts().cron

		const parsedSchedule = delay ? date(delay) : cron

		if (
			parsedSchedule instanceof Date &&
			parsedSchedule.getTime() < Date.now()
		) {
			schedule.error(`Scheduled time is in the past (${parsedSchedule})`)
		}

		const opts = schedule.optsWithGlobals()
		const payload = data?.length ? parsePayload(data) : undefined
		const timezone = opts.timezone
		const scheduler = await quickstart(pgConfig(opts))
		const client = new pg.Client(pgConfig(opts))
		await client.connect()
		try {
			const job = await scheduler.schedule(
				client,
				type,
				parsedSchedule,
				payload,
				{ timezone }
			)
			console.log(`Job scheduled "${type}"`, job)
		} finally {
			client.end()
		}
	})
