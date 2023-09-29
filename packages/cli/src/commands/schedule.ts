#!/usr/bin/env node

import jobs from '@pgqueue/jobs'
import { Command, Option } from 'commander'
import pg from 'pg'
import { PAYLOAD_FORMAT_HELP, parsePayload, pgConfig } from './utils.js'
import date from 'date.js-ts'

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
	.action(async (name, data) => {
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
		const client = new pg.Client(pgConfig(opts))
		await client.connect()
		try {
			const payload = data?.length ? parsePayload(data) : undefined
			const queue = await jobs.queue(client)
			const job = await queue.schedule(name, parsedSchedule, payload)
			console.log(`Job scheduled "${name}"`, job)
		} finally {
			await client.end()
		}
	})
