import { ScheduledJob, quickstart } from '@pgqueue/schedule'
import { Command, Option } from 'commander'
import pg from 'pg'
import { PAYLOAD_FORMAT_HELP, ask, parsePayload, pgConfig } from './utils.js'
import date from 'date.js-two'
import chalk from 'chalk'

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

export const execute = new Command('execute')
execute
	.description('Execute scheduled jobs')
	.addOption(
		new Option('--format <format>', 'Output format')
			.default('json')
			.choices(['short', 'long', 'json'])
	)
	.option('-y --yes', 'Skip confirmation')
	.argument('type', 'job type')
	.argument('[result...]', `job result - ${PAYLOAD_FORMAT_HELP}`)
	.action(async (type, data) => {
		const opts = execute.optsWithGlobals()
		if (!opts.yes) {
			const answer = await ask(`
${chalk.red(
	'WARNING'
)}: You are about to start executing scheduled jobs "${chalk.bold(type)}".
This will consume scheduled jobs and process them.
Jobs captured by this process will not be available for other consumers.
			
Enter "yes" to continue.\n`)
			if (answer.trim() !== 'yes') {
				console.log('Bye')
				return
			}
		}

		const jobs = await quickstart(pgConfig(opts))
		const result = data?.length ? parsePayload(data) : undefined
		console.warn(
			`Processing events ${
				result ? 'with result: \n' + JSON.stringify(result, null, 2) + '\n' : ''
			}${chalk.red('THIS WILL CONSUME JOBS !!!')}`
		)
		const formatter =
			JOB_FORMATTERS[opts.format as keyof typeof JOB_FORMATTERS] ||
			JOB_FORMATTERS.json
		let count = 0
		jobs.addHandler(type, async job => {
			console.log(formatter(++count, job))
			return result
		})
		jobs.start()
	})

const JOB_FORMATTERS = {
	short: (count: number, job: ScheduledJob<unknown>) =>
		`Job ${count} processed at ${new Date().toTimeString()}: ${chalk.green(
			job.id
		)}`,
	long: (count: number, job: ScheduledJob<unknown>) => `
		Job ${count} processed at ${new Date().toTimeString()}: ${chalk.green(
			job.id
		)}, scheduled at ${JSON.stringify(job.schedule, null, 2)}`,
	json: (_count: number, job: unknown) => JSON.stringify(job, null, 2),
}
