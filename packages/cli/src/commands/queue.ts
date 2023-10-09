#!/usr/bin/env node

import { PGQueue } from '@pgqueue/jobs'
import { Command, Option } from 'commander'
import chalk from 'chalk'
import pg from 'pg'
import { PAYLOAD_FORMAT_HELP, ask, parsePayload, pgConfig } from './utils.js'

export const push = new Command('push')
push
	.description('Push a job to queue')
	.option(
		'--priority <number>',
		'job priority, lower value means higher priority',
		parseInt
	)
	.argument('type', 'job type')
	.argument('payload...', `job payload - ${PAYLOAD_FORMAT_HELP}`)
	.action(async (type, data) => {
		const opts = push.opts()
		const client = new pg.Client(pgConfig(push.optsWithGlobals()))
		await client.connect()
		try {
			const payload = data?.length ? parsePayload(data) : undefined
			const queue = PGQueue.fromClient(client, {})
			const job = await queue.push(client, {
				type,
				payload,
				priority: opts.priority,
			})
			console.log(`Job pushed "${chalk.green(type)}"`, job)
		} finally {
			await client.end()
		}
	})

export const poll = new Command('poll')
poll
	.description('Listen to queue and consume jobs.')
	.addOption(
		new Option('--output <format>', 'Output format')
			.default('json')
			.choices(['short', 'json'])
	)
	.option('-y --yes', 'Skip confirmation')
	.argument('name', 'job name')
	.argument('[result...]', `job result - ${PAYLOAD_FORMAT_HELP}`)
	.action(async (name, data) => {
		const opts = poll.optsWithGlobals()
		if (!opts.yes) {
			const answer = await ask(`
${chalk.red('WARNING')}: You are about to start polling the queue "${chalk.bold(
				name
			)}".
This will consume jobs from the queue and process them.
Jobs captured by this process will not be available for other consumers.
If you just want to watch the queue without altering it's contents, use the "peek" command instead.
			
Enter "yes" to continue.\n`)
			if (answer.trim() !== 'yes') {
				console.log('Bye')
				return
			}
		}

		const pool = new pg.Pool(pgConfig(opts))
		await pool.connect()
		const queue = PGQueue.fromPool(pool, {})
		const result = data?.length ? parsePayload(data) : undefined
		console.warn(
			`Subscribed to queue "${chalk.bold(name)}" ${
				result ? 'with result: \n' + JSON.stringify(result, null, 2) + '\n' : ''
			}${chalk.red('THIS WILL CONSUME JOBS !!!')}`
		)
		const formatter =
			JOB_FORMATTERS[opts.output as keyof typeof JOB_FORMATTERS] ||
			JOB_FORMATTERS.json
		let count = 0
		queue.addHandler(name, async job => {
			console.log(formatter(++count, job))
			return result
		})
		queue.start()
	})

const JOB_FORMATTERS = {
	short: (count: number, job: { id: string }) =>
		`Job ${count} processed: ${chalk.green(job.id)}`,
	json: (_count: number, job: unknown) => JSON.stringify(job, null, 2),
}
// export const peek = new Command('peek')
// peek
// 	.description('Listen to queue and peek at jobs.')
// 	.argument('name', 'job name')
// 	.action(async name => {
// 		const opts = push.optsWithGlobals()
// 		const pool = new pg.Pool(pgConfig(opts))
// 		await pool.connect()
// 		const queue = await queues.fromPool(pool)
// 		queue.peek(name, async job => {
// 			console.log(`Job consumed"${name}"`, job)
// 			console.log(`Job consumed"${name}"`, job)
// 		})
// 		console.log(`Monitoring ${name}...`)
// 		queue.start()
// 	})
