#!/usr/bin/env node

import { queues, processors } from '@pgqueue/jobs'
import { Command } from 'commander'
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
	.argument('name', 'job name')
	.argument('payload...', `job payload - ${PAYLOAD_FORMAT_HELP}`)
	.action(async (name, data) => {
		const opts = push.opts()
		const client = new pg.Client(pgConfig(push.optsWithGlobals()))
		await client.connect()
		try {
			const payload = data?.length ? parsePayload(data) : undefined
			const queue = await queues.create()(client)
			const job = await queue.push(name, payload, { priority: opts.priority })
			console.log(`Job pushed "${name}"`, job)
		} finally {
			await client.end()
		}
	})

export const poll = new Command('poll')
poll
	.description('Listen to queue and consume jobs.')
	.argument('name', 'job name')
	.argument('[result...]', `job result - ${PAYLOAD_FORMAT_HELP}`)
	.action(async (name, data) => {
		if (!poll.opts().yes) {
			const answer = await ask(`
WARNING: You are about to start polling the queue "${name}".
This will consume jobs from the queue and process them.
Jobs captured by this process will not be available for other consumers.
If you just want to watch the queue without altering it's contents, use the "peek" command instead.
			
Enter "yes" to continue.\n`)
			if (answer.trim() !== 'yes') {
				console.log('Bye')
				return
			}
		}

		const opts = push.optsWithGlobals()
		const pool = new pg.Pool(pgConfig(opts))
		await pool.connect()
		const queue = await processors.fromPool(pool)
		const result = data?.length ? parsePayload(data) : undefined
		console.warn(
			`Polling queue ${name} ${
				result ? 'with result: \n' + JSON.stringify(result, null, 2) + '\n' : ''
			}(THIS WILL CONSUME JOBS !!!)`
		)
		queue.on(name, async job => {
			console.log(`Job consumed: "${name}"`, job)
			return result
		})
		queue.start()
	})

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
