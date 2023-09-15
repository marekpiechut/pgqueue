import { afterEach, describe, it } from 'node:test'
import broadcaster, { Broadcaster } from '~/broadcast'
import { assert } from '~/utils/chai.test'
import * as db from './db'

describe('Broadcast', { timeout: 2000 }, () => {
	describe('Connection', () => {
		let events: Broadcaster
		afterEach(async () => {
			if (events) await events.shutdown()
		})

		it('connects to postgres', async () => {
			const client = db.client()
			events = broadcaster.fromClient(client)
			await events.start()
		})

		it('receives sent event', (_t, done) => {
			const client = db.client()
			events = broadcaster.fromClient(client)

			events.on('super-event', event => {
				assert.equal(event.type, 'super-event')
				assert.deepEqual(event.payload, { msg: 'hello' })
				assert.isTrue(event.self)
				done()
			})

			events.start().then(() => events.emit('super-event', { msg: 'hello' }))
		})
	})
})
