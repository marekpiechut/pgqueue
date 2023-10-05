import { assert, async } from '@pgqueue/test-utils'
import pg from 'pg'
import sinon from 'sinon'
import * as broadcaster from './index.js'

describe('broadcaster', async () => {
	describe('start/stop', () => {
		let pgConstructor: typeof pg.Client
		beforeEach(() => {
			pgConstructor = pg.Client
		})
		afterEach(() => {
			pg.Client = pgConstructor
		})

		it('should start with any client', async () => {
			const client = sinon.createStubInstance(pg.Client)
			const events = broadcaster.fromClient(client)
			return await events.start()
		})

		it('can be started with client factory', async () => {
			const client = mockClient()
			const acquire = sinon.stub().returns(client)
			const release = sinon.stub().returns(Promise.resolve())
			const events = await broadcaster.fromFactory({ acquire, release })
			await events.start()
			sinon.assert.calledOnce(acquire)
		})

		it('does not open connection on start', async () => {
			const client = mockClient()
			const events = broadcaster.fromClient(client)
			await events.start()
			sinon.assert.notCalled(client.connect)
		})

		it('does not end client on shutdown', async () => {
			const client = mockClient()
			const events = broadcaster.fromClient(client)

			await events.start()
			await events.shutdown()

			sinon.assert.notCalled(client.end)
		})

		it('fails shutdown if UNLISTEN failed', async () => {
			const client = mockClient()
			client.query.throws(new Error('UNLISTEN failed'))

			const events = broadcaster.fromClient(client)
			await events.start()
			await assert.isRejected(events.shutdown())
		})

		it('UNLISTENS all on shutdown', async () => {
			const client = mockClient()

			const events = broadcaster.fromClient(client)
			await events.start()
			await events.on('test', () => {})

			client.query.resetHistory()
			await events.shutdown()

			assert.match(client.query.getCall(0).args[0], /UNLISTEN \*/i)
		})

		describe('listen/unlisten', () => {
			it('LISTENS on first listener for given type', async () => {
				const client = mockClient()
				const events = broadcaster.fromClient(client)
				await events.start()

				client.query.resetHistory()
				await events.on('test', () => {})

				const query = client.query.getCall(0).args[0]
				assert.include(query, 'pgqueue:broadcast:test')
				assert.include(query, 'LISTEN')
			})

			it('LISTENS only once for given type', async () => {
				const client = mockClient()

				const events = broadcaster.fromClient(client)
				await events.start()

				await events.on('test', () => {})
				await events.on('test', () => {})

				sinon.assert.calledOnce(client.query)
			})

			it('UNLISTENS only on last listener', async () => {
				const client = mockClient()
				const events = broadcaster.fromClient(client)
				await events.start()

				client.query.resetHistory()
				const unsubscribe1 = await events.on('test', () => {})
				const unsubscribe2 = await events.on('test', () => {})

				client.query.resetHistory()
				await unsubscribe1()
				sinon.assert.notCalled(client.query)

				await unsubscribe2()
				const query = client.query.getCall(0).args[0]
				assert.include(query, 'pgqueue:broadcast:test')
				assert.include(query, 'pgqueue:broadcast:test')
			})

			it('LISTENS for events registered before start', async () => {
				const client = mockClient()

				const events = broadcaster.fromClient(client)
				await events.on('test2', () => {})
				await events.start()

				const query = client.query.getCall(0).args[0]
				assert.include(query, 'pgqueue:broadcast:test2')
				assert.include(query, 'LISTEN')
			})
		})

		describe('emit events', () => {
			const mockPGListener = (
				client: ReturnType<typeof mockClient>
			): { ref: (message: pg.Notification) => void } => {
				const res: { ref: (message: pg.Notification) => void } = {
					ref: () => {
						throw new Error('No PG listener registered')
					},
				}
				client.on.callsFake((type, fn) => {
					if (type === 'notification') {
						res.ref = fn
					}
				})
				return res
			}
			it('sends emitted events to Postgres', async () => {
				const client = mockClient()
				const events = broadcaster.fromClient(client)
				await events.start()
				const listener = sinon.spy()
				await events.on('test', listener)
				const payload = { foo: 'bar' }

				client.query.resetHistory()
				await events.emit('test', payload)
				sinon.assert.calledOnce(client.query)

				const query = client.query.lastCall.firstArg
				assert.startsWith(query, 'NOTIFY')
				assert.include(query, 'pgqueue:broadcast:test')
				assert.include(query, JSON.stringify(payload))
			})

			it('receives emitted events', async () => {
				const client = mockClient()
				const pgListener = mockPGListener(client)
				const events = broadcaster.fromClient(client)
				await events.start()
				const listener = sinon.spy()
				await events.on('test', listener)
				const payload = { foo: 'bar' }
				await pgListener.ref({
					processId: 123,
					channel: 'pgqueue:broadcast:test',
					payload: JSON.stringify({ created: new Date(), payload }),
				})
				sinon.assert.calledOnce(listener)
				assert.equal(listener.firstCall.firstArg.type, 'test')
				assert.deepEqual(listener.firstCall.firstArg.payload, payload)
			})

			it('calls onError handler if could not process event', async () => {
				const client = mockClient()
				const pgListener = mockPGListener(client)
				const onError = sinon.spy()
				const events = broadcaster.fromClient(client, { onError })
				await events.start()
				const listener = sinon.spy()
				await events.on('test', listener)
				await pgListener.ref({
					processId: 123,
					channel: 'pgqueue:broadcast:test',
					payload: '{invalid: json}',
				})
				sinon.assert.calledOnce(onError)
			})

			it('calls onError handler if listener failed', async () => {
				const client = mockClient()
				const pgListener = mockPGListener(client)
				const onError = sinon.spy()
				const events = broadcaster.fromClient(client, { onError })
				await events.start()
				const listener = (): void => {
					throw new Error('Listener failed')
				}
				await events.on('test', listener)
				await pgListener.ref({
					processId: 123,
					channel: 'pgqueue:broadcast:test',
					payload: JSON.stringify({ foo: 'bar' }),
				})
				sinon.assert.calledOnce(onError)
			})

			it('continues with other listeners even if one has failed', async () => {
				const client = mockClient()
				const pgListener = mockPGListener(client)
				const onError = sinon.spy()
				const events = broadcaster.fromClient(client, { onError })
				await events.start()
				const failingListener = (): void => {
					throw new Error('Listener failed')
				}
				const listener = sinon.spy()
				await events.on('test', failingListener)
				await events.on('test', listener)
				await pgListener.ref({
					processId: 123,
					channel: 'pgqueue:broadcast:test',
					payload: JSON.stringify({ foo: 'bar' }),
				})
				sinon.assert.calledOnce(onError)
				sinon.assert.calledOnce(listener)
			})

			it('uses transaction connection for emitting events', async () => {
				const client = mockClient()
				const events = broadcaster.fromClient(client)
				await events.start()
				const payload = { foo: 'bar' }
				const txClient = sinon.createStubInstance(pg.Client)

				await events.withTx(txClient).emit('test', payload)
				client.query.resetHistory()
				sinon.assert.notCalled(client.query)
				sinon.assert.calledOnce(txClient.query)
			})
		})

		describe('reliability', () => {
			it('reconnects on connection error', async () => {
				const clients = [
					mockClient({
						on: sinon.stub().withArgs('error'),
					}),
					mockClient(),
				]
				let clientIndex = 0
				const acquire = sinon.stub().callsFake(() => clients[clientIndex++])
				const release = sinon.stub().returns(Promise.resolve())
				const events = await broadcaster.fromFactory(
					{ acquire, release },
					{ reconnectDelay: { type: 'constant', delay: 0 } }
				)
				events.on('test', () => {})
				await events.start()
				const onError = clients[0].on.firstCall.args[1] as (
					error: Error
				) => void
				onError?.(new Error('Dummy error'))

				await async.waitFor(() => acquire.callCount, 2, {
					label: 'Expected acquire called twice',
				})
				await async.waitFor(() => release.firstCall.firstArg, clients[0], {
					label: 'Expected release called with first client',
				})
			})
		})
	})
})

const mockClient = (
	customizer?: Parameters<typeof sinon.createStubInstance>[1]
): sinon.SinonStubbedInstance<pg.Client> => {
	const orig = new pg.Client({})
	const client = sinon.createStubInstance(pg.Client, customizer)
	client.escapeIdentifier.callsFake(s => orig.escapeIdentifier(s))
	client.escapeLiteral.callsFake(s => orig.escapeLiteral(s))
	return client
}
