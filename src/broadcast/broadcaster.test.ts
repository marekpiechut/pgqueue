import { afterEach, beforeEach, describe, it, mock } from 'node:test'
import { assert } from '~/utils/chai.test'

import pg from 'pg'
import broadcaster from './broadcaster'
import { waitFor } from '~/utils/utils.test'

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
			const { client } = mockClient()
			const events = broadcaster.fromClient(client)
			return await events.start()
		})

		it('can be started with config', async () => {
			mock.method(pg.Client.prototype, 'on', () => {})
			const clientConstructorMock = mock.method(pg, 'Client', function () {
				const client = new pgConstructor({})
				mock.method(client, 'connect', () => Promise.resolve())
				mock.method(client, 'on', () => Promise.resolve())
				return client
			})
			const events = broadcaster.fromConfig({ user: 'zigi' })
			await events.start()

			assert.equal(clientConstructorMock.mock.calls.length, 1)
			assert.propertyVal(
				clientConstructorMock.mock.calls[0].arguments[0],
				'user',
				'zigi'
			)
		})

		it('can be started with client factory', async () => {
			const factory = mock.fn(() => mockClient().client)
			const events = broadcaster.fromClientFactory(factory)
			await events.start()
			assert.equal(factory.mock.calls.length, 1)
		})

		it('opens connection on start', async () => {
			const { client, mocks } = mockClient()
			const events = broadcaster.fromClient(client)
			await events.start()
			assert.equal(mocks.connect.mock.calls.length, 1)
		})

		it('closes client on shutdown', async () => {
			const { client } = mockClient()
			const endMock = mock.method(client, 'end', () => Promise.resolve())
			mock.method(client, 'query', () => Promise.resolve({}))

			const events = broadcaster.fromClient(client)

			await events.start()
			await events.shutdown()

			assert.equal(endMock.mock.calls.length, 1)
		})

		it('fails shutdown if UNLISTEN failed', async () => {
			const { client } = mockClient()
			mock.method(client, 'end', () => Promise.resolve())
			mock.method(client, 'query', () =>
				Promise.reject(new Error('UNLISTEN failed'))
			)

			const events = broadcaster.fromClient(client)
			await events.start()
			assert.isRejected(events.shutdown())
		})

		it('fails shutdown if client.end failed', async () => {
			const { client } = mockClient()
			mock.method(client, 'end', () =>
				Promise.reject(new Error('client.end failed'))
			)
			mock.method(client, 'query', () => Promise.resolve({}))

			const events = broadcaster.fromClient(client)
			await events.start()

			assert.isRejected(events.shutdown())
		})

		it('UNLISTENS all on shutdown', async () => {
			const { client } = mockClient()
			const queryMock = mock.method(client, 'query', () => Promise.resolve({}))

			const events = broadcaster.fromClient(client)
			await events.start()
			await events.on('test', () => {})

			queryMock.mock.resetCalls()
			await events.shutdown()

			assert.match(queryMock.mock.calls[0].arguments[0] || '', /UNLISTEN \*/i)
		})
	})

	describe('listen/unlisten', () => {
		it('LISTENS on first listener for given type', async () => {
			const { client } = mockClient()
			const queryMock = mock.method(client, 'query', () => Promise.resolve({}))

			const events = broadcaster.fromClient(client)
			await events.start()

			queryMock.mock.resetCalls()
			await events.on('test', () => {})

			const query = queryMock.mock.calls[0].arguments[0]
			assert.include(query, 'pgqueue:broadcast:test')
			assert.include(query, 'LISTEN')
		})

		it('LISTENS only once for given type', async () => {
			const { client } = mockClient()
			const queryMock = mock.method(client, 'query', () => Promise.resolve({}))

			const events = broadcaster.fromClient(client)
			await events.start()

			queryMock.mock.resetCalls()

			await events.on('test', () => {})
			await events.on('test', () => {})

			assert.equal(queryMock.mock.calls.length, 1)
		})

		it('UNLISTENS only on last listener', async () => {
			const { client } = mockClient()
			const queryMock = mock.method(client, 'query', () => Promise.resolve({}))

			const events = broadcaster.fromClient(client)
			await events.start()

			queryMock.mock.resetCalls()
			const unsubscribe1 = await events.on('test', () => {})
			const unsubscribe2 = await events.on('test', () => {})

			queryMock.mock.resetCalls()
			await unsubscribe1()
			assert.equal(queryMock.mock.calls.length, 0)

			await unsubscribe2()
			const query = queryMock.mock.calls[0].arguments[0]
			assert.include(query, 'pgqueue:broadcast:test')
			assert.include(query, 'pgqueue:broadcast:test')
		})

		it('LISTENS for events registered before start', async () => {
			const { client } = mockClient()
			const queryMock = mock.method(client, 'query', () => Promise.resolve({}))

			const events = broadcaster.fromClient(client)
			await events.on('test2', () => {})
			await events.start()

			const query = queryMock.mock.calls[0].arguments[0]
			assert.include(query, 'pgqueue:broadcast:test2')
			assert.include(query, 'LISTEN')
		})
	})

	describe('emit events', () => {
		const mockPGListener = (
			client: pg.Client
		): { ref: (message: pg.Notification) => void } => {
			const res: { ref: (message: pg.Notification) => void } = {
				ref: () => {
					throw new Error('No PG listener registered')
				},
			}

			mock.method(
				client,
				'on',
				(type: string, fn: (message: pg.Notification) => void) => {
					if (type === 'notification') {
						res.ref = fn
					}
				}
			)
			return res
		}
		it('sends emitted events to Postgres', async () => {
			const { client, mocks } = mockClient()
			const events = broadcaster.fromClient(client)
			await events.start()

			const listener = mock.fn()
			await events.on('test', listener)

			const payload = { foo: 'bar' }

			mocks.query.mock.resetCalls()
			await events.emit('test', payload)

			assert.equal(mocks.query.mock.calls.length, 1)
			const query = mocks.query.mock.calls[0].arguments[0] as string

			assert.startsWith(query, 'NOTIFY')
			assert.include(query, 'pgqueue:broadcast:test')
			assert.include(query, JSON.stringify(payload))
		})

		it('receives emitted events', async () => {
			const { client } = mockClient()
			const pgListener = mockPGListener(client)

			const events = broadcaster.fromClient(client)
			await events.start()

			const listener = mock.fn()
			await events.on('test', listener)

			const payload = { foo: 'bar' }
			await pgListener.ref({
				processId: 123,
				channel: 'pgqueue:broadcast:test',
				payload: JSON.stringify({ created: new Date(), payload }),
			})

			assert.equal(listener.mock.calls.length, 1)
			assert.equal(listener.mock.calls[0].arguments[0].type, 'test')
			assert.deepEqual(listener.mock.calls[0].arguments[0].payload, payload)
		})

		it('calls onError handler if could not process event', async () => {
			const { client } = mockClient()
			const pgListener = mockPGListener(client)

			const onError = mock.fn()
			const events = broadcaster.fromClient(client, { onError })
			await events.start()

			const listener = mock.fn()
			await events.on('test', listener)

			await pgListener.ref({
				processId: 123,
				channel: 'pgqueue:broadcast:test',
				payload: '{invalid: json}',
			})

			assert.equal(onError.mock.calls.length, 1)
		})

		it('calls onError handler if listener failed', async () => {
			const { client } = mockClient()
			const pgListener = mockPGListener(client)

			const onError = mock.fn()
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

			assert.equal(onError.mock.calls.length, 1)
		})

		it('continues with other listeners even if one has failed', async () => {
			const { client } = mockClient()
			const pgListener = mockPGListener(client)

			const onError = mock.fn()
			const events = broadcaster.fromClient(client, { onError })
			await events.start()

			const failingListener = (): void => {
				throw new Error('Listener failed')
			}
			const listener = mock.fn()
			await events.on('test', failingListener)
			await events.on('test', listener)

			await pgListener.ref({
				processId: 123,
				channel: 'pgqueue:broadcast:test',
				payload: JSON.stringify({ foo: 'bar' }),
			})

			assert.equal(onError.mock.callCount(), 1)
			assert.equal(listener.mock.callCount(), 1)
		})

		it('uses transaction connection for emitting events', async () => {
			const { client, mocks } = mockClient()

			const events = broadcaster.fromClient(client)
			await events.start()

			const payload = { foo: 'bar' }
			const txClient = new pg.Client({})
			const txQuery = mock.method(txClient, 'query', () => Promise.resolve({}))

			await events.withTx(txClient).emit('test', payload)

			mocks.query.mock.resetCalls()
			assert.equal(mocks.query.mock.calls.length, 0)
			assert.equal(txQuery.mock.calls.length, 1)
		})
	})

	describe('reliability', { timeout: 2000 }, () => {
		it('reconnects on connection error', async () => {
			const clients = [mockClient(), mockClient()]
			let clientIndex = 0

			const clientFactory = mock.fn(() => {
				return clients[clientIndex++].client
			})
			const events = broadcaster.fromClientFactory(clientFactory, {
				reconnectDelay: { type: 'constant', delay: 0 },
			})
			events.on('test', () => {})
			await events.start()

			const onError = clients[0].mocks.on.mock.calls.find(
				c => c.arguments[0] === 'error'
			)?.arguments[1] as (error: string) => void

			onError?.('Dummy error')

			await waitFor(() => clients[0].mocks.end.mock.calls.length, 1)
			await waitFor(() => clients[1].mocks.connect.mock.calls.length, 1)
			await waitFor(
				() => clients[1].mocks.query.mock.calls,
				value => {
					return !!value.find(
						c => c?.arguments?.[0]?.toString().match(/LISTEN\s+"?.*test"?/i)
					)
				}
			)
		})
	})
})

const mockClient = (
	instance?: pg.Client
): {
	client: pg.Client
	mocks: Record<keyof pg.Client, ReturnType<typeof mock.method>>
} => {
	const client = instance || new pg.Client({})
	const mocks: Record<string, ReturnType<typeof mock.method>> = {}
	mocks.connect = mock.method(client, 'connect', () => Promise.resolve())
	mocks.end = mock.method(client, 'end', () => Promise.resolve())
	mocks.query = mock.method(client, 'query', () => Promise.resolve({}))
	mocks.on = mock.method(client, 'on', () => {})
	return { client, mocks }
}
