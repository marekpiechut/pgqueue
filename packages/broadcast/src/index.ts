import { collections, errors, logger, psql } from '@pgqueue/core'
import EventEmitter from 'events'
import pg from 'pg'

const log = logger.create('broadcaster')

/**
 * Event is a simple object that is emitted by broadcaster. You'll receive it in your listener.
 */
export type Event<B> = {
	/** Type of the event, exactly like one passed during **emit**. */
	type: string
	/** Timestamp when event was emitted. According to clock on emitting system. */
	created: Date
	/** Deserialized body of the event. */
	payload: B
	/** If event origin is the current server, so you can skip events you have just emitted yourself. */
	self: boolean
}

type MsgV1<B> = {
	created: number
	payload: B
}

export type BroadcastListener<P> = (event: Event<P>) => Promise<void> | void

/**
 * Broadcaster is a simple event emitter that uses Postgres LISTEN/NOTIFY.
 * It's a perfect solution for ephemeral events that should be processed by
 * all cluster nodes and don't require any persistence.
 */
export class Broadcaster extends EventEmitter {
	private started = false
	private handlers: collections.Multimap<string, BroadcastListener<unknown>> =
		new collections.Multimap()

	constructor(private persistentConnection: psql.SharedPersistentConnection) {
		super()
	}

	public async publish<P>(
		client: pg.ClientBase,
		type: string,
		payload: P
	): Promise<void> {
		return publish(client, type, payload)
	}

	public withTx<P>(
		client: pg.ClientBase
	): (type: string, payload: P) => Promise<void> {
		return (type: string, payload: P): Promise<void> =>
			publish(client, type, payload)
	}

	public async start(): Promise<void> {
		log.info('Starting broadcaster')
		await this.persistentConnection.acquire(this.onConnection)
		this.started = true
		this.emit('started')
	}

	public async stop(force?: boolean): Promise<void> {
		try {
			await this.unlisten(...this.handlers.keys())
			const client = await this.persistentConnection.getClient()
			client.off('notification', this.onEvent)
		} catch (err) {
			this.emit('error', err)
			if (!force) throw err
		} finally {
			await this.persistentConnection.release(this.onConnection)
			this.started = false
		}

		this.emit('stopped')
	}

	public subscribe(
		type: string,
		listener: BroadcastListener<unknown>
	): () => Promise<void> {
		if (this.handlers.set(type, listener) && this.started) {
			this.listen(type)
		}
		return () => {
			return this.unsubscribe(type, listener)
		}
	}

	public async unsubscribe(
		type: string,
		listener: BroadcastListener<unknown>
	): Promise<void> {
		if (this.handlers.delete(type, listener) && this.started) {
			await this.unlisten(type)
		}
	}

	private onConnection = (async (client: pg.ClientBase): Promise<void> => {
		log.info('Persistent connection established, subscribing to events')
		client.on('notification', this.onEvent)

		await this.listen(...this.handlers.keys())
	}).bind(this)

	private onEvent = (async (msg: pg.Notification): Promise<void> => {
		const type = msg.channel
		const typeHandlers = this.handlers.get(type)
		if (!typeHandlers?.length) return

		let event: Event<unknown>
		try {
			if (!msg.payload) throw new Error('No payload')
			const client = await this.persistentConnection.getClient()
			const content = JSON.parse(msg.payload) as MsgV1<unknown>
			event = {
				type,
				created: new Date(content.created),
				payload: content.payload,
				// processID is not in TS types, but it's there
				// eslint-disable-next-line @typescript-eslint/no-explicit-any
				self: (client as any).processID === msg.processId,
			}
		} catch (e) {
			log.error('Error parsing event', msg.channel, e, msg)
			const error = errors.toError(e)
			this.emit('error', error)
			return
		}

		await Promise.all(
			typeHandlers.map(listener => {
				try {
					return listener(event)
				} catch (e) {
					log.error('Error in broadcast listener', msg.channel, e)
					const error = errors.toError(e)
					this.emit('error', error)
				}
			})
		)
	}).bind(this)

	private async listen(...types: string[]): Promise<void> {
		log.debug('Subscribing to event types', types)

		const client = await this.persistentConnection.getClient()
		for (const type of types) {
			log.debug('Listening for events:', type)
			await client.query(`LISTEN ${client.escapeIdentifier(type)}`)
		}
	}

	private async unlisten(...types: string[]): Promise<void> {
		log.debug('Unsubscribing from event types', types)

		const client = await this.persistentConnection.getClient()
		for (const type of types) {
			log.debug('Unlistening events:', type)
			await client.query(`UNLISTEN ${client.escapeIdentifier(type)}`)
		}
	}
}

export const publish = async <P>(
	client: pg.ClientBase,
	type: string,
	payload: P
): Promise<void> => {
	const msg: MsgV1<P> = {
		created: Date.now(),
		payload: payload,
	}
	const json = JSON.stringify(msg)
	await client.query(
		`NOTIFY ${client.escapeIdentifier(type)}, ${client.escapeLiteral(json)}`
	)
}
export const withTx =
	(client: pg.ClientBase) =>
	<P>(type: string, payload: P): Promise<void> =>
		publish(client, type, payload)

export const quickstart = async (
	configOrPool?: pg.PoolConfig | pg.Pool
): Promise<Broadcaster> => {
	const pool =
		configOrPool instanceof pg.Pool ? configOrPool : new pg.Pool(configOrPool)
	const persistentConnection = new psql.SharedPersistentConnection(pool)

	return new Broadcaster(persistentConnection)
}

export default {
	Broadcaster,
	quickstart,
	publish,
	withTx,
}
