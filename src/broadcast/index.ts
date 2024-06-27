// import pg from 'pg'
// import * as collections from '~/common/collections'
// import logger from '~/common/logger'
// import { DBConnection } from '~/common/psql'
// import { DBConnectionSpec } from '~/common/sql'

// const log = logger('pgqueue:broadcast')

// /**
//  * Event is a simple object that is emitted by broadcaster. You'll receive it in your listener.
//  */
// export type Event<B> = {
// 	/** Type of the event, exactly like one passed during **emit**. */
// 	type: string
// 	/** Timestamp when event was emitted. According to clock on emitting system. */
// 	created: Date
// 	/** Deserialized body of the event. */
// 	payload: B
// 	/** If event origin is the current server, so you can skip events you have just emitted yourself. */
// 	self: boolean
// }

// type MsgV1<B> = {
// 	created: number
// 	payload: B
// }

// export type BroadcastListener<P> = (event: Event<P>) => Promise<void> | void

// export class Broadcaster {
// 	private connection: DBConnection
// 	private clientId: number | null = null
// 	private started = false
// 	private handlers: collections.Multimap<string, BroadcastListener<unknown>> =
// 		new collections.Multimap()

// 	private constructor(connection: DBConnection) {
// 		this.connection = connection
// 	}

// 	public static create(connectionSpec: DBConnectionSpec): Broadcaster {
// 		const connection = DBConnection.create(connectionSpec)
// 		return new Broadcaster(connection)
// 	}

// 	public async start(): Promise<void> {
// 		log.info('Starting broadcaster')
// 		await this.connection.acquire(this.onConnection)
// 		this.started = true
// 	}

// 	private onConnection = (async (client: pg.ClientBase): Promise<void> => {
// 		log.info('Persistent connection established, subscribing to events')
// 		client.on('notification', this.onEvent)

// 		//ProcessID is not in types, but it's there
// 		// eslint-disable-next-line @typescript-eslint/no-explicit-any
// 		this.clientId = (client as any).processID
// 		log.info('Client ID:', this.clientId)
// 		await this.listen(...this.handlers.keys())
// 	}).bind(this)

// 	public async stop(): Promise<void> {
// 		if (!this.started) return
// 		await this.unlisten(...this.handlers.keys())
// 		const client = await this.connection.getClient()
// 		client.off('notification', this.onEvent)
// 		await this.connection.release(this.onConnection)
// 		this.started = false
// 	}

// 	public async emit<P>(
// 		client: pg.ClientBase,
// 		type: string,
// 		payload: P
// 	): Promise<void> {
// 		return emit(client, type, payload)
// 	}

// 	public withTx<P>(
// 		client: pg.ClientBase
// 	): (type: string, payload: P) => Promise<void> {
// 		return (type: string, payload: P): Promise<void> =>
// 			emit(client, type, payload)
// 	}

// 	public subscribe(
// 		type: string,
// 		listener: BroadcastListener<unknown>
// 	): () => Promise<void> {
// 		if (this.handlers.set(type, listener) && this.started) {
// 			this.listen(type)
// 		}
// 		return () => {
// 			return this.unsubscribe(type, listener)
// 		}
// 	}

// 	public async unsubscribe(
// 		type: string,
// 		listener: BroadcastListener<unknown>
// 	): Promise<void> {
// 		if (this.handlers.delete(type, listener) && this.started) {
// 			await this.unlisten(type)
// 		}
// 	}

// 	private onEvent = (async (msg: pg.Notification): Promise<void> => {
// 		const type = msg.channel

// 		const typeHandlers = this.handlers.get(type)
// 		if (!typeHandlers?.length) return

// 		let event: Event<unknown>
// 		try {
// 			if (!msg.payload) throw new Error('No payload')
// 			const content = JSON.parse(msg.payload) as MsgV1<unknown>
// 			event = {
// 				type,
// 				created: new Date(content.created),
// 				payload: content.payload,
// 				self: this.clientId === msg.processId,
// 			}
// 		} catch (e) {
// 			log.error('Error parsing event', msg.channel, e, msg)
// 			return
// 		}

// 		await Promise.allSettled(
// 			typeHandlers.map(listener => {
// 				try {
// 					return listener(event)
// 				} catch (e) {
// 					log.error('Error in broadcast listener', msg.channel, e)
// 				}
// 			})
// 		)
// 	}).bind(this)

// 	private async listen(...types: string[]): Promise<void> {
// 		log.debug('Subscribing to event types', types)

// 		const client = await this.connection.getClient()
// 		if (!client) throw new Error('No client, cannot unlisten')
// 		for (const type of types) {
// 			log.debug('Listening for events:', type)
// 			await client.query(`LISTEN ${client.escapeIdentifier(type)}`)
// 		}
// 	}

// 	private async unlisten(...types: string[]): Promise<void> {
// 		log.debug('Unsubscribing from event types', types)
// 		const client = await this.connection.getClient()
// 		if (!client) throw new Error('No client, cannot unlisten')
// 		for (const type of types) {
// 			log.debug('Unlistening events:', type)
// 			await client.query(`UNLISTEN ${client.escapeIdentifier(type)}`)
// 		}
// 	}
// }

// export const emit = async <P>(
// 	client: pg.ClientBase,
// 	type: string,
// 	payload: P
// ): Promise<void> => {
// 	const msg: MsgV1<P> = {
// 		created: Date.now(),
// 		payload: payload,
// 	}
// 	const json = JSON.stringify(msg)
// 	await client.query(
// 		`NOTIFY ${client.escapeIdentifier(type)}, ${client.escapeLiteral(json)}`
// 	)
// }
