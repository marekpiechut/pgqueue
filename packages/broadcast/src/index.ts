import pg from 'pg'
import { logger, retry, psql, errors } from '@pgqueue/core'

const log = logger.create('broadcaster')

const MAX_RESTART_DELAY = 5000

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
	/** If event origin is the current server */
	self: boolean
}

type MsgV1<B> = {
	created: number
	payload: B
}

type Unsubscribe = () => Promise<void>
type Emitter<P> = (type: string, payload: P) => Promise<void>
export type BroadcastListener<P> = (event: Event<P>) => void | Promise<void>
/**
 * Broadcaster is a simple event emitter that uses Postgres LISTEN/NOTIFY.
 * It's a perfect solution for ephemeral events that should be processed by
 * all cluster nodes and don't require any persistence.
 */
export type Broadcaster = {
	/**
	 * Start broadcaster and listren for events.
	 * Make sure all your listeners are registered before calling this method
	 * or you might miss some events.
	 */
	start: () => Promise<void>
	/**
	 * Subscribe to events of a particular type. Only first listener will execute
	 * database `LISTEN` command, all subsequent listeners will just be added to local list.
	 *
	 *
	 * @param type Event type to listen to
	 * @param listener Event handler, can be async and throwing errors will not crash the broadcaster.
	 * @returns Unsubscribe function, call it to stop listening for events with this particular listener.
	 */
	on: <P>(type: string, listener: BroadcastListener<P>) => Promise<Unsubscribe>

	/**
	 * Emit event to a global channel. All active listeners on all systems will receive it.
	 * Remember that current system is not filtered out and if you listen for the same event type
	 * **this system will also be notified** and will process the event. Usually it's a desired behavior
	 * as system should be operational even if there's only one working instance. If you want different behaviour
	 * you can filter out current system in your listener.
	 *
	 * @param event Event to emit, make sure it is **JSON serializable**.
	 */
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	emit: Emitter<any>
	/**
	 * Get an emitter using a different client. Useful to emit events within transaction.
	 *
	 * This method is very useful and allows for transactional events. Whenever you want to emit
	 * events indicating that database entity you probably should use this method **instead of `emit`**.
	 * This way, if transaction was rolled back, events will not be emitted. This provides a very powerful
	 * way to keep your database and event bus in sync.
	 * @see {@link https://www.postgresql.org/docs/current/sql-notify.html} for more details about how Postgres handles transactions and notifications.
	 *
	 * If you use this method you are responsible for closing the client and committing the transaction.
	 *
	 * @see {@link emit} for details about event emission itself.
	 * @param client client to use, probably wit a running transaction
	 * @returns Emitter that uses provided client
	 */
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	withTx: (client: pg.Client) => { emit: Emitter<any> }
	/**
	 * Shutdown broadcaster. Unsuscribe all listeners **and close database connection**.
	 */
	shutdown: () => Promise<void>
}

const EVENT_BASE = 'pgqueue:broadcast'

export type BroadcasterConfig = {
	/**
	 * Custom handler for listener errors. Errors will be logged anyway, so if you don't need anything
	 * special, you can skip it. Note that **errors are not recoverable** and will not block other listeners.
	 *
	 * Pass in your handler if you have some external system that you need to notify of errors, or need a custom
	 * logging.
	 */
	onError?: (e: Error) => void
	/**
	 * Custom error handler for client errors. Client errors are fatal and will restart the connection.
	 * Note that **errors are not recoverable**.
	 *
	 * Pass in your handler if you have some external system that you need to notify of errors, or need a custom
	 * logging.
	 */
	onClientError?: (e: Error) => void
	/**
	 * Custom reconnect delay. By default it will retry every second,
	 * but you can change it to anything you like.
	 */
	reconnectDelay?: retry.Delay
}

/**
 * Create new broadcaster. Usually you want to create only one
 * per application and share it across all modules.
 *
 * This instance will **not** handle reconnects on client errors.
 * You need to handle them yourself and re-create the broadcaster.
 * Check Client.on('error') event to register error handler.
 *
 * It's usually better to use {@link fromPool} instead.
 * As it will handle all errors and reconnect when needed.
 *
 * PG client **must not** be from the pool and should not be reused for other purposes
 * as crashing the connection might break the broadcaster.
 * Best is to just create a new Client, connect it, pass here and forget about it until you need to shut down.
 * Remember, that **you are responsible for closing the client** after shutdown.
 *
 * Make sure you **register all listeners before calling `start`** method or you might miss some events.
 *
 * @param client PG client **must not** be from the pool, and has to be connected. You are responsible for closing it after shutdown.
 */
export const fromClient = (
	client: pg.Client | pg.PoolClient,
	config?: BroadcasterConfig
): Broadcaster & { setClient: (client: pg.Client | pg.PoolClient) => void } => {
	//eslint-disable-next-line @typescript-eslint/no-explicit-any
	const listeners: Record<string, BroadcastListener<any>[]> = {}

	return {
		start: async (): Promise<void> => {
			log.info('Starting broadcaster')
			client.on('notification', async msg => {
				const type = msg.channel.substring(EVENT_BASE.length + 1)
				const eventListeners = listeners[type]
				if (!eventListeners?.length) return

				let event: Event<unknown>
				try {
					if (!msg.payload) throw new Error('No payload')
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
					config?.onError?.(error)
					return
				}

				eventListeners.forEach(listener => {
					try {
						listener(event)
					} catch (e) {
						log.error('Error in broadcast listener', msg.channel, e)
						const error = errors.toError(e)
						config?.onError?.(error)
					}
				})
			})

			for (const type in listeners) {
				log.debug('Listening for events:', type)
				await client.query(
					`LISTEN ${client.escapeIdentifier(EVENT_BASE + ':' + type)}`
				)
			}
		},
		on: async (type, listener): Promise<Unsubscribe> => {
			if (listeners[type]) {
				listeners[type].push(listener)
			} else {
				listeners[type] = [listener]
				await client.query(
					`LISTEN ${client.escapeIdentifier(EVENT_BASE + ':' + type)}`
				)
			}
			return async () => {
				listeners[type] = listeners[type].filter(l => l !== listener)
				if (listeners[type].length === 0) {
					await client.query(
						`UNLISTEN ${client.escapeIdentifier(EVENT_BASE + ':' + type)}`
					)
					delete listeners[type]
				}
			}
		},
		emit: <P>(type: string, payload: P) => emit(client, type, payload),
		withTx: client => ({
			emit: (type, payload) => emit(client, type, payload),
		}),
		setClient: (newClient): void => {
			client = newClient
		},
		shutdown: async (): Promise<void> => {
			log.info('Shutting down broadcaster')
			for (const type in listeners) {
				delete listeners[type]
			}
			await client.query('UNLISTEN *')
			log.info('Broadcaster shutdown complete')
		},
	}
}

/**
 * **This is the preferred way to create broadcaster in production apps**
 *
 * Creates new broadcaster. Usually you want to create only one
 * per application and share it across all modules.
 *
 * Broadcaster created with this method will handle all errors
 * and reconnect when needed.
 *
 * Keep in mind that creating a new broadcaster will permanenly occupy
 * one of pool connections.
 *
 * Make sure you **register all listeners before calling `start`** method or you might miss some events.
 */
export const fromPool = async (
	pool: pg.Pool,
	config?: BroadcasterConfig
): Promise<Broadcaster> => {
	return fromFactory(poolConnectionFactory(pool), config)
}

export const fromFactory = async (
	factory: psql.ClientFactory,
	config?: BroadcasterConfig
): Promise<Broadcaster> => {
	const retryDelay = config?.reconnectDelay ?? { type: 'constant', delay: 1000 }

	let attempt = 0
	const reconnect = async (): Promise<void> => {
		client = await factory.acquire()
		client.on('error', handleError)

		instance.setClient(client)
		instance
			.start()
			.then(() => {
				attempt = 0
			})
			.catch(e => {
				log.error('Error starting broadcaster', e)
				const waitTime = retry.nextRun(retryDelay, ++attempt, MAX_RESTART_DELAY)
				setTimeout(reconnect, waitTime)
			})
	}
	const handleError = async (e: Error): Promise<void> => {
		log.error(e, 'Client error, restarting connection')
		try {
			config?.onClientError?.(e)
		} catch (e) {
			log.error(e, 'Error in onClientError handler')
		}

		if (client) factory.release(client).catch(log.error)
		const waitTime = retry.nextRun(retryDelay, attempt++, MAX_RESTART_DELAY)
		setTimeout(reconnect, waitTime)
	}

	let client: pg.Client | pg.PoolClient = await factory.acquire()
	const instance = fromClient(client, config)
	client.on('error', handleError)

	return {
		...instance,
		shutdown: async (): Promise<void> => {
			try {
				await instance.shutdown()
			} finally {
				log.debug("Releasing broadcaster's client")
				await factory.release(client)
			}
		},
	}
}

const emit = async <P>(
	client: pg.Client | pg.PoolClient,
	type: string,
	payload: P
): Promise<void> => {
	const id = EVENT_BASE + ':' + type
	const msg: MsgV1<P> = {
		created: Date.now(),
		payload: payload,
	}
	const json = JSON.stringify(msg)
	await client.query(
		`NOTIFY ${client.escapeIdentifier(id)}, ${client.escapeLiteral(json)}`
	)
}

const poolConnectionFactory = (pool: pg.Pool): psql.ClientFactory => ({
	acquire: async (): Promise<pg.Client | pg.PoolClient> => {
		return pool.connect()
	},
	release: async (client: pg.Client | pg.PoolClient): Promise<void> => {
		if ((client as pg.PoolClient).release) {
			return (client as pg.PoolClient).release()
		} else {
			throw new Error('Cannot release client not from the pool')
		}
	},
})

export default {
	fromClient,
	fromPool,
}
