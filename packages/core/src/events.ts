import EventEmitter from 'events'

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type EmittedEvents = Record<string | symbol, (...args: any[]) => any>

export declare interface TypedEventEmitter<_Events extends EmittedEvents> {
	on<E extends keyof _Events>(event: E, listener: _Events[E]): this

	emit<E extends keyof _Events>(
		event: E,
		...args: Parameters<_Events[E]>
	): boolean
}

// eslint-disable-next-line @typescript-eslint/no-unsafe-declaration-merging
export class TypedEventEmitter<
	_Events extends EmittedEvents,
> extends EventEmitter {}
