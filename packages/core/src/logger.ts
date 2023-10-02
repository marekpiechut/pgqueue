//TODO: change to non-blocking logger

const LEVELS = {
	debug: 40,
	info: 30,
	warn: 20,
	error: 10,
	none: -1,
}
let level = LEVELS.info
let delegate: LoggerDelegate = {
	log: (
		_level: Level,
		name: string,
		message: string,
		...args: unknown[]
	): void => {
		console.log(`[${name}] ${message}`, ...args)
	},
	error: (
		error: unknown | null,
		message?: string,
		...args: unknown[]
	): void => {
		console.error(`[error] ${message}`, error, ...args)
	},
}

export const setLevel = (newLevel: Level | null | undefined): void => {
	level = (newLevel && LEVELS[newLevel]) || LEVELS.info
}
export const setDelegate = (newDelegate: LoggerDelegate): void => {
	delegate = newDelegate
}

export type LoggerDelegate = {
	log: (level: Level, name: string, message: string, ...args: unknown[]) => void
	error: (
		error: unknown | null,
		name: string,
		message?: string,
		...args: unknown[]
	) => void
}
export type Level = keyof typeof LEVELS

export type Logger = {
	debug: (message: string, ...args: unknown[]) => void
	info: (message: string, ...args: unknown[]) => void
	warn: (message: string, ...args: unknown[]) => void
	error: (error: unknown | null, message?: string, ...args: unknown[]) => void
}
export const create = (name: string): Logger => {
	return {
		debug: (message: string, ...args: unknown[]): void => {
			if (level >= LEVELS.debug) {
				delegate.log('debug', name, message, ...args)
			}
		},
		info: (message: string, ...args: unknown[]): void => {
			if (level >= LEVELS.info) {
				delegate.log('info', name, message, ...args)
			}
		},
		warn: (message: string, ...args: unknown[]): void => {
			if (level >= LEVELS.warn) {
				delegate.log('warn', name, message, ...args)
			}
		},
		error: (
			error: unknown | null,
			message?: string,
			...args: unknown[]
		): void => {
			if (level >= LEVELS.error) {
				delegate.error(error, name, message, ...args)
			}
		},
	}
}
