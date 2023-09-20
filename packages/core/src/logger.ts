//TODO: change to non-blocking logger

const LEVELS = {
	debug: 40,
	info: 30,
	warn: 20,
	error: 10,
	none: -1,
}
let level = LEVELS.info

export const setLevel = (newLevel: Level | null | undefined): void => {
	level = (newLevel && LEVELS[newLevel]) || LEVELS.info
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
				console.debug(`[${name}] ${message}`, ...args)
			}
		},
		info: (message: string, ...args: unknown[]): void => {
			if (level >= LEVELS.info) {
				console.info(`[${name}] ${message}`, ...args)
			}
		},
		warn: (message: string, ...args: unknown[]): void => {
			if (level >= LEVELS.warn) {
				console.warn(`[${name}] ${message}`, ...args)
			}
		},
		error: (
			error: unknown | null,
			message?: string,
			...args: unknown[]
		): void => {
			if (level >= LEVELS.error) {
				console.error(`[${name}] ${message}`, error, ...args)
			}
		},
	}
}
