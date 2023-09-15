//TODO: change to non-blocking logger

const LEVELS = {
	debug: 40,
	info: 30,
	warn: 20,
	error: 10,
	none: -1,
}
const level =
	LEVELS[process.env.PGQUEUE_LOG_LEVEL as keyof typeof LEVELS] ?? LEVELS.info

export type Logger = {
	debug: (message: string, ...args: unknown[]) => void
	info: (message: string, ...args: unknown[]) => void
	warn: (message: string, ...args: unknown[]) => void
	error: (message: string, ...args: unknown[]) => void
}
export const logger = (name: string): Logger => {
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
		error: (message: string, ...args: unknown[]): void => {
			if (level >= LEVELS.error) {
				console.error(`[${name}] ${message}`, ...args)
			}
		},
	}
}
