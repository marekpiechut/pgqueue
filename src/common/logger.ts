/**
 * This module provides a simple logging interface.
 * You should use some more sophisticated delegate in production.
 *
 * Check setDelegate function
 */

const LEVELS = {
	debug: 40,
	info: 30,
	warn: 20,
	error: 10,
	none: -1,
}
let delegate: LoggerDelegate = {
	log: (
		level: Level,
		name: string,
		message: string,
		...args: unknown[]
	): void => {
		if (level !== 'none') {
			console[level](`[${name}] ${message}`, ...args)
		}
	},
	error: (
		error: unknown | null,
		name: string,
		message?: string,
		...args: unknown[]
	): void => {
		console.error(`[${name}] ${message}`, error, ...args)
	},
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
	error: (error: unknown, message?: string, ...args: unknown[]) => void
}
export const logger = (name: string): Logger => {
	return {
		debug: (message: string, ...args: unknown[]): void => {
			delegate.log('debug', name, message, ...args)
		},
		info: (message: string, ...args: unknown[]): void => {
			delegate.log('info', name, message, ...args)
		},
		warn: (message: string, ...args: unknown[]): void => {
			delegate.log('warn', name, message, ...args)
		},
		error: (error: unknown, message?: string, ...args: unknown[]): void => {
			delegate.error(error, name, message, ...args)
		},
	}
}

export default logger
