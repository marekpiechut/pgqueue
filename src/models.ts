import pg from 'pg'
import { LogLevel } from './utils/logger'

export const KEY_BASE = 'pgqueue'
export const DEV_DB_PREFIX = 'PGQUEUE_'

type RetryDelayLinear = {
	type: 'linear'
	delay: number
}
type RetryDelayConstant = {
	type: 'constant'
	delay: number
}
type RetryDelayExponential = {
	type: 'exponential'
	delay: number
}
export type RetryDelay =
	| RetryDelayLinear
	| RetryDelayConstant
	| RetryDelayExponential

export type ClientFactory = {
	acquire: () => Promise<pg.Client>
	release: (client: pg.Client) => Promise<void>
}

export type Config = {
	logLevel?: LogLevel
	dbPrefix?: string
}
