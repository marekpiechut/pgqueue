export const KEY_BASE = 'pgqueue'

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
