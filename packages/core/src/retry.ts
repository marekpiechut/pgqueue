type RetryDelayLinear = {
	type: 'linear'
	delay: number
	max?: number
}
type RetryDelayConstant = {
	type: 'constant'
	delay: number
}
type RetryDelayExponential = {
	type: 'exponential'
	delay: number
	max?: number
}
export type Delay =
	| RetryDelayLinear
	| RetryDelayConstant
	| RetryDelayExponential

export const nextRun = (delay: Delay, attempt: number): number => {
	let timeMs = 0
	const max = (delay as RetryDelayLinear | RetryDelayExponential).max
	const randomTime =
		attempt > 0 ? Math.round(Math.random() * Math.min(100, delay.delay)) : 0

	switch (delay.type) {
		case 'linear':
			timeMs = delay.delay * attempt + randomTime
			break
		case 'constant':
			timeMs = delay.delay
			break
		case 'exponential':
			timeMs = delay.delay * attempt ** 2 + randomTime
			break
	}
	return max ? Math.min(timeMs, max) : timeMs
}
