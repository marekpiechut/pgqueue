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
export type Delay =
	| RetryDelayLinear
	| RetryDelayConstant
	| RetryDelayExponential

export const nextRun = (
	delay: Delay,
	attempt: number,
	max?: number
): number => {
	let timeMs = 0
	switch (delay.type) {
		case 'linear':
			timeMs = delay.delay * attempt
			break
		case 'constant':
			timeMs = delay.delay
			break
		case 'exponential':
			timeMs = delay.delay * attempt ** 2
			break
	}
	return max ? Math.min(timeMs, max) : timeMs
}
