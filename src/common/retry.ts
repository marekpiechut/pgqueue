import { Duration } from './duration'

type BaseDelay = {
	tries: number
	delay: Duration
}
type RetryLinear = BaseDelay & {
	type: 'linear'
	max: Duration
}
type RetryConstant = BaseDelay & {
	type: 'constant'
}
type RetryExponential = BaseDelay & {
	type: 'exponential'
	max: Duration
}
export type RetryPolicy = RetryLinear | RetryConstant | RetryExponential

export const nextRun = (delay: RetryPolicy, attempt: number): Date | null => {
	const waitFor = nextRunDelay(delay, attempt)
	return waitFor == null ? null : new Date(Date.now() + waitFor)
}
export const nextRunDelay = (
	delay: RetryPolicy,
	attempt: number
): number | null => {
	if (attempt > delay.tries) return null

	let timeMs = 0
	const max = (delay as RetryLinear | RetryExponential).max
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
