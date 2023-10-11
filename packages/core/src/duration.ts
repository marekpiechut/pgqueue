export type Duration = number

export const seconds = (n: number): Duration => n * 1000
export const toSeconds = (n: Duration): number => n / 1000
export const minutes = (n: number): Duration => n * 60000
export const hours = (n: number): Duration => n * 3600000
export const days = (n: number): Duration => n * 86400000
export const formatDuration = (n: Duration): string => {
	if (n < 1000) {
		return `${n}ms`
	} else if (n < 60000) {
		return `${Math.round(n / 1000)}s`
	} else if (n < 3600000) {
		return `${Math.round(n / 60000)}m`
	} else if (n < 86400000) {
		return `${Math.round(n / 3600000)}h`
	} else {
		return `${Math.round(n / 86400000)}d`
	}
}

export const durations = {
	seconds,
	minutes,
	hours,
	days,
	formatDuration,
}
