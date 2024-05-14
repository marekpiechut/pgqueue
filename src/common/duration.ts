export type Duration = number

export const seconds = (n: number): Duration => n * 1000
export const toSeconds = (n: Duration): number => n / 1000
export const minutes = (n: number): Duration => n * 60000
export const hours = (n: number): Duration => n * 3600000
export const days = (n: number): Duration => n * 86400000
export const months = (n: number): Duration => n * 2592000000
export const years = (n: number): Duration => n * 31536000000
export const format = (n: Duration): string => {
	if (n < 1000) {
		return `${n}ms`
	} else if (n < 60000) {
		return `${Math.round(n / 1000)}s`
	} else if (n < 3600000) {
		return `${Math.round(n / 60000)}m`
	} else if (n < 86400000) {
		return `${Math.round(n / 3600000)}h`
	} else if (n < 2592000000) {
		return `${Math.round(n / 86400000)}d`
	} else if (n < 31536000000) {
		return `${Math.round(n / 2592000000)}mo`
	} else {
		return `${Math.round(n / 31536000000)}y`
	}
}

export default {
	seconds,
	minutes,
	hours,
	days,
	months,
	years,
	format,
}
