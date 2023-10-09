export type Duration = number

export const seconds = (n: number): Duration => n * 1000
export const toSeconds = (n: Duration): number => n / 1000
export const minutes = (n: number): Duration => n * 60000
export const hours = (n: number): Duration => n * 3600000
export const days = (n: number): Duration => n * 86400000

export const durations = {
	seconds,
	minutes,
	hours,
	days,
}
