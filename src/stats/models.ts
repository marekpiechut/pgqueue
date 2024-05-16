export type BasicStats = {
	queues: number
	pending: number
	failedToday: number
	lastFailedAt?: Date
}

export type StatsHistogram = {
	date: Date
	completed: number
	failed: number
}
