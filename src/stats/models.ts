export type BasicStats = {
	queues: number
	schedules: number
	pending: number
	failedToday: number
	lastFailedAt?: Date
	nextSchedule?: Date
}

export type StatsHistogram = {
	date: Date
	completed: number
	failed: number
}
