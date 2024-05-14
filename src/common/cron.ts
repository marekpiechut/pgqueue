import cron from 'cron-parser'
import { DateTime } from 'luxon'

type CronOptions = {
	tz?: string
}
export type ScheduleConfig = BasicSchedule | CronSchedule
export const nextRun = (input: ScheduleConfig, options?: CronOptions): Date => {
	validateTimeZone(options?.tz)

	if (input instanceof Date) {
		return DateTime.fromJSDate(input)
			.setZone(options?.tz, { keepLocalTime: true })
			.toJSDate()
	} else if (typeof input === 'string') {
		return cron.parseExpression(input, options).next().toDate()
	} else if (input.type === 'basic') {
		return DateTime.fromJSDate(input.startAt ?? new Date(), {
			zone: options?.tz,
		})
			.plus({ [input.interval]: input.every })
			.toJSDate()
	} else if (input.type === 'cron') {
		return cron
			.parseExpression(input.cron, {
				tz: options?.tz,
				currentDate: input.startAt,
			})
			.next()
			.toDate()
	}

	throw new Error(`Unknown schedule type: ${JSON.stringify(input)}`)
}
export const serialize = (input: ScheduleConfig): string => {
	if (typeof input === 'string') {
		return `C=${input}`
	} else if (input.type === 'basic') {
		const intervalKey =
			input.interval === 'months' ? 'mo' : input.interval.charAt(0)
		return `B=${input.every} ${intervalKey}${
			input.startAt ? ' ' + input.startAt.getTime() : ''
		}`
	} else if (input.type === 'cron') {
		return `C=${input.cron}`
	}

	throw new Error(`Unknown schedule type: ${JSON.stringify(input)}`)
}

const INTERVALS = {
	s: 'seconds',
	m: 'minutes',
	h: 'hours',
	d: 'days',
	mo: 'months',
	y: 'years',
} as const

export const deserialize = (input: string): ScheduleConfig => {
	const [type, value] = input.split('=')
	if (type === 'C') {
		return {
			type: 'cron',
			cron: value,
		}
	} else if (type === 'B') {
		const [every, interval, startAt] = value.split(' ')
		const fullInterval = INTERVALS[interval as keyof typeof INTERVALS]
		return {
			type: 'basic',
			every: parseInt(every),
			interval: fullInterval,
			startAt: startAt ? new Date(parseInt(startAt)) : undefined,
		}
	}

	throw new Error(`Unknown schedule type: ${type}`)
}

type BasicSchedule = {
	type: 'basic'
	every: number
	interval: 'seconds' | 'minutes' | 'hours' | 'days' | 'months' | 'years'
	startAt?: Date
}
type CronSchedule = {
	type: 'cron'
	cron: string
	startAt?: Date
}

export const isFuture = (input: ScheduleConfig): boolean => {
	if (input instanceof Date) {
		return input.getTime() > Date.now()
	}
	//TODO: do we need to validate cron & basic schedule?
	//it might have a pattern, that has no future runs
	return true
}

export const validateTimeZone = (input?: string): void => {
	if (input && !DateTime.local().setZone(input).isValid) {
		throw new Error(`Invalid timezone: ${input}`)
	}
}

export default {
	nextRun,
	serialize,
	deserialize,
	isFuture,
}
