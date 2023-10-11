import cron from 'cron-parser'
import { duration } from '@pgqueue/core'

type CronOptions = {
	tz?: string
}
export type Schedule = BasicSchedule | CronSchedule | Date | string
export const nextRun = (input: Schedule, options: CronOptions): Date => {
	if (input instanceof Date) {
		return input
	} else if (typeof input === 'string') {
		return cron.parseExpression(input, options).next().toDate()
	} else if (input.type === 'basic') {
		return new Date(
			(input.startAt?.getTime() ?? Date.now()) +
				duration.durations[input.interval](input.every)
		)
	} else if (input.type === 'cron') {
		return cron.parseExpression(input.cron, options).next().toDate()
	}

	throw new Error(`Unknown schedule type: ${JSON.stringify(input)}`)
}
export const serialize = (input: Schedule): string => {
	if (input instanceof Date) {
		return `D=${input.toISOString()}`
	} else if (typeof input === 'string') {
		return `C=${input}`
	} else if (input.type === 'basic') {
		return `B=${input.every} ${input.interval.charAt(0)}${
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
} as const

export const deserialize = (input: string): Schedule => {
	const [type, value] = input.split('=')
	if (type === 'D') {
		return new Date(value)
	} else if (type === 'C') {
		return value
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
	interval: 'seconds' | 'minutes' | 'hours' | 'days'
	startAt?: Date
}
type CronSchedule = {
	type: 'cron'
	cron: string
	startAt?: Date
}

export const isFuture = (input: Schedule): boolean => {
	if (input instanceof Date) {
		return input.getTime() > Date.now()
	}
	//TODO: do we need to validate cron & basic schedule?
	//it might have a pattern, that has no future runs
	return true
}

export default {
	nextRun,
	serialize,
	deserialize,
	isFuture,
}
