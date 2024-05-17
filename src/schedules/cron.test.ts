import { describe, it, beforeEach, afterEach } from 'node:test'
import { expect } from 'chai'
import * as cron from './cron'

describe('Cron NextRun calculation', () => {
	const originalTZ = process.env.TZ

	beforeEach(() => {
		process.env.TZ = 'UTC'
	})

	afterEach(() => {
		process.env.TZ = originalTZ
	})

	// describe('Plain date', () => {
	// 	it('transforms local time to UTC accorcding to timezone', () => {
	// 		const next = cron.nextRun(new Date('2024-05-09T09:30:54.716Z'), {
	// 			tz: 'America/New_York',
	// 		})

	// 		expect(next.toISOString()).to.equal('2024-05-09T13:30:54.716Z')
	// 	})

	// 	it('keeps date as is if no timezone is provided', () => {
	// 		const next = cron.nextRun(new Date('2024-05-09T09:30:54.716Z'))

	// 		expect(next.toISOString()).to.equal('2024-05-09T09:30:54.716Z')
	// 	})
	// })

	describe('Basic expression', () => {
		it('calculates correct next run for days on daylight saving change', () => {
			const date = new Date('2024-03-30T20:00:00.000Z')
			const next = cron.nextRun(
				{
					type: 'basic',
					every: 2,
					interval: 'days',
					startAt: date,
				},
				{ tz: 'Europe/Warsaw' }
			)
			expect(next.toISOString()).to.equal('2024-04-01T19:00:00.000Z')
		})

		it('calculates correct next run for days without timezone', () => {
			const date = new Date('2024-03-30T20:00:00.000Z')
			const next = cron.nextRun({
				type: 'basic',
				every: 2,
				interval: 'days',
				startAt: date,
			})
			expect(next.toISOString()).to.equal('2024-04-01T20:00:00.000Z')
		})
	})

	describe('Cron expression', () => {
		it('calculates correct next run for cron expression without timezone', () => {
			const next = cron.nextRun({
				type: 'cron',
				cron: '0 0 * * *',
				startAt: new Date('2024-03-30T20:00:00.000Z'),
			})
			expect(next.toISOString()).to.equal('2024-03-31T00:00:00.000Z')
		})

		it('calculates correct next run for cron expression with timezone', () => {
			const next = cron.nextRun(
				{
					type: 'cron',
					cron: '0 0 * * *',
					startAt: new Date('2024-03-30T20:00:00.000Z'),
				},
				{ tz: 'Europe/Warsaw' }
			)
			expect(next.toISOString()).to.equal('2024-03-30T23:00:00.000Z')
		})

		it('calculates correct next run for cron expression with timezone at daylight saving date', () => {
			const next = cron.nextRun(
				{
					type: 'cron',
					cron: '0 3 * * *',
					startAt: new Date('2024-03-30T20:00:00.000Z'),
				},
				{ tz: 'Europe/Warsaw' }
			)
			expect(next.toISOString()).to.equal('2024-03-31T01:00:00.000Z')
		})
	})
	describe('Cron Simulation', () => {
		it('guesses granularity from cron expression', () => {
			const values = {
				'12 4 1 3 */3': 'years',
				'12 4 1 3 *': 'years',
				'12 4 1 */3 *': 'months',
				'0 0 * * *': 'hours',
				'0 0 */3 * *': 'hours',
				'0 */2 * * *': 'hours',
				'0 * * * *': 'hours',
				'* 1,2,3 * * *': 'minutes',
				'* * * * *': 'minutes',
			}
			Object.entries(values).forEach(([value, granularity]) => {
				expect(
					cron.simulate(
						{ type: 'cron', cron: value },
						{ seconds: 1, minutes: 1, hours: 1, days: 1, months: 1, years: 1 }
					).granularity,
					'for value: ' + value
				).to.equal(granularity)
			})
		})
	})
	it('simulates cron expression', () => {
		expect(
			cron
				.simulate(
					{
						type: 'cron',
						cron: '0 0 * * *',
						startAt: new Date('2021-03-03T11:22:23'),
					},
					{ hours: 5 }
				)
				.runs.map(d => d.toISOString())
		).to.deep.equal([
			'2021-03-04T00:00:00.000Z',
			'2021-03-05T00:00:00.000Z',
			'2021-03-06T00:00:00.000Z',
			'2021-03-07T00:00:00.000Z',
			'2021-03-08T00:00:00.000Z',
		])
	})
})
