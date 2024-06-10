import { addMonths, format, isValid, startOfMonth } from 'date-fns'
import { Query, first, identity, sql } from '~/common/sql'

export type Queries = ReturnType<typeof withSchema>
export const withSchema = (schema: string) =>
	({
		createHistoryPartition: (month: Date): Query<void> => {
			const start = startOfMonth(month)
			const end = addMonths(start, 1)

			return {
				text: `
					CREATE TABLE ${schema}.QUEUE_HISTORY_${format(start, 'yyyy_MM')}
					PARTITION OF ${schema}.QUEUE_HISTORY
					FOR VALUES FROM ('${format(start, 'yyyy-MM-dd')}') TO ('${format(end, 'yyyy-MM-dd')}')
				`,
				mapper: identity,
				extractor: identity,
				values: [],
			}
		},
		fetchLastHistoryPartition: () => sql(schema, partitionDateMapper, first)`
			SELECT * FROM information_schema.tables WHERE table_schema='{{schema}}'
			AND table_name LIKE 'queue_history_%'
			ORDER BY table_name DESC LIMIT 1;
		`,
	}) as const

const partitionDateMapper = (row: { table_name: string }): Date => {
	const split = row.table_name.split('_')
	const year = split[split.length - 2]
	const month = split[split.length - 1]
	const date = new Date(`${year}-${month}-01`)
	if (isValid(date)) {
		return date
	} else {
		throw new Error(`Invalid date ${date} in table name ${row.table_name}`)
	}
}
