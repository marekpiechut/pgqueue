import { first, identity, last, take } from 'lodash'
import pg from 'pg'
import { PagedResult, UUID } from '~/common/models'
import { QueryArg, SortOrder, countArguments } from '~/common/psql'

export type PagedFetcher<T> = (
	client: pg.ClientBase,
	baseArgs: QueryArg[],
	limit?: number,
	after?: UUID | null | undefined | 'FIRST',
	before?: UUID | null | undefined | 'LAST',
	sort?: SortOrder
) => Promise<PagedResult<T>>

export function createPagedFetcher<R extends { id: UUID; created: Date }, M>(
	table: string,
	baseCondition: string,
	mapper: (row: R) => M
): PagedFetcher<M>
export function createPagedFetcher<R extends { id: UUID; created: Date }>(
	table: string,
	baseCondition: string
): PagedFetcher<R>
export function createPagedFetcher<R extends { id: UUID; created: Date }, M>(
	table: string,
	baseCondition: string,
	mapper?: (row: R) => M
): PagedFetcher<M> {
	const baseArgsCount = countArguments(baseCondition)
	const queries = buildQueries(table, baseCondition, baseArgsCount + 1)
	const countAll = async (
		client: pg.ClientBase,
		baseArgs: QueryArg[]
	): Promise<number> => {
		const res = await client.query(queries.countAll, baseArgs)
		return res.rows[0].count
	}

	const countItemsSkipped = async (
		client: pg.ClientBase,
		baseArgs: QueryArg[],
		cursor: UUID | 'LAST' | null | undefined,
		reverse?: boolean
	): Promise<number> => {
		if (!cursor) return 0
		const res = await client.query(
			reverse ? queries.countSkippedReverse : queries.countSkipped,
			[...baseArgs, cursor]
		)
		return res.rows[0].count
	}

	return async (
		client: pg.ClientBase,
		baseArgs: QueryArg[],
		limit: number = 100,
		after?: UUID | null | undefined | 'FIRST',
		before?: UUID | null | undefined | 'LAST',
		sort?: SortOrder
	): Promise<PagedResult<M>> => {
		const totalPromise = countAll(client, baseArgs)
		let itemsSkippedPromise

		if (after === 'LAST' || before === 'LAST') {
			itemsSkippedPromise = totalPromise.then(total => {
				const mod = total % limit
				if (mod === 0) return total - limit
				return total - mod
			})
		} else {
			itemsSkippedPromise = countItemsSkipped(
				client,
				baseArgs,
				before || after,
				sort === 'DESC'
			)
		}
		let itemsPromise: Promise<{
			rows: R[]
			hasNext: boolean
			hasPrev: boolean
		}>

		//NOTE: Make sure to sanity/escape all input to query (sort!)
		if (after === 'LAST' || before === 'LAST') {
			const total = await totalPromise
			const lastPageSize = total % limit === 0 ? limit : total % limit
			itemsPromise = client
				.query<R>(sort === 'DESC' ? queries.fetchLastN : queries.fetchFirstN, [
					...baseArgs,
					lastPageSize,
				])
				.then(res => ({
					rows: sort === 'DESC' ? res.rows.reverse() : res.rows,
					hasNext: false,
					hasPrev: sort === 'DESC',
				}))
		} else if (after) {
			itemsPromise = client
				.query<R>(queries.fetchAfter, [...baseArgs, after, limit + 1])
				.then(res => {
					const hasMore = res.rows.length > limit
					const rows = take(res.rows, limit)
					return {
						rows: sort === 'DESC' ? rows.reverse() : rows,
						hasNext: sort === 'DESC' ? true : hasMore,
						hasPrev: sort === 'DESC' ? hasMore : true,
					}
				})
		} else if (before) {
			itemsPromise = client
				.query<R>(queries.fetchBefore, [...baseArgs, before, limit + 1])
				.then(res => {
					const hasMore = res.rows.length > limit
					const rows = take(res.rows, limit)
					return {
						rows: sort === 'DESC' ? rows : rows.reverse(),
						hasNext: sort === 'DESC' ? hasMore : true,
						hasPrev: sort === 'DESC' ? true : hasMore,
					}
				})
		} else {
			itemsPromise = client
				.query(sort === 'DESC' ? queries.fetchLastN : queries.fetchFirstN, [
					...baseArgs,
					limit + 1,
				])
				.then(res => ({
					rows: take(res.rows, limit),
					hasNext: res.rows.length > limit,
					hasPrev: false,
				}))
		}

		const [total, items, itemsSkipped] = await Promise.all([
			totalPromise,
			itemsPromise,
			itemsSkippedPromise,
		])

		const firstId = first(items.rows)?.id
		const lastId = last(items.rows)?.id

		let pageNo
		if ((sort === 'DESC' && after) || (sort === 'ASC' && before && !after)) {
			pageNo = Math.floor(itemsSkipped / limit) - 1
		} else {
			pageNo = Math.ceil(itemsSkipped / limit)
		}

		return {
			page: {
				pages: Math.ceil(total / limit),
				pageNo: pageNo,
				total: total,
				prevCursor: items.hasPrev ? firstId : undefined,
				nextCursor: items.hasNext ? lastId : undefined,
				endCursor: 'LAST',
			},
			items: items.rows.map(mapper || identity) as M[],
		}
	}
}

type Queries = {
	countAll: string
	countSkipped: string
	countSkippedReverse: string
	fetchFirstN: string
	fetchLastN: string
	fetchBefore: string
	fetchAfter: string
}
const buildQueries = (
	table: string,
	baseCondition: string,
	baseIndex: number
): Queries => ({
	countAll: `SELECT count(*) FROM ${table} WHERE ${baseCondition}`,
	countSkipped: `
		SELECT count(*)::Int FROM ${table} WHERE
		${baseCondition} AND
		(created, id) <= (SELECT created, id FROM ${table} WHERE id=$${baseIndex})
	`,
	countSkippedReverse: `
		SELECT count(*)::Int FROM ${table} WHERE ${baseCondition} AND
		(created, id) >= (SELECT created, id FROM ${table} WHERE id=$${baseIndex})
	`,
	fetchFirstN: `
		SELECT * FROM ${table}
		WHERE ${baseCondition}
		ORDER BY (created, id) ASC
		LIMIT $${baseIndex}
	`,
	fetchLastN: `
		SELECT * FROM ${table}
		WHERE ${baseCondition}
		ORDER BY (created, id) DESC 
		LIMIT $${baseIndex}
	`,
	fetchBefore: `
		WITH before AS (
			SELECT created, id FROM ${table} WHERE id = $${baseIndex}
		)
		SELECT * FROM ${table}
		WHERE ${baseCondition}
		AND created < (SELECT created FROM before) OR (
			created = (SELECT created FROM before) AND id < (SELECT id FROM before)
		)
		ORDER BY (created, id) DESC 
		limit $${baseIndex + 1}
	`,
	fetchAfter: `
		WITH after AS (
			SELECT created, id FROM ${table} WHERE id = $${baseIndex}
		)
		SELECT * FROM ${table}
		WHERE queue=$1
		AND created > (SELECT created FROM after) OR (
			created = (SELECT created FROM after) AND id > (SELECT id FROM after)
		)
		ORDER BY (created, id) ASC
		limit $${baseIndex + 1}
	`,
})
