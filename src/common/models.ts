export const MAX_ERROR_LEN = 4095

export type TenantId = string
export type UUID = string
export type MimeType = string

export const MimeTypes = {
	JSON: 'application/json',
	TEXT: 'text/plain',
	BINARY: 'application/octet-stream',
	XML: 'application/xml',
	JPEG: 'image/jpeg',
	PNG: 'image/png',
}

export type PageInfo = {
	pages: number
	pageNo: number
	total: number
	prevCursor?: string
	nextCursor?: string
	endCursor?: string
}
export type PagedResult<T> = {
	page: PageInfo
	items: T[]
}
