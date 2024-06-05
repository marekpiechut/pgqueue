export const MAX_ERROR_LEN = 4095
export const MAX_KEY_LEN = 255
export const MAX_NAME_LEN = 255

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

const PATTERN_UUID =
	/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/
export const isUUID = (id: string): boolean => {
	return PATTERN_UUID.test(id)
}

export type AnyObject = Record<string, unknown>
