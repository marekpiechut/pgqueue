import { randomUUID } from 'crypto'

export const uuid = (): string => {
	return randomUUID()
}
