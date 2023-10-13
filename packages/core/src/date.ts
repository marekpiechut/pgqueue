export const isBefore = (date: Date, other: Date): boolean => {
	return date.getTime() < other.getTime()
}
export const isPast = (date: Date): boolean => {
	return date.getTime() < Date.now()
}
