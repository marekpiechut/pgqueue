export const isBefore = (date: Date, other: Date): boolean => {
	return date.getTime() < other.getTime()
}
