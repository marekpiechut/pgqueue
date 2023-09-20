type WaitForConfig = {
	max?: number
	label?: string
}
export const waitFor = <T>(
	getter: () => T,
	expected: T | ((value: T) => boolean),
	config?: WaitForConfig
): Promise<T> => {
	const max = config?.max ?? 1000
	const start = Date.now()
	return new Promise((resolve, reject) => {
		const interval = setInterval(() => {
			if (Date.now() - start > max) {
				clearInterval(interval)
				reject(
					Error(`waitFor ${config?.label ? config.label + ' ' : ''}timed out`)
				)
				return
			}
			const value = getter()
			if (typeof expected === 'function') {
				if ((expected as (value: T) => boolean)(value)) {
					clearInterval(interval)
					resolve(value)
				}
			} else if (value === expected) {
				clearInterval(interval)
				resolve(value)
			}
		}, 10)
	})
}
