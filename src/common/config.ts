export const mergeConfig = <
	T1 extends Record<string, unknown> | undefined,
	T2 extends Record<string, unknown> | undefined,
>(
	c1: T1,
	c2: T2
): T1 & T2 => {
	return [c1, c2].reduce((acc, config) => {
		for (const key in config) {
			if (config[key] !== undefined) {
				acc[key] = config[key]
			}
		}
		return acc
		// biome-ignore lint/suspicious/noExplicitAny: Expected any type
	}, {} as any)
}
