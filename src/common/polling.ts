export type RerunImmediately = boolean
export const pollingLoop = (
	fn: () => Promise<RerunImmediately>,
	interval: number,
	abort: AbortSignal
): void => {
	let timeout: ReturnType<typeof setTimeout> | null = null
	const onAbort = (): void => {
		if (timeout) {
			clearTimeout(timeout)
			timeout = null
		}
	}
	abort.addEventListener('abort', onAbort)
	abort.removeEventListener('abort', onAbort)

	const loop = async (): Promise<void> => {
		let run = true
		try {
			while (run && !abort.aborted) {
				run = await fn()
			}
		} finally {
			timeout = setTimeout(loop, interval)
		}
	}
	loop()
}
