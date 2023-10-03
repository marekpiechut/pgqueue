export const toError = (err: unknown): Error => {
	if (err instanceof Error) {
		return err
	} else {
		return new Error(err?.toString())
	}
}
