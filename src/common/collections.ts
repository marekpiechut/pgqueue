import { groupBy } from 'lodash'

export class Multimap<K, V> {
	private delegate = new Map<K, Array<V>>()

	public get size(): number {
		return this.delegate.size
	}

	public get(key: K): V[] | undefined {
		return this.delegate.get(key)
	}

	/**
	 * @returns true if added a new key (no values existed before)
	 */
	public set(key: K, value: V): boolean {
		const values = this.delegate.get(key)
		if (values) {
			values.push(value)
			return false
		} else {
			this.delegate.set(key, [value])
			return true
		}
	}

	/**
	 * @returns true if removed all values for the key
	 */
	public delete(key: K, value: V): boolean {
		const values = this.delegate.get(key)
		if (values) {
			const idx = values.indexOf(value)
			if (idx >= 0) {
				values.splice(idx, 1)
				if (values.length === 0) {
					this.delegate.delete(key)
				}
			}
		}
		return !values?.length
	}

	public keys(): IterableIterator<K> {
		return this.delegate.keys()
	}
}

export const shuffleBy = <T>(items: T[], field: keyof T): T[] => {
	const grouped = groupBy(items, field)
	let done = false
	const shuffled: T[] = []
	do {
		done = true
		for (const value in grouped) {
			const keyItems = grouped[value]
			if (keyItems.length) {
				const item = keyItems.shift()
				if (item) {
					shuffled.push(item)
					done = false
				}
			}
		}
	} while (!done)
	return shuffled
}
