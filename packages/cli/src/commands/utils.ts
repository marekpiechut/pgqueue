import { OptionValues } from 'commander'
import pg from 'pg'
import readline from 'readline'

export const PAYLOAD_FORMAT_HELP = `"key=value" pairs or single JSN object {"key":"value",...}"`
export const parsePayload = (data: string[]): Record<string, string> => {
	if (data.length === 1 && data[0].trim().startsWith('{')) {
		return JSON.parse(data[0])
	} else {
		return data.reduce(
			(acc, curr) => {
				const [key, value] = curr.split('=')
				acc[key] = value
				return acc
			},
			{} as Record<string, string>
		)
	}
}

export const pgConfig = (opts: OptionValues): pg.ClientConfig => ({
	port: opts.port,
	host: opts.host,
	user: opts.user,
	password: opts.pass,
	database: opts.db,
})

export const ask = (question: string): Promise<string> => {
	const rl = readline.createInterface({
		input: process.stdin,
		output: process.stdout,
	})

	return new Promise(resolve => {
		rl.question(question, answer => {
			rl.close()
			resolve(answer)
		})
	})
}
