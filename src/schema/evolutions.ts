import pg from 'pg'
import crypto from 'crypto'
import { logger } from '~/utils/logger'

const log = logger('schema:evolutions')
export type EvolutionExpression = string

export type Config = {
	baseName: string
}

export type Evolution = {
	ups: EvolutionExpression[]
	downs?: EvolutionExpression[]
}

export type LoadedEvolution = Evolution & {
	checksum: string
	version: number
}

const loadEvolutions = async <C extends Config>(
	files: string[],
	config: C
): Promise<{
	version: number
	evolutions: LoadedEvolution[]
}> => {
	validateConfig(config)

	const evolutions: LoadedEvolution[] = await Promise.all(
		files.map((file, i) =>
			import(file)
				.then(evo => evo(config))
				.then(evo => ({
					...evo,
					checksum: generateChecksum(evo),
					version: i + 1,
				}))
		)
	)
	const version = evolutions.length

	return {
		evolutions,
		version,
	}
}

export const apply = async (
	files: string[],
	client: pg.Client,
	config: Config
): Promise<void> => {
	log.info('Applying evolutions')
	const expected = await loadEvolutions(files, config)

	const queries = new EvolutionQueries(client, config)
	const current = await queries.getCurrentVersion()

	if (current.version === expected.version) {
		const lastEvo = expected.evolutions[expected.evolutions.length - 1]
		if (!current.checksum || lastEvo.checksum === current.checksum) {
			log.info('Database schema is up to date')
			return
		}
	} else {
		log.info(
			`Updating database schema from ${current.version} to ${expected.version}`
		)
		await Promise.all(
			expected.evolutions.slice(current.version).map(async evo => {
				log.info(`Applying evolution ${evo.version}`)
				try {
					client.query(`BEGIN;`)
					for (const up of evo.ups) {
						log.debug(up)
						await client.query(up)
					}
					queries.evolutionApplied(evo)
					client.query('COMMIT;')
				} catch (e) {
					log.error(
						e,
						`Failed to apply evolution ${evo.version}, your database might be in inconsistent state.`
					)
					client.query(`ROLLBACK;`)
					throw e
				}
			})
		)
	}
}

const validateConfig = (config: Config): void => {
	if (!config.baseName) {
		throw Error('Missing baseName in config')
	}
}

const generateChecksum = (evo: Evolution): string => {
	const sql = evo.ups.map(s => s.trim()).join('; ')

	return crypto.createHash('sha1').update(sql).digest('hex')
}

class EvolutionQueries {
	constructor(
		private client: pg.Client,
		private config: Config
	) {}

	async getCurrentVersion(): Promise<{ version: number; checksum?: string }> {
		const { rows } = await this.client.query(
			`SELECT 1 as success FROM information_schema.tables
				WHERE table_schema=ANY(current_schemas(FALSE))
				AND table_name='${this.config.baseName}_SCHEMA}';
			`
		)

		if (rows[0]?.success === 1) {
			const { rows: versionRows } = await this.client.query(
				`SELECT * FROM ${this.config.baseName}_SCHEMA
					ORDER BY version DESC LIMIT 1;
				`
			)
			const row = versionRows[0]
			if (row) {
				return { version: row.version, checksum: row.checksum }
			} else {
				log.warn('No version in schema. Assuming no data.')
				return { version: 0 }
			}
		} else {
			log.info('Could not find schema in DB. Assuming no data.')
			return { version: 0 }
		}
	}

	async evolutionApplied(evo: {
		version: number
		checksum: string
	}): Promise<void> {
		await this.client.query(
			`INSERT INTO ${this.config.baseName}_SCHEMA (version, checksum)
				VALUES (${evo.version}, '${evo.checksum}');
			`
		)
	}
}
