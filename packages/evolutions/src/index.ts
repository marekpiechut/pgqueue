import pg from 'pg'
import crypto from 'crypto'
import { logger } from '@pgqueue/core'
import schemaSql from './schema.sql.js'

const log = logger.create('schema:evolutions')
export type EvolutionExpression = string

export type Config = {
	schema?: string
	destroy_my_data_AllowDownMigration?: boolean
}

const CONFIG_DEFAULTS = {
	schema: 'public',
}

export type Evolution = {
	ups: EvolutionExpression[]
	downs?: EvolutionExpression[]
}

export type LoadedEvolution = Evolution & {
	checksum: string
	version: number
}

const loadEvolutions = async (
	evolutions: Evolution[]
): Promise<LoadedEvolution[]> => {
	return evolutions.map((evo, i) => ({
		...evo,
		checksum: generateChecksum(evo),
		version: i + 1,
	}))
}

export const apply = async (
	schema: Evolution[],
	client: pg.ClientBase,
	config: Config
): Promise<void> => {
	log.info('Applying evolutions')
	const loaded = await loadEvolutions(schema)
	const configWithDefaults = { ...CONFIG_DEFAULTS, ...config }
	const actions = new EvolutionActions(client, configWithDefaults, loaded)
	const hasSchema = await actions.hasSchema()

	if (!hasSchema) {
		log.info('Creating evolutions schema')
		await actions.createSchema()
		log.info('Schema created')
	}

	if (await actions.hasDown()) {
		if (config.destroy_my_data_AllowDownMigration) {
			log.warn(
				`!!! WARNING !!! Database has down migrations. This will DESTROY YOUR DATA! You have 10 seconds to cancel if you're not sure...`
			)
			await new Promise(resolve => {
				setTimeout(resolve, 10000)
			})
			await actions.applyDown()
		} else {
			throw Error(
				'Database has down migrations. For development use destroy_my_data_AllowDownMigration to apply them. NEVER USE FOR PRODUCTION!'
			)
		}
	}

	await actions.applyUp()
	log.info('Evolutions applied, database is up to date.')
}

const generateChecksum = (evo: Evolution): string => {
	const sql = evo.ups.map(s => s.trim()).join('; ')

	return crypto.createHash('sha1').update(sql).digest('hex')
}

class EvolutionActions {
	constructor(
		private client: pg.ClientBase,
		private config: typeof CONFIG_DEFAULTS & Config,
		private evolutions: LoadedEvolution[]
	) {}

	async createSchema(): Promise<{ version: number }> {
		const sqls = schemaSql(this.config)
		await this.client.query(`BEGIN;`)
		for (const up of sqls) {
			log.debug(up)
			await this.client.query(up)
		}
		await this.client.query('COMMIT;')
		return { version: 0 }
	}

	async hasDown(): Promise<boolean> {
		const current = await this.getCurrent()
		if (!current) return false

		const expected = this.evolutions[current.version - 1]

		const needsDowngrade = current.version > expected.version
		const checksumMismatch = current.checksum !== expected.checksum

		if (needsDowngrade) {
			log.warn(
				`Database needs a downgrade from version${current.version} to ${expected.version}!`
			)
		} else if (checksumMismatch) {
			log.warn('Database checksum mismatch!')
		}
		return needsDowngrade || checksumMismatch
	}

	async applyDown(): Promise<void> {
		let current = await this.getCurrent()
		while (current) {
			const expected = this.evolutions[current.version - 1]

			if (!expected || current.checksum !== expected.checksum) {
				log.warn(`Downgrading database to version ${current.version - 1}`)
				const downs = current.downs
				await this.client.query(`BEGIN;`)
				try {
					if (downs) {
						await this.apply(downs)
					}
					await this.evolutionDropped(current)
					await this.client.query('COMMIT;')
				} catch (e) {
					await this.client.query(`ROLLBACK;`)
					throw e
				}
			} else {
				log.info('All down migrations applied')
				return
			}
			current = await this.getCurrent()
		}
	}

	async applyUp(): Promise<void> {
		const current = await this.getCurrent()
		const currentVersion = current ? current.version : 0
		const toApply = this.evolutions.slice(currentVersion)
		for (const evo of toApply) {
			log.info(`Applying up migration ${evo.version}`)
			await this.client.query(`BEGIN;`)
			try {
				await this.apply(evo.ups)
				await this.evolutionApplied(evo)
				await this.client.query('COMMIT;')
			} catch (e) {
				await this.client.query(`ROLLBACK;`)
				throw e
			}
		}
	}

	async apply(sqls: EvolutionExpression[]): Promise<void> {
		for (const up of sqls) {
			log.debug(up)
			await this.client.query(up)
		}
	}

	async hasSchema(): Promise<boolean> {
		const { rows } = await this.client.query(
			`SELECT 1 as success FROM information_schema.tables
				WHERE table_schema='${this.config.schema}'
				AND table_name ilike 'SCHEMA';
			`
		)
		return rows[0]?.success === 1
	}

	async getCurrent(): Promise<LoadedEvolution | null> {
		const { rows: versionRows } = await this.client.query(
			`SELECT * FROM ${this.config.schema}.SCHEMA
					ORDER BY version DESC LIMIT 1;
				`
		)
		const row = versionRows[0]
		if (row) {
			return {
				version: row.version,
				checksum: row.checksum,
				ups: row.ups,
				downs: row.downs,
			}
		} else {
			log.warn('No version in schema. Assuming no data.')
			return null
		}
	}

	async evolutionDropped(evo: LoadedEvolution): Promise<void> {
		await this.client.query(
			`DELETE FROM ${this.config.schema}.SCHEMA WHERE version = $1`,
			[evo.version]
		)
	}

	async evolutionApplied(evo: LoadedEvolution): Promise<void> {
		await this.client.query(
			`INSERT INTO ${this.config.schema}.SCHEMA (version, checksum, ups, downs)
				VALUES ($1, $2, $3, $4);
			`,
			[
				evo.version,
				evo.checksum,
				JSON.stringify(evo.ups),
				JSON.stringify(evo.downs),
			]
		)
	}
}

export default {
	apply,
}
