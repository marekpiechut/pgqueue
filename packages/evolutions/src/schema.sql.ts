import { Config, EvolutionExpression } from './index'

export default (config: Config): EvolutionExpression[] => [
	`CREATE TABLE IF NOT EXISTS ${config.baseName}_SCHEMA (
		version INTEGER NOT NULL,
		checksum VARCHAR(64),
		applied TIMESTAMP NOT NULL DEFAULT NOW(),
		ups TEXT,
		downs TEXT,
		PRIMARY KEY(version)
	)`,
]
