import { Evolution } from '@pgqueue/evolutions'

//TODO: create indexes

type Config = {
	schema: string
	typeSize: number
	eventBase: string
}
export default (config: Config): Evolution => ({
	ups: [
		`CREATE SCHEMA IF NOT EXISTS ${config.schema}`,
		`CREATE TABLE ${config.schema}.QUEUE (
			id UUID NOT NULL,
			key VARCHAR(64) UNIQUE DEFAULT NULL,
			type VARCHAR(${config.typeSize}) NOT NULL,
			created TIMESTAMP NOT NULL,
			updated TIMESTAMP,
			state VARCHAR(16) NOT NULL,
			next_run TIMESTAMP,
			schedule VARCHAR(64),
			priority INTEGER,
			tries INTEGER,
			payload JSONB,
			error TEXT,
			PRIMARY KEY(id)
		)`,
		`CREATE TABLE ${config.schema}.QUEUE_HISTORY (
			id UUID NOT NULL,
			key VARCHAR(64),
			type VARCHAR(${config.typeSize}) NOT NULL,
			created TIMESTAMP NOT NULL,
			updated TIMESTAMP,
			state VARCHAR(16) NOT NULL,
			schedule VARCHAR(64),
			priority INTEGER,
			tries INTEGER,
			payload JSONB,
			result JSONB,
			error TEXT,
			PRIMARY KEY(id)
		)`,

		`CREATE OR REPLACE FUNCTION ${config.schema}.QUEUE_ADDED() RETURNS trigger AS $$
			BEGIN
				PERFORM pg_notify('${config.eventBase}:added', NEW.type);
				RETURN NEW;
			END;
			$$ LANGUAGE plpgsql;
		`,
		`CREATE OR REPLACE TRIGGER TR_QUEUE_ADDED
			AFTER INSERT ON ${config.schema}.QUEUE
			FOR EACH STATEMENT EXECUTE PROCEDURE
			${config.schema}.QUEUE_ADDED();
		`,
	],
	downs: [
		`DROP TABLE ${config.schema}.QUEUE`,
		`DROP TABLE ${config.schema}.QUEUE_HISTORY`,
	],
})
