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
			created TIMESTAMP NOT NULL,
			state VARCHAR(16) NOT NULL,
			type VARCHAR(${config.typeSize}) NOT NULL,
			priority INTEGER,
			tries INTEGER,
			payload JSONB,
			PRIMARY KEY(id)
		)`,
		`CREATE TABLE ${config.schema}.QUEUE_HISTORY (
			id UUID NOT NULL,
			created TIMESTAMP NOT NULL,
			state VARCHAR(16) NOT NULL,
			type VARCHAR(${config.typeSize}) NOT NULL,
			priority INTEGER,
			tries INTEGER,
			payload JSONB,
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

		`DROP TRIGGER IF EXISTS TR_QUEUE_ADDED ON ${config.schema}.QUEUE;`,
		`CREATE OR REPLACE TRIGGER TR_QUEUE_ADDED
			AFTER INSERT ON ${config.schema}.QUEUE
			FOR EACH STATEMENT EXECUTE PROCEDURE
			${config.schema}.QUEUE_ADDED();
		`,
	],
	downs: [
		`DROP TABLE ${config.schema}.QUEUE`,
		`DROP TABLE ${config.schema}.QUEUE_HISTORY`,
		`DROP TRIGGER ${config.schema}.TR_QUEUE_ADDED ON ${config.schema}.QUEUE;`,
	],
})
