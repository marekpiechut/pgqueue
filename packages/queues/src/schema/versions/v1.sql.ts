import { Evolution } from '@pgqueue/evolutions'

//TODO: create indexes

type Config = {
	baseName: string
	typeSize: number
	eventBase: string
}
export default (config: Config): Evolution => ({
	ups: [
		`CREATE TABLE ${config.baseName}_QUEUE (
			id UUID NOT NULL,
			created TIMESTAMP NOT NULL,
			state VARCHAR(16) NOT NULL,
			type VARCHAR(${config.typeSize}) NOT NULL,
			priority INTEGER,
			tries INTEGER,
			payload JSONB,
			PRIMARY KEY(id)
		)`,
		`CREATE TABLE ${config.baseName}_QUEUE_HISTORY (
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

		`CREATE OR REPLACE FUNCTION ${config.baseName}_QUEUE_ADDED() RETURNS trigger AS $$
			BEGIN
				PERFORM pg_notify('${config.eventBase}:added', NEW.type);
				RETURN NEW;
			END;
			$$ LANGUAGE plpgsql;
		`,

		`DROP TRIGGER IF EXISTS ${config.baseName}_TR_QUEUE_ADDED ON events;`,
		`CREATE OR REPLACE TRIGGER ${config.baseName}_TR_QUEUE_ADDED
			AFTER INSERT ON ${config.baseName}_QUEUE
			FOR EACH STATEMENT EXECUTE PROCEDURE
			${config.baseName}_QUEUE_ADDED();
		`,
	],
	downs: [
		`DROP TABLE ${config.baseName}_QUEUE`,
		`DROP TABLE ${config.baseName}_QUEUE_HISTORY`,
		`DROP TRIGGER ${config.baseName}_TR_QUEUE_ADDED ON events;`,
	],
})
