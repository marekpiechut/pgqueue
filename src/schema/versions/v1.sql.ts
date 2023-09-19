import { QUEUE_EVENT_BASE } from '~/queue/queue'
import { Config } from '../index'
import { Evolution } from '../evolutions'

export default (config: Config): Evolution => ({
	ups: [
		`CREATE TABLE ${config.baseName}_QUEUE (
			uuid UUID NOT NULL,
			created TIMESTAMP NOT NULL,
			state VARCHAR(16) NOT NULL,
			type VARCHAR(${config.typeSize}) NOT NULL,
			priority INTEGER,
			tries INTEGER,
			body JSONB,
			PRIMARY KEY(uuid),
			INDEX ${config.baseName}_QUEUE_TYPE_PRIORITY_CREATED (type, priority, created)
		)`,
		`CREATE TABLE ${config.baseName}_QUEUE_HISTORY (
			uuid UUID NOT NULL,
			created TIMESTAMP NOT NULL,
			state VARCHAR(16) NOT NULL,
			type VARCHAR(${config.typeSize}) NOT NULL,
			priority INTEGER,
			tries INTEGER,
			body JSONB,
			error TEXT,
			PRIMARY KEY(uuid),
		)`,

		`CREATE OR REPLACE FUNCTION ${config.baseName}_QUEUE_ADDED() RETURNS trigger AS $$
			BEGIN
				PERFORM pg_notify('${QUEUE_EVENT_BASE}:added', NEW.type);
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
