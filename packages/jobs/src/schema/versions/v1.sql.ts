import { Evolution } from '@pgqueue/evolutions'

//TODO: create indexes

type Config = {
	schema: string
}
export default (config: Config): Evolution => ({
	ups: [
		`CREATE SCHEMA IF NOT EXISTS ${config.schema}`,
		`CREATE TABLE ${config.schema}.QUEUE (
			id UUID NOT NULL,
			key VARCHAR(64) UNIQUE DEFAULT NULL,
			lock_key VARCHAR(64) DEFAULT NULL,
			lock_timeout TIMESTAMP DEFAULT NULL,
			version INTEGER NOT NULL DEFAULT 0,
			tries INTEGER NOT NULL DEFAULT 0,
			type VARCHAR(64) NOT NULL,
			created TIMESTAMP NOT NULL,
			updated TIMESTAMP,
			started TIMESTAMP,
			state VARCHAR(16) NOT NULL,
			run_after TIMESTAMP,
			priority INTEGER,
			payload JSONB,
			error TEXT,
			PRIMARY KEY(id)
		)`,
		`CREATE TABLE ${config.schema}.QUEUE_HISTORY (
			id UUID NOT NULL,
			key VARCHAR(64),
			type VARCHAR(64) NOT NULL,
			created TIMESTAMP NOT NULL,
			updated TIMESTAMP,
			state VARCHAR(16) NOT NULL,
			run_after TIMESTAMP,
			priority INTEGER,
			tries INTEGER,
			payload JSONB,
			result JSONB,
			error TEXT,
			PRIMARY KEY(id)
		)`,
		`CREATE TABLE ${config.schema}.SCHEDULE (
			id UUID NOT NULL,
			key VARCHAR(64) UNIQUE DEFAULT NULL,
			type VARCHAR(64) NOT NULL,
			created TIMESTAMP NOT NULL,
			updated TIMESTAMP,
			next_run TIMESTAMP,
			schedule VARCHAR(64),
			-- Longest timezone name is 28 chars --
			timezone VARCHAR(32),
			tries INTEGER,
			payload JSONB,
			PRIMARY KEY(id)
		)`,
		`CREATE TABLE ${config.schema}.SCHEDULE_RUNS (
			id UUID NOT NULL,
			schedule_id UUID NOT NULL,
			key VARCHAR(64) UNIQUE DEFAULT NULL,
			type VARCHAR(64) NOT NULL,
			created TIMESTAMP NOT NULL,
			updated TIMESTAMP,
			ran_at TIMESTAMP,
			payload JSONB,
			PRIMARY KEY(id)
		)`,

		`CREATE OR REPLACE FUNCTION ${config.schema}.QUEUE_ADDED() RETURNS trigger AS $$
			BEGIN
				PERFORM pg_notify('pgqueue:queue:added', NEW.type);
				RETURN NEW;
			END;
			$$ LANGUAGE plpgsql;
		`,
		`CREATE OR REPLACE TRIGGER TR_SCHEDULE_ADDED
			AFTER INSERT ON ${config.schema}.QUEUE
			FOR EACH STATEMENT EXECUTE PROCEDURE
			${config.schema}.QUEUE_ADDED();
		`,
		`CREATE OR REPLACE FUNCTION ${config.schema}.SCHEDULE_ADDED() RETURNS trigger AS $$
			BEGIN
				PERFORM pg_notify('pgqueue:schedule:added', NEW.type);
				RETURN NEW;
			END;
			$$ LANGUAGE plpgsql;
		`,
		`CREATE OR REPLACE TRIGGER TR_SCHEDULE_ADDED
			AFTER INSERT ON ${config.schema}.SCHEDULE
			FOR EACH STATEMENT EXECUTE PROCEDURE
			${config.schema}.SCHEDULE_ADDED();
		`,
	],
	downs: [
		`DROP TABLE ${config.schema}.QUEUE`,
		`DROP TABLE ${config.schema}.QUEUE_HISTORY`,
		`DROP FUNCTION ${config.schema}.QUEUE_ADDED`,
		`DROP TABLE ${config.schema}.SCHEDULE`,
		`DROP TABLE ${config.schema}.SCHEDULE_RUNS`,
		`DROP FUNCTION ${config.schema}.SCHEDULE_ADDED`,
	],
})
