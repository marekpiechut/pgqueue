import { Evolution } from '@pgqueue/evolutions'

//TODO: create indexes

type Config = {
	schema: string
}
export default (config: Config): Evolution => ({
	ups: [
		`CREATE SCHEMA IF NOT EXISTS ${config.schema}`,
		// -- TABLES -- //
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
		// -- TRIGGERS -- //
		`CREATE OR REPLACE FUNCTION ${config.schema}.SCHEDULE_UPDATED() RETURNS trigger AS $$
			BEGIN
				PERFORM pg_notify('pgqueue:schedule:updated', to_json(NEW)::TEXT);
				return NEW;
			END;
			$$ LANGUAGE plpgsql;
		`,
		`CREATE OR REPLACE TRIGGER TR_SCHEDULE_UPDATED
			AFTER INSERT OR DELETE OR UPDATE OF next_run, timezone
			ON ${config.schema}.SCHEDULE
			FOR EACH ROW EXECUTE PROCEDURE
			${config.schema}.SCHEDULE_UPDATED();
		`,
	],
	downs: [
		`DROP TABLE ${config.schema}.SCHEDULE`,
		`DROP TABLE ${config.schema}.SCHEDULE_RUNS`,
		`DROP FUNCTION ${config.schema}.SCHEDULE_UPDATED`,
	],
})
