import { Evolution } from '@pgqueue/evolutions'

//TODO: create indexes

type Config = {
	schema: string
}
export default (config: Config): Evolution => ({
	ups: [
		`CREATE SCHEMA IF NOT EXISTS ${config.schema}`,
		`CREATE TYPE ${config.schema}.JOB_STATE AS ENUM ('WAITING', 'RUNNING', 'PAUSED', 'FAILED', 'COMPLETED')`,
		`CREATE TYPE ${config.schema}.JOB_RUN_STATE AS ENUM ('FAILED', 'COMPLETED')`,
		// -- TABLES -- //
		`CREATE TABLE ${config.schema}.SCHEDULE (
			id UUID NOT NULL,
			key VARCHAR(64) UNIQUE DEFAULT NULL,
			type VARCHAR(64) NOT NULL,
			state ${config.schema}.JOB_STATE NOT NULL,
			created TIMESTAMP NOT NULL,
			updated TIMESTAMP,
			started TIMESTAMP,
			next_run TIMESTAMP,
			lock_key VARCHAR(64) DEFAULT NULL,
			lock_timeout TIMESTAMP DEFAULT NULL,
			schedule VARCHAR(64),
			-- Longest timezone name is 28 chars --
			timezone VARCHAR(32),
			payload JSONB,
			PRIMARY KEY(id)
		)`,
		`CREATE INDEX IX_SCHEDULE_NEXT_RUN ON ${config.schema}.SCHEDULE (next_run)`,
		`CREATE INDEX IX_SCHEDULE_LOCK_KEY ON ${config.schema}.SCHEDULE (lock_key)`,
		`CREATE TABLE ${config.schema}.SCHEDULE_RUNS (
			id UUID NOT NULL,
			schedule_id UUID NOT NULL,
			key VARCHAR(64) UNIQUE DEFAULT NULL,
			state ${config.schema}.JOB_RUN_STATE NOT NULL,
			type VARCHAR(64) NOT NULL,
			ran_at TIMESTAMP NOT NULL,
			payload JSONB,
			result JSONB,
			error TEXT,
			PRIMARY KEY(id)
		)`,
		`CREATE INDEX IX_SCHEDULE_RUNS_SCHED_RAN_AT ON ${config.schema}.SCHEDULE_RUNS (schedule_id, ran_at)`,
		`CREATE INDEX IX_SCHEDULE_RUNS_STATE_RAN_AT ON ${config.schema}.SCHEDULE_RUNS (state, ran_at)`,
		// -- TRIGGERS -- //
		`CREATE OR REPLACE FUNCTION ${config.schema}.SCHEDULE_UPDATED() RETURNS trigger AS $$
			BEGIN
				PERFORM pg_notify('pgqueue_schedule_updated', to_json(NEW)::TEXT);
				return NEW;
			END;
			$$ LANGUAGE plpgsql;
		`,
		`CREATE OR REPLACE TRIGGER TR_SCHEDULE_UPDATED
			AFTER INSERT OR UPDATE OF next_run, timezone
			ON ${config.schema}.SCHEDULE
			FOR EACH ROW EXECUTE PROCEDURE
			${config.schema}.SCHEDULE_UPDATED();
		`,
	],
	downs: [
		`DROP TABLE ${config.schema}.SCHEDULE`,
		`DROP TABLE ${config.schema}.SCHEDULE_RUNS`,
		`DROP FUNCTION ${config.schema}.SCHEDULE_UPDATED`,
		`DROP TYPE ${config.schema}.JOB_STATE`,
		`DROP TYPE ${config.schema}.JOB_RUN_STATE`,
	],
})
