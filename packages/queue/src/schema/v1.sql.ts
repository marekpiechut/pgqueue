import { Evolution } from '@pgqueue/evolutions'

//TODO: create indexes

type Config = {
	schema: string
}
export default (config: Config): Evolution => ({
	ups: [
		`CREATE SCHEMA IF NOT EXISTS ${config.schema}`,
		`CREATE TYPE ${config.schema}.JOB_STATE AS ENUM ('PENDING', 'RUNNING', 'COMPLETED', 'FAILED')`,
		// -- TABLES -- //
		`CREATE TABLE ${config.schema}.LISTENERS (
			nodeId VARCHAR(64) NOT NULL,
			type VARCHAR(64) NOT NULL,
			pid INTEGER NOT NULL,
			PRIMARY KEY(nodeId, type)
		)`,
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
			state ${config.schema}.JOB_STATE NOT NULL,
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
		// -- SUBSCRIBE FUNCTIONS -- //
		`CREATE OR REPLACE FUNCTION
			${config.schema}.SUBSCRIBE(nodeId VARCHAR, types VARCHAR[]) RETURNS INTEGER AS $$
				BEGIN

				INSERT INTO ${config.schema}.LISTENERS (nodeId, type, pid)
				SELECT nodeId, type, pg_backend_pid()
				FROM unnest(types) AS type
				ON CONFLICT ON CONSTRAINT listeners_pkey DO UPDATE SET
				pid = EXCLUDED.pid;

				EXECUTE 'LISTEN ' || quote_ident('pgqueue_job_added_' || nodeId);
				RETURN cardinality(types);
				END;
			$$ LANGUAGE plpgsql;
		`,
		`CREATE OR REPLACE FUNCTION
		${config.schema}.UNSUBSCRIBE(nodeId VARCHAR, types VARCHAR[]) RETURNS BOOLEAN AS $$
			BEGIN

			IF cardinality(types) = 0 OR types IS NULL THEN
				DELETE FROM ${config.schema}.LISTENERS WHERE nodeId = nodeId OR pid = pg_backend_pid();
			ELSE
				DELETE FROM ${config.schema}.LISTENERS WHERE
				(nodeId = nodeId OR pid = pg_backend_pid()) AND type = ANY(types);
			END IF;

			EXECUTE 'UNLISTEN ' || quote_ident('pgqueue_job_added_' || nodeId);
			RETURN FOUND;
			END;
		$$ LANGUAGE plpgsql;
		`,
		`CREATE OR REPLACE PROCEDURE ${config.schema}.CLEAR_ORPHANED_LISTENERS() AS $$
			DELETE FROM ${config.schema}.LISTENERS WHERE pid NOT IN ( SELECT pid FROM pg_stat_activity);			
		$$ LANGUAGE sql;`,

		// -- TRIGGERS -- //
		`CREATE OR REPLACE FUNCTION ${config.schema}.QUEUE_ADDED() RETURNS trigger AS $$
			DECLARE
				cursor REFCURSOR;
				items INTEGER;
				row ${config.schema}.LISTENERS;
			BEGIN
				CALL ${config.schema}.CLEAR_ORPHANED_LISTENERS();
				OPEN cursor FOR SELECT * FROM ${config.schema}.LISTENERS WHERE type = NEW.type;
				MOVE FORWARD ALL FROM cursor;
				GET DIAGNOSTICS items = ROW_COUNT;

				IF items > 0 THEN
					items := floor(random() * items);
					MOVE ABSOLUTE (items) FROM cursor; 
					FETCH cursor INTO row;
					PERFORM pg_notify('pgqueue_job_added_' || row.nodeId, NEW.type);
				END IF;
				
				CLOSE cursor;
				RETURN NEW;
			END;
			$$ LANGUAGE plpgsql;
		`,
		`CREATE OR REPLACE TRIGGER TR_QUEUE_ADDED
			AFTER INSERT ON ${config.schema}.QUEUE
			FOR EACH ROW EXECUTE PROCEDURE
			${config.schema}.QUEUE_ADDED();
		`,
	],
	downs: [
		`DROP TABLE ${config.schema}.LISTENERS`,
		`DROP TABLE ${config.schema}.QUEUE`,
		`DROP TABLE ${config.schema}.QUEUE_HISTORY`,
		`DROP FUNCTION ${config.schema}.SUBSCRIBE`,
		`DROP FUNCTION ${config.schema}.UNSUBSCRIBE`,
		`DROP FUNCTION ${config.schema}.QUEUE_ADDED`,
		`DROP TYPE ${config.schema}.JOB_STATE`,
	],
})
