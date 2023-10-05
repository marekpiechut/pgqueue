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
			PRIMARY KEY(pid, type)
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
		// -- SUBSCRIBE FUNCTIONS -- //
		`CREATE OR REPLACE FUNCTION
			${config.schema}.SUBSCRIBE(nodeId VARCHAR, types VARCHAR[]) RETURNS INTEGER AS $$
				BEGIN

				INSERT INTO ${config.schema}.LISTENERS (nodeId, type, pid)
				SELECT nodeId, type, pg_backend_pid()
				FROM unnest(types) AS type
				ON CONFLICT DO NOTHING;

				EXECUTE 'LISTEN ' || quote_ident(format('pgqueue:listener:%s:%s', nodeId, pg_backend_pid()));

				RETURN cardinality(types);
				END;
			$$ LANGUAGE plpgsql;
		`,
		`CREATE OR REPLACE FUNCTION
		${config.schema}.UNSUBSCRIBE(id VARCHAR, types VARCHAR[]) RETURNS BOOLEAN AS $$
			BEGIN

			IF cardinality(types) = 0 OR types IS NULL THEN
				DELETE FROM ${config.schema}.LISTENERS WHERE nodeId = id OR pid = pg_backend_pid();
			ELSE
				DELETE FROM ${config.schema}.LISTENERS WHERE
				(nodeId = id OR pid = pg_backend_pid()) AND type = ANY(types);
			END IF;

			EXECUTE 'UNLISTEN ' || quote_ident(format('pgqueue:listener:%s:%s', nodeId, pg_backend_pid()));

			RETURN FOUND;
			END;
		$$ LANGUAGE plpgsql;
		`,
		`CREATE OR REPLACE PROCEDURE ${config.schema}.CLEAR_ORPHANED_LISTENERS() AS $$
			DELETE FROM ${config.schema}.LISTENERS WHERE pid NOT IN ( SELECT pid FROM pg_stat_activity);			
		$$ LANGUAGE sql;`,
		// -- QUEUE FUNCTIONS -- //
		`CREATE OR REPLACE FUNCTION
			${config.schema}.PUSH(type VARCHAR, payload JSON, priority INTEGER, key VARCHAR)
			RETURNS ${config.schema}.QUEUE AS $$
			DECLARE
				row ${config.schema}.QUEUE%rowtype;
			BEGIN

			INSERT INTO ${config.schema}.QUEUE
				(id, type, created, priority, key, payload, state)
			VALUES
				(gen_random_uuid(), type, now(), priority, key, payload, 'PENDING')
			ON CONFLICT ON CONSTRAINT QUEUE_key_key DO UPDATE SET
				updated		=	now(),
				priority	=	EXCLUDED.priority,
				payload		=	EXCLUDED.payload,
				version		=	QUEUE.version + 1
			RETURNING * INTO row;

			RETURN row;
			END;
		$$ LANGUAGE plpgsql;`,
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
				raise notice 'Items: %', items;

				IF items > 0 THEN
					items := floor(random() * items);
					MOVE ABSOLUTE (items) FROM cursor; 
					FETCH cursor INTO row;
					EXECUTE 'NOTIFY ' || quote_ident(format('pgqueue:listener:%s:%s', row.nodeId, row.pid)) || ', ' || quote_literal(NEW.type);
				END IF;
				
				CLOSE cursor;
				RETURN NEW;
			END;
			$$ LANGUAGE plpgsql;
		`,
		`CREATE OR REPLACE TRIGGER TR_QUEUE_ADDED
			AFTER INSERT OR UPDATE ON ${config.schema}.QUEUE
			FOR EACH ROW EXECUTE PROCEDURE
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
			FOR EACH ROW EXECUTE PROCEDURE
			${config.schema}.SCHEDULE_ADDED();
		`,
	],
	downs: [
		`DROP FUNCTION ${config.schema}.PUSH`,
		`DROP FUNCTION ${config.schema}.SUBSCRIBE`,
		`DROP FUNCTION ${config.schema}.UNSUBSCRIBE`,
		`DROP TABLE ${config.schema}.LISTENERS`,
		`DROP TABLE ${config.schema}.QUEUE`,
		`DROP TABLE ${config.schema}.QUEUE_HISTORY`,
		`DROP FUNCTION ${config.schema}.QUEUE_ADDED`,
		`DROP TABLE ${config.schema}.SCHEDULE`,
		`DROP TABLE ${config.schema}.SCHEDULE_RUNS`,
		`DROP FUNCTION ${config.schema}.SCHEDULE_ADDED`,
		`DROP TYPE ${config.schema}.JOB_STATE`,
	],
})
