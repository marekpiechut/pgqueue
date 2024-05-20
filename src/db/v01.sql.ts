import { Evolution } from '@dayone-labs/evolutions'
import { MAX_ERROR_LEN } from '~/common/models'

const apply = (schema: string): Evolution => ({
	ups: [
		`CREATE OR REPLACE FUNCTION add_role_if_not_exists(rolename NAME) RETURNS integer AS $$ BEGIN IF NOT EXISTS (
					SELECT
					FROM pg_roles
					WHERE rolname = rolename
				) THEN EXECUTE FORMAT('CREATE ROLE %I;', rolename);
			END IF;
			RETURN NULL;
			END;
			$$ language plpgsql;`,
		`CREATE SCHEMA IF NOT EXISTS ${schema};`,
		`SELECT add_role_if_not_exists('QUEUE_WORKER');`,
		`SELECT add_role_if_not_exists('QUEUE_USER');`,

		`GRANT ALL ON SCHEMA ${schema} TO QUEUE_USER;`,
		`GRANT ALL ON SCHEMA ${schema} TO QUEUE_WORKER;`,
		`GRANT ALL ON ALL TABLES IN SCHEMA ${schema} TO QUEUE_USER;`,
		`GRANT ALL ON ALL SEQUENCES IN SCHEMA ${schema} TO QUEUE_WORKER;`,

		`ALTER DEFAULT PRIVILEGES IN SCHEMA ${schema} GRANT ALL ON TABLES TO QUEUE_USER;`,
		`ALTER DEFAULT PRIVILEGES IN SCHEMA ${schema} GRANT ALL ON TABLES TO QUEUE_WORKER;`,
		//TODO: how to handle this???
		`GRANT QUEUE_USER TO lq`,
		`GRANT QUEUE_WORKER TO lqworker`,
		//END

		/**
		 * --- JOB QUEUE ---
		 */

		`CREATE TABLE ${schema}.QUEUE (
			tenant_id VARCHAR(40) NOT NULL,
			id UUID NOT NULL,
			version INTEGER NOT NULL DEFAULT 0,
			created TIMESTAMP NOT NULL,
			updated TIMESTAMP,
			started TIMESTAMP,
			key VARCHAR(255),
			schedule_id UUID,
			type VARCHAR(40) NOT NULL,
			tries INTEGER NOT NULL DEFAULT 0,
			queue VARCHAR(255) NOT NULL,
			state VARCHAR(40) NOT NULL,
			delay INTEGER,
			run_after TIMESTAMP,
			payload BYTEA,
			payload_type VARCHAR(255),
			target JSONB,
			retry_policy JSONB,
			result BYTEA,
			result_type VARCHAR(255),
			error VARCHAR(${MAX_ERROR_LEN}),
			worker_data JSONB,
			PRIMARY KEY(id),
			UNIQUE(tenant_id, queue, key)
		);`,
		`CREATE INDEX QUEUE_TENANT_QUEUE_KEY ON ${schema}.QUEUE (tenant_id, queue, key);`,
		`CREATE INDEX QUEUE_RUN_AFTER_CREATED ON ${schema}.QUEUE (run_after, created);`,

		/**
		 * --- QUEUE CONFIG ---
		 */

		`CREATE TABLE ${schema}.QUEUE_CONFIG (
			tenant_id VARCHAR(40) NOT NULL,
			queue VARCHAR(255) NOT NULL,
			created TIMESTAMP NOT NULL DEFAULT now(),
			updated TIMESTAMP,
			version INTEGER NOT NULL DEFAULT 0,
			display_name VARCHAR(255),
			paused BOOLEAN NOT NULL DEFAULT FALSE,
			retry_policy JSONB,
			PRIMARY KEY(tenant_id, queue)
		);`,
		`ALTER TABLE ${schema}.QUEUE ENABLE ROW LEVEL SECURITY;`,
		`CREATE POLICY TENANT_POLICY on ${schema}.QUEUE USING (
			tenant_id = current_setting('pgqueue.current_tenant', true)
		);`,
		`CREATE POLICY WORKER_POLICY on ${schema}.QUEUE TO QUEUE_WORKER USING (TRUE);`,

		/**
		 * --- QUEUE HISTORY ---
		 */
		`CREATE TABLE ${schema}.QUEUE_HISTORY (
			tenant_id VARCHAR(40) NOT NULL,
			id UUID NOT NULL,
			key VARCHAR(255),
			queue VARCHAR(255) NOT NULL,
			schedule_id UUID,
			type VARCHAR(40) NOT NULL,
			created TIMESTAMP NOT NULL,
			scheduled TIMESTAMP NOT NULL,
			started TIMESTAMP NOT NULL,
			state VARCHAR(40) NOT NULL,
			delay INTEGER,
			tries INTEGER NOT NULL,
			payload BYTEA,
			payload_type VARCHAR(255),
			result BYTEA,
			result_type VARCHAR(255),
			error VARCHAR(${MAX_ERROR_LEN}),
			target JSONB,
			worker_data JSONB,
			PRIMARY KEY(id)
		);`,
		`CREATE INDEX QUEUE_HISTORY_TENANT_STATE_CREATED ON ${schema}.QUEUE_HISTORY (tenant_id, state, created);`,
		`ALTER TABLE ${schema}.QUEUE_HISTORY ENABLE ROW LEVEL SECURITY;`,
		`CREATE POLICY TENANT_POLICY on ${schema}.QUEUE_HISTORY USING (
			tenant_id = current_setting('pgqueue.current_tenant', true)
		);`,
		`CREATE POLICY WORKER_POLICY on ${schema}.QUEUE_HISTORY TO QUEUE_WORKER USING (TRUE);`,

		/**
		 * --- WORK QUEUE ---
		 */

		`CREATE TABLE ${schema}.WORK_QUEUE (
			id UUID NOT NULL,
			tenant_id VARCHAR(40) NOT NULL,
			version INTEGER NOT NULL DEFAULT 0,
			created TIMESTAMP NOT NULL DEFAULT now(),
			updated TIMESTAMP,
			batch_order INTEGER NOT NULL DEFAULT 0,
			started TIMESTAMP,
			lock_key VARCHAR(255),
			lock_timeout TIMESTAMP,
			PRIMARY KEY(id),
			FOREIGN KEY(id) REFERENCES ${schema}.QUEUE(id) ON DELETE CASCADE
		)`,
		`CREATE INDEX WORK_QUEUE_CREATED_ORDER ON ${schema}.WORK_QUEUE (created, batch_order);`,

		/**
		 * --- SCHEDULE ---
		 */
		`CREATE TABLE ${schema}.SCHEDULES (
			id UUID NOT NULL,
			tenant_id VARCHAR(40) NOT NULL DEFAULT current_setting('pgqueue.current_tenant', false),
			queue VARCHAR(255) NOT NULL,
			type VARCHAR(40) NOT NULL,
			name VARCHAR(255),
			paused BOOLEAN DEFAULT FALSE,
			retry_policy JSONB,
			created TIMESTAMP NOT NULL DEFAULT now(),
			updated TIMESTAMP,
			version INTEGER NOT NULL DEFAULT 0,
			tries INTEGER NOT NULL DEFAULT 0,
			next_run TIMESTAMP,
			payload BYTEA,
			payload_type VARCHAR(255),
			target JSONB,
			schedule VARCHAR(64),
			timezone VARCHAR(32) NOT NULL,
			PRIMARY KEY(id)
		);`,
		`CREATE UNIQUE INDEX SCHEDULE_TENANT_NAME ON ${schema}.SCHEDULES (tenant_id, name);`,
		//TODO: do we need index on queue name?
		`CREATE INDEX SCHEDULE_NEXT_RUN ON ${schema}.SCHEDULES (next_run, paused);`,
		`ALTER TABLE ${schema}.SCHEDULES ENABLE ROW LEVEL SECURITY;`,
		`CREATE POLICY TENANT_POLICY on ${schema}.SCHEDULES USING (
			tenant_id = current_setting('pgqueue.current_tenant', true)
		);`,
		`CREATE POLICY WORKER_POLICY on ${schema}.SCHEDULES TO QUEUE_WORKER USING (TRUE);`,
	],
	downs: [
		`DROP TABLE IF EXISTS ${schema}.WORK_QUEUE;`,
		`DROP TABLE IF EXISTS ${schema}.QUEUE_CONFIG;`,
		`DROP TABLE IF EXISTS ${schema}.QUEUE_HISTORY;`,
		`DROP TABLE IF EXISTS ${schema}.QUEUE;`,
		`DROP TABLE IF EXISTS ${schema}.SCHEDULES;`,
		`DROP TABLE IF EXISTS ${schema}.WORK;`,
	],
})

export default apply
