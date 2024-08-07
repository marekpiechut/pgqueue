import { Evolution } from '@dayone-labs/evolutions'
import { addMonths, format, startOfMonth } from 'date-fns'
import { MAX_ERROR_LEN, MAX_NAME_LEN } from '~/common/models'
import { MAX_KEY_LEN } from '~/common/models'

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
			key VARCHAR(${MAX_KEY_LEN}),
			schedule_id UUID,
			type VARCHAR(40) NOT NULL,
			tries INTEGER NOT NULL DEFAULT 0,
			queue VARCHAR(${MAX_NAME_LEN}) NOT NULL,
			state VARCHAR(40) NOT NULL,
			delay BIGINT,
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
		`CREATE INDEX QUEUE_STATE_RUN_AFTER_CREATED ON ${schema}.QUEUE (state, run_after, created);`,

		/**
		 * --- QUEUE CONFIG ---
		 */
		`CREATE TABLE ${schema}.QUEUE_CONFIG (
			tenant_id VARCHAR(40) NOT NULL,
			queue VARCHAR(${MAX_NAME_LEN}) NOT NULL,
			created TIMESTAMP NOT NULL DEFAULT now(),
			updated TIMESTAMP,
			version INTEGER NOT NULL DEFAULT 0,
			display_name VARCHAR(${MAX_NAME_LEN}),
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
			key VARCHAR(${MAX_KEY_LEN}),
			queue VARCHAR(${MAX_NAME_LEN}) NOT NULL,
			schedule_id UUID,
			type VARCHAR(40) NOT NULL,
			created TIMESTAMP NOT NULL,
			scheduled TIMESTAMP NOT NULL,
			started TIMESTAMP NOT NULL,
			state VARCHAR(40) NOT NULL,
			delay BIGINT,
			tries INTEGER NOT NULL,
			payload BYTEA,
			payload_type VARCHAR(255),
			result BYTEA,
			result_type VARCHAR(255),
			error VARCHAR(${MAX_ERROR_LEN}),
			target JSONB,
			worker_data JSONB
		) PARTITION BY RANGE (created);`,
		//Cannot create ID on partitioned table, need an index for id
		`CREATE INDEX QUEUE_HISTORY_ID ON ${schema}.QUEUE_HISTORY (id);`,
		`CREATE INDEX QUEUE_HISTORY_TENANT_STATE_CREATED ON ${schema}.QUEUE_HISTORY (tenant_id, state, created);`,
		`ALTER TABLE ${schema}.QUEUE_HISTORY ENABLE ROW LEVEL SECURITY;`,
		`CREATE POLICY TENANT_POLICY on ${schema}.QUEUE_HISTORY USING (
			tenant_id = current_setting('pgqueue.current_tenant', true)
		);`,
		`CREATE POLICY WORKER_POLICY on ${schema}.QUEUE_HISTORY TO QUEUE_WORKER USING (TRUE);`,
		//Create partitions for next few months
		...createPartitions(schema, 3),

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
		 * --- WORKER METADATA ---
		 */
		`CREATE TABLE ${schema}.WORKER_METADATA (
			tenant_id VARCHAR(40) NOT NULL DEFAULT current_setting('pgqueue.current_tenant', false),
			key VARCHAR(255) NOT NULL,
			value JSONB,
			version INTEGER NOT NULL DEFAULT 0,
			created TIMESTAMP NOT NULL DEFAULT now(),
			updated TIMESTAMP,
			PRIMARY KEY(tenant_id, key)
		);`,
		`ALTER TABLE ${schema}.WORKER_METADATA ENABLE ROW LEVEL SECURITY;`,
		`CREATE POLICY TENANT_POLICY on ${schema}.WORKER_METADATA USING (
			tenant_id = current_setting('pgqueue.current_tenant', true)
		);`,

		/**
		 * --- SCHEDULE ---
		 */
		`CREATE TABLE ${schema}.SCHEDULES (
			id UUID NOT NULL,
			tenant_id VARCHAR(40) NOT NULL DEFAULT current_setting('pgqueue.current_tenant', false),
			queue VARCHAR(${MAX_NAME_LEN}) NOT NULL,
			type VARCHAR(40) NOT NULL,
			key VARCHAR(${MAX_KEY_LEN}),
			paused BOOLEAN DEFAULT FALSE,
			retry_policy JSONB,
			created TIMESTAMP NOT NULL DEFAULT now(),
			updated TIMESTAMP,
			version INTEGER NOT NULL DEFAULT 0,
			tries INTEGER NOT NULL DEFAULT 0,
			next_run TIMESTAMP,
			last_run TIMESTAMP,
			payload BYTEA,
			payload_type VARCHAR(255),
			target JSONB,
			schedule VARCHAR(64),
			timezone VARCHAR(32) NOT NULL,
			PRIMARY KEY(id)
		);`,
		`CREATE UNIQUE INDEX SCHEDULE_TENANT_KEY ON ${schema}.SCHEDULES (tenant_id, key);`,
		`CREATE INDEX SCHEDULE_NEXT_RUN ON ${schema}.SCHEDULES (next_run, paused);`,
		`ALTER TABLE ${schema}.SCHEDULES ENABLE ROW LEVEL SECURITY;`,
		`CREATE POLICY TENANT_POLICY on ${schema}.SCHEDULES USING (
			tenant_id = current_setting('pgqueue.current_tenant', true)
		);`,
		`CREATE POLICY WORKER_POLICY on ${schema}.SCHEDULES TO QUEUE_WORKER USING (TRUE);`,
	],
	downs: [
		`DROP TABLE IF EXISTS ${schema}.WORK_QUEUE;`,
		`DROP TABLE IF EXISTS ${schema}.WORKER_METADATA;`,
		`DROP TABLE IF EXISTS ${schema}.QUEUE_CONFIG;`,
		`DROP TABLE IF EXISTS ${schema}.QUEUE_HISTORY;`,
		`DROP TABLE IF EXISTS ${schema}.QUEUE;`,
		`DROP TABLE IF EXISTS ${schema}.SCHEDULES;`,
	],
})

const createPartitions = (
	schema: string,
	months: number,
	startAt: number = 0
): string[] => {
	const currentMonth = startOfMonth(new Date())
	const sqls = []
	for (let i = 0; i < months; i++) {
		const start = addMonths(currentMonth, i + startAt)
		const end = addMonths(start, 1)
		sqls.push(`
		CREATE TABLE ${schema}.QUEUE_HISTORY_${format(start, 'yyyy_MM')}
		PARTITION OF ${schema}.QUEUE_HISTORY
		FOR VALUES FROM ('${format(start, 'yyyy-MM-dd')}') TO ('${format(end, 'yyyy-MM-dd')}')
	;`)
	}
	return sqls
}

export default apply
