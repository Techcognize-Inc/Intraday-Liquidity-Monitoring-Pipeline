-- ─────────────────────────────────────────────────────────────────
--  Airflow Metadata Database Setup
--
--  Runs automatically on first postgres container start via
--  /docker-entrypoint-initdb.d/ (mounted as 03_airflow_db.sql).
--
--  Creates a separate 'airflow' database and user so Airflow's
--  internal metadata (DAG runs, task states, XCom) is isolated
--  from the liquidity application data in the 'liquidity' database.
-- ─────────────────────────────────────────────────────────────────

CREATE USER airflow WITH PASSWORD 'airflow';

CREATE DATABASE airflow OWNER airflow;

GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
