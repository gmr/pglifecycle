-- Objects that only PostgreSQL 19 and later have. fixtures/schema.sql
-- includes this file on these servers only, so the gates of
-- PostgreSQL 18 do not see it.

-- Types that PostgreSQL 19 adds to pg_catalog
CREATE TABLE test.pg19_types (id oid8 PRIMARY KEY, db regdatabase);

-- An aggregate with a planner support function
CREATE AGGREGATE test.sum_supported(INTEGER) (
    SFUNC = int4pl, STYPE = INTEGER, SUPPORT = text_starts_with_support);

-- Publications for all tables but some, for all sequences, and for
-- both
CREATE TABLE test.not_replicated (id INTEGER PRIMARY KEY);
CREATE PUBLICATION pglifecycle_except
    FOR ALL TABLES EXCEPT (TABLE test.not_replicated, TABLE test.pg19_types);
CREATE PUBLICATION pglifecycle_sequences FOR ALL SEQUENCES;
CREATE PUBLICATION pglifecycle_everything FOR ALL TABLES, ALL SEQUENCES;

-- A wrapper with a connection function, and a subscription that gets
-- its connection from a server of the wrapper. postgres_fdw has the
-- function. The subscriptions do not connect, so they need no
-- publisher.
CREATE EXTENSION postgres_fdw SCHEMA public;
CREATE FOREIGN DATA WRAPPER gate_conn_fdw
    CONNECTION public.postgres_fdw_connection;
CREATE SERVER gate_pub_srv FOREIGN DATA WRAPPER postgres_fdw
    OPTIONS (dbname 'pglifecycle_nowhere');
CREATE USER MAPPING FOR postgres SERVER gate_pub_srv;
CREATE SUBSCRIPTION gate_srv_sub SERVER gate_pub_srv PUBLICATION gate_pub
    WITH (connect = false);
CREATE SUBSCRIPTION gate_pg19_sub
    CONNECTION 'dbname=pglifecycle_nowhere' PUBLICATION gate_pub
    WITH (connect = false, retain_dead_tuples = true,
          max_retention_duration = 60000, wal_receiver_timeout = '30s');
