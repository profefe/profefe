CREATE EXTENSION IF NOT EXISTS hstore;

DROP TABLE IF EXISTS services;

CREATE TABLE services (
  service_id  SERIAL PRIMARY KEY,
  build_id    VARCHAR(40) NOT NULL,
  token       VARCHAR(40) NOT NULL,
  name        TEXT NOT NULL,
  labels      hstore,
  created_at  TIMESTAMPTZ NOT NULL
);

DROP TABLE IF EXISTS pprof_samples_cpu;

CREATE TABLE pprof_samples_cpu (
  service       INTEGER REFERENCES services ON DELETE CASCADE,
  created_at    TIMESTAMPTZ NOT NULL,
  locations     INTEGER[],
  samples_count BIGINT,
  cpu_nanos     BIGINT,
  labels        jsonb
);

CREATE INDEX ON pprof_samples_cpu (service, created_at DESC);

DROP TABLE IF EXISTS pprof_samples_heap;

CREATE TABLE pprof_samples_heap (
  service       INTEGER REFERENCES services ON DELETE CASCADE,
  created_at    TIMESTAMPTZ NOT NULL,
  locations     INTEGER[],
  alloc_objects BIGINT,
  alloc_bytes   BIGINT,
  inuse_objects BIGINT,
  inuse_bytes   BIGINT,
  labels        jsonb
);

CREATE INDEX ON pprof_samples_heap (service, created_at DESC);

DROP TABLE IF EXISTS pprof_locations;

CREATE TABLE pprof_locations (
  location_id SERIAL PRIMARY KEY,
  func_name   TEXT NOT NULL,
  file_name   TEXT NOT NULL,
  line        INTEGER NOT NULL,

  UNIQUE (func_name, file_name, line)
);

CREATE INDEX ON pprof_locations (func_name, file_name, line);
