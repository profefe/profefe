CREATE EXTENSION IF NOT EXISTS hstore;

DROP TABLE IF EXISTS services;

CREATE TABLE services (
  build_id    VARCHAR(40) NOT NULL,
  token       VARCHAR(40) NOT NULL,
  name        TEXT NOT NULL,
  labels      hstore,
  created_at  TIMESTAMPTZ NOT NULL,

  PRIMARY KEY (build_id, token)
);

DROP TABLE IF EXISTS pprof_samples_cpu;

CREATE TABLE pprof_samples_cpu (
  build_id      VARCHAR(40) NOT NULL,
  token         VARCHAR(40) NOT NULL,
  created_at    TIMESTAMPTZ NOT NULL,
  locations     INTEGER[],
  samples_count BIGINT,
  cpu_nanos     BIGINT,
  labels        jsonb,

  FOREIGN KEY (build_id, token) REFERENCES services ON DELETE CASCADE
);

CREATE INDEX ON pprof_samples_cpu (build_id, token, created_at DESC);

DROP TABLE IF EXISTS pprof_samples_heap;

CREATE TABLE pprof_samples_heap (
  build_id      VARCHAR(40) NOT NULL,
  token         VARCHAR(40) NOT NULL,
  created_at    TIMESTAMPTZ NOT NULL,
  locations     INTEGER[],
  alloc_objects BIGINT,
  alloc_bytes   BIGINT,
  inuse_objects BIGINT,
  inuse_bytes   BIGINT,
  labels        jsonb,

  FOREIGN KEY (build_id, token) REFERENCES services ON DELETE CASCADE
);

CREATE INDEX ON pprof_samples_heap (build_id, token, created_at DESC);

DROP TABLE IF EXISTS pprof_locations;

CREATE TABLE pprof_locations (
  location_id SERIAL PRIMARY KEY,
  func_name   TEXT NOT NULL,
  file_name   TEXT NOT NULL,
  line        INTEGER NOT NULL,

  UNIQUE (func_name, file_name, line)
);

CREATE INDEX ON pprof_locations (func_name, file_name, line);
