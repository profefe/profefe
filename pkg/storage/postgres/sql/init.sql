DROP TABLE IF EXISTS services;

CREATE TABLE services (
  service_id SERIAL PRIMARY KEY,
  build_id   TEXT NOT NULL,
  token      TEXT NOT NULL,
  name       TEXT NOT NULL,
  labels     jsonb,
  created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX ON services (build_id, token);

CREATE TABLE pprof_profiles (
  profile_id SERIAL PRIMARY KEY,
  service_id INTEGER REFERENCES services ON DELETE CASCADE,
  created_at TIMESTAMPTZ NOT NULL,
  type       SMALLINT NOT NULL,
  period     BIGINT
);

CREATE INDEX ON pprof_profiles (service_id, created_at DESC);

DROP TABLE IF EXISTS pprof_samples_cpu;

CREATE TABLE pprof_samples_cpu (
  profile_id    INTEGER REFERENCES pprof_profiles ON DELETE CASCADE,
  locations     INTEGER[], -- array of location_ids
  samples_count BIGINT,
  cpu_nanos     BIGINT,
  labels        jsonb
);

DROP TABLE IF EXISTS pprof_samples_heap;

CREATE TABLE pprof_samples_heap (
  profile_id    INTEGER REFERENCES pprof_profiles ON DELETE CASCADE,
  locations     INTEGER[], -- array of location_ids
  alloc_objects BIGINT,
  alloc_bytes   BIGINT,
  inuse_objects BIGINT,
  inuse_bytes   BIGINT,
  labels        jsonb
);

DROP TABLE IF EXISTS pprof_functions;

CREATE TABLE pprof_functions (
  func_id   SERIAL PRIMARY KEY,
  func_name TEXT NOT NULL,
  file_name TEXT NOT NULL,

  UNIQUE (func_name, file_name)
);

DROP TABLE IF EXISTS pprof_mappings;

CREATE TABLE pprof_mappings (
  mapping_id SERIAL PRIMARY KEY,
  mapping    jsonb NOT NULL,

  UNIQUE (mapping)
);

DROP TABLE IF EXISTS pprof_locations;

CREATE TABLE pprof_locations (
  location_id SERIAL PRIMARY KEY,
  mapping_id  INTEGER REFERENCES pprof_mappings,
  address     BIGINT,
  lines       jsonb NOT NULL -- [{line:int, func_id:int}]
);
