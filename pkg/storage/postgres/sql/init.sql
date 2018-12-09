CREATE EXTENSION IF NOT EXISTS hstore;

DROP TABLE IF EXISTS services;

CREATE TABLE services(
  build_id    VARCHAR(40) NOT NULL,
  generation  VARCHAR(100) NOT NULL,
  name        TEXT NOT NULL,
  labels      hstore,

  PRIMARY KEY (build_id, generation)
);

DROP TABLE IF EXISTS profiles_pprof;

CREATE TABLE profiles_pprof(
  digest      VARCHAR(40) PRIMARY KEY UNIQUE,
  type        SMALLINT NOT NULL,
  size        BIGINT NOT NULL,
  created_at  TIMESTAMP without TIME ZONE NOT NULL,
  received_at TIMESTAMP without TIME ZONE NOT NULL,
  build_id    VARCHAR(40) NOT NULL,
  generation  VARCHAR(100) NOT NULL,

  CONSTRAINT fk_service FOREIGN KEY (build_id, generation) REFERENCES services (build_id, generation) ON DELETE CASCADE
);
