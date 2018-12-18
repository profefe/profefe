CREATE EXTENSION IF NOT EXISTS hstore;

DROP TABLE IF EXISTS services;

CREATE TABLE services(
  name        TEXT NOT NULL,
  build_id    VARCHAR(40) NOT NULL,
  token       VARCHAR(40) NOT NULL,
  created_at  TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  labels      hstore,

  PRIMARY KEY (build_id, token)
);

DROP TABLE IF EXISTS profiles_pprof;

CREATE TABLE profiles_pprof(
  digest      VARCHAR(40) PRIMARY KEY UNIQUE,
  type        SMALLINT NOT NULL,
  size        BIGINT NOT NULL,
  build_id    VARCHAR(40) NOT NULL,
  token       VARCHAR(40) NOT NULL,
  created_at  TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  received_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,

  CONSTRAINT fk_service FOREIGN KEY (build_id, token) REFERENCES services (build_id, token) ON DELETE CASCADE
);
