CREATE EXTENSION hstore;

CREATE TABLE IF NOT EXISTS services (
  build_id    VARCHAR(40) NOT NULL,
  generation  VARCHAR(100) NOT NULL,
  name        VARCHAR(255) NOT NULL,
  labels      hstore,

  PRIMARY KEY (build_id, generation)
);

CREATE TABLE IF NOT EXISTS profiles (
  digest      VARCHAR(40) PRIMARY KEY UNIQUE,
  type        SMALLINT NOT NULL,
  size        BIGINT NOT NULL,
  created_at  TIMESTAMP without TIME ZONE NOT NULL,
  received_at TIMESTAMP without TIME ZONE NOT NULL,
  build_id    VARCHAR(40) NOT NULL,
  generation  VARCHAR(100) NOT NULL,

  CONSTRAINT fk_service FOREIGN KEY (build_id, generation) REFERENCES services (build_id, generation) ON DELETE CASCADE
);
