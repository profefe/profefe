CREATE TABLE IF NOT EXISTS pprof_profiles (
    profile_key FixedString(12),
    profile_type Enum8(
        'cpu' = 1,
        'heap' = 2,
        'block' = 3,
        'mutex' = 4,
        'goroutine' = 5,
        'threadcreate' = 6,
        'other' = 100
    ),
    external_id String,
    service_name LowCardinality(String),
    created_at DateTime,
    labels Nested (
        key LowCardinality(String),
        value String
    )
)
ENGINE=MergeTree()
PARTITION BY (toYYYYMM(created_at), service_name)
ORDER BY (service_name, profile_type, created_at);

CREATE TABLE IF NOT EXISTS pprof_samples (
    profile_key FixedString(12),
    fingerprint UInt64,
    locations Nested (
        func_name LowCardinality(String),
        file_name LowCardinality(String),
        lineno UInt16
    ),
    values Array(Int64),
    values_type Array(LowCardinality(String)),
    values_unit Array(LowCardinality(String)),
    labels Nested (
        key String,
        value String
    )
)
ENGINE=ReplacingMergeTree()
ORDER BY (profile_key, fingerprint);

-- CREATE TABLE pprof_samples_buffer AS pprof_samples
-- ENGINE = Buffer(currentDatabase(), pprof_samples, 16, 10, 60, 1000, 10000, 1048576, 10485760);
