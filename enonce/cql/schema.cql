CREATE KEYSPACE IF NOT EXISTS elections
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE elections;

CREATE TABLE IF NOT EXISTS votes_by_city_minute (
    city_code text,
    minute_bucket text,
    candidate_id text,
    votes_count bigint,
    PRIMARY KEY ((city_code), minute_bucket, candidate_id)
) WITH CLUSTERING ORDER BY (minute_bucket DESC, candidate_id ASC);

CREATE TABLE IF NOT EXISTS votes_by_candidate_city (
    candidate_id text,
    city_code text,
    minute_bucket text,
    votes_count bigint,
    PRIMARY KEY ((candidate_id), city_code, minute_bucket)
) WITH CLUSTERING ORDER BY (city_code ASC, minute_bucket DESC);

CREATE TABLE IF NOT EXISTS votes_by_department_block (
    department_code text,
    block text,
    votes_count bigint,
    PRIMARY KEY ((department_code), block)
);
