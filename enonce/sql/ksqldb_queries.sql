SET 'auto.offset.reset'='earliest';

CREATE STREAM IF NOT EXISTS vote_events_valid_stream (
    vote_id VARCHAR,
    election_id VARCHAR,
    event_time VARCHAR,
    city_code VARCHAR,
    city_name VARCHAR,
    department_code VARCHAR,
    region_code VARCHAR,
    polling_station_id VARCHAR,
    candidate_id VARCHAR,
    candidate_name VARCHAR,
    candidate_party VARCHAR,
    candidate_block VARCHAR,
    channel VARCHAR,
    signature_ok BOOLEAN,
    voter_hash VARCHAR,
    ingestion_ts VARCHAR
) WITH (
    KAFKA_TOPIC='vote_events_valid',
    VALUE_FORMAT='JSON'
);

CREATE STREAM IF NOT EXISTS vote_events_rejected_stream (
    vote_id VARCHAR,
    election_id VARCHAR,
    event_time VARCHAR,
    city_code VARCHAR,
    city_name VARCHAR,
    department_code VARCHAR,
    region_code VARCHAR,
    polling_station_id VARCHAR,
    candidate_id VARCHAR,
    candidate_name VARCHAR,
    candidate_party VARCHAR,
    candidate_block VARCHAR,
    channel VARCHAR,
    signature_ok BOOLEAN,
    voter_hash VARCHAR,
    ingestion_ts VARCHAR,
    error_reason VARCHAR
) WITH (
    KAFKA_TOPIC='vote_events_rejected',
    VALUE_FORMAT='JSON'
);

CREATE TABLE IF NOT EXISTS vote_count_by_candidate AS
SELECT
    candidate_id,
    AS_VALUE(candidate_id) AS candidate_id_v,
    COUNT(*) AS total_votes
FROM vote_events_valid_stream
GROUP BY candidate_id
EMIT CHANGES;

CREATE TABLE IF NOT EXISTS vote_count_by_city_minute
WITH (KEY_FORMAT='JSON') AS
SELECT
    city_code,
    candidate_id,
    AS_VALUE(city_code) AS city_code_v,
    AS_VALUE(candidate_id) AS candidate_id_v,
    WINDOWSTART AS window_start,
    WINDOWEND AS window_end,
    COUNT(*) AS votes_in_minute
FROM vote_events_valid_stream
WINDOW TUMBLING (SIZE 1 MINUTE)
GROUP BY city_code, candidate_id
EMIT CHANGES;

CREATE TABLE IF NOT EXISTS rejected_by_reason AS
SELECT
    error_reason,
    COUNT(*) AS rejected_count
FROM vote_events_rejected_stream
GROUP BY error_reason
EMIT CHANGES;

CREATE TABLE IF NOT EXISTS vote_count_by_dept_block
WITH (KEY_FORMAT='JSON') AS
SELECT
    department_code,
    candidate_block,
    AS_VALUE(department_code) AS department_code_v,
    AS_VALUE(candidate_block) AS block_v,
    COUNT(*) AS total_votes
FROM vote_events_valid_stream
WHERE department_code IS NOT NULL
  AND candidate_block IS NOT NULL
GROUP BY department_code, candidate_block
EMIT CHANGES;
