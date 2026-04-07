#!/usr/bin/env python3
"""Chargeur des agrégats Kafka vers Cassandra."""

import json
import re

from cassandra.cluster import Cluster
from confluent_kafka import Consumer

BOOTSTRAP = "localhost:9092"
GROUP_ID = "agg-cassandra-loader"
CASSANDRA_HOSTS = ["127.0.0.1"]
KEYSPACE = "elections"

TOPICS = [
    "vote_agg_city_minute",
    "VOTE_COUNT_BY_CITY_MINUTE",
    "VOTE_COUNT_BY_DEPT_BLOCK",
]


def parse_msg_key(msg_key: bytes | None) -> tuple[str, str]:
    """Essaie d'extraire city_code et candidate_id depuis la clé Kafka."""
    if not msg_key:
        return "", ""
    s = msg_key.decode("utf-8", errors="ignore")

    city_match = re.search(r"\b(\d{5})\b", s)
    cand_match = re.search(r"\b(C\d{2})\b", s)

    city = city_match.group(1) if city_match else ""
    cand = cand_match.group(1) if cand_match else ""
    return city, cand


def main() -> None:
    cluster = Cluster(CASSANDRA_HOSTS)
    session = cluster.connect(KEYSPACE)

    stmt_city = session.prepare(
        """
        INSERT INTO votes_by_city_minute
        (city_code, minute_bucket, candidate_id, votes_count)
        VALUES (?, ?, ?, ?)
        """
    )
    stmt_candidate_city = session.prepare(
        """
        INSERT INTO votes_by_candidate_city
        (candidate_id, city_code, minute_bucket, votes_count)
        VALUES (?, ?, ?, ?)
        """
    )
    stmt_dept = session.prepare(
        """
        INSERT INTO votes_by_department_block
        (department_code, block, votes_count)
        VALUES (?, ?, ?)
        """
    )

    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe(TOPICS)

    print("Chargeur Cassandra démarré…")

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Erreur consumer:", msg.error())
            continue

        row = json.loads(msg.value().decode("utf-8"))
        topic = msg.topic()

        if topic == "VOTE_COUNT_BY_DEPT_BLOCK":
            department = (
                row.get("DEPARTMENT_CODE_V")
                or row.get("department_code_v")
                or row.get("DEPARTMENT_CODE")
                or row.get("department_code")
                or ""
            )
            block = (
                row.get("BLOCK_V")
                or row.get("block_v")
                or row.get("CANDIDATE_BLOCK")
                or row.get("candidate_block")
                or "autre"
            )
            votes = int(row.get("TOTAL_VOTES") or row.get("total_votes") or 0)

            if department:
                session.execute(stmt_dept, (str(department), str(block), votes))
                print(
                    f"[Cassandra] dept_block department={department} "
                    f"block={block} votes={votes}"
                )
            continue

        city = (
            row.get("CITY_CODE_V")
            or row.get("city_code_v")
            or row.get("CITY_CODE")
            or row.get("city_code")
            or ""
        )
        cand = (
            row.get("CANDIDATE_ID_V")
            or row.get("candidate_id_v")
            or row.get("CANDIDATE_ID")
            or row.get("candidate_id")
            or ""
        )
        minute_key = (
            row.get("WINDOWSTART")
            or row.get("WINDOW_START")
            or row.get("window_start")
            or row.get("MINUTE_BUCKET")
            or row.get("minute_bucket")
            or ""
        )
        votes = int(row.get("VOTES_IN_MINUTE") or row.get("votes_in_minute") or 0)

        if not city or not cand:
            key_city, key_cand = parse_msg_key(msg.key())
            city = city or key_city
            cand = cand or key_cand

        if city and cand and minute_key != "":
            minute_bucket = str(minute_key)
            session.execute(stmt_city, (str(city), minute_bucket, str(cand), votes))
            session.execute(
                stmt_candidate_city, (str(cand), str(city), minute_bucket, votes)
            )
            print(
                f"[Cassandra] city_minute city={city} cand={cand} "
                f"minute={minute_bucket} votes={votes}"
            )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nArrêt chargeur Cassandra")
