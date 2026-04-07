#!/usr/bin/env python3
"""Chargeur des agrégats Kafka vers Cassandra (implémentation à compléter)."""

from cassandra.cluster import Cluster
from confluent_kafka import Consumer
import json

BOOTSTRAP = "localhost:9092"
TOPIC = "vote_agg_city_minute"
GROUP_ID = "agg-cassandra-loader"

CASSANDRA_HOSTS = ["127.0.0.1"]
KEYSPACE = "elections"


def main() -> None:
    # =========================================================================
    # TODO 1 — Connexion Cassandra + requêtes préparées (prepared statements)
    # =========================================================================
    # 1) cluster = Cluster(CASSANDRA_HOSTS)
    # 2) session = cluster.connect(KEYSPACE)   # keyspace "elections" doit exister (schema.cql)
    # 3) Préparer 3 requêtes INSERT (points d’interrogation ? pour le bind) :
    #    - INSERT INTO votes_by_city_minute (city_code, minute_bucket, candidate_id, votes_count) VALUES (?,?,?,?)
    #    - INSERT INTO votes_by_candidate_city (candidate_id, city_code, minute_bucket, votes_count) VALUES (?,?,?,?)
    #    - INSERT INTO votes_by_department_block (department_code, block, votes_count) VALUES (?,?,?)
    #    stmt = session.prepare("...")
    #
    # Ne pas laisser session = None sinon le chargeur ne peut rien insérer.

    session = None

    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
        }
    )

    # =========================================================================
    # TODO 2 — Topics Kafka (liste, pas un seul topic)
    # =========================================================================
    # S’abonner à une LISTE contenant au minimum :
    #   "VOTE_COUNT_BY_CITY_MINUTE"
    #   "VOTE_COUNT_BY_DEPT_BLOCK"
    # Optionnel selon votre sujet : "vote_agg_city_minute"
    #
    # Exemple :
    #   consumer.subscribe(["vote_agg_city_minute", "VOTE_COUNT_BY_CITY_MINUTE", "VOTE_COUNT_BY_DEPT_BLOCK"])
    #
    # Astuce : si vous relancez souvent, changez GROUP_ID (suffixe uuid) pour relire depuis le début
    # sans effet de groupe consumer figé.

    consumer.subscribe([TOPIC])
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

        # =====================================================================
        # TODO 3 — Mapping (clés souvent en MAJUSCULES dans le JSON ksqlDB)
        # =====================================================================
        #
        # Branche A — si topic == "VOTE_COUNT_BY_DEPT_BLOCK" :
        #   department = row.get("DEPARTMENT_CODE_V") or row.get("department_code_v")
        #              or row.get("DEPARTMENT_CODE") or row.get("department_code") or ""
        #   block      = row.get("BLOCK_V") or row.get("block_v")
        #              or row.get("CANDIDATE_BLOCK") or row.get("candidate_block") or "autre"
        #   votes      = int(row.get("TOTAL_VOTES") or row.get("total_votes") or 0)
        #   Si department non vide : INSERT votes_by_department_block
        #
        # Branche B — sinon (topics fenêtre ville / minute) :
        #   city         = row.get("CITY_CODE_V") or row.get("city_code_v") or row.get("CITY_CODE") or row.get("city_code")
        #   cand         = row.get("CANDIDATE_ID_V") or row.get("candidate_id_v") or ...
        #   minute_key   = row.get("WINDOWSTART") or row.get("WINDOW_START") or row.get("window_start")  # souvent epoch ms
        #   votes        = int(row.get("VOTES_IN_MINUTE") or row.get("votes_in_minute") or 0)
        #   Si city/cand manquent : parser msg.key() (bytes) avec regex (code INSEE 5 chiffres, candidat C##).
        #
        # =====================================================================
        # TODO 4 — INSERT
        # =====================================================================
        # - Dept/bloc : session.execute(stmt_dept, (department, block, votes))
        # - Ville minute : session.execute(stmt_city, (city, str(minute_key), cand, votes))
        # - Même ligne aussi dans votes_by_candidate_city : (cand, city, str(minute_key), votes)
        #
        pass


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nArrêt chargeur Cassandra")
