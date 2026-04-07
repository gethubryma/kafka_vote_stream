#!/usr/bin/env python3
"""Validateur — topics vote_events_raw → valides / rejetés."""

import csv
import json
from pathlib import Path

from confluent_kafka import Consumer, Producer

BOOTSTRAP = "localhost:9092"
TOPIC_IN = "vote_events_raw"
TOPIC_VALID = "vote_events_valid"
TOPIC_REJECTED = "vote_events_rejected"
GROUP_ID = "votes-validator-group"

_ROOT = Path(__file__).resolve().parent.parent.parent
CANDIDATES_FILE = _ROOT / "data" / "candidates.csv"


def load_candidate_ids() -> set[str]:
    ids = set()
    with CANDIDATES_FILE.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            cid = str(row.get("candidate_id", "")).strip()
            if cid:
                ids.add(cid)
    return ids


def main() -> None:
    candidates = load_candidate_ids()

    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
        }
    )
    producer = Producer({"bootstrap.servers": BOOTSTRAP})

    consumer.subscribe([TOPIC_IN])

    stats = {"valid": 0, "rejected": 0}
    print("Validateur démarré…")

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Erreur consumer:", msg.error())
            continue

        evt = json.loads(msg.value().decode("utf-8"))

        # Ordre imposé
        if evt.get("signature_ok") is not True:
            is_valid = False
            reason = "INVALID_SIGNATURE"
        elif not evt.get("vote_id"):
            is_valid = False
            reason = "MISSING_VOTE_ID"
        elif evt.get("candidate_id") not in candidates:
            is_valid = False
            reason = "UNKNOWN_CANDIDATE"
        else:
            is_valid = True
            reason = ""

        out_key = (evt.get("city_code") or "UNKNOWN").encode("utf-8")
        out_value = json.dumps(evt, ensure_ascii=False).encode("utf-8")

        if is_valid:
            producer.produce(TOPIC_VALID, key=out_key, value=out_value)
            stats["valid"] += 1
        else:
            evt["error_reason"] = reason
            producer.produce(
                TOPIC_REJECTED,
                key=out_key,
                value=json.dumps(evt, ensure_ascii=False).encode("utf-8"),
            )
            stats["rejected"] += 1

        total = stats["valid"] + stats["rejected"]
        if total % 500 == 0:
            producer.flush()
            print(
                f"Progression total={total} "
                f"valid={stats['valid']} rejected={stats['rejected']}"
            )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nArrêt validateur")
