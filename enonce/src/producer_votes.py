#!/usr/bin/env python3
"""Producteur Kafka — topic vote_events_raw."""

import csv
import json
import os
import random
import time
import uuid
import datetime as dt
from pathlib import Path

from confluent_kafka import Producer

BOOTSTRAP = "localhost:9092"
TOPIC = "vote_events_raw"

_ROOT = Path(__file__).resolve().parent.parent.parent
COMMUNES_FILE = _ROOT / "data" / "communes_fr.json"
SAMPLE_COMMUNES = _ROOT / "data" / "communes_fr_sample.json"
CANDIDATES_FILE = _ROOT / "data" / "candidates.csv"

MAX_MESSAGES = int(os.getenv("MAX_MESSAGES", "0"))  # <=0: infinite live stream
START_DELAY_MS = float(os.getenv("START_DELAY_MS", "20"))


def now_utc_z() -> str:
    """Retourne une date ISO8601 UTC terminée par Z, compatible Python 3.10+."""
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def load_communes() -> list[dict]:
    """Charge les communes depuis le JSON principal ou l'échantillon."""
    source = COMMUNES_FILE if COMMUNES_FILE.exists() else SAMPLE_COMMUNES
    if not source.exists():
        raise RuntimeError(
            f"Aucun fichier communes trouvé : {COMMUNES_FILE} ou {SAMPLE_COMMUNES}"
        )

    content = source.read_text(encoding="utf-8")
    data = json.loads(content)

    if not isinstance(data, list):
        raise RuntimeError(f"Le fichier {source} doit contenir une liste JSON.")

    communes: list[dict] = []
    for row in data:
        if not isinstance(row, dict):
            continue
        code = str(row.get("code", "")).strip()
        nom = str(row.get("nom", "")).strip()
        if not code or not nom:
            continue

        communes.append(
            {
                "code": code,
                "nom": nom,
                "codeDepartement": str(row.get("codeDepartement", "")).strip(),
                "codeRegion": str(row.get("codeRegion", "")).strip(),
                "population": row.get("population", 0),
            }
        )

    if not communes:
        raise RuntimeError("Aucune commune exploitable n'a été chargée.")

    return communes


def load_candidates() -> list[dict]:
    """Charge les candidats depuis candidates.csv."""
    if not CANDIDATES_FILE.exists():
        raise RuntimeError(f"Fichier candidats introuvable : {CANDIDATES_FILE}")

    candidates: list[dict] = []
    with CANDIDATES_FILE.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            cid = str(row.get("candidate_id", "")).strip()
            if not cid:
                continue
            candidates.append(
                {
                    "candidate_id": cid,
                    "candidate_name": str(row.get("candidate_name", "")).strip(),
                    "party": str(row.get("party", "")).strip(),
                    "political_block": str(row.get("political_block", "autre")).strip() or "autre",
                }
            )

    if not candidates:
        raise RuntimeError("Aucun candidat exploitable trouvé dans candidates.csv")

    return candidates


def choose_candidate(candidates: list[dict]) -> dict:
    """Tirage simple uniforme parmi les candidats."""
    return random.choice(candidates)


def build_realtime_event(communes: list[dict], candidates: list[dict], sent: int) -> dict:
    """Construit un vote temps réel."""
    c = random.choice(communes)
    cand = choose_candidate(candidates)
    ts = now_utc_z()

    evt = {
        "vote_id": str(uuid.uuid4()),
        "election_id": "muni_2026",
        "event_time": ts,
        "city_code": str(c["code"]),
        "city_name": str(c["nom"]),
        "department_code": str(c.get("codeDepartement", "")),
        "region_code": str(c.get("codeRegion", "")),
        "polling_station_id": f"PS_{random.randint(1, 999):03d}",
        "candidate_id": str(cand["candidate_id"]),
        "candidate_name": str(cand.get("candidate_name", "")),
        "candidate_party": str(cand.get("party", "")),
        "candidate_block": str(cand.get("political_block", "autre")),
        "channel": random.choice(["booth", "mobile", "assist_terminal"]),
        "signature_ok": True,
        "voter_hash": uuid.uuid4().hex,
        "ingestion_ts": ts,
    }

    # Injecter quelques erreurs pour alimenter le flux de rejets
    p = random.random()
    if p < 0.03:
        evt["signature_ok"] = False
    elif p < 0.05:
        evt["vote_id"] = ""
    elif p < 0.08:
        evt["candidate_id"] = "C99"
        evt["candidate_name"] = "Unknown Candidate"
        evt["candidate_party"] = ""
        evt["candidate_block"] = "autre"

    return evt


def main() -> None:
    producer = Producer({"bootstrap.servers": BOOTSTRAP})

    sent = 0
    communes = load_communes()
    candidates = load_candidates()

    print(f"Communes chargées : {len(communes)}")
    print(f"Candidats chargés : {len(candidates)}")
    print("Producteur démarré…")

    while True:
        evt = build_realtime_event(communes, candidates, sent)

        key = (evt.get("city_code") or "UNKNOWN").encode("utf-8")
        value = json.dumps(evt, ensure_ascii=False).encode("utf-8")

        producer.produce(TOPIC, key=key, value=value)
        sent += 1

        if sent % 500 == 0:
            producer.flush()
            print(f"Envoyé {sent} message(s) sur {TOPIC}")

        if START_DELAY_MS > 0:
            time.sleep(START_DELAY_MS / 1000.0)

        if MAX_MESSAGES > 0 and sent >= MAX_MESSAGES:
            break

    producer.flush()
    print(f"Envoyé {sent} message(s) sur {TOPIC} (flux temps réel)")


if __name__ == "__main__":
    main()
