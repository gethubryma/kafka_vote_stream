#!/usr/bin/env python3
"""Producteur Kafka — topic vote_events_raw (implémentation à compléter selon les repères TODO)."""

import json
import os
import time
import random
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


def load_communes() -> list[dict]:
    """
    TODO 0 — Charger les communes (à implémenter)

    Étapes obligatoires :
    1) Lire le fichier JSON : priorité à COMMUNES_FILE ; s’il est absent, utiliser SAMPLE_COMMUNES.
    2) Ouvrir en UTF-8, lire tout le texte, puis json.loads(...) sur ce contenu.
    3) Le fichier fourni est en général une LISTE d’objets ; chaque objet a au minimum :
         - "code" (str INSEE, ex. "75056")   → city_code
         - "nom" (str)                       → city_name
         - "codeDepartement" (str, ex. "75") → department_code (indispensable pour ksqlDB + carte)
         - "codeRegion" (str)                → region_code (optionnel mais utile)
    4) Filtrer : ne garder que les dicts où code et nom sont non vides.
    5) Si la liste finale est vide : lever une erreur explicite (RuntimeError ou message + arrêt) —
       sinon random.choice lèvera une erreur plus tard.

    Ne pas boucler sur votes_municipales_sample.jsonl pour publier : le sujet impose le mode live (génération).
    """
    return []


def build_realtime_event(communes: list[dict], sent: int) -> dict:
    """
    TODO 4 — Événement JSON généré à la volée (aligné validateur + ksqlDB + chargeur)

    Préparation :
    - Tirer une commune : c = random.choice(communes) (vérifier que la liste n’est pas vide avant).
    - Lire data/candidates.csv (csv ou split) pour choisir un candidate_id EXISTANT et sa colonne
      political_block → exposer la même valeur sous la clé candidate_block dans le JSON.

    Dictionnaire à retourner (clés exactes recommandées, types JSON) :
      vote_id            : str uuid unique (str(uuid.uuid4()))
      election_id        : str fixe ex. "muni_2026"
      event_time         : str ISO8601 UTC finissant par Z (maintenant UTC)
      city_code          : str = c["code"]
      city_name          : str = c["nom"]
      department_code    : str = str(c.get("codeDepartement", ""))  # OBLIGATOIRE pour vote_count_by_dept_block
      region_code        : str = str(c.get("codeRegion", ""))
      polling_station_id : str inventé ex. "PS_001"
      candidate_id       : str depuis le CSV
      candidate_name     : str optionnel (colonne du CSV si présente)
      candidate_party    : str optionnel (colonne party du CSV)
      candidate_block    : str = political_block du CSV  # OBLIGATOIRE pour agrégat département × bloc
      channel            : str parmi "booth" | "mobile" | "assist_terminal"
      signature_ok       : bool True en général
      voter_hash         : str inventé
      ingestion_ts       : str ISO comme event_time

    Optionnel mais très utile pour le validateur / KPI rejets :
    - Dans ~3 à 8 % des cas : signature_ok False OU candidate_id inconnu (ex. "C99") OU vote_id "".
      Sinon rejected_by_reason restera vide et les métriques « rejets » seront fausses.

    Contrainte sujet : ne pas utiliser une boucle « for line in open(jsonl) » comme source principale d’envoi.
    """
    return {
        "vote_id": str(uuid.uuid4()),
        "election_id": "muni_2026",
        "event_time": dt.datetime.now(dt.UTC).isoformat().replace("+00:00", "Z"),
        "city_code": "00000",
        "city_name": "TODO_CITY",
        "candidate_id": "C01",
        "signature_ok": True,
        "ingestion_ts": dt.datetime.now(dt.UTC).isoformat().replace("+00:00", "Z"),
    }


# TODO 5 (avancé) — Territoire non uniforme
#
# Objectif : un département / une commune ne vote pas comme une autre (poids par bloc politique).
# Piste minimale acceptable :
#   - Construire un dict dept_code → distribution { "gauche": 0.3, "droite": 0.25, ... }, tirer un bloc,
#     puis un candidat du CSV dont political_block correspond (après normalisation : ex. "ecologiste" → "gauche"
#     si vous vous alignez avec le sujet carte / blocs).
# Piste avancée : télécharger un CSV data.gouv (résultats agrégés par dept), parser, en déduire des poids.
#
# Sans TODO 5 : tirage uniforme parmi les candidats du CSV reste une version minimale acceptable.


def main() -> None:
    producer = Producer({"bootstrap.servers": BOOTSTRAP})
    sent = 0
    communes = load_communes()
    # Ordre conseillé : finir TODO 0 et TODO 4 avant de tester la boucle.

    while True:
        evt = build_realtime_event(communes, sent)

        # TODO 1 — Clé Kafka (partitionnement)
        # Attendu : bytes UTF-8, stable par zone ; exemple :
        #   key = (evt.get("city_code") or "UNKNOWN").encode("utf-8")
        # Éviter key=None si vous voulez un partitionnement prévisible par commune.
        key = None  # à compléter

        # TODO 2 — Valeur = JSON UTF-8
        # Attendu :
        #   value = json.dumps(evt, ensure_ascii=False).encode("utf-8")
        # ensure_ascii=False conserve les accents ; .encode("utf-8") est requis pour confluent-kafka.
        value = None  # à compléter

        if key is None or value is None:
            raise RuntimeError(
                "Complétez les TODO 1 (clé Kafka en bytes UTF-8) et 2 (valeur JSON encodée en UTF-8) avant d'exécuter."
            )

        producer.produce(TOPIC, key=key, value=value)
        sent += 1

        # TODO 3 (optionnel) — Rythme
        # Après chaque message (ou chaque rafale), time.sleep(START_DELAY_MS / 1000.0) pour voir le flux dans Kafka UI.
        if START_DELAY_MS > 0:
            time.sleep(START_DELAY_MS / 1000.0)

        if MAX_MESSAGES > 0 and sent >= MAX_MESSAGES:
            break

    producer.flush()
    print(f"Envoyé {sent} message(s) sur {TOPIC} (flux temps réel)")


if __name__ == "__main__":
    main()
