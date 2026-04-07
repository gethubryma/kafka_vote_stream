#!/usr/bin/env python3
"""Tableau de bord Streamlit — lecture Cassandra (implémentation à compléter)."""

import streamlit as st
import pandas as pd
import plotly.express as px
from cassandra.cluster import Cluster

st.set_page_config(page_title="Municipales 2026", layout="wide")
st.title("Municipales 2026 - Dashboard France")

# =============================================================================
# TODO 1 — Connexion + lectures Cassandra
# =============================================================================
# 1) Cluster(["127.0.0.1"]) puis session = cluster.connect("elections")
# 2) Exécuter des SELECT sur les 3 tables (colonnes alignées sur schema.cql) :
#      votes_by_city_minute        -> city_code, minute_bucket, candidate_id, votes_count
#      votes_by_candidate_city     -> candidate_id, city_code, minute_bucket, votes_count
#      votes_by_department_block   -> department_code, block, votes_count   (peut être vide au début)
# 3) Option KPI « votes valides / rejetés » : soit lecture des topics ksqlDB compacts VOTE_COUNT_BY_CANDIDATE
#    et REJECTED_BY_REASON (consumer rapide), soit approximation depuis les DataFrames si vous manquez de temps.
# 4) Si tables vides : afficher st.warning et st.stop() pour éviter des graphiques vides trompeurs.

cluster = Cluster(["127.0.0.1"])
session = cluster.connect("elections")

# =============================================================================
# TODO 2 — DataFrames
# =============================================================================
# Pour chaque jeu de résultats Cassandra : listes de dicts ou boucle sur les lignes puis pd.DataFrame(...).
# Forcer votes_count en numérique : pd.to_numeric(df["votes_count"], errors="coerce").fillna(0)
# Si une table est vide mais l’autre non : voir sujet (repli / fallback) pour ne pas casser l’app.

df_city_minute = pd.DataFrame(
    columns=["city_code", "minute_bucket", "candidate_id", "votes_count"]
)
df_candidate_city = pd.DataFrame(
    columns=["candidate_id", "city_code", "minute_bucket", "votes_count"]
)

# =============================================================================
# TODO 3 — Bloc / parti (candidates.csv)
# =============================================================================
# Charger data/candidates.csv : pour chaque candidate_id récupérer political_block → colonne "block"
# et party → colonne "party" sur le DataFrame des votes (map ou merge).
# Règle sujet : pour les graphiques PAR BLOC, ECO / écologiste est rattaché au bloc "gauche"
# (normaliser avant groupby : si block == "ecologiste" → "gauche" pour ce graphe uniquement).
# Ne pas mélanger les conventions « bloc » et « parti » sur un même graphique.

# Exemple — à remplacer par une lecture de data/candidates.csv (political_block par candidate_id).
POLITICAL_BLOCK = {
    "C01": "centre",
    "C02": "gauche",
    "C03": "droite",
    "C04": "droite_nationale",
    "C05": "ecologiste",
}

# KPI (placeholders) — remplacer par des valeurs calculées (entiers ou chaîne formatée)
col1, col2, col3, col4 = st.columns(4)
col1.metric("Votes valides", "TODO")
col2.metric("Votes rejetés", "TODO")
col3.metric("Taux rejet", "TODO")
col4.metric("Top candidat", "TODO")

st.subheader("Votes par bloc politique")
# =============================================================================
# TODO 4 — Bar chart par bloc
# =============================================================================
# df_candidate_city.groupby("block")["votes_count"].sum() -> reset_index
# px.bar(..., x=nom_affiche_bloc, y=votes_count, color="block", color_discrete_map={...})
# Légendes lisibles (extrême_gauche, gauche, …).

st.info("À compléter (TODO 4) : graphique par blocs politiques")

st.subheader("Votes par parti (nuance)")
# =============================================================================
# TODO 4bis — Bar chart par parti (nuance)
# =============================================================================
# groupby("party") ; couleurs par parti (ex. ECO vert) — PAS les mêmes couleurs que le graphe par bloc.
# Deux graphiques distincts obligatoires.

st.info("À compléter (TODO 4bis) : graphique par parti (nuances / couleurs)")

st.subheader("Votes par minute")
# =============================================================================
# TODO 5 — Série temporelle
# =============================================================================
# Convertir minute_bucket : souvent epoch ms → pd.to_datetime(..., unit="ms", utc=True) puis fuseau d’affichage
# ou parser une chaîne ISO.
# Grouper par minute (somme votes_count), px.line(x=time, y=votes_count).

st.info("À compléter (TODO 5) : série temporelle")

st.subheader("Carte France (votes par commune)")
# =============================================================================
# TODO 6 — Carte
# =============================================================================
# Priorité : agréger votes_by_department_block (department_code, block) pour carte dept × couleur du bloc dominant.
# Repli si table vide : barres « top communes » depuis votes_by_city_minute + noms depuis communes_fr.json.
# Géo : GeoJSON départements (URL publique) ou points communes si vous avez lat/lon dans le JSON.

st.info("À compléter (TODO 6) : carte France")

st.subheader("Top communes / Top candidats")
left, right = st.columns(2)
with left:
    # TODO 7 — Top communes : groupby city_code sur df_city_minute, tri desc, head(10), joindre le nom commune si possible
    st.info("À compléter (TODO 7) : top communes")
with right:
    # TODO 7 — Top candidats : groupby candidate_id, tri desc
    st.info("À compléter (TODO 7) : top candidats")
