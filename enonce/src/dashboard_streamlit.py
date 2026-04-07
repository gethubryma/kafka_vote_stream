#!/usr/bin/env python3
"""Tableau de bord Streamlit — lecture Cassandra."""

import csv
import json
from pathlib import Path

import pandas as pd
import plotly.express as px
import requests
import streamlit as st
from cassandra.cluster import Cluster

st.set_page_config(page_title="Municipales 2026", layout="wide")
st.title("Municipales 2026 - Dashboard France")

_ROOT = Path(__file__).resolve().parent.parent.parent
CANDIDATES_FILE = _ROOT / "data" / "candidates.csv"
COMMUNES_FILE = _ROOT / "data" / "communes_fr.json"
SAMPLE_COMMUNES = _ROOT / "data" / "communes_fr_sample.json"


@st.cache_resource
def get_session():
    cluster = Cluster(["127.0.0.1"])
    return cluster.connect("elections")


@st.cache_data
def load_candidates_meta() -> pd.DataFrame:
    rows = []
    with CANDIDATES_FILE.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(
                {
                    "candidate_id": str(row.get("candidate_id", "")).strip(),
                    "candidate_name": str(row.get("candidate_name", "")).strip(),
                    "party": str(row.get("party", "")).strip(),
                    "block": str(row.get("political_block", "autre")).strip() or "autre",
                }
            )
    return pd.DataFrame(rows)


@st.cache_data
def load_communes_meta() -> pd.DataFrame:
    source = COMMUNES_FILE if COMMUNES_FILE.exists() else SAMPLE_COMMUNES
    if not source.exists():
        return pd.DataFrame(columns=["city_code", "city_name", "department_code"])

    data = json.loads(source.read_text(encoding="utf-8"))
    rows = []
    for row in data:
        rows.append(
            {
                "city_code": str(row.get("code", "")).strip(),
                "city_name": str(row.get("nom", "")).strip(),
                "department_code": str(row.get("codeDepartement", "")).strip(),
            }
        )
    return pd.DataFrame(rows)


@st.cache_data
def load_departements_geojson():
    geojson_url = "https://france-geojson.gregoiredavid.fr/repo/departements.geojson"
    response = requests.get(geojson_url, timeout=15)
    response.raise_for_status()
    return response.json()


def rows_to_df(rows, columns):
    return pd.DataFrame([dict(r._asdict()) for r in rows], columns=columns)


session = get_session()

city_rows = session.execute(
    "SELECT city_code, minute_bucket, candidate_id, votes_count FROM votes_by_city_minute"
)
candidate_city_rows = session.execute(
    "SELECT candidate_id, city_code, minute_bucket, votes_count FROM votes_by_candidate_city"
)
dept_rows = session.execute(
    "SELECT department_code, block, votes_count FROM votes_by_department_block"
)

df_city_minute = rows_to_df(
    city_rows, ["city_code", "minute_bucket", "candidate_id", "votes_count"]
)
df_candidate_city = rows_to_df(
    candidate_city_rows, ["candidate_id", "city_code", "minute_bucket", "votes_count"]
)
df_dept_block = rows_to_df(
    dept_rows, ["department_code", "block", "votes_count"]
)

for df in (df_city_minute, df_candidate_city, df_dept_block):
    if "votes_count" in df.columns:
        df["votes_count"] = pd.to_numeric(df["votes_count"], errors="coerce").fillna(0)

if df_city_minute.empty and df_candidate_city.empty and df_dept_block.empty:
    st.warning("Aucune donnée dans Cassandra pour le moment. Lance d'abord ksqlDB + le chargeur Cassandra.")
    st.stop()

candidates_meta = load_candidates_meta()
communes_meta = load_communes_meta()

if not df_candidate_city.empty:
    df_candidate_city = df_candidate_city.merge(
        candidates_meta, on="candidate_id", how="left"
    )
    df_candidate_city = df_candidate_city.merge(
        communes_meta[["city_code", "city_name", "department_code"]],
        on="city_code",
        how="left",
    )

if not df_city_minute.empty:
    df_city_minute = df_city_minute.merge(
        communes_meta[["city_code", "city_name", "department_code"]],
        on="city_code",
        how="left",
    )
    df_city_minute = df_city_minute.merge(
        candidates_meta[["candidate_id", "candidate_name", "party", "block"]],
        on="candidate_id",
        how="left",
    )

if not df_candidate_city.empty:
    df_candidate_city["block_grouped"] = df_candidate_city["block"].replace(
        {"ecologiste": "gauche"}
    )

votes_valides = int(df_candidate_city["votes_count"].sum()) if not df_candidate_city.empty else 0
votes_rejetes = "N/A"
taux_rejet = "N/A"

top_candidat = "N/A"
if not df_candidate_city.empty:
    top_candidate_df = (
        df_candidate_city.groupby(["candidate_id", "candidate_name"], dropna=False)["votes_count"]
        .sum()
        .reset_index()
        .sort_values("votes_count", ascending=False)
    )
    if not top_candidate_df.empty:
        row = top_candidate_df.iloc[0]
        name = row["candidate_name"] if pd.notna(row["candidate_name"]) and row["candidate_name"] else row["candidate_id"]
        top_candidat = f"{name} ({int(row['votes_count'])})"

col1, col2, col3, col4 = st.columns(4)
col1.metric("Votes valides", f"{votes_valides}")
col2.metric("Votes rejetés", votes_rejetes)
col3.metric("Taux rejet", taux_rejet)
col4.metric("Top candidat", top_candidat)

st.subheader("Votes par bloc politique")
if not df_candidate_city.empty and "block_grouped" in df_candidate_city.columns:
    block_df = (
        df_candidate_city.groupby("block_grouped", dropna=False)["votes_count"]
        .sum()
        .reset_index()
        .sort_values("votes_count", ascending=False)
    )
    fig_block = px.bar(
        block_df,
        x="block_grouped",
        y="votes_count",
        color="block_grouped",
        title="Répartition des votes par bloc",
    )
    st.plotly_chart(fig_block, use_container_width=True)
else:
    st.info("Pas encore de données pour le graphe par bloc.")

st.subheader("Votes par parti (nuance)")
if not df_candidate_city.empty and "party" in df_candidate_city.columns:
    party_df = (
        df_candidate_city.groupby("party", dropna=False)["votes_count"]
        .sum()
        .reset_index()
        .sort_values("votes_count", ascending=False)
    )
    fig_party = px.bar(
        party_df,
        x="party",
        y="votes_count",
        color="party",
        title="Répartition des votes par parti",
    )
    st.plotly_chart(fig_party, use_container_width=True)
else:
    st.info("Pas encore de données pour le graphe par parti.")

st.subheader("Votes par minute")
if not df_city_minute.empty:
    time_df = df_city_minute.copy()
    time_df["minute_bucket_num"] = pd.to_numeric(time_df["minute_bucket"], errors="coerce")

    if time_df["minute_bucket_num"].notna().any():
        time_df["minute_dt"] = pd.to_datetime(
            time_df["minute_bucket_num"], unit="ms", utc=True, errors="coerce"
        )
    else:
        time_df["minute_dt"] = pd.to_datetime(
            time_df["minute_bucket"], utc=True, errors="coerce"
        )

    time_series = (
        time_df.groupby("minute_dt", dropna=True)["votes_count"]
        .sum()
        .reset_index()
        .sort_values("minute_dt")
    )

    if not time_series.empty:
        fig_time = px.line(
            time_series,
            x="minute_dt",
            y="votes_count",
            title="Votes agrégés par minute",
        )
        st.plotly_chart(fig_time, use_container_width=True)
    else:
        st.info("Horodatages non encore disponibles pour la série temporelle.")
else:
    st.info("Pas encore de données pour la série temporelle.")

st.subheader("Carte France par département")
if not df_dept_block.empty:
    try:
        dept_dom = (
            df_dept_block.sort_values("votes_count", ascending=False)
            .groupby("department_code", as_index=False)
            .first()
            .sort_values("department_code")
            .copy()
        )

        dept_dom["department_code"] = dept_dom["department_code"].astype(str).str.zfill(2)

        geojson = load_departements_geojson()

        fig_map = px.choropleth(
            dept_dom,
            geojson=geojson,
            locations="department_code",
            featureidkey="properties.code",
            color="block",
            hover_name="department_code",
            hover_data={"votes_count": True, "block": True},
            title="Bloc dominant par département",
        )

        fig_map.update_geos(fitbounds="locations", visible=False)
        st.plotly_chart(fig_map, use_container_width=True)
    except Exception as e:
        st.warning(f"Impossible d'afficher la carte : {e}")
else:
    st.info("Pas encore de données département × bloc.")

st.subheader("Top communes / Top candidats")
left, right = st.columns(2)

with left:
    if not df_city_minute.empty:
        city_rank = (
            df_city_minute.groupby(["city_code", "city_name"], dropna=False)["votes_count"]
            .sum()
            .reset_index()
            .sort_values("votes_count", ascending=False)
            .head(10)
        )
        st.dataframe(city_rank, use_container_width=True)
    else:
        st.info("Pas encore de données top communes.")

with right:
    if not df_candidate_city.empty:
        cand_rank = (
            df_candidate_city.groupby(["candidate_id", "candidate_name"], dropna=False)["votes_count"]
            .sum()
            .reset_index()
            .sort_values("votes_count", ascending=False)
            .head(10)
        )
        st.dataframe(cand_rank, use_container_width=True)
    else:
        st.info("Pas encore de données top candidats.")
