import html
import os

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, text

DB_URL = (
    f"postgresql+psycopg2://{os.environ['DB_USER']}:{os.environ['DB_PASSWORD']}"
    f"@{os.environ.get('DB_HOST', 'postgres')}:{os.environ.get('DB_PORT', '5432')}"
    f"/{os.environ.get('DB_NAME', 'airports')}"
)
SOURCES = ["ourairports", "openflights", "wikipedia"]
BADGE_COLORS = {"success": "#1a7f37", "failed": "#cf222e", "upstream_failed": "#bc4c00"}


@st.cache_resource
def get_engine():
    return create_engine(DB_URL, pool_pre_ping=True)


@st.cache_data(ttl=60)
def query(sql: str, params: dict | None = None) -> pd.DataFrame:
    with get_engine().connect() as conn:
        return pd.read_sql(text(sql), conn, params=params or {})


def build_where(countries, types, sources) -> tuple[str, dict]:
    clauses, params = [], {}
    if countries:
        clauses.append("country = ANY(:countries)")
        params["countries"] = list(countries)
    if types:
        clauses.append("airport_type = ANY(:types)")
        params["types"] = list(types)
    if sources:
        clauses.append("sources LIKE ANY(:sources)")
        params["sources"] = [f"%{s}%" for s in sources]
    return ("WHERE " + " AND ".join(clauses)) if clauses else "", params


def dashboard():
    st.title("Global Airports")

    countries_opt = query("SELECT DISTINCT country FROM airports WHERE country IS NOT NULL ORDER BY 1")
    types_opt = query("SELECT DISTINCT airport_type FROM airports WHERE airport_type IS NOT NULL ORDER BY 1")
    if countries_opt.empty and types_opt.empty:
        st.info("No airports loaded yet - trigger the `airports_etl` DAG in Airflow.")
        return

    st.sidebar.header("Filters")
    countries = st.sidebar.multiselect("Country", countries_opt["country"].tolist())
    types = st.sidebar.multiselect("Airport type", types_opt["airport_type"].tolist())
    sources = st.sidebar.multiselect("Source", SOURCES)
    where, params = build_where(countries, types, sources)

    kpi = query(
        f"""SELECT count(*) AS total, count(DISTINCT country) AS countries,
                   count(DISTINCT city) AS cities,
                   round(100.0 * count(iata_code) / NULLIF(count(*), 0), 1) AS pct_iata
            FROM airports {where}""",
        params,
    ).iloc[0]
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Airports", f"{int(kpi['total']):,}")
    c2.metric("Countries", f"{int(kpi['countries']):,}")
    c3.metric("Cities", f"{int(kpi['cities']):,}")
    c4.metric("With IATA code", f"{kpi['pct_iata'] or 0}%")
    if not kpi["total"]:
        st.warning("No airports match the current filters.")
        return

    left, right = st.columns(2)
    top = query(
        f"""SELECT country, count(*) AS airports FROM airports {where}
            GROUP BY country ORDER BY airports DESC LIMIT 10""",
        params,
    )
    left.plotly_chart(
        px.bar(top, x="airports", y="country", orientation="h", title="Top 10 countries")
        .update_layout(yaxis={"categoryorder": "total ascending"}),
        use_container_width=True,
    )
    by_type = query(
        f"""SELECT coalesce(airport_type, 'unknown') AS airport_type, count(*) AS airports
            FROM airports {where} GROUP BY 1 ORDER BY 2 DESC""",
        params,
    )
    right.plotly_chart(
        px.pie(by_type, names="airport_type", values="airports", title="Airports by type", hole=0.4),
        use_container_width=True,
    )

    per_source = query("SELECT source, count(*) AS rows FROM airports_raw GROUP BY source ORDER BY 2 DESC")
    if per_source.empty:
        st.info("airports_raw is empty.")
    else:
        st.plotly_chart(
            px.bar(per_source, x="source", y="rows", color="source", text="rows",
                   title="Raw rows per source (airports_raw)"),
            use_container_width=True,
        )

    st.subheader("Airports")
    search = st.text_input("Search name, city, IATA or ICAO").strip()
    table_where, table_params = where, dict(params)
    if search:
        cond = "(airport_name ILIKE :q OR city ILIKE :q OR iata_code ILIKE :q OR icao_code ILIKE :q)"
        table_where = f"{where} AND {cond}" if where else f"WHERE {cond}"
        table_params["q"] = f"%{search}%"
    table = query(
        f"""SELECT airport_name, city, country, airport_type, iata_code, icao_code, sources
            FROM airports {table_where} ORDER BY country, airport_name LIMIT 1000""",
        table_params,
    )
    st.caption(f"Showing {len(table):,} rows (max 1000)")
    st.dataframe(table, use_container_width=True, hide_index=True)


def badge(state) -> str:
    state = state or "n/a"
    color = BADGE_COLORS.get(state, "#6e7781")
    return (f'<span style="background:{color};color:#fff;padding:2px 8px;'
            f'border-radius:10px;font-size:0.8em">{html.escape(state)}</span>')


def pipeline_status():
    st.title("Pipeline status")
    runs = query(
        """SELECT run_id, source_status, rows_raw, rows_final, quality_passed,
                  quality_notes, finished_at
           FROM pipeline_runs ORDER BY finished_at DESC NULLS LAST LIMIT 10"""
    )
    if runs.empty:
        st.info("No pipeline runs recorded yet.")
        return

    header = ["Run", *SOURCES, "Rows raw", "Rows final", "Quality", "Finished"]
    rows = []
    for r in runs.itertuples():
        status = r.source_status or {}
        quality = {True: "success", False: "failed"}.get(r.quality_passed)
        cells = [
            html.escape(r.run_id),
            *(badge(status.get(s)) for s in SOURCES),
            "" if pd.isna(r.rows_raw) else f"{int(r.rows_raw):,}",
            "" if pd.isna(r.rows_final) else f"{int(r.rows_final):,}",
            f'<span title="{html.escape(r.quality_notes or "")}">'
            f'{badge(quality) if quality else badge(None)}</span>',
            "" if pd.isna(r.finished_at) else r.finished_at.strftime("%Y-%m-%d %H:%M UTC"),
        ]
        rows.append("<tr>" + "".join(f"<td>{c}</td>" for c in cells) + "</tr>")
    st.markdown(
        "<table><thead><tr>" + "".join(f"<th>{h}</th>" for h in header)
        + "</tr></thead><tbody>" + "".join(rows) + "</tbody></table>",
        unsafe_allow_html=True,
    )
    st.caption("Hover a quality badge to see its notes.")


st.set_page_config(page_title="Global Airports", layout="wide")
page = st.sidebar.radio("Page", ["Dashboard", "Pipeline status"])
try:
    dashboard() if page == "Dashboard" else pipeline_status()
except Exception as exc:  # DB down or tables missing
    st.error(f"Database error: {exc}")
