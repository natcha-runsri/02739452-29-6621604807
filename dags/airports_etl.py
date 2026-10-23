"""Global airports ETL: OurAirports + OpenFlights + Wikipedia (scraped) -> PostgreSQL.

Tasks exchange data via files in data/<run_id_safe>/ (raw/, clean/, ref/); XCom holds only counts.
"""
import json
import logging
import os
import re
from io import BytesIO, StringIO
from pathlib import Path
from urllib.robotparser import RobotFileParser

import pandas as pd
import pendulum
import psycopg2
import requests
from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowException
from bs4 import BeautifulSoup
from psycopg2.extras import Json, execute_values

log = logging.getLogger(__name__)

DATA_DIR = Path("/opt/airflow/data")
HEADERS = {
    "User-Agent": "AirportsETL/1.0 (student exam project; "
    "+https://github.com/natcha-runsri/02739452-29-6621604807) python-requests"
}
TIMEOUT = 30
OURAIRPORTS_BASE = "https://davidmegginson.github.io/ourairports-data"
OPENFLIGHTS_URL = "https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat"
WIKI_URL = "https://en.wikipedia.org/wiki/List_of_international_airports_by_country"
WIKI_ROBOTS = "https://en.wikipedia.org/robots.txt"

SOURCES = ["ourairports", "openflights", "wikipedia"]  # dedupe priority order
COLUMNS = [
    "source", "source_id", "airport_name", "city", "country", "country_code",
    "airport_type", "iata_code", "icao_code", "latitude", "longitude",
]
ATTRS = [
    "airport_name", "city", "country", "country_code", "airport_type",
    "iata_code", "icao_code", "latitude", "longitude",
]
FINAL_COLUMNS = ["airport_key", *ATTRS, "sources"]
# Names used by OpenFlights/Wikipedia that differ from OurAirports countries.csv
COUNTRY_ALIASES = {
    "czech republic": "CZ", "czechia": "CZ", "burma": "MM", "myanmar": "MM",
    "ivory coast": "CI", "cote d'ivoire": "CI", "côte d'ivoire": "CI",
    "congo (kinshasa)": "CD", "democratic republic of the congo": "CD",
    "congo (brazzaville)": "CG", "republic of the congo": "CG",
    "macau": "MO", "hong kong": "HK", "taiwan": "TW", "russia": "RU",
    "south korea": "KR", "north korea": "KP", "east timor": "TL", "timor-leste": "TL",
    "eswatini": "SZ", "swaziland": "SZ", "cape verde": "CV", "cabo verde": "CV",
    "united states": "US", "united kingdom": "GB", "turkey": "TR", "türkiye": "TR",
    "palestine": "PS", "vatican city": "VA", "micronesia": "FM",
    "the bahamas": "BS", "bahamas": "BS", "the gambia": "GM", "gambia": "GM",
}
EXTRACT_ARGS = {"retries": 1, "retry_delay": pendulum.duration(seconds=30)}


def run_dir(context) -> Path:
    return DATA_DIR / re.sub(r"[^A-Za-z0-9_.-]", "_", context["run_id"])


def fetch(url: str) -> requests.Response:
    resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    resp.raise_for_status()
    return resp


def read_csv(src, **kwargs) -> pd.DataFrame:
    # keep_default_na=False: "NA" is Namibia's ISO code, not a null
    return pd.read_csv(src, dtype=str, keep_default_na=False, na_values=[""], **kwargs)


def write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    df.to_csv(tmp, index=False)
    tmp.replace(path)  # atomic: a failed task never leaves a partial file


def save_raw(df: pd.DataFrame, source: str, context) -> int:
    if df.empty:
        raise AirflowException(f"{source}: extracted 0 rows")
    df = df.assign(source=source).reindex(columns=COLUMNS)
    write_csv(df, run_dir(context) / "raw" / f"{source}.csv")
    log.info("%s: %d rows written", source, len(df))
    return len(df)


def get_countries(context) -> pd.DataFrame:
    path = run_dir(context) / "ref" / "countries.csv"
    if not path.exists():
        write_csv(read_csv(BytesIO(fetch(f"{OURAIRPORTS_BASE}/countries.csv").content)), path)
    return read_csv(path)


def db_connect():
    return psycopg2.connect(os.environ["AIRFLOW_CONN_AIRPORTS_DB"])


def upsert(cur, table: str, cols: list, pk: list, ts_col: str, df: pd.DataFrame) -> int:
    rows = [
        tuple(None if pd.isna(v) else v for v in row)
        for row in df[cols].itertuples(index=False, name=None)
    ]
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c not in pk)
    sql = (
        f"INSERT INTO {table} ({', '.join(cols)}) VALUES %s "
        f"ON CONFLICT ({', '.join(pk)}) DO UPDATE SET {updates}, {ts_col} = now()"
    )
    execute_values(cur, sql, rows, page_size=1000)
    return len(rows)


# ---------------------------------------------------------------- extract
@task(**EXTRACT_ARGS)
def ourairports(**context):
    countries = get_countries(context)
    df = read_csv(BytesIO(fetch(f"{OURAIRPORTS_BASE}/airports.csv").content))
    df = df[df["type"] != "closed"]
    out = pd.DataFrame({
        "source_id": df["id"],
        "airport_name": df["name"],
        "city": df["municipality"],
        "country_code": df["iso_country"],
        "country": df["iso_country"].map(dict(zip(countries["code"], countries["name"]))),
        "airport_type": df["type"],
        "iata_code": df["iata_code"],
        "icao_code": df["icao_code"] if "icao_code" in df else df["gps_code"],
        "latitude": df["latitude_deg"],
        "longitude": df["longitude_deg"],
    })
    return save_raw(out, "ourairports", context)


@task(**EXTRACT_ARGS)
def openflights(**context):
    names = ["id", "name", "city", "country", "iata", "icao", "lat", "lon",
             "alt", "tz", "dst", "tzdb", "type", "source"]
    df = pd.read_csv(
        BytesIO(fetch(OPENFLIGHTS_URL).content), header=None, names=names,
        dtype=str, keep_default_na=False, na_values=["\\N", ""],
    )
    df = df[df["type"].fillna("airport") == "airport"]  # drop train stations / ports
    out = pd.DataFrame({
        "source_id": df["id"],
        "airport_name": df["name"],
        "city": df["city"],
        "country": df["country"],
        "airport_type": df["type"],
        "iata_code": df["iata"],
        "icao_code": df["icao"],
        "latitude": df["lat"],
        "longitude": df["lon"],
    })
    return save_raw(out, "openflights", context)


@task(**EXTRACT_ARGS)
def wikipedia(**context):
    robots = RobotFileParser()
    robots.parse(fetch(WIKI_ROBOTS).text.splitlines())
    if not robots.can_fetch(HEADERS["User-Agent"], WIKI_URL):
        raise AirflowException(f"robots.txt disallows {WIKI_URL}")

    soup = BeautifulSoup(fetch(WIKI_URL).text, "lxml")  # single page request
    frames, skipped = [], 0
    for table in soup.select("table.wikitable"):
        heading = table.find_previous(["h2", "h3"])  # h4 sub-headings are skipped over
        if heading is None or heading.name != "h3":
            skipped += 1
            continue
        country = re.sub(r"\[.*?\]", "", heading.get_text(" ", strip=True)).strip()

        df = pd.read_html(StringIO(str(table)))[0]
        df.columns = [" ".join(map(str, c)) if isinstance(c, tuple) else str(c) for c in df.columns]
        mapping = {}
        for col in df.columns:
            low = col.lower()
            if "iata" in low:
                mapping.setdefault("iata_code", col)
            elif "location" in low or "city" in low:
                mapping.setdefault("city", col)
            elif "airport" in low:
                mapping.setdefault("airport_name", col)
        if "airport_name" not in mapping:
            skipped += 1
            continue

        part = pd.DataFrame({k: df[v] for k, v in mapping.items()}).reindex(
            columns=["airport_name", "city", "iata_code"]
        ).astype("string")
        for col in ["airport_name", "city"]:
            part[col] = part[col].str.replace(r"\[[^\]]*\]", "", regex=True).str.strip()
        part["iata_code"] = part["iata_code"].str.split("/").str[0].str.extract(
            r"\b([A-Z]{3})\b", expand=False
        )
        part["country"] = country
        frames.append(part)

    if not frames:
        raise AirflowException("wikipedia: no usable tables parsed")
    out = pd.concat(frames, ignore_index=True).dropna(subset=["airport_name"])
    out = out[out["airport_name"] != ""]
    out["airport_type"] = "international_airport"
    out["source_id"] = out["iata_code"].fillna(out["country"] + ":" + out["airport_name"])
    out = out.drop_duplicates("source_id")
    log.info("wikipedia: %d tables parsed, %d skipped", len(frames), skipped)
    return save_raw(out, "wikipedia", context)


# -------------------------------------------------------------- transform
@task(trigger_rule="all_done")
def transform(**context):
    rd = run_dir(context)
    files = [rd / "raw" / f"{s}.csv" for s in SOURCES if (rd / "raw" / f"{s}.csv").exists()]
    if not files:
        raise AirflowException(f"No raw files in {rd / 'raw'} - all sources failed")
    raw = pd.concat([read_csv(f) for f in files], ignore_index=True).reindex(columns=COLUMNS)
    log.info("Raw files used: %s", [f.stem for f in files])

    for col in COLUMNS:
        s = raw[col].astype("string").str.strip()
        raw[col] = s.where(s != "")
    for col in ["iata_code", "icao_code", "country_code"]:
        raw[col] = raw[col].str.upper()
    raw["iata_code"] = raw["iata_code"].where(raw["iata_code"].str.fullmatch(r"[A-Z0-9]{3}", na=False))
    raw["icao_code"] = raw["icao_code"].where(raw["icao_code"].str.fullmatch(r"[A-Z0-9]{4}", na=False))
    for col in ["latitude", "longitude"]:
        raw[col] = pd.to_numeric(raw[col], errors="coerce")
    raw = raw.drop_duplicates(["source", "source_id"])

    # Country normalization via OurAirports countries.csv (name -> ISO2, case-insensitive)
    countries = get_countries(context)
    code_by_name = {n.lower(): c for c, n in zip(countries["code"], countries["name"])}
    code_by_name.update(COUNTRY_ALIASES)
    name_by_code = dict(zip(countries["code"], countries["name"]))
    need = raw["country_code"].isna() & raw["country"].notna()
    raw.loc[need, "country_code"] = raw.loc[need, "country"].str.lower().map(code_by_name)
    unmatched = need & raw["country_code"].isna()
    log.info("Country names unmatched: %d distinct (%d rows)",
             raw.loc[unmatched, "country"].nunique(), int(unmatched.sum()))
    raw["country"] = raw["country_code"].map(name_by_code).fillna(raw["country"])

    # Cross-source dedupe: priority order, first non-null value per field wins
    final = raw.assign(_prio=raw["source"].map({s: i for i, s in enumerate(SOURCES)}))
    final = final.sort_values("_prio", kind="stable")
    # Fill missing ICAO from higher-priority IATA->ICAO pairs so IATA-only rows merge
    icao_by_iata = (final.dropna(subset=["iata_code", "icao_code"])
                    .drop_duplicates("iata_code").set_index("iata_code")["icao_code"])
    fill = final["icao_code"].isna()
    final.loc[fill, "icao_code"] = final.loc[fill, "iata_code"].map(icao_by_iata)
    final["airport_key"] = (final["icao_code"].fillna(final["iata_code"])
                            .fillna(final["source"] + ":" + final["source_id"]))
    grouped = final.groupby("airport_key", sort=False)
    airports = grouped[ATTRS].first()
    airports["sources"] = grouped["source"].agg(lambda s: ",".join(dict.fromkeys(s)))
    airports = airports.reset_index()[FINAL_COLUMNS]

    write_csv(raw[COLUMNS], rd / "clean" / "raw_all.csv")
    write_csv(airports, rd / "clean" / "airports.csv")
    counts = {"rows_raw": len(raw), "rows_final": len(airports)}
    log.info("Transform counts: %s", counts)
    return counts


# ---------------------------------------------------------------- quality
@task.branch
def quality_check(**context):
    clean = run_dir(context) / "clean"
    df = read_csv(clean / "airports.csv")
    n = len(df)
    null_name = df["airport_name"].isna().mean() if n else 1.0
    null_country = df["country"].isna().mean() if n else 1.0
    reasons = []
    if n < 100:
        reasons.append(f"only {n} rows (<100)")
    if null_name >= 0.05:
        reasons.append(f"airport_name null {null_name:.1%} (>=5%)")
    if null_country >= 0.10:
        reasons.append(f"country null {null_country:.1%} (>=10%)")
    if not df["airport_key"].is_unique:
        reasons.append("airport_key not unique")
    result = {
        "passed": not reasons,
        "notes": "; ".join(reasons) or
                 f"ok: rows={n}, null_name={null_name:.1%}, null_country={null_country:.1%}",
    }
    (clean / "quality.json").write_text(json.dumps(result))
    log.info("Quality: %s", result)
    return "load" if result["passed"] else "quality_failed"


@task
def quality_failed(**context):
    result = json.loads((run_dir(context) / "clean" / "quality.json").read_text())
    log.error("Quality check failed: %s", result["notes"])
    raise AirflowException(f"Quality check failed: {result['notes']}")


# ------------------------------------------------------------------- load
@task
def load(**context):
    clean = run_dir(context) / "clean"
    raw = read_csv(clean / "raw_all.csv")
    airports = read_csv(clean / "airports.csv")
    for df in (raw, airports):
        for col in ["latitude", "longitude"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    conn = db_connect()
    try:
        with conn, conn.cursor() as cur:
            n_raw = upsert(cur, "airports_raw", COLUMNS, ["source", "source_id"], "loaded_at", raw)
            n_final = upsert(cur, "airports", FINAL_COLUMNS, ["airport_key"], "updated_at", airports)
    finally:
        conn.close()
    log.info("Loaded airports_raw=%d, airports=%d", n_raw, n_final)
    return {"loaded_raw": n_raw, "loaded_final": n_final}


# ----------------------------------------------------------------- status
@task(trigger_rule="all_done")
def record_status(**context):
    dag_run = context["dag_run"]
    me = context["ti"].task_id
    states = {ti.task_id: (str(ti.state) if ti.state else None)
              for ti in dag_run.get_task_instances()}
    source_status = {s: states.get(f"extract.{s}") for s in SOURCES}
    counts = context["ti"].xcom_pull(task_ids="transform") or {}
    qpath = run_dir(context) / "clean" / "quality.json"
    quality = json.loads(qpath.read_text()) if qpath.exists() else {
        "passed": None, "notes": "quality check not run"}

    conn = db_connect()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipeline_runs (run_id, started_at, finished_at, source_status,
                    rows_raw, rows_final, quality_passed, quality_notes)
                VALUES (%s, %s, now(), %s, %s, %s, %s, %s)
                ON CONFLICT (run_id) DO UPDATE SET
                    started_at = EXCLUDED.started_at, finished_at = EXCLUDED.finished_at,
                    source_status = EXCLUDED.source_status, rows_raw = EXCLUDED.rows_raw,
                    rows_final = EXCLUDED.rows_final, quality_passed = EXCLUDED.quality_passed,
                    quality_notes = EXCLUDED.quality_notes
                """,
                (dag_run.run_id, dag_run.start_date, Json(source_status),
                 counts.get("rows_raw"), counts.get("rows_final"),
                 quality["passed"], quality["notes"]),
            )
    finally:
        conn.close()
    log.info("Run status recorded: sources=%s", source_status)

    # Partial extract failures stay red in Grid but don't fail the run; only a missed load does
    if states.get("load") != "success":
        failed = [t for t, s in states.items() if t != me and s in ("failed", "upstream_failed")]
        raise AirflowException(f"Load did not succeed (state={states.get('load')}); failed: {failed}")


@dag(
    dag_id="airports_etl",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    tags=["exam"],
    default_args={"owner": "exam"},
)
def airports_etl():
    @task_group(group_id="extract")
    def extract():
        ourairports()
        openflights()
        wikipedia()

    extracted = extract()
    transformed = transform()
    branch = quality_check()
    loaded = load()
    failed = quality_failed()
    status = record_status()

    extracted >> transformed >> branch
    branch >> [loaded, failed]
    loaded >> status
    failed >> status


airports_etl()
