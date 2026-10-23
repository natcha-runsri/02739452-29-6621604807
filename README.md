# Global Airports Pipeline

Daily pipeline that gathers airport data from 3 public sources, cleans and deduplicates it,
loads it into PostgreSQL, and shows it in a Streamlit dashboard. Orchestrated by Apache Airflow,
everything runs in Docker.

## Topic & scope
- **Topic:** airports worldwide - name, city, country (ISO2), type, IATA/ICAO codes, coordinates.
- **Scope:** all non-closed airports from OurAirports, all airports from OpenFlights, and the
  international airports listed on Wikipedia; merged into one table keyed by ICAO/IATA code.

## Data sources
| # | Source | Access method | Notes |
|---|--------|---------------|-------|
| 1 | [OurAirports](https://davidmegginson.github.io/ourairports-data/) `airports.csv` + `countries.csv` | CSV download (HTTP) | `type == closed` dropped; `countries.csv` used for ISO2 <-> country name |
| 2 | [OpenFlights](https://github.com/jpatokal/openflights) `airports.dat` | Raw data file (headerless CSV, `\N` = null) | only `type == airport` kept |
| 3 | [Wikipedia: List of international airports by country](https://en.wikipedia.org/wiki/List_of_international_airports_by_country) | **Web scraping** - `requests` + BeautifulSoup + `pd.read_html` | `robots.txt` checked first (abort if disallowed), one page request, descriptive User-Agent, 30 s timeout |

**PDPA:** all sources are public facts about airports (infrastructure). No personal data is collected or stored.

## Architecture
```
 OurAirports CSV ──┐
 OpenFlights .dat ─┼─> [Airflow DAG airports_etl, @daily]
 Wikipedia (scrape)┘
     extract (TaskGroup, 3 parallel tasks, retries=1)
        └─> data/<run_id>/raw/<source>.csv
     transform (all_done) ─> clean, normalize country, dedupe
        └─> data/<run_id>/clean/{raw_all,airports}.csv
     quality_check (branch) ──> load (UPSERT) ──┐
                            └─> quality_failed ─┴─> record_status (all_done)
                                     │                     │
                                     v                     v
            PostgreSQL "airports": airports_raw, airports, pipeline_runs
                                     │
                                     v
                     Streamlit dashboard (:8501, plotly)
```
- Tasks pass data through files in a per-run folder (never stale data from earlier runs); XCom carries only counts.
- Dedupe priority: ourairports > openflights > wikipedia; first non-null value per field wins; `sources` lists contributors.
- A failing source turns its task red, but the run continues with the remaining sources. The run fails only if `load` does not succeed.

## Database schema (`sql/init.sql`, DB `airports`)
- `airports_raw` - every row from every source, PK `(source, source_id)`
- `airports` - deduplicated final table, PK `airport_key` (ICAO, else IATA, else `source:source_id`)
- `pipeline_runs` - one row per DAG run: per-source state (jsonb), row counts, quality result

## Levels achieved
- **Level 1, 2, 3** - 3 sources (incl. web scraping) -> Airflow ETL -> PostgreSQL -> Streamlit dashboard
- **Bonus** - "Pipeline status" page: last 10 runs with per-source success/failed badges, counts, quality result

## How to run
```bash
cp .env.example .env              # edit passwords if desired
docker compose up -d --build
```
- Airflow: http://localhost:8080 - login `admin` / `admin` (from `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` in `.env`)
- Streamlit: http://localhost:8501
- In Airflow, unpause `airports_etl` and trigger it, or:
  `docker compose exec airflow-scheduler airflow dags trigger airports_etl`

Credentials live only in `.env` (gitignored); `.env.example` holds placeholder/demo values.

## Verification
- **Run 1:** `airports_raw` = 81,940 rows (ourairports 72,694 / openflights 7,698 / wikipedia 1,548);
  `airports` after cross-source dedupe = 73,963 (~7,977 duplicates removed).
- **Idempotency:** re-ran the DAG -> counts unchanged (81,940 / 73,963), so the UPSERT does not duplicate rows.
- **Source failure:** pointed the OpenFlights URL at a nonexistent file -> `extract.openflights` failed (red)
  after 1 retry; transform / quality_check / load / record_status succeeded and the run was green;
  the Pipeline status page showed openflights as failed. URL reverted afterwards.

See `NOTES.md` for problems encountered and how they were fixed.
