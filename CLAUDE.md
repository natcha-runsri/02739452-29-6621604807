# Global Airports Pipeline
Stack: Airflow 2.10.5 (LocalExecutor) -> PostgreSQL 16 -> Streamlit (plotly). Host: Windows + Docker Desktop.

## Layout
- dags/ Airflow DAGs | data/ raw downloads (gitignored) | logs/ Airflow logs
- sql/init.sql DB bootstrap | web/ Streamlit app (Dockerfile, app.py)
- docker-compose.yml, .env (secrets, from .env.example), requirements.txt (Airflow extra pip)
- NOTES.md problem log (symptom / how found cause / fix)

## DB
- `airflow` = Airflow metadata; `airports` = project data (conn id `airports_db`)
- airports_raw(source, source_id PK pair, airport_name, city, country, country_code, airport_type, iata_code, icao_code, latitude, longitude, loaded_at)
- airports(airport_key PK, same attrs, sources, updated_at) - deduplicated final table
- pipeline_runs(run_id PK, started_at, finished_at, source_status jsonb, rows_raw, rows_final, quality_passed, quality_notes)

## Rules
- NEVER read/print data files fully: only `head -5` / `wc -l`.
- `docker logs` only with `--tail 30`.
- Keep replies short.
- UI: Airflow http://localhost:8080, Streamlit http://localhost:8501
