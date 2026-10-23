# Global Airports Pipeline

Airflow -> PostgreSQL -> Streamlit.

## Setup
1. `cp .env.example .env` and set passwords
2. `docker compose up -d`
3. Airflow: http://localhost:8080 (login `admin` / `admin`, from `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` in `.env`) | Streamlit: http://localhost:8501

## Data sources
TODO

## Pipeline (DAG)
TODO

## Database schema
See `sql/init.sql`.

## Data quality
TODO

## Dashboard
TODO
