CREATE DATABASE airports;
\connect airports

CREATE TABLE IF NOT EXISTS airports_raw (
    source        text NOT NULL,
    source_id     text NOT NULL,
    airport_name  text,
    city          text,
    country       text,
    country_code  text,
    airport_type  text,
    iata_code     text,
    icao_code     text,
    latitude      double precision,
    longitude     double precision,
    loaded_at     timestamptz DEFAULT now(),
    PRIMARY KEY (source, source_id)
);

CREATE TABLE IF NOT EXISTS airports (
    airport_key   text PRIMARY KEY,
    airport_name  text,
    city          text,
    country       text,
    country_code  text,
    airport_type  text,
    iata_code     text,
    icao_code     text,
    latitude      double precision,
    longitude     double precision,
    sources       text,
    updated_at    timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id          text PRIMARY KEY,
    started_at      timestamptz,
    finished_at     timestamptz,
    source_status   jsonb,
    rows_raw        int,
    rows_final      int,
    quality_passed  boolean,
    quality_notes   text
);
