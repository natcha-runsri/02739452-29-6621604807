# Problem Log

## Problem 1: Airflow login failed
- **Symptom:** Could not log in to the Airflow UI with the admin credentials.
- **How I found the cause:** `airflow-init` logs showed no user was created; the compose file's
  `airflow users create` command was split across lines without `\`, so the arguments ran as a
  separate command and `|| true` hid the error.
- **Fix:** Added line continuations and removed `|| true` in `docker-compose.yml`; verified with
  `docker compose run --rm airflow-init` ("User admin created"). Credentials documented in
  `.env.example` and README.

## Problem 2: Airflow UI "Internal Server Error" on every page
- **Symptom:** After the admin user was recreated, every Airflow page returned Internal Server Error.
- **How I found the cause:** All containers were Up, and the UI worked in a private window -> the
  browser still held a session cookie for the old (deleted) user.
- **Fix:** Cleared localhost cookies and logged in again.

## Problem 3: psql count command printed garbled output in PowerShell
- **Symptom:** `docker compose exec postgres psql ... -c "select ..."` returned garbled output/errors.
- **How I found the cause:** PowerShell strips inner double quotes when passing arguments to native commands.
- **Fix:** Opened psql interactively instead: `docker compose exec postgres sh -c 'psql -U $POSTGRES_USER -d airports'`.
