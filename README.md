# CBS StatLine → Postgres → Superset (with DVC)

This project spins up a local data stack that pulls open data from
Statistics Netherlands (CBS) StatLine into Postgres using a reproducible
DVC pipeline and visualises it with Apache Superset.

Datasets (default):

- Population; key figures — `37296ENG`
- Population dynamics; birth, death and migration per region — `37259ENG`
- Consumer prices; price index 2015=100 — `83131ENG`
- Learners & students; education type, region of residence — `85701NED`

All data © CBS, reused under the CBS open-data terms. Please attribute
CBS in downstream dashboards.

## Quickstart

```bash
cp .env.example .env

docker compose up -d postgres superset etl

python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
dvc init
dvc repro load_postgres
```

Then open http://localhost:8088 and log in with `admin` / `admin`.
Add the Postgres database and create datasets under schema `cbs` to
start building dashboards.
