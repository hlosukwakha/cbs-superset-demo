# CBS StatLine → Postgres → Superset (with DVC & Docker)

[![Python](https://img.shields.io/badge/Python-3.11+-blue?logo=python)](https://www.python.org/)
[![Docker](https://img.shields.io/badge/Docker-Containerized-blue?logo=docker)](https://www.docker.com/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-blue?logo=postgresql)](https://www.postgresql.org/)
[![Apache Superset](https://img.shields.io/badge/Apache_Superset-Dashboards-orange?logo=apache)](https://superset.apache.org/)
[![DVC](https://img.shields.io/badge/DVC-Data_Version_Control-purple?logo=dvc)](https://dvc.org/)
[![pandas](https://img.shields.io/badge/pandas-DataFrames-150458?logo=pandas)](https://pandas.pydata.org/)
[![SQLAlchemy](https://img.shields.io/badge/SQLAlchemy-ORM-red)](https://www.sqlalchemy.org/)
[![psycopg2](https://img.shields.io/badge/psycopg2-Postgres_Driver-lightgrey)](https://www.psycopg.org/)
[![pyarrow](https://img.shields.io/badge/pyarrow-Parquet-brightgreen)](https://arrow.apache.org/)
[![cbsodata](https://img.shields.io/badge/cbsodata-CBS_API-yellow)](https://pypi.org/project/cbsodata/)

---

## What this project is about (and why it matters)

This repository is a **mini analytics platform in a box**.

It shows how you can take **official open data from Statistics Netherlands (CBS StatLine)**,
pipeline it through **versioned data ingestion** (DVC + Python), land it into **Postgres**,
and finally explore and visualise everything using **Apache Superset** dashboards.

In most organisations, visibility is the real bottleneck:

- Engineers can *collect* data.
- Analysts can *query* data.
- But very few teams have a simple way to turn that data into **living dashboards** that
  anyone can explore, filter and remix without touching SQL every day.

That’s where Superset shines.

### Superset as a visibility superpower

**Apache Superset** is a powerful, open-source BI and dashboarding platform. It gives you:

- Browser-based chart builder: time series, maps, pivot tables, KPIs, heatmaps and more.
- Rich filters, drill-downs and interactive dashboards.
- SQL Lab for power users, plus a low/no-code experience for others.
- Native support for Postgres and tons of other databases.

Whether you are an:

- **Observability engineer** stitching together service metrics and events,
- **Analytics developer** building reusable dashboards for business stakeholders,
- **Data engineer** just trying to validate a new data pipeline,
- **Report analyst** replacing static Excel exports with something live,

…Superset can take your **“I know the data is there somewhere”** and turn it into
**“I can show you that right now”**.

This project gives you a ready-made sandbox to explore that workflow using Dutch open data.

---

## Data source: CBS StatLine

The data comes from **CBS StatLine**, the open data portal of Statistics Netherlands.

CBS provides high-quality, official statistics about the Netherlands, and exposes them via:

- An **OData API** (used here via the `cbsodata` Python package), and
- Downloadable CSV/Excel files.

In this demo we ingest (by default):

- **Population; key figures** — table `37296ENG`
- **Population dynamics; birth, death and migration per region** — table `37259ENG`
- **Consumer prices; price index 2015=100** — table `83131ENG`
- **Learners & students; education type, region of residence** — table `85701NED`

All of these are **open data**. When you use/reshare dashboards built on top of them,
you should attribute CBS appropriately.

---

## How everything fits together

Conceptually, the stack looks like this:

1. **CBS StatLine API (cbsodata)**  
   Python script (`scripts/cbs_pipeline.py`) fetches raw CBS tables via the OData API.

2. **DVC + local data folders**  
   Raw CSVs are stored under `data/raw/`, cleaned & enriched data under `data/processed/`.
   DVC can track and version these artefacts so you always know *which* data produced
   a given dashboard.

3. **Postgres**  
   Processed data is loaded into a `cbs` schema in Postgres, one table per dataset
   (e.g. `cbs.population_keyfigures`). This is your analytical warehouse for this project.

4. **Superset**  
   Superset connects to Postgres, treats tables as datasets, and lets you build charts and
   dashboards on top of them (time series, maps, KPIs, etc.).

5. **Docker Compose**  
   A single `docker-compose.yml` brings up:
   - `postgres` (database)
   - `superset` (BI UI)
   - `cbs_etl` (Python ETL & DVC environment)

Together, this gives you a realistic **“data ingestion → warehouse → BI dashboard”**
flow in a single, reproducible project.

---

## Project tree

```text
cbs-superset-demo/
├── docker-compose.yml         # Orchestrates Postgres, Superset, ETL
├── Dockerfile.etl             # Python image for the ETL/DVC container
├── requirements.txt           # Python dependencies
├── dvc.yaml                   # DVC pipeline (download → transform → load)
├── .dvcignore                 # Files/dirs ignored by DVC
├── .env.example               # Example env vars (copy to .env)
├── .gitignore                 # Git ignore rules
├── scripts/
│   └── cbs_pipeline.py        # Main ETL: CBS → CSV → Parquet → Postgres
├── superset/
│   └── superset_config.py     # Optional Superset overrides
└── data/
    ├── raw/                   # Raw CBS CSVs (DVC output)
    └── processed/             # Cleaned Parquet files (DVC output)
```

---

## Cloning the project

```bash
git clone https://github.com/hlosukwakha/cbs-superset-demo.git
cd cbs-superset-demo
```

If you want to customize secrets, copy the env file:

```bash
cp .env.example .env
# Edit .env as needed (Postgres password, Superset secret key, etc.)
```

---

## How to start and run

### 1. Start the stack

From the project root:

```bash
docker compose up -d --build
```

This will:

- Build the `etl` image
- Start **Postgres**
- Start **Superset**
- Start the **ETL container** (`cbs_etl`) which runs:
  - `python scripts/cbs_pipeline.py download`
  - `python scripts/cbs_pipeline.py transform`
  - `python scripts/cbs_pipeline.py load`

You can follow the ETL logs with:

```bash
docker compose logs -f etl
```

### 2. Confirm tables in Postgres

```bash
docker compose exec postgres       psql -U superset -d superset       -c "SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema = 'cbs';"
```

You should see tables like:

- `population_keyfigures`
- `population_dynamics_region`
- `cpi`
- `pupils_region`

### 3. Log into Superset

Open:

```text
http://localhost:8088
```

The initial admin credentials (created by `docker-compose.yml`) are:

- **Username:** `admin`
- **Password:** `admin`

### 4. Register the Postgres database in Superset

In Superset:

1. Go to **Settings → Database Connections → + Database**.
2. Choose **PostgreSQL**.
3. Use the SQLAlchemy URI:

   ```text
   postgresql+psycopg2://superset:superset@postgres:5432/superset
   ```

4. Test & save.

### 5. Create datasets and build dashboards

For each table (`population_keyfigures`, `population_dynamics_region`, `cpi`, `pupils_region`):

1. Go to **Data → Datasets → + Dataset**.
2. Select:
   - **Database:** your Postgres connection
   - **Schema:** `cbs`
   - **Table:** one of the above
3. Save.

From there you can:

- Build a **time-series chart**: total population vs net migration over time.
- Build **regional comparison** charts or maps from `population_dynamics_region`.
- Build an **inflation dashboard** from `cpi`.
- Explore **education & pupils** by region from `pupils_region`.

---

## Adding a second ingestor

One of the strengths of this setup is that it’s easy to add more data sources and
feed them into the same Postgres + Superset instance.

Example pattern for a second ingestor:

1. **Add a new table ID or source**  
   - Extend `TABLES` in `scripts/cbs_pipeline.py` with a new key, e.g.
     `"housing_prices": "XXXXXX"` for another CBS StatLine table, **or**
   - Add logic that reads from a different API or a local CSV/JSON file.

2. **Update the transform step**  
   - In `transform()`, add any dataset-specific cleaning logic for your new source.

3. **Let the load step create a new Postgres table**  
   - The `load()` function will automatically write your new dataset into the
     `cbs` schema as `cbs.<name>` (e.g. `cbs.housing_prices`).

4. **Add a DVC stage (optional but recommended)**  
   In `dvc.yaml`, you can either:
   - Reuse the existing stages (they will already include your new files if you update the script), or
   - Add new stages if you want separate pipelines for different ingestors.

5. **Expose it in Superset**  
   - Create a new dataset in Superset pointing at the new Postgres table.
   - Build charts & dashboards as usual.

You can also add **non-CBS sources** this way: Kafka sinks, log exports, CSVs from
internal systems, etc. – as long as your Python code writes them into Postgres,
Superset doesn’t care where they came from.

---

## Data source details

- Provider: **Statistics Netherlands (CBS)**  
- Portal: **CBS StatLine** (open data)  
- Access method: **OData API** via the `cbsodata` Python package  
- Licence: open data (attribution to CBS required for re-use)  

In production, you might:

- Pin specific table versions or snapshots with DVC.
- Store large data files in a remote DVC storage (S3, GCS, SSH, etc.).
- Add tests/validations to the pipeline (row counts, schema checks, etc.).

---

## Issues and troubleshooting

| Issue | Explanation | Fix | Sanity checks / commands |
|-------|-------------|-----|---------------------------|
| **No tables under schema `cbs` in Postgres** | ETL didn’t complete the `load` step, or failed earlier. | Check ETL logs, fix underlying error (e.g. missing dependency, network error), then rerun ETL. | `docker compose logs -f etl` and `docker compose exec postgres psql -U superset -d superset -c "SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema = 'cbs';"` |
| **`ChunkedEncodingError` / incomplete read from CBS API** | CBS StatLine sometimes cuts big responses mid-stream; `requests` raises `ChunkedEncodingError`. | The ETL script includes retry logic, but if it still fails, rerun the ETL or try again later. Problem is external (network / remote server). | Check ETL logs for `[download]` retries; rerun: `docker compose restart etl` or run `python scripts/cbs_pipeline.py download` inside the ETL container. |
| **`ImportError: Unable to find a usable engine; tried using: 'pyarrow', 'fastparquet'`** | Pandas needs a Parquet engine to write `*.parquet` files. | Ensure `pyarrow` (or `fastparquet`) is in `requirements.txt`, rebuild `etl` image: `docker compose build etl`, then rerun ETL. | `docker compose exec etl python -c "import pyarrow; print(pyarrow.__version__)"` |
| **Superset error: `relation "<something>" does not exist`** | Dataset in Superset points to a table name that doesn’t match the actual table in Postgres (e.g. `37259ENG` instead of `population_dynamics_region`). | Edit the dataset in Superset and set **Schema = `cbs`**, **Table = correct table name** (e.g. `population_dynamics_region`). | In Superset, go to **Data → Datasets**, open dataset, check table; in Postgres, list tables to confirm names. |
| **Superset shows 404 errors in logs (static files)** | Browser requested an old or non-existent static asset; Flask logs a 404 but app otherwise works. | Usually safe to ignore for local/dev. Only investigate if the UI itself is broken or missing assets. | Check browser dev tools (Network tab) and Superset logs; confirm the main UI loads correctly. |
| **Warning: Using in-memory storage for rate limits** | Flask-Limiter uses in-memory backend by default; not ideal for production. | Safe to ignore for local/dev. For production, configure a Redis or similar backend as per Superset docs. | Ensure it’s only a WARNING in logs; app should still function. |
| **Superset cannot connect to Postgres** | Wrong connection string, DB not ready, or container not running. | Verify `postgres` container is up, check env vars, update connection string in Superset. | `docker compose ps`, `docker compose logs postgres`, and in Superset test DB connection under **Settings → Database Connections**. |

---

## Next steps / ideas

- Add more CBS tables (housing, labour market, health, etc.).
- Layer in **geo data** (municipality shapes) for choropleth maps.
- Schedule the ETL with Airflow/Prefect to refresh data regularly.
- Use DVC remotes to version data snapshots over time.

If you’re an observability engineer, analytics developer, data engineer or report analyst,
this repo is a good starting point for building **real dashboards on real public data**
with a stack you can also run in production.
