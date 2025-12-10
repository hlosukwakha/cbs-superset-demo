import os
import sys
import time
from pathlib import Path

import pandas as pd
import cbsodata
from sqlalchemy import create_engine, text
from requests.exceptions import ChunkedEncodingError

ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "raw"
PROC_DIR = ROOT / "data" / "processed"

TABLES = {
    "population_keyfigures": "37296ENG",
    "population_dynamics_region": "37259ENG",
    "cpi": "83131ENG",
    "pupils_region": "85701NED",
}

DEFAULT_PG_DSN = "postgresql://superset:superset@postgres:5432/superset"


def ensure_dirs():
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    PROC_DIR.mkdir(parents=True, exist_ok=True)


def add_year_from_perioden(df: pd.DataFrame) -> pd.DataFrame:
    if "Perioden" in df.columns:
        year = pd.to_numeric(df["Perioden"].astype(str).str.slice(0, 4),
                             errors="coerce")
        df["Year"] = year
    return df


def fetch_table_with_retry(table_id: str, retries: int = 5, delay: int = 5):
    """
    Fetch a CBS table using cbsodata.get_data with simple retry logic to
    handle intermittent ChunkedEncodingError / incomplete reads.
    """
    for attempt in range(1, retries + 1):
        try:
            print(f"[download] Attempt {attempt}/{retries} for table {table_id}")
            return cbsodata.get_data(table_id)
        except ChunkedEncodingError as e:
            print(f"[download] ChunkedEncodingError for {table_id}: {e}")
            if attempt == retries:
                print("[download] Giving up after maximum retries")
                raise
            sleep_for = delay * attempt
            print(f"[download] Retrying in {sleep_for} seconds...")
            time.sleep(sleep_for)
        except Exception as e:
            # Catch-all for other transient issues (network hiccups, etc.)
            print(f"[download] Error for {table_id}: {e}")
            if attempt == retries:
                print("[download] Giving up after maximum retries")
                raise
            sleep_for = delay * attempt
            print(f"[download] Retrying in {sleep_for} seconds...")
            time.sleep(sleep_for)


def download():
    ensure_dirs()
    for name, table_id in TABLES.items():
        print(f"[download] Fetching {table_id} -> {name}")
        try:
            data = fetch_table_with_retry(table_id)
        except Exception as e:
            # If one table fails completely, log and move on so others can still load
            print(f"[download] FAILED for {table_id}: {e}")
            continue

        df = pd.DataFrame(data)
        out_path = RAW_DIR / f"{name}.csv"
        df.to_csv(out_path, index=False)
        print(f"[download] Saved {out_path}")


def transform():
    ensure_dirs()

    for name in TABLES.keys():
        in_path = RAW_DIR / f"{name}.csv"
        if not in_path.exists():
            print(f"[transform] Skipping {name}: raw file {in_path} not found")
            continue

        print(f"[transform] Processing {in_path}")
        df = pd.read_csv(in_path)

        df = add_year_from_perioden(df)

        if name == "pupils_region":
            for col in ("Geslacht", "Onderwijssoort"):
                if col in df.columns:
                    df = df[df[col].astype(str).str.contains("Totaal", na=False)]

        out_path = PROC_DIR / f"{name}.parquet"
        df.to_parquet(out_path, index=False)
        print(f"[transform] Saved {out_path}")


def load():
    pg_dsn = os.getenv("PG_DSN", DEFAULT_PG_DSN)
    print(f"[load] Using PG_DSN={pg_dsn}")
    engine = create_engine(pg_dsn)

    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS cbs"))

    for name in TABLES.keys():
        in_path = PROC_DIR / f"{name}.parquet"
        if not in_path.exists():
            print(f"[load] Skipping {name}: processed file {in_path} not found")
            continue

        print(f"[load] Loading {in_path} into Postgres as cbs.{name}")
        df = pd.read_parquet(in_path)
        df.to_sql(
            name=name,
            con=engine,
            schema="cbs",
            if_exists="replace",
            index=False,
            chunksize=10_000,
        )

    print("[load] Done.")


def main():
    if len(sys.argv) < 2:
        print("Usage: python cbs_pipeline.py [download|transform|load]")
        sys.exit(1)

    step = sys.argv[1].lower()

    if step == "download":
        download()
    elif step == "transform":
        transform()
    elif step == "load":
        load()
    else:
        raise ValueError(f"Unknown step: {step}")


if __name__ == "__main__":
    main()