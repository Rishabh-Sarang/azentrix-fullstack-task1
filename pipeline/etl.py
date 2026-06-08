"""
ETL Pipeline - Open-Meteo Weather Data
Extracts hourly weather data for multiple cities, transforms it,
and loads into SQLite. Runs on a 24-hour schedule.
"""

import os
import sqlite3
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
from pathlib import Path
import argparse
from apscheduler.schedulers.blocking import BlockingScheduler

# ── Paths ──────────────────────────────────────────────────────────────────────
BASE_DIR   = Path(__file__).resolve().parent.parent
DB_PATH    = BASE_DIR / "db" / "weather.db"
LOG_PATH   = BASE_DIR / "logs" / "pipeline.log"

# ── Logger ─────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("etl")

# ── Cities to track ────────────────────────────────────────────────────────────
CITIES = [
    {"name": "Mumbai",        "lat": 19.0760,  "lon": 72.8777},
    {"name": "Delhi",         "lat": 28.6139,  "lon": 77.2090},
    {"name": "Indore",        "lat": 22.7196,  "lon": 75.8577},
    {"name": "Bangalore",     "lat": 12.9716,  "lon": 77.5946},
    {"name": "Chennai",       "lat": 13.0827,  "lon": 80.2707},
    {"name": "Kolkata",       "lat": 22.5726,  "lon": 88.3639},
    {"name": "Hyderabad",     "lat": 17.3850,  "lon": 78.4867},
    {"name": "Pune",          "lat": 18.5204,  "lon": 73.8567},
]

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"

# ── Extract ────────────────────────────────────────────────────────────────────
def extract(city: dict) -> dict:
    """Fetch last 24 h of hourly weather data from Open-Meteo (free, no key)."""
    end   = datetime.now(timezone.utc).replace(tzinfo=None)
    start = end - timedelta(days=1)

    params = {
        "latitude":              city["lat"],
        "longitude":             city["lon"],
        "hourly":                [
            "temperature_2m",
            "relative_humidity_2m",
            "wind_speed_10m",
            "precipitation",
            "apparent_temperature",
            "weathercode",
        ],
        "start_date":            start.strftime("%Y-%m-%d"),
        "end_date":              end.strftime("%Y-%m-%d"),
        "timezone":              "UTC",
        "wind_speed_unit":       "kmh",
    }

    resp = requests.get(OPEN_METEO_URL, params=params, timeout=15)
    resp.raise_for_status()
    return resp.json()


# ── Transform ──────────────────────────────────────────────────────────────────
def transform(raw: dict, city_name: str) -> pd.DataFrame:
    """Clean, normalize, and derive new columns from raw API response."""
    hourly = raw.get("hourly", {})
    df = pd.DataFrame(hourly)

    # Rename for clarity
    df = df.rename(columns={
        "time":                   "recorded_at",
        "temperature_2m":         "temp_c",
        "apparent_temperature":   "feels_like_c",
        "relative_humidity_2m":   "humidity_pct",
        "wind_speed_10m":         "wind_kmh",
        "precipitation":          "precip_mm",
        "weathercode":            "wmo_code",
    })

    # ── Null handling ──────────────────────────────────────────────────────────
    df["temp_c"]       = pd.to_numeric(df["temp_c"],       errors="coerce")
    df["feels_like_c"] = pd.to_numeric(df["feels_like_c"], errors="coerce")
    df["humidity_pct"] = pd.to_numeric(df["humidity_pct"], errors="coerce")
    df["wind_kmh"]     = pd.to_numeric(df["wind_kmh"],     errors="coerce")
    df["precip_mm"]    = pd.to_numeric(df["precip_mm"],    errors="coerce")

    df["temp_c"]       = df["temp_c"].fillna(df["temp_c"].median())
    df["humidity_pct"] = df["humidity_pct"].fillna(df["humidity_pct"].median())
    df["wind_kmh"]     = df["wind_kmh"].fillna(0.0)
    df["precip_mm"]    = df["precip_mm"].fillna(0.0)
    df["feels_like_c"] = df["feels_like_c"].fillna(df["temp_c"])

    # ── Normalize timestamps ───────────────────────────────────────────────────
    df["recorded_at"] = pd.to_datetime(df["recorded_at"], errors="coerce")
    df = df.dropna(subset=["recorded_at"])
    df["recorded_at"] = df["recorded_at"].dt.strftime("%Y-%m-%d %H:%M:%S")

    # ── Derived column 1: heat index category ────────────────────────────────
    def heat_category(row):
        t = row["temp_c"]
        h = row["humidity_pct"]
        if t >= 41:                        return "extreme"
        if t >= 35 and h >= 60:            return "very_hot"
        if t >= 28:                        return "hot"
        if t >= 20:                        return "warm"
        if t >= 10:                        return "cool"
        return "cold"

    df["heat_category"] = df.apply(heat_category, axis=1)

    # ── Derived column 2: discomfort index (Thom's DI) ───────────────────────
    # DI = T - 0.55*(1 - H/100)*(T - 14.5)
    df["discomfort_index"] = (
        df["temp_c"]
        - 0.55 * (1 - df["humidity_pct"] / 100) * (df["temp_c"] - 14.5)
    ).round(2)

    # ── Derived column 3: wind chill (simple Steadman formula, kmh) ──────────
    df["wind_chill_c"] = (
        13.12
        + 0.6215 * df["temp_c"]
        - 11.37 * df["wind_kmh"] ** 0.16
        + 0.3965 * df["temp_c"] * df["wind_kmh"] ** 0.16
    ).round(2)

    df["city"] = city_name
    df["ingested_at"] = datetime.now(timezone.utc).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")

    return df[[
        "city", "recorded_at", "temp_c", "feels_like_c",
        "humidity_pct", "wind_kmh", "precip_mm", "wmo_code",
        "heat_category", "discomfort_index", "wind_chill_c", "ingested_at",
    ]]


# ── Load ───────────────────────────────────────────────────────────────────────
def init_db():
    """Create tables if they don't exist yet."""
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS weather_hourly (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                city             TEXT    NOT NULL,
                recorded_at      TEXT    NOT NULL,
                temp_c           REAL,
                feels_like_c     REAL,
                humidity_pct     REAL,
                wind_kmh         REAL,
                precip_mm        REAL,
                wmo_code         INTEGER,
                heat_category    TEXT,
                discomfort_index REAL,
                wind_chill_c     REAL,
                ingested_at      TEXT    NOT NULL,
                UNIQUE(city, recorded_at)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                run_at      TEXT    NOT NULL,
                status      TEXT    NOT NULL,
                rows_loaded INTEGER,
                message     TEXT
            )
        """)
        conn.commit()


def load(df: pd.DataFrame, conn: sqlite3.Connection) -> int:
    """Insert rows, skip duplicates via INSERT OR IGNORE."""
    cursor = conn.executemany(
        """
        INSERT OR IGNORE INTO weather_hourly
            (city, recorded_at, temp_c, feels_like_c, humidity_pct,
             wind_kmh, precip_mm, wmo_code, heat_category,
             discomfort_index, wind_chill_c, ingested_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        df[[
            "city", "recorded_at", "temp_c", "feels_like_c", "humidity_pct",
            "wind_kmh", "precip_mm", "wmo_code", "heat_category",
            "discomfort_index", "wind_chill_c", "ingested_at",
        ]].itertuples(index=False, name=None),
    )
    conn.commit()
    return cursor.rowcount


# ── Pipeline run ───────────────────────────────────────────────────────────────
def run_pipeline():
    log.info("=" * 60)
    log.info("Pipeline run started")
    run_at      = datetime.now(timezone.utc).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")
    total_rows  = 0
    errors      = []

    try:
        init_db()
        with sqlite3.connect(DB_PATH) as conn:
            for city in CITIES:
                try:
                    log.info(f"  Extracting → {city['name']}")
                    raw = extract(city)

                    log.info(f"  Transforming → {city['name']}")
                    df  = transform(raw, city["name"])

                    rows = load(df, conn)
                    total_rows += rows
                    log.info(f"  Loaded {rows} new rows for {city['name']}")

                except Exception as city_err:
                    msg = f"{city['name']}: {city_err}"
                    log.warning(f"  SKIP — {msg}")
                    errors.append(msg)

            status  = "partial" if errors else "success"
            message = "; ".join(errors) if errors else None

            conn.execute(
                "INSERT INTO pipeline_runs (run_at, status, rows_loaded, message) VALUES (?,?,?,?)",
                (run_at, status, total_rows, message),
            )
            conn.commit()

        log.info(f"Run complete — status={status}, rows_loaded={total_rows}")

    except Exception as fatal:
        log.error(f"Fatal pipeline error: {fatal}")
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute(
                "INSERT INTO pipeline_runs (run_at, status, rows_loaded, message) VALUES (?,?,?,?)",
                (run_at, "failure", 0, str(fatal)),
            )
            conn.commit()

    log.info("=" * 60)


# ── Scheduler ──────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Run the weather ETL pipeline.")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run the pipeline once and exit instead of starting the scheduler.",
    )
    args = parser.parse_args()

    if args.once:
        log.info("Running pipeline once via cron/no-scheduler mode")
        run_pipeline()
        return

    log.info("Starting ETL scheduler — interval: 24 h")
    run_pipeline()          # run immediately on start

    scheduler = BlockingScheduler(timezone="UTC")
    scheduler.add_job(run_pipeline, "interval", hours=24, id="etl_job")
    log.info("Next run scheduled in 24 hours. Press Ctrl+C to stop.")


if __name__ == "__main__":
    main()

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        log.info("Scheduler stopped.")


if __name__ == "__main__":
    main()
