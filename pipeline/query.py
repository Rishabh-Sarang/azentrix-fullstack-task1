"""
query.py — Quick inspection tool for the weather SQLite database.
Usage:  python pipeline/query.py
"""

import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent / "db" / "weather.db"


def show(title, sql, conn):
    print(f"\n{'─'*60}")
    print(f"  {title}")
    print(f"{'─'*60}")
    cur = conn.execute(sql)
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    print("  " + "  |  ".join(f"{c:>18}" for c in cols))
    print("  " + "─" * (22 * len(cols)))
    for r in rows:
        print("  " + "  |  ".join(f"{str(v):>18}" for v in r))
    print(f"  ({len(rows)} rows)")


with sqlite3.connect(DB_PATH) as conn:

    show("Pipeline run log (last 10)",
         "SELECT run_at, status, rows_loaded, message FROM pipeline_runs ORDER BY id DESC LIMIT 10",
         conn)

    show("Total rows per city",
         "SELECT city, COUNT(*) as total_rows FROM weather_hourly GROUP BY city ORDER BY city",
         conn)

    show("Latest reading per city",
         """SELECT city, recorded_at, temp_c, humidity_pct, wind_kmh, heat_category
            FROM weather_hourly
            WHERE (city, recorded_at) IN (
                SELECT city, MAX(recorded_at) FROM weather_hourly GROUP BY city
            )
            ORDER BY city""",
         conn)

    show("Avg discomfort index per city (all time)",
         """SELECT city,
                   ROUND(AVG(discomfort_index),2) AS avg_di,
                   ROUND(MAX(temp_c),1)           AS max_temp_c,
                   ROUND(MIN(temp_c),1)           AS min_temp_c
            FROM weather_hourly
            GROUP BY city ORDER BY avg_di DESC""",
         conn)

    show("Heat category distribution",
         """SELECT heat_category, COUNT(*) as count
            FROM weather_hourly GROUP BY heat_category ORDER BY count DESC""",
         conn)
