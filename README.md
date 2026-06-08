# azentrix-fullstack-task1 — Automated ETL Pipeline

An automated ETL pipeline that pulls **hourly weather data** from the
[Open-Meteo API](https://open-meteo.com/) (free, no API key required, updates
every hour), transforms it, and loads it into a local **SQLite** database.
Designed for one-shot execution via cron job.

---

## Data Source

**Open-Meteo Forecast API** — `https://api.open-meteo.com/v1/forecast`

- Free and open, no authentication needed
- Hourly resolution, updated continuously
- Tracks 8 Indian cities: Mumbai, Delhi, Indore, Bangalore, Chennai, Kolkata, Hyderabad, Pune

Variables extracted per city per hour:
`temperature_2m`, `apparent_temperature`, `relative_humidity_2m`,
`wind_speed_10m`, `precipitation`, `weathercode`

---

## Derived Columns

| Column | Formula / Logic |
|---|---|
| `heat_category` | Rule-based bucketing on temp + humidity → `cold / cool / warm / hot / very_hot / extreme` |
| `discomfort_index` | Thom's Discomfort Index: `T - 0.55*(1 - H/100)*(T - 14.5)` |
| `wind_chill_c` | Steadman wind-chill: `13.12 + 0.6215T - 11.37V^0.16 + 0.3965T*V^0.16` |

---

## Project Structure

```
azentrix-fullstack-task1/
├── pipeline/
│   └── etl.py          # Extract → Transform → Load
├── db/
│   └── weather.db      # SQLite database (auto-created on first run)
├── logs/
│   └── pipeline.log    # Run log (auto-created on first run)
├── requirements.txt
├── run_etl_once.sh     # cron wrapper
└── README.md
```

---

## Setup (Arch Linux)

### 1. Clone and enter the repo

```bash
git clone https://github.com/YOUR_USERNAME/azentrix-fullstack-task1.git
cd azentrix-fullstack-task1
```

### 2. Create a virtual environment

```bash
python -m venv .venv
source .venv/bin/activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Run the pipeline manually (first test)

```bash
python pipeline/etl.py
```

The pipeline will run immediately, then schedule itself every 24 hours.
Check `logs/pipeline.log` and `db/weather.db` after the first run.

---

## Running with cronie / cron

This repo supports one-shot ETL execution using cron. The script is designed
to run once per schedule, log its output, and then exit.

### 1. Install cronie

```bash
sudo pacman -S cronie
sudo systemctl enable --now cronie
```

### 2. Create a cron entry

Edit your user crontab with:

```bash
crontab -e
```

Then add a line like this to run once per day at midnight UTC:

```cron
0 0 * * * /home/YOUR_USER/azentrix-fullstack-task1/run_etl_once.sh >> /home/YOUR_USER/azentrix-fullstack-task1/logs/cron.log 2>&1
```

If you prefer using the Python interpreter directly, use:

```cron
0 0 * * * /home/YOUR_USER/azentrix-fullstack-task1/.venv/bin/python /home/YOUR_USER/azentrix-fullstack-task1/pipeline/etl.py >> /home/YOUR_USER/azentrix-fullstack-task1/logs/cron.log 2>&1
```

### 3. Verify the cron job

```bash
crontab -l
journalctl -u cronie -f
```

---

## Database Schema

This repo now supports one-shot ETL execution via `python pipeline/etl.py --once`, which is suitable for cron jobs.

### 1. Install cronie

```bash
sudo pacman -S cronie
sudo systemctl enable --now cronie
```

### 2. Create a cron entry

Edit your user crontab with:

```bash
crontab -e
```

Then add a line like this to run once per day at midnight UTC:

```cron
0 0 * * * /home/YOUR_USER/azentrix-fullstack-task1/run_etl_once.sh >> /home/YOUR_USER/azentrix-fullstack-task1/logs/cron.log 2>&1
```

If you prefer using the Python interpreter directly, use:

```cron
0 0 * * * /home/YOUR_USER/azentrix-fullstack-task1/.venv/bin/python /home/YOUR_USER/azentrix-fullstack-task1/pipeline/etl.py --once >> /home/YOUR_USER/azentrix-fullstack-task1/logs/cron.log 2>&1
```

### 3. Verify the cron job

```bash
crontab -l
journalctl -u cronie -f
```

---

## Database Schema

### `weather_hourly`

| Column | Type | Description |
|---|---|---|
| `id` | INTEGER PK | Auto-increment |
| `city` | TEXT | City name |
| `recorded_at` | TEXT | UTC timestamp of the weather reading |
| `temp_c` | REAL | Temperature at 2 m (°C) |
| `feels_like_c` | REAL | Apparent temperature (°C) |
| `humidity_pct` | REAL | Relative humidity (%) |
| `wind_kmh` | REAL | Wind speed at 10 m (km/h) |
| `precip_mm` | REAL | Precipitation (mm) |
| `wmo_code` | INTEGER | WMO weather interpretation code |
| `heat_category` | TEXT | **Derived** — comfort bucket |
| `discomfort_index` | REAL | **Derived** — Thom's DI |
| `wind_chill_c` | REAL | **Derived** — Steadman wind chill |
| `ingested_at` | TEXT | UTC timestamp of pipeline ingestion |

`UNIQUE(city, recorded_at)` — duplicate runs are safely idempotent.

### `pipeline_runs`

| Column | Type | Description |
|---|---|---|
| `id` | INTEGER PK | Auto-increment |
| `run_at` | TEXT | UTC timestamp of run start |
| `status` | TEXT | `success` / `partial` / `failure` |
| `rows_loaded` | INTEGER | New rows inserted this run |
| `message` | TEXT | Error details if any |

---

## Logging

Every run appends to `logs/pipeline.log`:

```
2026-06-08 10:00:01  INFO      Pipeline run started
2026-06-08 10:00:01  INFO        Extracting → Mumbai
2026-06-08 10:00:02  INFO        Transforming → Mumbai
2026-06-08 10:00:02  INFO        Loaded 24 new rows for Mumbai
...
2026-06-08 10:00:09  INFO      Run complete — status=success, rows_loaded=192
```

---

## Approach

1. **Extract** — HTTP GET to Open-Meteo for the last 24 h of hourly data per
   city. No auth, rate-limit friendly (one request per city).
2. **Transform** — pandas DataFrame: coerce types, fill nulls with
   median/zero, normalize timestamps to `YYYY-MM-DD HH:MM:SS`, derive three
   new columns.
3. **Load** — `INSERT OR IGNORE` into SQLite so reruns are safe and
   idempotent. Each successful or failed run is recorded in `pipeline_runs`.
4. **Schedule** — use cron to run the pipeline once on a fixed schedule.
