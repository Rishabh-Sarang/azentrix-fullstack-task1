# azentrix-fullstack-task1 — Automated ETL Pipeline

An automated ETL pipeline that pulls **hourly weather data** from the
[Open-Meteo API](https://open-meteo.com/) (free, no API key required, updates
every hour), transforms it, and loads it into a local **SQLite** database.
Runs on a 24-hour schedule via APScheduler.

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
│   └── etl.py          # Extract → Transform → Load + scheduler
├── db/
│   └── weather.db      # SQLite database (auto-created on first run)
├── logs/
│   └── pipeline.log    # Run log (auto-created on first run)
├── requirements.txt
├── weather-etl.service # systemd unit for Arch Linux
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

## Running as a systemd Service (Arch Linux)

This keeps the pipeline alive across reboots.

### 1. Edit the service file

Open `weather-etl.service` and replace `YOUR_USER` with your actual username
and adjust the paths if your project is in a different location.

### 2. Install and enable

```bash
# Copy to systemd user services
cp weather-etl.service ~/.config/systemd/user/weather-etl.service

# Reload daemon
systemctl --user daemon-reload

# Enable (starts on login) and start now
systemctl --user enable --now weather-etl.service
```

### 3. Check status

```bash
systemctl --user status weather-etl.service

# Live logs
journalctl --user -u weather-etl.service -f
```

### 4. To run at boot even without login (linger)

```bash
loginctl enable-linger $USER
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
4. **Schedule** — APScheduler `BlockingScheduler` with a 24-hour interval,
   firing immediately on start then every 24 h thereafter.
5. **Service** — systemd user service for Arch Linux ensures the process
   survives reboots without root privileges.
