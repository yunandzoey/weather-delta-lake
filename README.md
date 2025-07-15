# Weather → Delta Lake (Databricks CE)

> **Status:** Bronze & Silver layers live • Pipeline hourly on Serverless • DQ ✅

![architecture](assets/architecture.png)

---

## ✨ Project Overview

A lightweight pipeline that ingests hourly weather data from the **Open‑Meteo API**, lands it in **Delta Lake** on Databricks Community/Free Edition, and surfaces a clean daily table ready for analytics.

| Layer      | Format           | Schedule | Key Notebook                  | Output Table            |
| ---------- | ---------------- | -------- | ----------------------------- | ----------------------- |
| **Bronze** | JSON → Delta     | Hourly   | `01_ingest_weather_bronze`    | `weather_bronze.hourly` |
| **Silver** | Aggregated Delta | Hourly   | `02_transform_weather_silver` | `weather_silver.daily`  |

---

## 📂 Repository Layout

```
weather-delta-lake/
├── src/
│   ├── 01_ingest_weather_bronze.ipynb
│   └── 02_transform_weather_silver.py
├── conf/
│   ├── job_weather_bronze.json          # legacy single‑task job (kept for reference)
│   └── job_weather_pipeline.json        # live 2‑task pipeline
├── tests/
│   ├── test_ingest_weather.py
│   └── test_silver_transform.py
├── docs/architecture.png               # high‑level diagram
└── README.md                            # you are here
```

---

## ✅ Milestones Completed

### Milestone 1 — Bronze Ingest

* Hourly notebook pulls 168‑h forecast → writes to `weather_bronze.hourly` (Delta).
* Serverless job **weather\_bronze\_ingest\_hourly** created via JSON & CLI.
* Smoke test verifies row‑count (168) + schema.

### Milestone 2 — Silver Daily + Data Quality

* Deduplicates Bronze rows, aggregates to daily grain.
* Expectations: full coverage (24 rows), temp −60 … 60 °C, humidity 0–100 %.
* Upsert (MERGE) logic ensures one row per date + location.
* Two‑task job **weather\_pipeline\_hourly** (Bronze ➜ Silver) live on Serverless.
* All finished days show `row_count = 24` & `dq_passed = true`.

---

## 🔄 Getting Started

1. **Clone repo into Databricks Repos** → authenticate via PAT or GitHub App.
2. **Compute:** Free/CE uses *Serverless* automatically; no cluster config needed.
3. **Secrets (optional):**

   ```bash
   databricks secrets create-scope --scope weather_scope
   # put SLACK_WEBHOOK or future API keys here
   ```
4. **Deploy jobs:**

   ```bash
   # Bronze only
   databricks jobs create --json @conf/job_weather_bronze.json

   # Full pipeline
   databricks jobs create --json @conf/job_weather_pipeline.json
   ```
5. **Run tests locally or in CI**

   ```bash
   pip install -r requirements-dev.txt
   pytest -q
   ```

---

## 📊 Query Examples

```sql
-- Latest Bronze timestamp
SELECT MAX(timestamp_utc) FROM weather_bronze.hourly;

-- Daily KPIs
SELECT date,
       avg_temp_c,
       max_wind_kmh
FROM   weather_silver.daily
WHERE  dq_passed = true
ORDER  BY date DESC;
```

---

## 🚧 Roadmap / Next Steps

| Milestone                  | Focus                                                                                         |
| -------------------------- | --------------------------------------------------------------------------------------------- |
| **3 – CI/CD + Monitoring** | GitHub Action auto‑deploys job JSON; Slack/email alerts; weekly OPTIMIZE + VACUUM task        |
| **4 – Performance Tuning** | Partition/Z‑order validation; Bronze retention policy (VACUUM 7 days)                         |
| **5 – Analytics & Docs**   | Databricks SQL dashboard (current temp, 7‑day avg, max wind); Loom video demo; project badges |

---

## 🏷 Badges (placeholders)

![build](https://img.shields.io/badge/build-passing-brightgreen)
![license](https://img.shields.io/badge/license-MIT-blue)

---

## 🎥 Loom Demo *(coming soon)*

*Add a short walkthrough once the dashboard is live.*

---

## 📝 Lessons Learned (copy‑paste ready for LinkedIn)

> Just shipped a Weather → Delta Lake pipeline on Databricks CE:
> • **Bronze** hourly ingest via Open‑Meteo
> • **Silver** daily roll‑ups with Delta MERGE + expectations
> • 100 % Serverless—no cluster config 🎉  Next up: CI/CD & Slack alerts.
