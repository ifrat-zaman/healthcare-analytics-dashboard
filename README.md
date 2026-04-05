# Hospital Analytics Dashboard

A production-ready healthcare analytics pipeline and Streamlit dashboard built on four hospital operations datasets from Kaggle (`jaderz/hospital-beds-management`).

## Prerequisites

- Python 3.9+
- PostgreSQL 16+ running on localhost (Homebrew default: port 5433 if EDB occupies 5432)
- Kaggle credentials in `Kaggle.txt` (see below)

## Setup

1. **Install dependencies**
   ```bash
   pip3 install -r requirements.txt
   ```

2. **Create the database** (once)
   ```bash
   # Homebrew PostgreSQL 16 on port 5433
   psql -h localhost -p 5433 -U ifratzaman -c "CREATE DATABASE hospital_analytics;" postgres
   ```

3. **Add Kaggle credentials** — create `Kaggle.txt` in the project root:
   ```
   Kaggle username: <your_username>
   Api token: <your_api_token>
   ```

4. **Copy environment template**
   ```bash
   cp .env.example .env
   ```

## Running the Pipeline

```bash
python3 scripts/pipeline.py
```

To skip the Kaggle download (if raw CSVs already exist):

```bash
python3 scripts/pipeline.py --skip-extract
```

## Launching the Dashboard

```bash
streamlit run scripts/dashboard.py
```

Open [http://localhost:8501](http://localhost:8501) in your browser.

## Project Structure

```
├── scripts/
│   ├── auth.py          # Kaggle credential loader
│   ├── extract.py       # Download datasets from Kaggle
│   ├── transform.py     # Clean data, engineer features → Parquet
│   ├── load.py          # Load into PostgreSQL
│   ├── query.py         # Run analytical SQL, export CSVs
│   ├── dashboard.py     # Streamlit app (5 tabs)
│   └── pipeline.py      # Orchestrates steps 1–4
├── sql/
│   └── analysis_queries.sql
├── data/
│   ├── raw/             # Downloaded CSVs (gitignored)
│   └── processed/       # Parquet files (gitignored)
└── reports/
    └── query_results/   # Per-query CSV exports
```

## Dashboard Tabs

| Tab | Contents |
|---|---|
| Overview | Headline KPIs + per-service summary table |
| Bed Management | Occupancy trend line + refusals heatmap |
| Patient Flow | Admissions vs demand, LOS box plot, age distribution |
| Staff Operations | Attendance by role/service, staffing vs satisfaction scatter, morale trend |
| Event Impact | Key metrics by event type, refusal rate box plot, summary table |
