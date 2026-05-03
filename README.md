# Playoff League App

A full-stack fantasy football league application for the NFL playoffs. Each participant drafts real NFL players before the postseason begins and earns fantasy points based on their players' live statistical performance across each playoff round.

## How It Works

**Data Pipeline**
The app consumes the [nflverse REST API](https://github.com/nflverse/nflverse-data) to fetch up-to-date NFL player statistics. A Python/pandas pipeline processes the raw data, calculates fantasy points using a custom scoring engine, and persists results to a Supabase PostgreSQL database via PostgREST API. The pipeline runs automatically twice daily via GitHub Actions and triggers a redeployment of the frontend on every successful run.

**Scoring System**
Fantasy points are calculated separately for offensive and defensive players across 35+ statistical categories:

- **Offense (QB/RB/WR/TE):** Passing yards, TDs, interceptions, rushing yards, receptions, receiving yards, fumbles, 2-point conversions, return TDs
- **Defense (DL/LB/DB):** Tackles, sacks, interceptions, passes defended, forced fumbles, defensive TDs, safeties

**Frontend**
Static HTML/CSS/JS dashboards display per-round scoring breakdowns (Wild Card, Divisional, Championship, Super Bowl) for each of the 8 league participants, plus overall offense and defense leaderboards. All data is loaded client-side from CSV files — no server required.

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Data ingestion | Python, `requests`, nflverse API |
| Data processing | `pandas`, `numpy` |
| Database | Supabase (PostgreSQL via PostgREST) |
| Frontend | HTML, CSS, JavaScript |
| Automation | GitHub Actions (scheduled cron) |
| Hosting | GitHub Pages |

## Project Structure

```
playoff_league_app/
├── index/                  # Frontend — HTML pages and stylesheet
│   ├── index.html          # League standings
│   ├── offense.html        # Offense leaderboard
│   ├── defense.html        # Defense leaderboard
│   ├── ethan.html          # Per-participant team pages
│   └── styles.css
├── league_pages/           # Generated CSV data consumed by the frontend
├── data/                   # Raw stat CSVs (populated by the pipeline)
├── playground.ipynb        # Data pipeline notebook
├── web/
│   └── main.py             # Supabase ingestion script
└── .github/workflows/
    ├── run-notebook.yml    # Scheduled data refresh (twice daily)
    └── deploy-pages.yml    # GitHub Pages deployment (triggers after refresh)
```

## Local Development

**Prerequisites:** Python 3.11+

```bash
pip install -r requirements.txt
```

Run the data pipeline (generates `league_pages/*.csv`):
```bash
jupyter nbconvert --to notebook --execute playground.ipynb
```

Open `index/index.html` in a browser. Because data is loaded via `fetch()`, you need a local server:
```bash
cd index
python -m http.server 8000
```

Then visit `http://localhost:8000`.

## Deployment

The app deploys automatically to GitHub Pages on every successful notebook run. To trigger a manual refresh, dispatch the `Run notebook and commit outputs` workflow from the Actions tab.
