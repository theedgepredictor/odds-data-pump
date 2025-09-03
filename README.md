# odds-data-pump

A small set of Python utilities and runners to collect, clean and persist sports betting odds and player prop data (NFL game lines and player props) into parquet datasets used by downstream models and analysis.

This repository focuses on ingesting raw odds data, transforming it into a stable parquet layout under `data/processed`, and providing small runner scripts to produce / refresh those datasets.

## Quick facts

- Language: Python
- Dependencies: pinned in `requirements.txt`
- Main entry points: `event_odds_runner.py`, `player_props_runner.py` and helpers under `src/`
- Data layout: raw files under `data/raw` and final parquet outputs under `data/processed`

## Install

1. Create and activate a virtual environment:

2. Install dependencies:

```
pip install -r requirements.txt
```

## Project structure (important files)

- `event_odds_runner.py` - top-level runner that processes event/game level odds into the processed dataset
- `player_props_runner.py` - top-level runner for player prop data
- `consts.py` - global constants used by the scripts
- `src/` - utility modules and alternative runners (`action_games_runner.py`, `action_props_runner.py`, `utils.py`)
- `data/raw/` - raw input data (organized by sport/year/day)
- `data/processed/football/nfl/` - final parquet datasets (game_lines and player_props)
- `requirements.txt` - Python dependencies

## Data layout

Example layout found in the repo:

- `data/raw/football/nfl/game_lines/<year>/<week>/...`  raw dumps per week
- `data/processed/football/nfl/game_lines/<year>.parquet`  compiled per-year parquet files
- `data/processed/football/nfl/player_props/<year>.parquet`  compiled player props per year

When adding new raw data, follow the same directory structure and re-run the appropriate runner to regenerate the processed parquet files.

## Usage

Run the runners from the repository root. 

## Contributing

Open a PR with a clear description of changes. If adding data formats, include one small sample raw file and an automated test that the loader can parse it.

