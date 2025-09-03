import json
import os
import random
import time
from typing import List

import pandas as pd
from dotenv import load_dotenv
from espn_api_orm.consts import ESPNSportLeagueTypes
from espn_api_orm.league.api import ESPNLeagueAPI
from nfl_data_loader.utils.utils import get_seasons_to_update, get_dataframe, find_year_for_season, find_week_for_season, put_dataframe

from consts import ACTION_NETWORK_ID_MAPPER
from src.action_props_runner import get_player_props
from src.utils import (
    polite_sleep_block,
    ensure_open_lines,              # (optional if you only call via merge)
    keep_only_latest_per_book,
    merge_with_existing_and_dedupe,
)

load_dotenv()

OPEN_BOOK_ID = 30
OPEN_FALLBACK_PRIORITY = [15, 68, 69, 79]   # 15 (CONSENSUS) first, then DK, FD, bet365

UNIQ_KEYS_W_BOOK: List[str] = [
    "bet_type","event_id","book_id","join_name","position","position_group","line_type",
    "period","side","team","player_id","season","week"
]
UNIQ_KEYS_NO_BOOK: List[str] = [k for k in UNIQ_KEYS_W_BOOK if k != "book_id"]

def ensure_dir(path: str):
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)


if __name__ == '__main__':
    root_path = './data/raw'
    START_SEASON = 2022

    access_token = os.environ.get("ACTION_NETWORK_ACCESS_TOKEN", None)
    debug = True

    sport_league_pairs = [
        ESPNSportLeagueTypes.FOOTBALL_NFL,
    ]

    for sport_league in sport_league_pairs:
        sport_str, league_str = sport_league.value.split("/")
        raw_proj_path = f"{root_path}/{sport_str}/{league_str}/player_props/"
        processed_proj_path = f"./data/processed/{sport_str}/{league_str}/player_props/"
        ensure_dir(raw_proj_path)
        ensure_dir(processed_proj_path)

        league_api = ESPNLeagueAPI(sport_str, league_str)
        if not league_api.is_active():
            print("Running in OffSeason")

        update_seasons = get_seasons_to_update(f"./data/processed/{sport_str}/{league_str}", "player_props")
        update_seasons = [i for i in update_seasons if i >= START_SEASON]
        if not update_seasons:
            print("No seasons to update.")
            continue

        print(f"Running Player Props Pump for: {sport_league.value} from {min(update_seasons)} to {max(update_seasons)}")

        for update_season in update_seasons:
            season_raw_proj_path = f"{raw_proj_path}{update_season}/"
            ensure_dir(season_raw_proj_path)

            processed_season_path = f"{processed_proj_path}{update_season}.parquet"
            processed_df = get_dataframe(processed_season_path)  # may be empty

            # Determine weeks
            if update_season == find_year_for_season():
                current_week = find_week_for_season()
                if processed_df.shape[0] != 0:
                    max_processed_week = 1 if current_week == 1 else current_week-1
                    processed_df = processed_df[processed_df.week <= current_week+1].copy()
                else:
                    max_processed_week = 1
                # re/build from max_processed_week through current_week (+1 to also include the current week snapshot)
                update_weeks = list(range(max_processed_week, current_week + 1 + 1))
            else:
                update_week = processed_df.week.max() if processed_df.shape[0] != 0 else 1
                update_weeks = list(range(update_week, (22 + 1 if update_season >= 2021 else 21 + 1)))

            print(f"Season {update_season} -> weeks: {update_weeks}")

            shift = (18 if update_season >= 2021 else 17)
            season_rows = []

            for canonical_week in update_weeks:

                polite_sleep_block()

                # Determine season_type + the "API week" used by Action Network
                if canonical_week > shift:
                    season_type = "post"
                    season_type_week = canonical_week - shift
                    if update_season <= 2022 and canonical_week==22:
                        continue # No Wild Card Week
                else:
                    season_type = "reg"
                    season_type_week = canonical_week

                # Pull
                df = get_player_props(
                    season=update_season,
                    week=season_type_week,
                    season_type=season_type,
                    access_token=access_token,
                )
                if df.shape[0] == 0:
                    print(f"No data for {canonical_week} yet")
                    continue

                # IMPORTANT: store canonical NFL week number (1..22) for consistency on disk
                df = df.copy()
                df["week"] = canonical_week

                # Load existing weekly parquet (if any)
                week_dir = f"{season_raw_proj_path}{canonical_week}/"
                ensure_dir(week_dir)
                weekly_path = f"{week_dir}player_props.parquet"
                current_df = get_dataframe(weekly_path)  # empty df if not found

                # Merge + fill OPEN + keep latest per (… + book_id)
                merged_week_df = merge_with_existing_and_dedupe(
                    current_df, df,
                    uniq_keys_with_book=UNIQ_KEYS_W_BOOK,
                    open_book_id=OPEN_BOOK_ID,
                    fallback_priority=OPEN_FALLBACK_PRIORITY,
                    protect_open=True,
                    prefer_real_open=False,
                )

                # Save weekly
                put_dataframe(merged_week_df, weekly_path)

                print(f"Saved week {canonical_week}: {merged_week_df.shape[0]} rows "
                      f"({merged_week_df.book_id.value_counts(dropna=False).to_dict()})")

                season_rows.append(merged_week_df)

            # Optional: write/refresh season-level processed parquet (concat of weekly files already deduped weekly)
            if season_rows:

                season_df = pd.concat(season_rows, ignore_index=True)
                season_df = season_df.rename(columns={'player_id': 'action_network_player_id'})
                season_df['action_network_player_id'] = season_df['action_network_player_id'].fillna(-1).astype(int).astype(str)
                season_df['player_id'] = season_df['action_network_player_id'].map(ACTION_NETWORK_ID_MAPPER)
                # Keep only latest per composite key again just in case multiple runs in same session
                season_df = keep_only_latest_per_book(
                    season_df, UNIQ_KEYS_W_BOOK
                )
                season_df = merge_with_existing_and_dedupe(
                    processed_df, season_df,
                    uniq_keys_with_book=UNIQ_KEYS_W_BOOK,
                    open_book_id=OPEN_BOOK_ID,
                    fallback_priority=OPEN_FALLBACK_PRIORITY,
                    protect_open=True,
                    prefer_real_open=False,
                )
                ensure_dir(processed_proj_path)
                put_dataframe(season_df, processed_season_path)
                print(f"Updated processed season parquet: {processed_season_path} "
                      f"({season_df.shape[0]} rows)")







