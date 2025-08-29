import os
import pandas as pd
import datetime as dt
from typing import List, Iterable, Optional

from dotenv import load_dotenv
from espn_api_orm.consts import ESPNSportLeagueTypes
from espn_api_orm.league.api import ESPNLeagueAPI
from nfl_data_loader.utils.utils import (
    get_seasons_to_update, get_dataframe, put_dataframe,
    find_year_for_season, find_week_for_season,
)
from src.utils import polite_sleep_block  # reuse your jitter sleeper
from src.action_games_runner import GameLinesClient  # <-- your class from prior message

load_dotenv()

# ----------------- CONFIG ----------------- #
OPEN_BOOK_ID = 30
# Prefer consensus (15), then DK (68), FD (69), bet365 (79) to synthesize OPEN
OPEN_FALLBACK_PRIORITY = [15, 68, 69, 79]

# What uniquely defines a single *latest* line we want to retain (per book)
UNIQ_KEYS_W_BOOK: List[str] = [
    "line_type",          # moneyline | spread | total
    "event_id",
    "book_id",
    "period",             # event | firsthalf | firstquarter | ...
    "side",               # home/away for ML/Spread, over/under for totals
    "team_id",            # 0 for totals, otherwise team id
    "season",
    "week",
]
UNIQ_KEYS_NO_BOOK: List[str] = [k for k in UNIQ_KEYS_W_BOOK if k != "book_id"]

# Default books + periods
DEFAULT_BOOK_IDS = [15, 30, 68, 69, 79]
DEFAULT_PERIODS = ["event", "firsthalf", "secondhalf",
                   "firstquarter", "secondquarter", "thirdquarter", "fourthquarter"]


# --------------- OPEN (30) BACKFILL --------------- #
def ensure_open_lines(df: pd.DataFrame) -> pd.DataFrame:
    """
    If a group (ignoring book) lacks an OPEN (book_id=30), duplicate from the
    first available in OPEN_FALLBACK_PRIORITY, tagging open_inferred/source.
    """
    if df.empty:
        return df

    df = df.copy()

    # Types we depend on
    for col in ("book_id", "event_id", "team_id", "season", "week"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Fast lookup by (group + book_id)
    df_idx = df.set_index(UNIQ_KEYS_NO_BOOK + ["book_id"], drop=False)

    rows_to_add = []
    present_books_by_group = (
        df.groupby(UNIQ_KEYS_NO_BOOK, dropna=False)["book_id"]
          .apply(lambda s: set(pd.to_numeric(s, errors="coerce").dropna().astype(int)))
    )
    groups_missing_open = present_books_by_group[~present_books_by_group.apply(lambda s: OPEN_BOOK_ID in s)]

    for group_key, _ in groups_missing_open.items():
        chosen = None
        for bid in OPEN_FALLBACK_PRIORITY:
            try:
                candidate = df_idx.loc[group_key + (bid,)]
            except KeyError:
                continue
            # If multiples, keep latest by last_updated
            if isinstance(candidate, pd.DataFrame):
                candidate = candidate.sort_values("last_updated").iloc[-1]
            chosen = candidate
            break

        if chosen is not None:
            r = chosen.to_dict()
            r["book_id"] = OPEN_BOOK_ID
            r["open_inferred"] = True
            r["open_source_book_id"] = int(chosen["book_id"]) if "book_id" in chosen else None
            rows_to_add.append(r)

    if rows_to_add:
        add_df = pd.DataFrame(rows_to_add)
        # align columns
        for col in df.columns:
            if col not in add_df.columns:
                add_df[col] = pd.NA
        df = pd.concat([df, add_df[df.columns]], ignore_index=True)

    # flags on all rows
    if "open_inferred" not in df.columns:
        df["open_inferred"] = False
    if "open_source_book_id" not in df.columns:
        df["open_source_book_id"] = pd.NA
    df["open_inferred"] = df["open_inferred"].fillna(False)

    return df


# --------------- DEDUPE: KEEP LATEST PER BOOK --------------- #
def keep_only_latest_per_book(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    if "last_updated" not in df.columns:
        df = df.assign(last_updated=pd.Timestamp("1970-01-01"))
    return (
        df.sort_values("last_updated")
          .drop_duplicates(UNIQ_KEYS_W_BOOK, keep="last")
          .reset_index(drop=True)
    )


def merge_with_existing_and_dedupe(current_df: pd.DataFrame, new_df: pd.DataFrame) -> pd.DataFrame:
    new_df = ensure_open_lines(new_df)

    # guard columns
    for col in UNIQ_KEYS_W_BOOK + ["last_updated", "open_inferred", "open_source_book_id"]:
        if col not in new_df.columns:
            new_df[col] = pd.NA

    if current_df is None or current_df.empty:
        combined = new_df
    else:
        # align schemas
        for col in set(new_df.columns) - set(current_df.columns):
            current_df[col] = pd.NA
        for col in set(current_df.columns) - set(new_df.columns):
            new_df[col] = pd.NA
        combined = pd.concat([current_df[new_df.columns], new_df[current_df.columns]], ignore_index=True)

    return keep_only_latest_per_book(combined)


# --------------- FETCH ONE WEEK OF GAME LINES --------------- #
def get_game_lines(
    *,
    season: int,
    week: int,
    season_type: str,
    access_token: Optional[str] = None,
    book_ids: Optional[Iterable[int]] = None,
    periods: Optional[Iterable[str]] = None,
    timeout: int = 20,
) -> pd.DataFrame:
    """
    Single endpoint call; returns a FLAT DataFrame of game market outcomes
    (moneyline/spread/total) across requested books & periods.
    """
    hdrs = {"access_token": access_token} if access_token else None
    client = GameLinesClient(default_headers=hdrs)

    games_df, game_lines_df = client.fetch_games_and_market_lines_df(
        season=season,
        week=week,
        season_type=season_type,
        book_ids=book_ids or DEFAULT_BOOK_IDS,
        periods=periods or DEFAULT_PERIODS,
        timeout=timeout,
    )

    if game_lines_df.empty:
        return game_lines_df

    # Stamp update time now for dedupe ordering
    game_lines_df = game_lines_df.copy()
    game_lines_df["last_updated"] = dt.datetime.now()

    # Make sure columns you rely on exist
    must_have = [
        "line_type","event_id","book_id","period","side","value","odds",
        "odds_coefficient_score","team_id","team","season","week",
        "total_bets_on_event","tickets_percent","money_percent"
    ]
    for c in must_have:
        if c not in game_lines_df.columns:
            game_lines_df[c] = pd.NA

    # Optional: keep only the projection of columns you care to persist
    # (You can comment this out to keep the whole payload schema)
    keep_cols = [
        "line_type","event_id","book_id","period","side","value","odds",
        "odds_coefficient_score","team_id","team","season","week",
        "total_bets_on_event","tickets_percent","money_percent",
        "last_updated","open_inferred","open_source_book_id"  # helpful if filled later
    ]
    # keep any that exist from keep_cols
    keep_cols = [c for c in keep_cols if c in game_lines_df.columns]
    game_lines_df = game_lines_df[keep_cols]

    return game_lines_df


# --------------- MAIN ETL LOOP (weekly + season rollup) --------------- #
if __name__ == "__main__":
    root_path = "./data/raw"
    START_SEASON = 2016

    access_token = os.environ.get("ACTION_NETWORK_ACCESS_TOKEN", None)

    sport_league_pairs = [ESPNSportLeagueTypes.FOOTBALL_NFL]

    for sport_league in sport_league_pairs:
        sport_str, league_str = sport_league.value.split("/")
        raw_path = f"{root_path}/{sport_str}/{league_str}/game_lines/"
        processed_path = f"./data/processed/{sport_str}/{league_str}/game_lines/"
        os.makedirs(raw_path, exist_ok=True)
        os.makedirs(processed_path, exist_ok=True)

        league_api = ESPNLeagueAPI(sport_str, league_str)
        if not league_api.is_active():
            print("Running in OffSeason")

        # Decide seasons to update based on processed dir
        update_seasons = get_seasons_to_update(f"./data/processed/{sport_str}/{league_str}", "game_lines")
        update_seasons = [s for s in update_seasons if s >= START_SEASON]
        if not update_seasons:
            print("No seasons to update.")
            continue

        print(f"Running Game Lines Pump for: {sport_league.value} from {min(update_seasons)} to {max(update_seasons)}")

        for update_season in update_seasons:
            season_raw_path = os.path.join(raw_path, str(update_season))
            os.makedirs(season_raw_path, exist_ok=True)

            processed_season_path = os.path.join(processed_path, f"{update_season}.parquet")
            processed_df = get_dataframe(processed_season_path)  # may be empty

            # Determine weeks
            if update_season == find_year_for_season():
                current_week = find_week_for_season()
                if processed_df.shape[0] != 0:
                    max_processed_week = 1 if current_week == 1 else current_week - 1
                    # keep only up to (current_week + 1) snapshot
                    processed_df = processed_df[processed_df.week <= current_week + 1].copy()
                else:
                    max_processed_week = 1
                update_weeks = list(range(max_processed_week, current_week + 1 + 1))
            else:
                update_week_start = processed_df.week.max() if processed_df.shape[0] != 0 else 1
                update_weeks = list(range(update_week_start, (22 + 1 if update_season >= 2021 else 21 + 1)))

            print(f"Season {update_season} -> weeks: {update_weeks}")

            shift = 18 if update_season >= 2021 else 17
            season_rows = []

            for canonical_week in update_weeks:
                polite_sleep_block()  # be nice between weeks

                # Map canonical NFL week -> (season_type, api_week)
                if canonical_week > shift:
                    season_type = "post"
                    season_type_week = canonical_week - shift  # Action uses 1.. for post
                    if update_season <= 2022 and canonical_week == 22:
                        # 2021–2022 had different playoff week structure
                        continue
                else:
                    season_type = "reg"
                    season_type_week = canonical_week

                # Fetch
                df = get_game_lines(
                    season=update_season,
                    week=season_type_week,
                    season_type=season_type,
                    access_token=access_token,
                    book_ids=DEFAULT_BOOK_IDS,
                    periods=DEFAULT_PERIODS,
                )
                if df.shape[0] == 0:
                    print(f"No game-line data for week {canonical_week} yet")
                    continue

                # Store canonical week
                df = df.copy()
                df["week"] = canonical_week

                # Load existing weekly parquet (if any)
                week_dir = os.path.join(season_raw_path, str(canonical_week))
                os.makedirs(week_dir, exist_ok=True)
                weekly_path = os.path.join(week_dir, "game_lines.parquet")
                current_df = get_dataframe(weekly_path)

                # Fill OPEN (30) + dedupe latest per book
                merged_week_df = merge_with_existing_and_dedupe(current_df, df)

                # Save weekly
                put_dataframe(merged_week_df, weekly_path)
                print(
                    f"Saved week {canonical_week}: {merged_week_df.shape[0]} rows "
                    f"({merged_week_df.book_id.value_counts(dropna=False).to_dict()})"
                )

                season_rows.append(merged_week_df)

            # Season rollup (already deduped weekly; dedupe again just in case)
            if season_rows:
                season_df = pd.concat(season_rows, ignore_index=True)
                season_df = keep_only_latest_per_book(season_df)
                season_df = merge_with_existing_and_dedupe(processed_df, season_df)

                os.makedirs(processed_path, exist_ok=True)
                put_dataframe(season_df, processed_season_path)
                print(f"Updated processed season parquet: {processed_season_path} ({season_df.shape[0]} rows)")
