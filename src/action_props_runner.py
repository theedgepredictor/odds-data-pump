import datetime
import random
import time

import requests
import pandas as pd
from typing import Dict, Any, Iterable, Optional, Tuple, List

import unicodedata
from nfl_data_loader.api.sources.players.general.players import collect_players
from nfl_data_loader.api.sources.players.rosters.rosters import collect_roster
from nfl_data_loader.schemas.players.position import POSITION_MAPPER
from nfl_data_loader.utils.formatters.general import df_rename_fold
from nfl_data_loader.utils.formatters.reformat_team_name import team_id_repl

from consts import ACTION_NETWORK_ID_MAPPER
from src.utils import clean_player_names

MY_LINES = {
    15: "CONSENSUS",
    30: "OPEN",
    68: "DK",
    69: "FD",
    79: "BET365"
}  # Consensus, Open Line, DK NJ, FD NJ, bet365 NJ

PROP_COLS = [
    #'scope',
    'bet_type',
    #'type',
    'market_id',
    'outcome_id',
    'event_id',
    'book_id',
    'player_id',
    'option_type_id',
    #'team_id',
    #'competitor_id',
    'line_type',

    'period',

    'side',
    'value',
    'odds',
    #'tickets_value',
    #'tickets_percent',
    #'money_value',
    #'money_percent',
    #'is_live',
    #'line_status',
    #'deeplink_id',
    #'prop_type_id',
    #'odds_coefficient_score',
    #'custom_pick_type_name',
    # 'custom_pick_type_display_name',

    #'game_id'
]

# ---------------- BET TYPE MAP ---------------- #
BET_TYPE_MAP: Dict[str, str] = {
    'core_bet_type_62_anytime_touchdown_scorer': 'anytime_touchdown_scorer',
    'core_bet_type_63_last_touchdown_scorer': 'last_touchdown_scorer',
    'core_bet_type_56_first_touchdown_scorer': 'first_touchdown_scorer',
    'core_bet_type_67_to_score_2_or_more_touchdowns': 'to_score_2_or_more_touchdowns',
    'core_bet_type_68_to_score_3_or_more_touchdowns': 'to_score_3_or_more_touchdowns',
    'core_bet_type_9_passing_yards': 'passing_yards',
    'core_bet_type_60_longest_completion': 'longest_completion',
    'core_bet_type_65_interceptions': 'passing_interceptions',
    'core_bet_type_10_pass_completions': 'completions',
    'core_bet_type_30_passing_attempts': 'attempts',
    'core_bet_type_11_passing_tds': 'passing_tds',
    'core_bet_type_528_1020_player_rushing_yards_milestones_90_or_more': 'player_rushing_yards_milestones_90_or_more',
    'core_bet_type_528_1018_player_rushing_yards_milestones_70_or_more': 'player_rushing_yards_milestones_70_or_more',
    'core_bet_type_528_1014_player_rushing_yards_milestones_25_or_more': 'player_rushing_yards_milestones_25_or_more',
    'core_bet_type_528_1016_player_rushing_yards_milestones_50_or_more': 'player_rushing_yards_milestones_50_or_more',
    'core_bet_type_528_1017_player_rushing_yards_milestones_60_or_more': 'player_rushing_yards_milestones_60_or_more',
    'core_bet_type_528_1015_player_rushing_yards_milestones_40_or_more': 'player_rushing_yards_milestones_40_or_more',
    'core_bet_type_528_1021_player_rushing_yards_milestones_100_or_more': 'player_rushing_yards_milestones_100_or_more',
    'core_bet_type_528_1019_player_rushing_yards_milestones_80_or_more': 'player_rushing_yards_milestones_80_or_more',
    'core_bet_type_528_1022_player_rushing_yards_milestones_110_or_more': 'player_rushing_yards_milestones_110_or_more',
    'core_bet_type_525_1011_player_passing_yards_milestones_325_or_more': 'player_passing_yards_milestones_325_or_more',
    'core_bet_type_525_1012_player_passing_yards_milestones_350_or_more': 'player_passing_yards_milestones_350_or_more',
    'core_bet_type_525_1008_player_passing_yards_milestones_250_or_more': 'player_passing_yards_milestones_250_or_more',
    'core_bet_type_524_1037_player_passing_touchdowns_milestones_4_or_more': 'player_passing_touchdowns_milestones_4_or_more',
    'core_bet_type_525_1046_player_passing_yards_milestones_150_or_more': 'player_passing_yards_milestones_150_or_more',
    'core_bet_type_525_1006_player_passing_yards_milestones_200_or_more': 'player_passing_yards_milestones_200_or_more',
    'core_bet_type_525_1045_player_passing_yards_milestones_175_or_more': 'player_passing_yards_milestones_175_or_more',
    'core_bet_type_524_1035_player_passing_touchdowns_milestones_2_or_more': 'player_passing_touchdowns_milestones_2_or_more',
    'core_bet_type_525_1009_player_passing_yards_milestones_275_or_more': 'player_passing_yards_milestones_275_or_more',
    'core_bet_type_525_1010_player_passing_yards_milestones_300_or_more': 'player_passing_yards_milestones_300_or_more',
    'core_bet_type_525_1007_player_passing_yards_milestones_225_or_more': 'player_passing_yards_milestones_225_or_more',
    'core_bet_type_525_1013_player_passing_yards_milestones_400_or_more': 'player_passing_yards_milestones_400_or_more',
    'core_bet_type_524_1034_player_passing_touchdowns_milestones_1_or_more': 'player_passing_touchdowns_milestones_1_or_more',
    'core_bet_type_524_1036_player_passing_touchdowns_milestones_3_or_more': 'player_passing_touchdowns_milestones_3_or_more',
    'core_bet_type_530_1029_player_receiving_yards_milestones_90_or_more': 'player_receiving_yards_milestones_90_or_more',
    'core_bet_type_530_1031_player_receiving_yards_milestones_110_or_more': 'player_receiving_yards_milestones_110_or_more',
    'core_bet_type_532_1039_player_receptions_milestones_3_or_more': 'player_receptions_milestones_3_or_more',
    'core_bet_type_532_1040_player_receptions_milestones_4_or_more': 'player_receptions_milestones_4_or_more',
    'core_bet_type_530_1026_player_receiving_yards_milestones_60_or_more': 'player_receiving_yards_milestones_60_or_more',
    'core_bet_type_530_1023_player_receiving_yards_milestones_25_or_more': 'player_receiving_yards_milestones_25_or_more',
    'core_bet_type_532_1057_player_receptions_milestones_10_or_more': 'player_receptions_milestones_10_or_more',
    'core_bet_type_530_1030_player_receiving_yards_milestones_100_or_more': 'player_receiving_yards_milestones_100_or_more',
    'core_bet_type_530_1025_player_receiving_yards_milestones_50_or_more': 'player_receiving_yards_milestones_50_or_more',
    'core_bet_type_530_1024_player_receiving_yards_milestones_40_or_more': 'player_receiving_yards_milestones_40_or_more',
    'core_bet_type_532_1043_player_receptions_milestones_7_or_more': 'player_receptions_milestones_7_or_more',
    'core_bet_type_530_1027_player_receiving_yards_milestones_70_or_more': 'player_receiving_yards_milestones_70_or_more',
    'core_bet_type_532_1038_player_receptions_milestones_2_or_more': 'player_receptions_milestones_2_or_more',
    'core_bet_type_532_1044_player_receptions_milestones_8_or_more': 'player_receptions_milestones_8_or_more',
    'core_bet_type_530_1033_player_receiving_yards_milestones_150_or_more': 'player_receiving_yards_milestones_150_or_more',
    'core_bet_type_530_1032_player_receiving_yards_milestones_125_or_more': 'player_receiving_yards_milestones_125_or_more',
    'core_bet_type_532_1042_player_receptions_milestones_6_or_more': 'player_receptions_milestones_6_or_more',
    'core_bet_type_532_1041_player_receptions_milestones_5_or_more': 'player_receptions_milestones_5_or_more',
    'core_bet_type_530_1028_player_receiving_yards_milestones_80_or_more': 'player_receiving_yards_milestones_80_or_more',
    'core_bet_type_532_1056_player_receptions_milestones_9_or_more': 'player_receptions_milestones_9_or_more',
    'core_bet_type_16_receiving_yards': 'receiving_yards',
    'core_bet_type_59_longest_reception': 'longest_reception',
    'core_bet_type_15_receptions': 'receptions',
    'core_bet_type_12_rushing_yards': 'rushing_yards',
    'core_bet_type_58_longest_rush': 'longest_rush',
    'core_bet_type_18_rushing_attempts': 'rushing_attempts',
    'core_bet_type_66_rushing_receiving_yards': 'rushing_receiving_yards',
    'core_bet_type_71_passing_rushing_yards': 'passing_rushing_yards',
    'core_bet_type_43_kicking_points': 'kicking_points',
    'core_bet_type_213_field_goals_made': 'field_goals_made',
    'core_bet_type_212_extra_points_made': 'extra_points_made',
    'core_bet_type_524_1089_player_passing_touchdowns_milestones_5_or_more': 'player_passing_touchdowns_milestones_5_or_more',
    'core_bet_type_70_tackles_assists': 'tackles_assists',
    "core_bet_type_6_team_score": "team_score",
}

# ============================== #
# 1) GAMES ONLY
# ============================== #
class SimpleGamesClient:
    BASE_URL = "https://api.actionnetwork.com/web/v2/scoreboard/nfl/markets"

    def __init__(
        self,
        default_headers: Optional[Dict[str, str]] = None,
        session: Optional[requests.Session] = None,
        team_abbr_map: Optional[Dict[str, str]] = None,
    ):
        self.session = session or requests.Session()
        self.headers = {
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.9",
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/119.0.0.0 Safari/537.36"
            ),
            "Origin": "https://www.actionnetwork.com",
            "Referer": "https://www.actionnetwork.com/",
        }
        if default_headers:
            self.headers.update(default_headers)
        self.team_abbr_map = team_abbr_map or {}

    def fetch_games_df(
        self,
        *,
        line_type: str,
        season: int,
        week: int,
        season_type: str = "reg",
        book_ids: Optional[Iterable[int]] = None,
        extra_params: Optional[Dict[str, Any]] = None,
        extra_headers: Optional[Dict[str, str]] = None,
        timeout: int = 20,
    ) -> pd.DataFrame:
        params = {
            "week": week,
            "season": season,
            "seasonType": season_type,
            "customPickTypes": line_type,
        }
        if book_ids:
            params["bookIds"] = ",".join(map(str, book_ids))
        if extra_params:
            params.update(extra_params)

        headers = dict(self.headers)
        if extra_headers:
            headers.update(extra_headers)

        resp = self.session.get(self.BASE_URL, params=params, headers=headers, timeout=timeout)
        if resp.status_code != 200:
            raise requests.HTTPError(f"{resp.status_code} for {resp.url}\n{resp.text[:800]}")

        data = resp.json()
        games = data.get("games", []) or []
        rows = []
        for g in games:
            home = g.get("home_team") or {}
            away = g.get("away_team") or {}
            rows.append({
                "id": g.get("id"),
                "home_team_abbr": self.team_abbr_map.get(home.get("abbr"), home.get("abbr")),
                "away_team_abbr": self.team_abbr_map.get(away.get("abbr"), away.get("abbr")),
                "season": g.get("season"),
                "week": g.get("week"),
                "num_bets": g.get("num_bets"),
                "home_team_id": home.get("id"),
                "away_team_id": away.get("id"),
            })

        df = pd.DataFrame(rows)
        for col in ("id", "season", "week", "num_bets", "home_team_id", "away_team_id"):
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="ignore")
        return df


# ============================== #
# 2) PROPS (per game)
# ============================== #
class GamePropsClient:
    BASE_URL_TMPL = "https://api.actionnetwork.com/web/v2/games/{game_id}/props"

    def __init__(
        self,
        default_headers: Optional[Dict[str, str]] = None,
        session: Optional[requests.Session] = None,
        bet_type_map: Optional[Dict[str, str]] = None,
    ):
        self.session = session or requests.Session()
        self.headers = {
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.9",
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/119.0.0.0 Safari/537.36"
            ),
            "Origin": "https://www.actionnetwork.com",
            "Referer": "https://www.actionnetwork.com/",
        }
        if default_headers:
            self.headers.update(default_headers)

        self.bet_type_map = bet_type_map or BET_TYPE_MAP

    def _get_with_retry(self, url, *, params=None, headers=None, timeout=20, max_retries=5,
                        base_sleep=0.5, max_sleep=8.0):
        """GET with exponential backoff + full jitter; honors Retry-After when present."""
        for attempt in range(max_retries):
            resp = self.session.get(url, params=params, headers=headers, timeout=timeout)

            # Success
            if resp.status_code < 400:
                return resp

            # If told to back off, do so
            if resp.status_code in (429, 500, 502, 503, 504):
                retry_after = resp.headers.get("Retry-After")
                if retry_after:
                    try:
                        sleep_s = float(retry_after)
                    except ValueError:
                        sleep_s = base_sleep
                else:
                    # Exponential backoff with FULL JITTER
                    sleep_s = min(max_sleep, base_sleep * (2 ** attempt)) * random.random()
                time.sleep(sleep_s)
                continue

            # Other errors: raise immediately
            resp.raise_for_status()

        # If we fell out of the loop, last response still errored
        resp.raise_for_status()

    def fetch_props_for_games(
        self,
        game_ids: Iterable[int],
        *,
        state_code: str,
        book_ids: Iterable[int],
        extra_params: Optional[Dict[str, Any]] = None,
        extra_headers: Optional[Dict[str, str]] = None,
        timeout: int = 20,
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        all_player_rows: List[Dict[str, Any]] = []
        all_game_rows: List[Dict[str, Any]] = []
        all_players: List[Dict[str, Any]] = []

        for game_id in game_ids:
            p_df, g_df, players = self._fetch_one_game(
                game_id=game_id,
                state_code=state_code,
                book_ids=book_ids,
                extra_params=extra_params,
                extra_headers=extra_headers,
                timeout=timeout,
            )
            if not p_df.empty:
                all_player_rows.extend(p_df.to_dict("records"))
            if not g_df.empty:
                all_game_rows.extend(g_df.to_dict("records"))
            if players:
                all_players.extend(players)

        player_props_df = pd.DataFrame(all_player_rows)
        game_props_df = pd.DataFrame(all_game_rows)

        # players may be a dict keyed by player_id; convert to list and drop image
        cleaned_players: List[Dict[str, Any]] = []
        for p in all_players:
            if isinstance(p, dict):
                q = dict(p)
                q.pop("image", None)
                cleaned_players.append(q)

        players_df = pd.DataFrame(cleaned_players)
        if not players_df.empty:
            # Dedup on id and/or player_id (some payloads include both)
            subset_cols = [c for c in ("id", "player_id") if c in players_df.columns]
            if subset_cols:
                players_df = players_df.drop_duplicates(subset=subset_cols).reset_index(drop=True)
            else:
                players_df = players_df.drop_duplicates().reset_index(drop=True)

        return player_props_df, game_props_df, players_df

    def _fetch_one_game(
        self,
        *,
        game_id: int,
        state_code: str,
        book_ids: Iterable[int],
        extra_params: Optional[Dict[str, Any]],
        extra_headers: Optional[Dict[str, str]],
        timeout: int,
    ) -> Tuple[pd.DataFrame, pd.DataFrame, List[Dict[str, Any]]]:
        params = {"stateCode": state_code, "bookIds": ",".join(map(str, book_ids))}
        if extra_params:
            params.update(extra_params)

        headers = dict(self.headers)
        if extra_headers:
            headers.update(extra_headers)

        url = self.BASE_URL_TMPL.format(game_id=game_id)
        resp = self._get_with_retry(url, params=params, headers=headers, timeout=timeout)
        if resp.status_code != 200:
            raise requests.HTTPError(f"{resp.status_code} for {resp.url}\n{resp.text[:800]}")
        blob = resp.json() or {}

        # players can be a dict keyed by player_id
        players_blob = blob.get("players") or {}
        if isinstance(players_blob, dict):
            players = list(players_blob.values())
        elif isinstance(players_blob, list):
            players = players_blob
        else:
            players = []

        player_props_df = self._props_blob_to_df(blob.get("player_props") or {}, scope="player")
        game_props_df   = self._props_blob_to_df(blob.get("game_props")   or {}, scope="game")

        # Tag with game
        if not player_props_df.empty:
            player_props_df["game_id"] = game_id
        if not game_props_df.empty:
            game_props_df["game_id"] = game_id

        return player_props_df, game_props_df, players

    def _props_blob_to_df(self, props_blob: Dict[str, Any], scope: str) -> pd.DataFrame:
        """
        props_blob shape:
          {
            "<line_type_key>": [
              {
                id, market_id, game_id, type, line_type, ...,
                lines: { "15": [offer, ...], "68": [offer, ...] }
              }, ...
            ],
            ...
          }
        """
        rows: List[Dict[str, Any]] = []

        for line_type_key, markets in props_blob.items():
            if not isinstance(markets, list):
                continue

            # Preferred human mapping for the bet type
            mapped = self.bet_type_map.get(line_type_key)

            for m in markets:
                market_id = m.get("market_id") or m.get("id")
                raw_type  = m.get("type") or line_type_key  # fallback to key
                mapped_bt = mapped or self.bet_type_map.get(raw_type) or self.bet_type_map.get(m.get("line_type")) or raw_type

                base_row = {
                    "market_id": market_id,
                    "type": raw_type,               # keep original
                    "bet_type": mapped_bt,          # <-- mapped column you asked for
                    "line_type": m.get("line_type"),
                    "custom_pick_type_name": m.get("custom_pick_type_name"),
                    "custom_pick_type_display_name": m.get("custom_pick_type_display_name"),
                    "scope": scope,  # "player" or "game"
                }

                lines = m.get("lines") or {}
                if not isinstance(lines, dict):
                    continue

                for book_key, offers in lines.items():
                    try:
                        book_id = int(book_key)
                    except Exception:
                        book_id = None

                    if not isinstance(offers, list):
                        continue

                    for o in offers:
                        if not isinstance(o, dict):
                            continue

                        r = dict(base_row)
                        r.update({
                            "book_id": book_id or o.get("book_id"),
                            "event_id": o.get("event_id"),
                            "option_type_id": o.get("option_type_id"),
                            "side": o.get("side"),
                            "period": o.get("period"),
                            "player_id": o.get("player_id"),
                            "team_id": o.get("team_id"),
                            "competitor_id": o.get("competitor_id"),
                            "value": o.get("value"),
                            "odds": o.get("odds"),
                            "is_live": o.get("is_live"),
                            "line_status": o.get("line_status"),
                            "deeplink_id": o.get("deeplink_id"),
                            "prop_type_id": o.get("prop_type_id"),
                            "odds_coefficient_score": o.get("odds_coefficient_score"),
                            "outcome_id": o.get("outcome_id"),
                        })
                        bet_info = o.get("bet_info") or {}
                        tickets = bet_info.get("tickets") or {}
                        money = bet_info.get("money") or {}
                        r["tickets_value"] = tickets.get("value")
                        r["tickets_percent"] = tickets.get("percent")
                        r["money_value"] = money.get("value")
                        r["money_percent"] = money.get("percent")

                        for extra in ("edge", "edge_grade", "projection", "bet_quality"):
                            if extra in o:
                                r[extra] = o.get(extra)

                        rows.append(r)

        df = pd.DataFrame(rows)
        order = [
            "scope", "bet_type", "type", "line_type", "market_id",
            "game_id", "event_id",
            "book_id", "player_id", "team_id", "competitor_id",
            "period", "option_type_id", "side",
            "value", "odds",
            "tickets_value", "tickets_percent", "money_value", "money_percent",
            "is_live", "line_status", "deeplink_id",
            "prop_type_id", "odds_coefficient_score",
            "edge", "edge_grade", "projection", "bet_quality",
            "custom_pick_type_name", "custom_pick_type_display_name",
        ]
        if not df.empty:
            cols = [c for c in order if c in df.columns] + [c for c in df.columns if c not in order]
            df = df[cols]
        return df

def _get_games(season, week, season_type, access_token=None):
    if access_token:
        default_headers = {
            "access_token": access_token
        }
    else:
        default_headers = None
    line_type = "core_bet_type_62_anytime_touchdown_scorer"
    games_client = SimpleGamesClient(default_headers=default_headers)
    games_df = games_client.fetch_games_df(
        line_type=line_type,
        season=season,
        week=week,
        season_type=season_type,
        book_ids=MY_LINES.keys(),
    )
    games_df = games_df.copy()
    return games_df

def hunt_player_merge_ids(players_df, season, week, season_type):
    players_df = players_df[['player_id', 'join_name', 'position', 'team']].rename(columns={'player_id': 'action_network_player_id'}).copy()
    players_df['season'] = season
    players_df['week'] = week
    players_df['position_group'] = players_df.position
    players_df['position_group'] = players_df.position_group.map(POSITION_MAPPER)

    import os
    def ensure_dir(path: str):
        if not os.path.exists(path):
            os.makedirs(path, exist_ok=True)
    ensure_dir(f'./data/raw/football/nfl/players/{season}/')
    shift = (18 if season >= 2021 else 17)
    if season_type=='post':
        save_week = week + shift
    else:
        save_week = week

    roster_df = collect_roster(season)
    roster_df = roster_df[((roster_df['season'] == season) & (roster_df['week'] == save_week))].copy()
    roster_df['join_name'] = roster_df['name']
    roster_df['join_name'] = clean_player_names(roster_df['join_name'], lowercase=True)
    roster_df['join_name'] = roster_df['join_name'].str[0] + '.' + roster_df['join_name'].str.split(' ').str[1]

    team_df = players_df[players_df.position == 'D'].copy()
    p_id = collect_players(include_teams_as_players=True)
    p_id = p_id[p_id.position == 'D'].copy()
    p_id = p_id[['player_id', 'latest_team']].rename(columns={'latest_team': 'team'})
    team_df = pd.merge(team_df, p_id, on=['team'], how='left')

    players_df = players_df[players_df.position != 'D'].copy()
    players_df = pd.merge(players_df, roster_df[['season', 'week', 'team', 'player_id', 'position_group', 'join_name']], how='left', on=['season', 'week', 'team', 'position_group', 'join_name'], )
    round_2_without_team = players_df[players_df.player_id.isnull()].copy().drop(columns=['player_id'])
    round_2_without_team = pd.merge(round_2_without_team, roster_df[['season', 'week', 'player_id', 'position_group', 'join_name']], how='left', on=['season', 'week', 'position_group', 'join_name'], )

    round_3_without_pos_group = round_2_without_team[round_2_without_team.player_id.isnull()].copy().drop(columns=['player_id'])

    round_3_without_pos_group = pd.merge(round_3_without_pos_group, roster_df[['season', 'week', 'player_id', 'join_name', 'team']], how='left', on=['season', 'week', 'join_name', 'team'], )
    master = pd.concat([
        players_df[players_df.player_id.notnull()].copy(),
        round_2_without_team[round_2_without_team.player_id.notnull()].copy(),
        round_3_without_pos_group[round_3_without_pos_group.player_id.notnull()].copy(),
    ], ignore_index=True).drop_duplicates(subset=['action_network_player_id'], keep='first')
    cant_match = players_df[~players_df.action_network_player_id.isin(master['action_network_player_id'].unique())].copy()

    master['action_network_player_id'] = master['action_network_player_id'].astype(int).astype(str)

    master = master[~master.action_network_player_id.isin(ACTION_NETWORK_ID_MAPPER.keys())].copy()

    res = dict(zip(list(master['action_network_player_id']), list(master['player_id'])))
    print(f"{len(res)} New Players found on hunt")
    print(res)


    if cant_match.shape[0] !=0:
        cant_match.to_parquet(f'./data/raw/football/nfl/players/{season}/{save_week}.parquet', index=False)

        print(f"------------ {cant_match.shape[0]} Players need manual Merge ----------")

def get_player_props(season, week, season_type, access_token=None):
    if access_token:
        default_headers = {
            "access_token": access_token
        }
    else:
        default_headers = None

    games_df = _get_games(season, week, season_type, access_token)
    # 2) For each game, fetch props (ALL line types) in the specified state and books

    if games_df.shape[0] == 0:
        return pd.DataFrame()

    game_ids = games_df["id"].tolist()  # limit for testing
    props_client = GamePropsClient(default_headers=default_headers)
    player_props_df, game_props_df, players_df = props_client.fetch_props_for_games(
        game_ids,
        state_code="NJ",
        book_ids=MY_LINES.keys(),
    )
    if player_props_df.shape[0] == 0:
        return pd.DataFrame()

    team_id_df = df_rename_fold(games_df, t1_prefix="home_", t2_prefix="away_")
    team_id_df = team_id_repl(team_id_df)
    id_to_team = team_id_df[['team_abbr', 'team_id']].rename(columns={'team_abbr': 'team'})
    id_to_team = pd.concat([id_to_team, pd.DataFrame([{'team': 'FA', 'team_id': 0}])], ignore_index=True)

    game_props_df = pd.merge(game_props_df, id_to_team, on=['team_id'], how='left' )
    game_props_df = game_props_df[PROP_COLS+['team']]

    players_df.team_id = players_df.team_id.fillna(0).astype(int)
    players_df = pd.merge(players_df, id_to_team, how='left', on='team_id')
    players_df['position'] = players_df['display_text'].str.split('- ').str[1]
    players_df['position_group'] = players_df.position
    players_df['position_group'] = players_df.position_group.map(POSITION_MAPPER)

    players_df = players_df[['player_id', 'abbr', 'position','position_group', 'team']].rename(columns={ 'abbr': 'join_name'}).copy()
    players_df['join_name'] = clean_player_names(players_df['join_name'], lowercase=True)
    players_df['join_name'] = players_df['join_name'].str[0] + '.' + players_df['join_name'].str[1:]

    player_props_df = pd.merge(player_props_df[PROP_COLS], players_df[['team','player_id','join_name','position','position_group']], on=['player_id'], how='left')

    player_props_df = pd.concat([player_props_df, game_props_df], ignore_index=True)
    player_props_df = player_props_df[player_props_df.book_id.isin(MY_LINES.keys())].copy()
    player_props_df = pd.merge(player_props_df, games_df[['id','num_bets']].rename(columns={'id':'event_id','num_bets':'total_bets_on_event'}), on=['event_id'], how='left')
    player_props_df['season'] = season
    player_props_df['week'] = week
    player_props_df = player_props_df.drop(columns=['market_id','outcome_id','option_type_id'])
    player_props_df['last_updated'] = datetime.datetime.now()
    return player_props_df


if __name__ == '__main__':
    path = '../data/raw/football/nfl/players/2022/'
    df = pd.concat([pd.read_parquet(f"{path}{i}.parquet") for i in range(1, 22)])
    df.to_parquet('../data/players.parquet',index=False)
