import requests
import pandas as pd
from typing import Dict, Any, Iterable, Optional, List, Tuple

class GameLinesClient:
    BASE_URL = "https://api.actionnetwork.com/web/v2/scoreboard/nfl"
    PERIOD_KEYS_DEFAULT = (
        "event", "firsthalf", "secondhalf", "firstquarter", "secondquarter", "thirdquarter", "fourthquarter"
    )

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

    # ----------- PUBLIC: one request, two DataFrames -----------
    def fetch_games_and_market_lines_df(
        self,
        *,
        season: int,
        week: int,
        season_type: str = "reg",
        book_ids: Optional[Iterable[int]] = None,
        periods: Optional[Iterable[str]] = None,  # override if needed
        extra_params: Optional[Dict[str, Any]] = None,
        extra_headers: Optional[Dict[str, str]] = None,
        timeout: int = 20,
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Single GET → (games_df, game_lines_df) across requested periods & books."""
        payload = self._fetch_payload(
            season=season,
            week=week,
            season_type=season_type,
            book_ids=book_ids,
            periods=periods,
            extra_params=extra_params,
            extra_headers=extra_headers,
            timeout=timeout,
        )
        games = payload.get("games", []) or []
        games_df = self._parse_games_flat(games)
        game_lines_df = self._parse_game_markets_flat(games)
        return games_df, game_lines_df

    # ----------- INTERNAL: one GET -----------
    def _fetch_payload(
        self,
        *,
        season: int,
        week: int,
        season_type: str,
        book_ids: Optional[Iterable[int]],
        periods: Optional[Iterable[str]],
        extra_params: Optional[Dict[str, Any]],
        extra_headers: Optional[Dict[str, str]],
        timeout: int,
    ) -> Dict[str, Any]:
        params = {
            "week": week,
            "season": season,
            "seasonType": season_type,
        }
        if book_ids:
            params["bookIds"] = ",".join(map(str, book_ids))

        # periods to request
        period_list = list(periods) if periods else list(self.PERIOD_KEYS_DEFAULT)
        params["periods"] = ",".join(period_list)

        if extra_params:
            params.update(extra_params)

        headers = dict(self.headers)
        if extra_headers:
            headers.update(extra_headers)

        resp = self.session.get(self.BASE_URL, params=params, headers=headers, timeout=timeout)
        if resp.status_code != 200:
            raise requests.HTTPError(f"{resp.status_code} for {resp.url}\n{resp.text[:800]}")
        return resp.json() or {}

    # ----------- INTERNAL: parse flat games -----------
    def _parse_games_flat(self, games: List[Dict[str, Any]]) -> pd.DataFrame:
        rows: List[Dict[str, Any]] = []
        for g in games:
            home_id = g.get("home_team_id")
            away_id = g.get("away_team_id")

            # team_id -> abbr map from embedded teams
            id_to_abbr: Dict[int, Optional[str]] = {}
            for t in (g.get("teams") or []):
                if isinstance(t, dict):
                    tid = t.get("id")
                    abbr = t.get("abbr")
                    id_to_abbr[tid] = abbr

            rows.append({
                "id": g.get("id"),
                "season": g.get("season"),
                "week": g.get("week"),
                "type": g.get("type"),
                "status": g.get("status"),
                "real_status": g.get("real_status"),
                "start_time": g.get("start_time"),
                "num_bets": g.get("num_bets"),
                "league_name": g.get("league_name"),
                "core_id": g.get("core_id"),
                "home_rotation_number": g.get("home_rotation_number"),
                "away_rotation_number": g.get("away_rotation_number"),
                "broadcast_network": (g.get("broadcast") or {}).get("network"),
                "broadcast_network_short": (g.get("broadcast") or {}).get("network_short"),
                "home_team_id": home_id,
                "away_team_id": away_id,
                "home_team_abbr": self._map_abbr(id_to_abbr.get(home_id)),
                "away_team_abbr": self._map_abbr(id_to_abbr.get(away_id)),
            })

        df = pd.DataFrame(rows)
        for col in ("id", "season", "week", "num_bets", "home_team_id", "away_team_id",
                    "home_rotation_number", "away_rotation_number", "core_id"):
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="ignore")
        return df

    # ----------- INTERNAL: parse flat game market lines (all periods) -----------
    def _parse_game_markets_flat(self, games: List[Dict[str, Any]]) -> pd.DataFrame:
        rows: List[Dict[str, Any]] = []

        for g in games:
            event_id = g.get("id")
            season_g = g.get("season")
            week_g = g.get("week")
            num_bets = g.get("num_bets")

            # team_id -> abbr map
            id_to_abbr: Dict[int, Optional[str]] = {}
            for t in (g.get("teams") or []):
                if isinstance(t, dict):
                    tid = t.get("id")
                    abbr = t.get("abbr")
                    id_to_abbr[tid] = self._map_abbr(abbr)

            markets = g.get("markets") or {}
            # markets: { "<book_id>": { "<period>": { "moneyline":[...], "spread":[...], "total":[...] }, ... }, ... }
            for book_key, book_blob in markets.items():
                try:
                    book_id = int(book_key)
                except Exception:
                    continue

                period_container = book_blob or {}
                # iterate through the known period keys; skip missing ones gracefully
                for period_key in self.PERIOD_KEYS_DEFAULT:
                    period_blob = period_container.get(period_key) or {}
                    if not isinstance(period_blob, dict):
                        continue

                    # within each period -> market type -> list of outcomes
                    for market_type, offers in period_blob.items():
                        if not isinstance(offers, list):
                            continue
                        for o in offers:
                            if not isinstance(o, dict):
                                continue

                            team_id = o.get("team_id", 0)
                            team_abbr = id_to_abbr.get(team_id)

                            bet_info = o.get("bet_info") or {}
                            tickets = bet_info.get("tickets") or {}
                            money = bet_info.get("money") or {}

                            rows.append({
                                # IDs
                                "event_id": o.get("event_id", event_id),
                                "market_id": o.get("market_id"),
                                "outcome_id": o.get("outcome_id"),
                                "book_id": book_id,

                                # Market typing (flat)
                                "type": o.get("type", market_type),     # moneyline | spread | total
                                "line_type": o.get("type", market_type),

                                # Period: prefer payload's explicit period, fall back to the block we iterated
                                "period": o.get("period", period_key),

                                "side": o.get("side"),

                                # Price/line
                                "value": o.get("value"),
                                "odds": o.get("odds"),
                                "is_live": o.get("is_live"),
                                "line_status": o.get("line_status"),
                                "deeplink_id": o.get("deeplink_id"),
                                "odds_coefficient_score": o.get("odds_coefficient_score"),

                                # Participant
                                "team_id": team_id,
                                "team": team_abbr,
                                "player_id": o.get("player_id", 0),
                                "competitor_id": o.get("competitor_id", 0),
                                "option_type_id": o.get("option_type_id", None),

                                # Meta
                                "season": season_g,
                                "week": week_g,
                                "total_bets_on_event": num_bets,

                                # Splits (if present)
                                "tickets_value": tickets.get("value"),
                                "tickets_percent": tickets.get("percent"),
                                "money_value": money.get("value"),
                                "money_percent": money.get("percent"),
                            })

        df = pd.DataFrame(rows)

        # dtype hygiene
        for col in ["event_id", "book_id", "team_id", "player_id", "competitor_id", "season", "week"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="ignore")

        order = [
            "type", "line_type",
            "market_id", "outcome_id",
            "event_id", "book_id",
            "player_id", "option_type_id",
            "period", "side",
            "value", "odds", "is_live",
            "line_status", "deeplink_id",
            "odds_coefficient_score",
            "team_id", "team",
            "competitor_id",
            "season", "week", "total_bets_on_event",
            "tickets_value", "tickets_percent", "money_value", "money_percent",
        ]
        if not df.empty:
            cols = [c for c in order if c in df.columns] + [c for c in df.columns if c not in order]
            df = df[cols].reset_index(drop=True)
        return df

    # ----------- small helper -----------
    def _map_abbr(self, abbr: Optional[str]) -> Optional[str]:
        if abbr is None:
            return None
        return self.team_abbr_map.get(abbr, abbr)
