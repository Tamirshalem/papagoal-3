"""
PapaGoal Market Recorder — V5 Clean Build
=========================================
"Don't predict football. Read the market."

Architecture
------------
OddsAPI.io        -> Bet365 live odds (1x2, Over/Under FT, Over/Under 1st-half)
Football API Pro  -> match minute + score
PostgreSQL        -> snapshots / goals / signals / paper_trades / rules / ai
Anthropic Claude  -> signal analysis + rule discovery
Flask             -> 7 dashboard pages

Everything self-contained in one file for Railway deploy.
"""

import os
import time
import json
import logging
import threading
import traceback
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import requests
import psycopg2
import psycopg2.extras
from psycopg2.extras import RealDictCursor, Json
from flask import Flask, jsonify, render_template_string, request

try:
    from anthropic import Anthropic
    _HAS_ANTHROPIC = True
except ImportError:
    _HAS_ANTHROPIC = False


# ============================================================================
# CONFIG / LOGGING
# ============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s :: %(message)s",
)
log = logging.getLogger("papagoal")

ODDSPAPI_KEY      = os.getenv("ODDSPAPI_KEY", "")
FOOTBALL_API_KEY  = os.getenv("FOOTBALL_API_KEY", "f3979dd5d8c7d1b4efd239c2b9a8e2a1")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
DATABASE_URL      = os.getenv("DATABASE_URL", "")
PORT              = int(os.getenv("PORT", "8080"))
SCAN_INTERVAL_SEC = int(os.getenv("SCAN_INTERVAL_SEC", "30"))

# Anthropic client
_anthropic_client = None
if _HAS_ANTHROPIC and ANTHROPIC_API_KEY:
    try:
        _anthropic_client = Anthropic(api_key=ANTHROPIC_API_KEY)
        log.info("Anthropic client ready")
    except Exception as e:
        log.warning(f"Anthropic init failed: {e}")


# ============================================================================
# EXPECTED ODDS CURVES
# ============================================================================
# Anchor points -- get_expected_odd interpolates linearly between them.
EXPECTED_OVER05_HT = {0: 1.25, 5: 1.28, 10: 1.32, 15: 1.38, 20: 1.45,
                     25: 1.55, 30: 1.68, 35: 1.85, 40: 2.10, 45: 2.50}
EXPECTED_OVER15_HT = {0: 2.10, 5: 2.15, 10: 2.22, 15: 2.32, 20: 2.45,
                     25: 2.65, 30: 2.90, 35: 3.20, 40: 3.60, 45: 4.20}
EXPECTED_NEXT_GOAL    = {0: 1.85, 25: 2.15, 45: 2.90, 70: 4.50, 80: 6.50,
                     85: 9.00, 88: 12.0, 90: 20.0}


def get_expected_odd(curve: dict, minute: int) -> Optional[float]:
    """Linear interpolation between the curve's anchor minutes."""
    if minute is None:
        return None
    keys = sorted(curve.keys())
    if minute <= keys[0]:
        return curve[keys[0]]
    if minute >= keys[-1]:
        return curve[keys[-1]]
    for i in range(len(keys) - 1):
        a, b = keys[i], keys[i + 1]
        if a <= minute <= b:
            ratio = (minute - a) / (b - a) if b > a else 0
            return curve[a] + (curve[b] - curve[a]) * ratio
    return None


def calculate_pressure(actual: Optional[float], expected: Optional[float]) -> float:
    """% pressure: how much lower the live odd is vs the expected curve.
    Positive = market thinks goal is closer than the curve says."""
    if not actual or not expected or expected <= 0:
        return 0.0
    diff = expected - actual
    return max(0.0, min(100.0, (diff / expected) * 100))


# ============================================================================
# RULE DEFINITIONS  (the 20 rules, plus pressure/curve rules 101-200)
# ============================================================================
RULES_CATALOG = [
    {"num": 1,   "name": "Early Draw Signal",      "action": "DRAW_UNDER", "desc": "Draw 1.57-1.66 + Over 1.83-2.10 between minute 21-25"},
    {"num": 2,   "name": "Frozen Over",            "action": "NO_ENTRY",   "desc": "Over stuck at 1.80-1.86 between minute 26-30"},
    {"num": 3,   "name": "Two Early Goals Trap",   "action": "TRAP",       "desc": "Over already 1.66-1.75 -- two early goals priced in"},
    {"num": 4,   "name": "Next Goal 2.10 Value",        "action": "GOAL",       "desc": "Next Goal odd >= 2.10 between minute 30-34"},
    {"num": 5,   "name": "1.66 Trap",              "action": "TRAP",       "desc": "Over hovering at exactly 1.66"},
    {"num": 6,   "name": "Pair Signal",            "action": "GOAL",       "desc": "Draw 1.61 + Over 1.90 -- both confirm goal pressure"},
    {"num": 7,   "name": "3rd Goal Moment",        "action": "GOAL",       "desc": "Over >= 2.15 between minute 65-70"},
    {"num": 8,   "name": "Market Shut",            "action": "NO_GOAL",    "desc": "Over >= 2.80 in minute 82+"},
    {"num": 11,  "name": "Early Drop Signal",      "action": "GOAL",       "desc": "Over <= 1.55 between minute 17-20"},
    {"num": 12,  "name": "Opening 1.30 Rule",      "action": "GOAL",       "desc": "Match opened with Next Goal at 1.30"},
    {"num": 13,  "name": "1.57 Entry Point",       "action": "GOAL",       "desc": "Over 1.54-1.60"},
    {"num": 14,  "name": "Duration HELD",          "action": "GOAL",       "desc": "Next Goal odd 2.30-2.70 held same value 2+ minutes"},
    {"num": 15,  "name": "Duration REJECTED",      "action": "NO_GOAL",    "desc": "Over jumped within 30s -- rejected by market"},
    {"num": 16,  "name": "Sharp Drop Signal",      "action": "GOAL",       "desc": "Over dropped 0.15+ in last snapshot"},
    {"num": 101, "name": "HT Pressure 0.5",        "action": "GOAL",       "desc": "Over 0.5 HT below expected curve, minute 15-45"},
    {"num": 102, "name": "HT Pressure 1.5",        "action": "GOAL",       "desc": "Over 1.5 HT below expected curve"},
    {"num": 103, "name": "Late Game Pressure",     "action": "GOAL",       "desc": "Over below expected by 0.8+ in minute 80+"},
    {"num": 104, "name": "Late Odd Sweet Spot",    "action": "GOAL",       "desc": "Next Goal odd 2.7-3.5 between minute 85-93"},
    {"num": 200, "name": "High Market Pressure",   "action": "GOAL",       "desc": "Composite pressure score >= 60%"},
    # ---- Movement-based rules (v6) ----
    {"num": 300, "name": "Steady Real Drop",       "action": "STRONG_GOAL", "desc": "STEADY_DROP + REAL_DROP + movement_score>=70 -- the gold-standard goal signal"},
    {"num": 301, "name": "Sharp Real Drop",        "action": "STRONG_GOAL", "desc": "SHARP_DROP + REAL_DROP + movement_score>=60 -- market just learned something"},
    {"num": 302, "name": "Fake Drop Trap",         "action": "TRAP",        "desc": "Drop followed by reversal -- looks like pressure, isn't"},
    {"num": 303, "name": "Chaotic Market",         "action": "NO_ENTRY",    "desc": "Mixed up/down movement with no clear direction -- pure noise"},
    {"num": 304, "name": "Market Rejection",       "action": "NO_GOAL",    "desc": "SPIKE_UP -- odds rising sharply, market rejecting goal"},
    {"num": 305, "name": "Flat Market Late",       "action": "NO_GOAL",    "desc": "FLAT pattern in minute 55+ -- market doesn't believe"},
]


# ============================================================================
# DATABASE
# ============================================================================
@contextmanager
def db_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL not configured")
    conn = psycopg2.connect(DATABASE_URL)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


@contextmanager
def db_cursor(dict_rows: bool = False):
    with db_conn() as conn:
        cur = conn.cursor(cursor_factory=RealDictCursor) if dict_rows else conn.cursor()
        try:
            yield cur
        finally:
            cur.close()


def _ensure_column(cur, table: str, column: str, ddl: str):
    """Add a column if it doesn't already exist. Used by migrations."""
    cur.execute(
        """SELECT 1 FROM information_schema.columns
           WHERE table_name = %s AND column_name = %s""",
        (table, column),
    )
    if not cur.fetchone():
        log.info(f"Migration: ALTER TABLE {table} ADD COLUMN {column}")
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")


# Bump this when you make schema changes. On boot, if the DB is on an older
# version we drop legacy PapaGoal tables and rebuild from scratch.
SCHEMA_VERSION = 7


def _read_schema_version() -> int:
    """Read the persisted schema version. Uses its own connection so a
    failure here doesn't poison subsequent migrations."""
    if not DATABASE_URL:
        return 0
    try:
        conn = psycopg2.connect(DATABASE_URL)
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.tables
                        WHERE table_name = 'schema_version')
                """)
                if not cur.fetchone()[0]:
                    return 0
                cur.execute("SELECT version FROM schema_version LIMIT 1")
                row = cur.fetchone()
                return row[0] if row else 0
        finally:
            conn.close()
    except Exception as e:
        log.warning(f"_read_schema_version failed (treating as 0): {e}")
        return 0


def _drop_legacy_tables():
    """Drop all PapaGoal tables. Each DROP runs in its own transaction so
    one failure can't poison the whole batch."""
    tables = ("paper_trades", "signals", "goals", "odds_snapshots",
              "ai_insights", "rules", "matches", "schema_version")
    for t in tables:
        try:
            conn = psycopg2.connect(DATABASE_URL)
            try:
                with conn.cursor() as cur:
                    cur.execute(f"DROP TABLE IF EXISTS {t} CASCADE")
                conn.commit()
                log.info(f"  dropped {t}")
            finally:
                conn.close()
        except Exception as e:
            log.warning(f"  drop {t} failed: {e}")


def init_db():
    """Idempotent schema setup -- runs every boot, safe to re-run."""
    if not DATABASE_URL:
        log.warning("DATABASE_URL missing -- skipping init_db")
        return

    current_version = _read_schema_version()
    log.info(f"init_db: current schema version = {current_version}, target = {SCHEMA_VERSION}")

    if current_version < SCHEMA_VERSION:
        log.warning(f"Upgrading schema {current_version} -> {SCHEMA_VERSION}: "
                    f"dropping legacy PapaGoal tables")
        _drop_legacy_tables()

    with db_cursor() as cur:
        # ---- matches ----
        cur.execute("""
        CREATE TABLE IF NOT EXISTS matches (
            id              SERIAL PRIMARY KEY,
            event_id        TEXT UNIQUE,
            league          TEXT,
            home            TEXT,
            away            TEXT,
            minute          INT,
            score_home      INT DEFAULT 0,
            score_away      INT DEFAULT 0,
            status          TEXT,
            opening_next_goal  NUMERIC(6,3),
            first_seen_at   TIMESTAMPTZ DEFAULT NOW(),
            last_updated_at TIMESTAMPTZ DEFAULT NOW()
        )""")

        # ---- odds_snapshots ----
        cur.execute("""
        CREATE TABLE IF NOT EXISTS odds_snapshots (
            id                       BIGSERIAL PRIMARY KEY,
            match_id                 INT REFERENCES matches(id) ON DELETE CASCADE,
            minute                   INT,
            captured_at              TIMESTAMPTZ DEFAULT NOW(),
            home_ml                  NUMERIC(6,3),
            draw_ml                  NUMERIC(6,3),
            away_ml                  NUMERIC(6,3),
            -- Primary market: Next Goal (Yes/Over)
            next_goal                NUMERIC(6,3),
            prev_next_goal           NUMERIC(6,3),
            opening_next_goal        NUMERIC(6,3),
            expected_next_goal       NUMERIC(6,3),
            -- Diagnostic markets (recorded but not used by rules)
            over_25                  NUMERIC(6,3),
            under_25                 NUMERIC(6,3),
            over_35                  NUMERIC(6,3),
            -- HT markets (used by HT-pressure rules)
            over_05_ht               NUMERIC(6,3),
            over_15_ht               NUMERIC(6,3),
            -- Snapshot meta
            direction                TEXT,
            held_seconds             INT DEFAULT 0,
            pressure                 NUMERIC(6,2),
            is_live                  BOOLEAN DEFAULT TRUE,
            goal_30s                 BOOLEAN,
            goal_60s                 BOOLEAN,
            goal_120s                BOOLEAN,
            goal_300s                BOOLEAN,
            -- Movement engine fields
            movement_pattern         TEXT,
            drop_type                TEXT,
            movement_score           INT,
            total_changes_count      INT,
            direction_consistency    INT,
            longest_stable_duration  INT,
            total_delta              NUMERIC(8,4),
            avg_time_between_changes INT
        )""")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_snap_match_time ON odds_snapshots(match_id, captured_at DESC)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_snap_captured ON odds_snapshots(captured_at DESC)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_snap_pattern ON odds_snapshots(movement_pattern)")

        # ---- goals ----
        cur.execute("""
        CREATE TABLE IF NOT EXISTS goals (
            id              BIGSERIAL PRIMARY KEY,
            match_id        INT REFERENCES matches(id) ON DELETE CASCADE,
            minute          INT,
            score_before    TEXT,
            score_after     TEXT,
            goal_time       TIMESTAMPTZ DEFAULT NOW(),
            had_snapshots   INT DEFAULT 0,
            odds_10s        JSONB,
            odds_30s        JSONB,
            odds_60s        JSONB,
            odds_120s       JSONB,
            odds_300s       JSONB
        )""")
        # Repair the legacy table the user mentioned ("goal_time column missing"):
        _ensure_column(cur, "goals", "goal_time",     "TIMESTAMPTZ DEFAULT NOW()")
        _ensure_column(cur, "goals", "had_snapshots", "INT DEFAULT 0")
        _ensure_column(cur, "goals", "odds_10s",      "JSONB")
        _ensure_column(cur, "goals", "odds_30s",      "JSONB")
        _ensure_column(cur, "goals", "odds_60s",      "JSONB")
        _ensure_column(cur, "goals", "odds_120s",     "JSONB")
        _ensure_column(cur, "goals", "odds_300s",     "JSONB")

        # ---- signals ----
        cur.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            id              BIGSERIAL PRIMARY KEY,
            match_id        INT REFERENCES matches(id) ON DELETE CASCADE,
            triggered_at    TIMESTAMPTZ DEFAULT NOW(),
            minute          INT,
            rule_num        INT,
            rule_name       TEXT,
            verdict         TEXT,
            confidence      NUMERIC(5,2),
            pressure_score  NUMERIC(5,2),
            next_goal_odd     NUMERIC(6,3),
            over_05_ht_odd  NUMERIC(6,3),
            over_15_ht_odd  NUMERIC(6,3),
            opening_ng NUMERIC(6,3),
            opening_draw    NUMERIC(6,3),
            opening_home    NUMERIC(6,3),
            opening_away    NUMERIC(6,3),
            details         JSONB
        )""")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sig_time ON signals(triggered_at DESC)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sig_match ON signals(match_id)")

        # ---- paper_trades ----
        cur.execute("""
        CREATE TABLE IF NOT EXISTS paper_trades (
            id              BIGSERIAL PRIMARY KEY,
            signal_id       BIGINT REFERENCES signals(id) ON DELETE CASCADE,
            match_id        INT REFERENCES matches(id) ON DELETE CASCADE,
            opened_at       TIMESTAMPTZ DEFAULT NOW(),
            entry_odd       NUMERIC(6,3),
            verdict         TEXT,
            rule_name       TEXT,
            minute_entry    INT,
            result          TEXT DEFAULT 'pending',  -- pending / success / miss
            settled_at      TIMESTAMPTZ,
            profit_loss     NUMERIC(8,3) DEFAULT 0
        )""")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_pt_result ON paper_trades(result)")

        # ---- rules ----
        cur.execute("""
        CREATE TABLE IF NOT EXISTS rules (
            id              SERIAL PRIMARY KEY,
            rule_num        INT UNIQUE,
            rule_name       TEXT UNIQUE,
            description     TEXT,
            action          TEXT,
            source          TEXT DEFAULT 'core',     -- core / claude / user
            is_active       BOOLEAN DEFAULT TRUE,
            total_signals   INT DEFAULT 0,
            success_count   INT DEFAULT 0,
            fail_count      INT DEFAULT 0,
            success_rate    NUMERIC(5,2) DEFAULT 0,
            created_at      TIMESTAMPTZ DEFAULT NOW()
        )""")

        # Seed core rules
        for r in RULES_CATALOG:
            cur.execute("""
            INSERT INTO rules (rule_num, rule_name, description, action, source, is_active)
            VALUES (%s, %s, %s, %s, 'core', TRUE)
            ON CONFLICT (rule_name) DO UPDATE
              SET description = EXCLUDED.description,
                  action      = EXCLUDED.action,
                  rule_num    = EXCLUDED.rule_num
            """, (r["num"], r["name"], r["desc"], r["action"]))

        # ---- ai_insights ----
        cur.execute("""
        CREATE TABLE IF NOT EXISTS ai_insights (
            id              BIGSERIAL PRIMARY KEY,
            created_at      TIMESTAMPTZ DEFAULT NOW(),
            insight_type    TEXT,
            content         TEXT,
            goals_analyzed  INT DEFAULT 0
        )""")

        # ---- schema_version (track current version so we don't re-drop on every boot) ----
        cur.execute("""
        CREATE TABLE IF NOT EXISTS schema_version (
            version    INT PRIMARY KEY,
            updated_at TIMESTAMPTZ DEFAULT NOW()
        )""")
        cur.execute("DELETE FROM schema_version")
        cur.execute("INSERT INTO schema_version (version) VALUES (%s)",
                    (SCHEMA_VERSION,))

    log.info(f"init_db: done (schema v{SCHEMA_VERSION})")


# ============================================================================
# ODDSAPI CLIENT  --  *** FIXED PARSER ***
# ============================================================================
ODDSPAPI_BASE = "https://api.odds-api.io/v3"


def fetch_oddsapi_events() -> list:
    """Get list of currently-live football events."""
    if not ODDSPAPI_KEY:
        return []
    try:
        r = requests.get(
            f"{ODDSPAPI_BASE}/events",
            params={"apiKey": ODDSPAPI_KEY, "sport": "football", "status": "live"},
            timeout=15,
        )
        r.raise_for_status()
        data = r.json()
        if isinstance(data, dict):
            data = data.get("data") or data.get("events") or []
        return data or []
    except Exception as e:
        log.warning(f"fetch_oddsapi_events failed: {e}")
        return []


def _fetch_oddsapi_odds_request(event_ids: list) -> tuple:
    """One HTTP call. Returns (ok, status_code, data_or_none)."""
    if not event_ids:
        return True, 200, []
    try:
        r = requests.get(
            f"{ODDSPAPI_BASE}/odds/multi",
            params={
                "apiKey": ODDSPAPI_KEY,
                "eventIds": ",".join(str(x) for x in event_ids),
                "bookmakers": "Bet365",
            },
            timeout=20,
        )
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, dict):
                data = data.get("data") or data.get("events") or [data]
            return True, 200, data if isinstance(data, list) else []
        return False, r.status_code, None
    except Exception:
        return False, 0, None


def fetch_oddsapi_odds(event_ids: list) -> list:
    """Get odds for a list of event ids from Bet365.

    Strategy: try the whole chunk first; if it fails with 400, recursively
    bisect until we either find the bad ID(s) or get the good ones through.
    Single bad IDs are logged once and skipped. This way one stale ID can't
    break the whole batch."""
    if not event_ids or not ODDSPAPI_KEY:
        return []

    out = []
    chunk_size = 15
    for i in range(0, len(event_ids), chunk_size):
        chunk = event_ids[i:i + chunk_size]
        out.extend(_fetch_with_bisect(chunk))
    return out


_BAD_EVENT_IDS: set = set()  # cache of confirmed bad IDs to skip on next loops


def _fetch_with_bisect(ids: list, depth: int = 0) -> list:
    """Recursively bisect a chunk on 400. Returns the odds rows we got."""
    # Filter out IDs we already know are bad
    ids = [x for x in ids if x not in _BAD_EVENT_IDS]
    if not ids:
        return []

    ok, status, data = _fetch_oddsapi_odds_request(ids)
    if ok:
        return data or []

    if status == 400:
        if len(ids) == 1:
            # Found a single bad ID -- mark and skip it
            bad = ids[0]
            if bad not in _BAD_EVENT_IDS:
                _BAD_EVENT_IDS.add(bad)
                log.info(f"OddsAPI: skipping bad event_id={bad}")
            return []
        # Bisect
        mid = len(ids) // 2
        left = _fetch_with_bisect(ids[:mid], depth + 1)
        right = _fetch_with_bisect(ids[mid:], depth + 1)
        return left + right

    # Non-400 (network, timeout, 5xx) -- log once and move on
    if depth == 0:
        log.warning(f"fetch_oddsapi_odds chunk failed ({len(ids)} ids): HTTP {status}")
    return []


def _to_float(v: Any) -> Optional[float]:
    """Convert string/number to float, ignore garbage."""
    if v is None or v == "" or v == "-":
        return None
    try:
        f = float(v)
        return f if f > 1.0 else None  # decimal odds must be > 1
    except (ValueError, TypeError):
        return None


# Diagnostic: log each unique OddsAPI market name once. Lets us see in
# Railway logs whether OddsAPI is shipping HT under a name we don't recognise.
_SEEN_MARKETS: set = set()


def _log_market_once(name: str, captured: bool):
    if not name or name in _SEEN_MARKETS:
        return
    _SEEN_MARKETS.add(name)
    tag = "captured" if captured else "SKIPPED"
    log.info(f"OddsAPI market seen [{tag}]: {name!r}")


def parse_match_odds(item: dict) -> Optional[dict]:
    """
    *** FIXED PARSER for OddsAPI v3 ***
    Input shape:
        {
          "id": 123, "home": "Arsenal", "away": "Chelsea",
          "league": "...", "minute": 23, "score": "0-1",
          "bookmakers": {
              "Bet365": [
                  {"name": "ML",          "odds": [{"home":"2.10","draw":"3.40","away":"3.20"}]},
                  {"name": "Over/Under",  "odds": [{"max":2.5,"over":"1.90","under":"1.90"},
                                                   {"max":1.5,"over":"1.40","under":"2.80"},
                                                   {"max":0.5,"over":"1.10","under":"6.50"}]}
              ]
          }
        }

    The Over/Under markets returned by OddsAPI are full-time goal lines.
    HT (1st-half) markets, when present, may appear under names like
    "1st Half Over/Under" or "First Half Goals" -- we capture them too if found.
    """
    if not isinstance(item, dict):
        return None

    def _as_str(v) -> str:
        """Coerce anything to a clean string. OddsAPI sometimes ships
        league/team/competition as a dict like {'name': 'Premier League', 'id': 1}
        instead of a plain string."""
        if v is None:
            return ""
        if isinstance(v, str):
            return v
        if isinstance(v, dict):
            for k in ("name", "title", "shortName", "fullName"):
                if v.get(k):
                    return str(v[k])
            return ""
        return str(v)

    event_id = item.get("id") or item.get("eventId")
    home = _as_str(item.get("home") or item.get("homeTeam"))
    away = _as_str(item.get("away") or item.get("awayTeam"))
    if not event_id or not home or not away:
        return None

    out = {
        "event_id": str(event_id),
        "home": home,
        "away": away,
        "league": _as_str(item.get("league") or item.get("competition") or ""),
        "minute": item.get("minute") if isinstance(item.get("minute"), int) else None,
        "score": _as_str(item.get("score") or ""),
        "home_ml": None, "draw_ml": None, "away_ml": None,
        "next_goal": None,                          # primary market
        "over_25": None, "under_25": None, "over_35": None,  # diagnostic only
        "over_05_ht": None, "over_15_ht": None,
    }

    bookmakers = item.get("bookmakers") or {}
    bet365 = bookmakers.get("Bet365") or bookmakers.get("bet365") or []
    if not isinstance(bet365, list):
        return out

    for market in bet365:
        if not isinstance(market, dict):
            continue
        market_name = (market.get("name") or "").strip()
        odds_list = market.get("odds") or []
        if not isinstance(odds_list, list) or not odds_list:
            continue

        mn_upper = market_name.upper()

        # ----- Full-time 1X2 -----
        if mn_upper in ("ML", "1X2", "MATCH WINNER", "MATCH RESULT"):
            _log_market_once(market_name, True)
            first = odds_list[0]
            if isinstance(first, dict):
                out["home_ml"] = _to_float(first.get("home"))
                out["draw_ml"] = _to_float(first.get("draw"))
                out["away_ml"] = _to_float(first.get("away"))

        # ----- Full-time Over/Under (Totals) -----
        # This is the goal-totals market (Over/Under 2.5/3.5 etc) -- NOT what
        # PapaGoal uses. We capture it for diagnostics only, since the Next
        # Goal market is the one that drives all our rules.
        elif mn_upper in ("TOTALS", "OVER/UNDER", "TOTAL GOALS",
                          "GOALS OVER/UNDER", "MATCH TOTALS"):
            _log_market_once(market_name, True)
            for entry in odds_list:
                if not isinstance(entry, dict):
                    continue
                try:
                    line = float(entry.get("hdp", entry.get("max", entry.get("line", 0))))
                except (ValueError, TypeError):
                    continue
                ov = _to_float(entry.get("over"))
                un = _to_float(entry.get("under"))
                if abs(line - 2.5) < 0.01:
                    out["over_25"] = ov          # diagnostic only
                    out["under_25"] = un
                elif abs(line - 3.5) < 0.01:
                    out["over_35"] = ov

        # ----- Next Goal market  (THE PRIMARY MARKET FOR PAPAGOAL) -----
        # "Will another goal be scored?" odds. OddsAPI may ship this under
        # several names; we cover the common ones.
        elif mn_upper in ("NEXT GOAL", "TO SCORE NEXT", "NEXT TEAM TO SCORE",
                          "NEXT GOALSCORER MARKET", "NEXTGOAL", "GOAL MARKET",
                          "WILL THERE BE ANOTHER GOAL", "ANOTHER GOAL"):
            _log_market_once(market_name, True)
            # The odds list usually has either:
            #   {"home": "1.85", "draw": "5.20", "away": "2.40"}  -- 3-way next-scorer
            #   {"yes": "1.85", "no": "2.10"}                     -- 2-way "another goal?"
            #   {"over": "1.85", "under": "2.10"}                 -- some providers
            first = odds_list[0]
            if isinstance(first, dict):
                # Prefer 2-way "yes/no" form -- that's the goal-or-no-goal odd.
                ng = (_to_float(first.get("yes"))
                      or _to_float(first.get("over"))
                      or _to_float(first.get("home"))   # fallback: home-scores-next
                      or _to_float(first.get("any")))
                if ng:
                    out["next_goal"] = ng

        # ----- Half-time 1X2 (captured for completeness, not currently used) -----
        elif mn_upper in ("ML HT", "1X2 HT", "1ST HALF ML",
                          "FIRST HALF ML", "MATCH WINNER HT"):
            _log_market_once(market_name, True)
            # not currently consumed by any rule -- placeholder

        # ----- Half-time Over/Under -----
        # OddsAPI ships this as "Totals HT". We want max=0.5 and max=1.5.
        elif mn_upper in ("TOTALS HT", "OVER/UNDER HT",
                          "1ST HALF OVER/UNDER", "FIRST HALF OVER/UNDER",
                          "HALF TIME OVER/UNDER", "1ST HALF GOALS",
                          "FIRST HALF GOALS", "1ST HALF TOTALS",
                          "HT TOTALS", "HALFTIME TOTALS"):
            _log_market_once(market_name, True)
            for entry in odds_list:
                if not isinstance(entry, dict):
                    continue
                try:
                    line = float(entry.get("hdp", entry.get("max", entry.get("line", 0))))
                except (ValueError, TypeError):
                    continue
                ov = _to_float(entry.get("over"))
                if abs(line - 0.5) < 0.01:
                    out["over_05_ht"] = ov
                elif abs(line - 1.5) < 0.01:
                    out["over_15_ht"] = ov

        else:
            _log_market_once(market_name, False)

    return out


# ============================================================================
# FOOTBALL API PRO  --  minute + score
# ============================================================================
FOOTBALL_API_BASE = "https://api.football-api-pro.com/v3"  # adjust if your provider differs


def fetch_football_live() -> list:
    """Get currently-live matches with minute + score."""
    if not FOOTBALL_API_KEY:
        return []
    try:
        r = requests.get(
            f"{FOOTBALL_API_BASE}/fixtures",
            params={"live": "all"},
            headers={"x-apisports-key": FOOTBALL_API_KEY},
            timeout=15,
        )
        if r.status_code != 200:
            return []
        return (r.json() or {}).get("response", []) or []
    except Exception as e:
        log.debug(f"fetch_football_live failed: {e}")
        return []


def index_football_live(fixtures: list) -> dict:
    """Build a lookup keyed by lowercase 'home|away' -> {minute, score}."""
    out = {}
    for fx in fixtures:
        try:
            teams = fx.get("teams", {})
            home = (teams.get("home") or {}).get("name", "")
            away = (teams.get("away") or {}).get("name", "")
            status = fx.get("fixture", {}).get("status", {})
            minute = status.get("elapsed")
            goals = fx.get("goals", {})
            score = f"{goals.get('home', 0)}-{goals.get('away', 0)}"
            if home and away:
                out[f"{home.lower()}|{away.lower()}"] = {
                    "minute": minute,
                    "score": score,
                    "score_home": goals.get("home", 0) or 0,
                    "score_away": goals.get("away", 0) or 0,
                }
        except Exception:
            continue
    return out


def lookup_football(idx: dict, home: str, away: str) -> Optional[dict]:
    if not home or not away:
        return None
    return idx.get(f"{home.lower()}|{away.lower()}")


# ============================================================================
# MOVEMENT ENGINE  --  classifies how the Next Goal odd has been moving
# ============================================================================
# Adapted from the base44 v1 engine. The idea: a snapshot's value alone is
# not enough -- the *pattern* of recent movement tells us whether we're
# looking at real pressure or a market trick.
#
#   STEADY_DROP  -> consistent decline = real pressure for a goal
#   SHARP_DROP   -> sudden big move = market just learned something
#   FAKE_DROP    -> drop then bounce-back within seconds = trap
#   SPIKE_UP     -> sharp rise = market is rejecting goal expectation
#   FLAT         -> no movement at all = market doesn't believe
#   CHAOTIC      -> mixed up/down with no direction = noise
#   UNKNOWN      -> too few samples
#
# A signal is much stronger when accompanied by REAL_DROP than when it's just
# "Next Goal is at 1.85 in minute 30".

MOVEMENT_BUFFER_SECONDS = 180  # look at the last 3 minutes of snapshots


def _build_movement_buffer(snaps: list) -> list:
    """snaps is a list of (captured_at_dt, next_goal_float) tuples, OLDEST first.
    Returns a list of dicts with delta vs previous, direction, time_since_prev."""
    out = []
    for i, (ts, odd) in enumerate(snaps):
        if odd is None:
            continue
        if i == 0:
            out.append({"ts": ts, "odd": odd, "delta": 0.0,
                        "direction": "stable", "time_since_prev": 0})
        else:
            prev_ts, prev_odd = snaps[i - 1]
            if prev_odd is None:
                out.append({"ts": ts, "odd": odd, "delta": 0.0,
                            "direction": "stable", "time_since_prev": 0})
                continue
            delta = float(odd) - float(prev_odd)
            time_since = (ts - prev_ts).total_seconds()
            direction = "down" if delta < -0.005 else "up" if delta > 0.005 else "stable"
            out.append({"ts": ts, "odd": float(odd), "delta": round(delta, 4),
                        "direction": direction, "time_since_prev": time_since})
    return out


def _compute_movement_metrics(buf: list) -> dict:
    """Aggregate stats from the movement buffer."""
    if len(buf) < 2:
        return {"total_changes_count": 0, "avg_time_between_changes": 0,
                "total_delta": 0.0, "direction_consistency": 0,
                "longest_stable_duration": 0, "movement_score": 0}

    changes = [b for b in buf if b["delta"] != 0]
    total_changes = len(changes)
    intervals = [b["time_since_prev"] for b in changes if b["time_since_prev"] > 0]
    avg_interval = sum(intervals) / len(intervals) if intervals else 0
    total_delta = buf[-1]["odd"] - buf[0]["odd"]
    downs = len([b for b in changes if b["delta"] < 0])
    ups = len([b for b in changes if b["delta"] > 0])
    dominant = max(downs, ups)
    direction_consistency = round((dominant / total_changes) * 100) if total_changes > 0 else 0

    # Longest stable run
    longest_stable = 0
    current_stable = 0
    for b in buf:
        if b["delta"] == 0:
            current_stable += b["time_since_prev"]
            longest_stable = max(longest_stable, current_stable)
        else:
            current_stable = 0

    # Composite movement score 0-100
    consistency_score = direction_consistency * 0.40
    duration_score = min(1, total_changes / 12) * 100 * 0.30
    flip_penalty = abs(ups - downs) / total_changes if total_changes > 0 else 0
    smoothness_score = (1 - min(1, flip_penalty)) * 100 * 0.20
    magnitude_score = min(1, abs(total_delta) / 0.5) * 100 * 0.10
    movement_score = round(max(0, min(100,
        consistency_score + duration_score + smoothness_score + magnitude_score)))

    return {
        "total_changes_count": total_changes,
        "avg_time_between_changes": round(avg_interval),
        "total_delta": round(total_delta, 4),
        "direction_consistency": direction_consistency,
        "longest_stable_duration": round(longest_stable),
        "movement_score": movement_score,
    }


def _classify_movement_pattern(buf: list, metrics: dict) -> str:
    """Pick one of: STEADY_DROP, SHARP_DROP, FAKE_DROP, SPIKE_UP, FLAT, CHAOTIC, UNKNOWN."""
    if not buf or len(buf) < 2:
        return "UNKNOWN"

    tcc = metrics["total_changes_count"]
    dc = metrics["direction_consistency"]
    td = metrics["total_delta"]
    lsd = metrics["longest_stable_duration"]
    atbc = metrics["avg_time_between_changes"]

    # FLAT: held same value
    if lsd >= 60 and tcc <= 1:
        return "FLAT"
    if abs(td) < 0.02 and tcc < 3:
        return "FLAT"

    # SPIKE_UP: rising fast
    if td > 0.05 and dc >= 60:
        return "SPIKE_UP"

    # Various drop types
    if td < -0.02:
        # Look at the last 3 changes for a trailing reversal
        last_changes = [b for b in buf if b["delta"] != 0][-3:]
        last_dir = last_changes[-1]["direction"] if last_changes else None
        prev_dir = last_changes[-2]["direction"] if len(last_changes) >= 2 else None
        last_int = last_changes[-1]["time_since_prev"] if last_changes else 999

        # Recent up-bounce after down = FAKE_DROP
        if last_dir == "up" and prev_dir == "down" and last_int <= 60:
            return "FAKE_DROP"
        # Few big steps = SHARP_DROP
        if tcc <= 4 and abs(td) >= 0.15:
            return "SHARP_DROP"
        if atbc < 15 and abs(td) >= 0.10:
            return "SHARP_DROP"
        # Many consistent steps = STEADY_DROP
        if dc >= 65 and tcc >= 3:
            return "STEADY_DROP"

    # No clear direction = CHAOTIC
    if dc < 50 and tcc >= 3:
        return "CHAOTIC"

    return "UNKNOWN"


def _classify_drop_type(buf: list, metrics: dict, pattern: str) -> str:
    """REAL_DROP / FAKE_DROP / NONE.

    A REAL_DROP is the gold standard: a drop that has been confirmed by
    sustained downward movement with no recent upward ticks. This is what
    the base44 system found to drive most successful signals."""
    if pattern in ("FAKE_DROP", "CHAOTIC"):
        return "FAKE_DROP"
    if pattern not in ("STEADY_DROP", "SHARP_DROP"):
        return "NONE"

    # Has there been any upward tick in the last 60s?
    if buf:
        latest_ts = buf[-1]["ts"]
        recent_60 = [b for b in buf
                     if (latest_ts - b["ts"]).total_seconds() <= 60]
        if any(b["delta"] > 0 for b in recent_60):
            return "FAKE_DROP"

    if (metrics["total_changes_count"] >= 3
            and metrics["avg_time_between_changes"] >= 10
            and metrics["direction_consistency"] >= 70):
        return "REAL_DROP"

    return "FAKE_DROP"


def fetch_movement_buffer(cur, match_id: int) -> list:
    """Pull the snapshots for a match in the last MOVEMENT_BUFFER_SECONDS."""
    cur.execute("""
        SELECT captured_at, next_goal
          FROM odds_snapshots
         WHERE match_id = %s
           AND captured_at >= NOW() - (%s || ' seconds')::interval
         ORDER BY captured_at ASC
    """, (match_id, MOVEMENT_BUFFER_SECONDS))
    rows = cur.fetchall()
    return [(r[0], float(r[1]) if r[1] is not None else None) for r in rows]


def analyze_movement(cur, match_id: int) -> dict:
    """High-level entry point. Returns dict with movement_pattern, drop_type,
    movement_score, total_changes, direction_consistency, etc."""
    snaps = fetch_movement_buffer(cur, match_id)
    buf = _build_movement_buffer(snaps)
    metrics = _compute_movement_metrics(buf)
    pattern = _classify_movement_pattern(buf, metrics)
    drop_type = _classify_drop_type(buf, metrics, pattern)
    return {
        "movement_pattern": pattern,
        "drop_type": drop_type,
        "movement_score": metrics["movement_score"],
        "total_changes_count": metrics["total_changes_count"],
        "direction_consistency": metrics["direction_consistency"],
        "longest_stable_duration": metrics["longest_stable_duration"],
        "total_delta": metrics["total_delta"],
        "avg_time_between_changes": metrics["avg_time_between_changes"],
    }


# ============================================================================
# RULES ENGINE
# ============================================================================
def evaluate_rules(snap: dict, prev: Optional[dict], opening: Optional[dict]) -> list:
    """Run all rules against the latest snapshot. Returns a list of signals.

    snap / prev / opening fields used:
        minute, next_goal, draw_ml, home_ml, away_ml, over_05_ht, over_15_ht,
        held_seconds (snap only), direction (snap only)
    """
    signals = []
    minute     = snap.get("minute")
    over       = snap.get("next_goal")
    draw       = snap.get("draw_ml")
    over05_ht  = snap.get("over_05_ht")
    over15_ht  = snap.get("over_15_ht")
    held       = snap.get("held_seconds", 0)
    prev_over  = (prev or {}).get("next_goal") if prev else None
    open_over  = (opening or {}).get("next_goal") if opening else None

    if minute is None:
        return signals

    def add(num, name, action, conf, extra=None):
        signals.append({
            "rule_num": num, "rule_name": name, "verdict": action,
            "confidence": conf, "details": extra or {},
        })

    # --- Rule 1: Early Draw Signal ---
    if draw and over and 21 <= minute <= 25:
        if 1.57 <= draw <= 1.66 and 1.83 <= over <= 2.10:
            add(1, "Early Draw Signal", "DRAW_UNDER", 65)

    # --- Rule 2: Frozen Over ---
    if over and 26 <= minute <= 30 and 1.80 <= over <= 1.86:
        add(2, "Frozen Over", "NO_ENTRY", 55)

    # --- Rule 3: Two Early Goals Trap ---
    if over and 1.66 <= over <= 1.75 and minute >= 15:
        add(3, "Two Early Goals Trap", "TRAP", 60)

    # --- Rule 4: Next Goal 2.10 Value ---
    if over and 30 <= minute <= 34 and over >= 2.10:
        add(4, "Next Goal 2.10 Value", "GOAL", 70)

    # --- Rule 5: 1.66 Trap ---
    if over and abs(over - 1.66) < 0.02 and minute >= 20:
        add(5, "1.66 Trap", "TRAP", 55)

    # --- Rule 6: Pair Signal ---
    if draw and over and abs(draw - 1.61) < 0.05 and abs(over - 1.90) < 0.05:
        add(6, "Pair Signal", "GOAL", 75)

    # --- Rule 7: 3rd Goal Moment ---
    if over and 65 <= minute <= 70 and over >= 2.15:
        add(7, "3rd Goal Moment", "GOAL", 70)

    # --- Rule 8: Market Shut ---
    if over and minute >= 82 and over >= 2.80:
        add(8, "Market Shut", "NO_GOAL", 80)

    # --- Rule 11: Early Drop Signal ---
    if over and 17 <= minute <= 20 and over <= 1.55:
        add(11, "Early Drop Signal", "GOAL", 75)

    # --- Rule 12: Opening 1.30 Rule ---
    if open_over and abs(open_over - 1.30) < 0.04 and minute >= 5:
        add(12, "Opening 1.30 Rule", "GOAL", 65)

    # --- Rule 13: 1.57 Entry Point ---
    if over and 1.54 <= over <= 1.60:
        add(13, "1.57 Entry Point", "GOAL", 60)

    # --- Rule 14: Duration HELD (>=2 min) ---
    if over and 2.30 <= over <= 2.70 and held >= 120:
        add(14, "Duration HELD", "GOAL", 70)

    # --- Rule 15: Duration REJECTED (<30s) ---
    if prev_over and over and snap.get("direction") == "UP" and held <= 30:
        if (over - prev_over) >= 0.10:
            add(15, "Duration REJECTED", "NO_GOAL", 65)

    # --- Rule 16: Sharp Drop Signal ---
    if prev_over and over and (prev_over - over) >= 0.15:
        add(16, "Sharp Drop Signal", "GOAL", 80)

    # --- Rule 101: HT Pressure 0.5 ---
    if over05_ht and 15 <= minute <= 45:
        exp = get_expected_odd(EXPECTED_OVER05_HT, minute)
        if exp and (exp - over05_ht) >= 0.10:
            pr = calculate_pressure(over05_ht, exp)
            add(101, "HT Pressure 0.5", "GOAL", min(95, 50 + pr),
                {"expected": round(exp, 3), "pressure": round(pr, 1)})

    # --- Rule 102: HT Pressure 1.5 ---
    if over15_ht and 15 <= minute <= 45:
        exp = get_expected_odd(EXPECTED_OVER15_HT, minute)
        if exp and (exp - over15_ht) >= 0.20:
            pr = calculate_pressure(over15_ht, exp)
            add(102, "HT Pressure 1.5", "GOAL", min(95, 50 + pr),
                {"expected": round(exp, 3), "pressure": round(pr, 1)})

    # --- Rule 103: Late Game Pressure ---
    if over and minute >= 80:
        exp = get_expected_odd(EXPECTED_NEXT_GOAL, minute)
        if exp and (exp - over) >= 0.80:
            pr = calculate_pressure(over, exp)
            add(103, "Late Game Pressure", "GOAL", min(95, 55 + pr / 2),
                {"expected": round(exp, 3), "pressure": round(pr, 1)})

    # --- Rule 104: Late Odd Sweet Spot ---
    if over and 85 <= minute <= 93 and 2.7 <= over <= 3.5:
        add(104, "Late Odd Sweet Spot", "GOAL", 75)

    # --- Rule 200: composite high pressure ---
    if snap.get("pressure") and snap["pressure"] >= 60:
        add(200, "High Market Pressure", "GOAL", min(95, 50 + snap["pressure"] / 2),
            {"pressure": snap["pressure"]})

    # ===== Movement-based rules (v6 / port from base44) =====
    # These look at the *pattern* of recent movement, not just the current odd.
    # The drop_type "REAL_DROP" means the market has been pushing the odd
    # downward consistently with no recent reversals -- the strongest
    # confirmation that a goal is coming.
    pat   = snap.get("movement_pattern")
    dt    = snap.get("drop_type")
    mscore = snap.get("movement_score") or 0
    dconst = snap.get("direction_consistency") or 0

    # --- Rule 300: STEADY_DROP confirmed (the gold-standard goal signal) ---
    if pat == "STEADY_DROP" and dt == "REAL_DROP" and mscore >= 70:
        add(300, "Steady Real Drop", "STRONG_GOAL", min(95, 70 + (mscore - 70) / 2),
            {"movement_score": mscore, "consistency": dconst})

    # --- Rule 301: SHARP_DROP confirmed -- market just learned something ---
    if pat == "SHARP_DROP" and dt == "REAL_DROP" and mscore >= 60:
        add(301, "Sharp Real Drop", "STRONG_GOAL", min(95, 65 + (mscore - 60) / 3),
            {"movement_score": mscore, "consistency": dconst})

    # --- Rule 302: FAKE_DROP trap -- looks like pressure, isn't ---
    if pat == "FAKE_DROP" or dt == "FAKE_DROP":
        add(302, "Fake Drop Trap", "TRAP", 75,
            {"movement_score": mscore})

    # --- Rule 303: CHAOTIC market -- noise, no information ---
    if pat == "CHAOTIC":
        add(303, "Chaotic Market", "NO_ENTRY", 60,
            {"consistency": dconst})

    # --- Rule 304: SPIKE_UP -- market is rejecting goal expectation ---
    if pat == "SPIKE_UP":
        add(304, "Market Rejection", "NO_GOAL", 70,
            {"movement_score": mscore})

    # --- Rule 305: FLAT in mid-late game -- market doesn't believe ---
    if pat == "FLAT" and minute and minute >= 55:
        add(305, "Flat Market Late", "NO_GOAL", 65,
            {"longest_stable": snap.get("longest_stable_duration", 0)})

    return signals


# ============================================================================
# SCANNER  --  the heart of the recorder
# ============================================================================
SCANNER_STATS = {
    "loops": 0, "errors": 0, "matches_seen": 0, "snapshots_saved": 0,
    "signals_fired": 0, "goals_recorded": 0, "last_run": None,
}


def _direction(prev: Optional[float], curr: Optional[float]) -> str:
    if prev is None or curr is None:
        return "INIT"
    if curr > prev + 0.005:
        return "UP"
    if curr < prev - 0.005:
        return "DOWN"
    return "FLAT"


def upsert_match(cur, parsed: dict, fb_data: Optional[dict]) -> Optional[int]:
    """Insert or update a match row. Returns internal match.id."""
    minute = (fb_data or {}).get("minute") or parsed.get("minute")
    score = (fb_data or {}).get("score") or parsed.get("score") or "0-0"
    score_home = (fb_data or {}).get("score_home", 0) or 0
    score_away = (fb_data or {}).get("score_away", 0) or 0

    cur.execute(
        "SELECT id, opening_next_goal, score_home, score_away FROM matches WHERE event_id = %s",
        (parsed["event_id"],),
    )
    row = cur.fetchone()
    if row:
        match_id, opening_next_goal, prev_sh, prev_sa = row
        new_opening = opening_next_goal
        if not opening_next_goal and parsed.get("next_goal"):
            new_opening = parsed["next_goal"]
        cur.execute("""
            UPDATE matches
               SET league = COALESCE(NULLIF(%s,''), league),
                   minute = COALESCE(%s, minute),
                   score_home = %s,
                   score_away = %s,
                   status = 'live',
                   opening_next_goal = COALESCE(opening_next_goal, %s),
                   last_updated_at = NOW()
             WHERE id = %s
        """, (parsed.get("league", ""), minute, score_home, score_away,
              new_opening, match_id))
        return match_id, prev_sh, prev_sa, opening_next_goal
    else:
        cur.execute("""
            INSERT INTO matches (event_id, league, home, away, minute,
                                 score_home, score_away, status, opening_next_goal)
            VALUES (%s,%s,%s,%s,%s,%s,%s,'live',%s)
            RETURNING id
        """, (parsed["event_id"], parsed.get("league", ""), parsed["home"],
              parsed["away"], minute, score_home, score_away,
              parsed.get("next_goal")))
        match_id = cur.fetchone()[0]
        return match_id, 0, 0, parsed.get("next_goal")


def save_snapshot(cur, match_id: int, parsed: dict, prev_snap: Optional[dict],
                  opening_ng: Optional[float], minute: Optional[int]) -> dict:
    """Insert one snapshot row, returning the snap dict (for rule eval).

    The movement analysis runs on the *previous* snapshots that already exist
    in the table (since this new row hasn't been inserted yet). We then store
    the resulting pattern/drop_type alongside this new row -- so each snapshot
    is tagged with the movement pattern that was active going into it."""
    over = parsed.get("next_goal")
    prev_over = prev_snap.get("next_goal") if prev_snap else None
    direction = _direction(prev_over, over)

    # held_seconds: roll forward if direction == FLAT
    held = 0
    if prev_snap and direction == "FLAT":
        held = (prev_snap.get("held_seconds") or 0) + SCAN_INTERVAL_SEC

    expected = get_expected_odd(EXPECTED_NEXT_GOAL, minute) if minute is not None else None
    pressure = calculate_pressure(over, expected)

    # ---- Movement analysis on existing snapshots (before this one is saved) ----
    movement = analyze_movement(cur, match_id)

    cur.execute("""
        INSERT INTO odds_snapshots
            (match_id, minute, home_ml, draw_ml, away_ml,
             next_goal, prev_next_goal, opening_next_goal, expected_next_goal,
             over_25, under_25, over_35, over_05_ht, over_15_ht,
             direction, held_seconds, pressure, is_live,
             movement_pattern, drop_type, movement_score, total_changes_count,
             direction_consistency, longest_stable_duration, total_delta,
             avg_time_between_changes)
        VALUES (%s,%s,%s,%s,%s,
                %s,%s,%s,%s,
                %s,%s,%s,%s,%s,
                %s,%s,%s,TRUE,
                %s,%s,%s,%s,%s,%s,%s,%s)
    """, (
        match_id, minute,
        parsed.get("home_ml"), parsed.get("draw_ml"), parsed.get("away_ml"),
        over, prev_over, opening_ng,
        round(expected, 3) if expected else None,
        parsed.get("over_25"), parsed.get("under_25"), parsed.get("over_35"),
        parsed.get("over_05_ht"), parsed.get("over_15_ht"),
        direction, held, round(pressure, 2),
        movement["movement_pattern"], movement["drop_type"],
        movement["movement_score"], movement["total_changes_count"],
        movement["direction_consistency"], movement["longest_stable_duration"],
        movement["total_delta"], movement["avg_time_between_changes"],
    ))

    return {
        "minute": minute,
        "next_goal": over,
        "draw_ml": parsed.get("draw_ml"),
        "home_ml": parsed.get("home_ml"),
        "away_ml": parsed.get("away_ml"),
        "over_05_ht": parsed.get("over_05_ht"),
        "over_15_ht": parsed.get("over_15_ht"),
        "direction": direction,
        "held_seconds": held,
        "pressure": round(pressure, 2),
        # Movement engine output exposed to rules
        "movement_pattern": movement["movement_pattern"],
        "drop_type": movement["drop_type"],
        "movement_score": movement["movement_score"],
        "direction_consistency": movement["direction_consistency"],
    }


def get_prev_snapshot(cur, match_id: int) -> Optional[dict]:
    cur.execute("""
        SELECT minute, next_goal, draw_ml, home_ml, away_ml,
               over_05_ht, over_15_ht, direction, held_seconds, pressure
          FROM odds_snapshots
         WHERE match_id = %s
         ORDER BY captured_at DESC
         LIMIT 1
    """, (match_id,))
    row = cur.fetchone()
    if not row:
        return None
    cols = ["minute", "next_goal", "draw_ml", "home_ml", "away_ml",
            "over_05_ht", "over_15_ht", "direction", "held_seconds", "pressure"]
    out = dict(zip(cols, row))
    for k in ("next_goal", "draw_ml", "home_ml", "away_ml",
              "over_05_ht", "over_15_ht", "pressure"):
        if out.get(k) is not None:
            out[k] = float(out[k])
    return out


def maybe_record_goal(cur, match_id: int, prev_sh: int, prev_sa: int,
                      new_sh: int, new_sa: int, minute: Optional[int]):
    """Detect goal -> record + capture historical odds windows."""
    if (new_sh + new_sa) <= (prev_sh + prev_sa):
        return
    score_before = f"{prev_sh}-{prev_sa}"
    score_after = f"{new_sh}-{new_sa}"

    # Pull windows of odds at -10s/-30s/-60s/-2m/-5m before now.
    windows = {"odds_10s": 10, "odds_30s": 30, "odds_60s": 60,
               "odds_120s": 120, "odds_300s": 300}
    payload = {}
    had = 0
    for col, secs in windows.items():
        cur.execute("""
            SELECT minute, next_goal, draw_ml, over_05_ht, over_15_ht,
                   pressure, direction, held_seconds, captured_at
              FROM odds_snapshots
             WHERE match_id = %s
               AND captured_at <= NOW() - (%s || ' seconds')::interval
             ORDER BY captured_at DESC
             LIMIT 1
        """, (match_id, secs))
        row = cur.fetchone()
        if row:
            had += 1
            payload[col] = {
                "minute": row[0],
                "next_goal": float(row[1]) if row[1] is not None else None,
                "draw_ml": float(row[2]) if row[2] is not None else None,
                "over_05_ht": float(row[3]) if row[3] is not None else None,
                "over_15_ht": float(row[4]) if row[4] is not None else None,
                "pressure": float(row[5]) if row[5] is not None else None,
                "direction": row[6],
                "held_seconds": row[7],
                "captured_at": row[8].isoformat() if row[8] else None,
            }
        else:
            payload[col] = None

    cur.execute("""
        INSERT INTO goals (match_id, minute, score_before, score_after,
                           had_snapshots, odds_10s, odds_30s, odds_60s,
                           odds_120s, odds_300s)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (match_id, minute, score_before, score_after, had,
          Json(payload["odds_10s"]), Json(payload["odds_30s"]),
          Json(payload["odds_60s"]), Json(payload["odds_120s"]),
          Json(payload["odds_300s"])))

    SCANNER_STATS["goals_recorded"] += 1
    log.info(f"GOAL match_id={match_id} {score_before} -> {score_after} @ {minute}'")


def fire_signals(cur, match_id: int, snap: dict, parsed: dict,
                 minute: int, opening: dict, signals: list):
    """Persist signals + open paper trades."""
    for sig in signals:
        cur.execute("""
            INSERT INTO signals (
                match_id, minute, rule_num, rule_name, verdict, confidence,
                pressure_score, next_goal_odd, over_05_ht_odd, over_15_ht_odd,
                opening_ng, opening_draw, opening_home, opening_away, details)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            RETURNING id
        """, (match_id, minute, sig["rule_num"], sig["rule_name"],
              sig["verdict"], sig["confidence"], snap.get("pressure"),
              snap.get("next_goal"), snap.get("over_05_ht"),
              snap.get("over_15_ht"),
              opening.get("next_goal"), opening.get("draw_ml"),
              opening.get("home_ml"), opening.get("away_ml"),
              Json(sig.get("details", {}))))
        signal_id = cur.fetchone()[0]

        # Bump rule stats
        cur.execute("""
            UPDATE rules SET total_signals = total_signals + 1
             WHERE rule_name = %s
        """, (sig["rule_name"],))

        # Open a paper trade only when verdict is GOAL or NO_GOAL (entry markets)
        if sig["verdict"] in ("GOAL", "NO_GOAL"):
            cur.execute("""
                INSERT INTO paper_trades
                    (signal_id, match_id, entry_odd, verdict, rule_name, minute_entry)
                VALUES (%s,%s,%s,%s,%s,%s)
            """, (signal_id, match_id, snap.get("next_goal"), sig["verdict"],
                  sig["rule_name"], minute))

        SCANNER_STATS["signals_fired"] += 1


def settle_paper_trades(cur):
    """Resolve PENDING trades that have aged out (>= 5 min after entry)."""
    cur.execute("""
        SELECT pt.id, pt.match_id, pt.opened_at, pt.entry_odd,
               pt.verdict, pt.minute_entry,
               m.score_home, m.score_away
          FROM paper_trades pt
          JOIN matches m ON m.id = pt.match_id
         WHERE pt.result = 'pending'
           AND pt.opened_at < NOW() - INTERVAL '5 minutes'
    """)
    pending = cur.fetchall()
    for tid, match_id, opened_at, entry, verdict, min_entry, sh, sa in pending:
        # Was a goal recorded within 5 minutes of opening?
        cur.execute("""
            SELECT COUNT(*) FROM goals
             WHERE match_id = %s
               AND goal_time BETWEEN %s AND %s + INTERVAL '5 minutes'
        """, (match_id, opened_at, opened_at))
        goal_in_window = cur.fetchone()[0] > 0

        success = (verdict == "GOAL" and goal_in_window) or \
                  (verdict == "NO_GOAL" and not goal_in_window)
        result = "success" if success else "miss"
        pl = (float(entry) - 1.0) if (success and entry) else (-1.0)

        cur.execute("""
            UPDATE paper_trades
               SET result = %s, settled_at = NOW(), profit_loss = %s
             WHERE id = %s
        """, (result, round(pl, 3), tid))

        # Update rule success rate
        cur.execute("""
            SELECT rule_name FROM paper_trades WHERE id = %s
        """, (tid,))
        rn = cur.fetchone()
        if rn and rn[0]:
            field = "success_count" if success else "fail_count"
            cur.execute(f"""
                UPDATE rules
                   SET {field} = {field} + 1,
                       success_rate = CASE
                           WHEN (success_count + fail_count + 1) = 0 THEN 0
                           ELSE ROUND(100.0 *
                               (success_count + (CASE WHEN %s THEN 1 ELSE 0 END))
                               / NULLIF(success_count + fail_count + 1, 0), 2)
                       END
                 WHERE rule_name = %s
            """, (success, rn[0]))


def update_goal_followups(cur):
    """Backfill goal_30s/60s/120s/300s flags on snapshots once goals exist."""
    # For each goal in last 30 min, mark snapshots that were within window.
    cur.execute("""
        SELECT id, match_id, goal_time FROM goals
         WHERE goal_time > NOW() - INTERVAL '30 minutes'
    """)
    goals = cur.fetchall()
    for _, match_id, goal_time in goals:
        for col, secs in (("goal_30s", 30), ("goal_60s", 60),
                          ("goal_120s", 120), ("goal_300s", 300)):
            cur.execute(f"""
                UPDATE odds_snapshots
                   SET {col} = TRUE
                 WHERE match_id = %s
                   AND captured_at BETWEEN %s - (%s || ' seconds')::interval AND %s
                   AND ({col} IS NULL OR {col} = FALSE)
            """, (match_id, goal_time, secs, goal_time))


def scan_once():
    """One full scan iteration."""
    SCANNER_STATS["loops"] += 1
    SCANNER_STATS["last_run"] = datetime.now(timezone.utc).isoformat()

    events = fetch_oddsapi_events()
    if not events:
        log.debug("No live events")
        return

    event_ids = []
    event_meta = {}
    for ev in events:
        eid = ev.get("id") or ev.get("eventId")
        if eid:
            event_ids.append(str(eid))
            event_meta[str(eid)] = ev

    odds_items = fetch_oddsapi_odds(event_ids)

    # Merge meta into odds rows so league/score/minute survive
    merged = []
    for row in odds_items:
        rid = str(row.get("id") or row.get("eventId") or "")
        meta = event_meta.get(rid, {})
        for k in ("league", "minute", "score", "home", "away"):
            if not row.get(k) and meta.get(k):
                row[k] = meta[k]
        merged.append(row)

    fb_idx = index_football_live(fetch_football_live())

    SCANNER_STATS["matches_seen"] = len(merged)

    with db_cursor() as cur:
        for row in merged:
            # Each row gets its own savepoint -- one row failing won't
            # cascade-abort the rest of the batch.
            cur.execute("SAVEPOINT row_sp")
            try:
                parsed = parse_match_odds(row)
                if not parsed:
                    cur.execute("RELEASE SAVEPOINT row_sp")
                    continue

                fb = lookup_football(fb_idx, parsed["home"], parsed["away"])
                # If Football API didn't have it, fall back to OddsAPI's minute/score
                if not fb:
                    sc = parsed.get("score") or "0-0"
                    try:
                        sh, sa = (int(x) for x in sc.split("-"))
                    except Exception:
                        sh, sa = 0, 0
                    fb = {"minute": parsed.get("minute"), "score": sc,
                          "score_home": sh, "score_away": sa}

                match_id, prev_sh, prev_sa, opening_ng = upsert_match(cur, parsed, fb)

                # Goal detection BEFORE saving the new snapshot
                maybe_record_goal(cur, match_id, prev_sh or 0, prev_sa or 0,
                                  fb["score_home"], fb["score_away"], fb["minute"])

                prev_snap = get_prev_snapshot(cur, match_id)
                snap = save_snapshot(cur, match_id, parsed, prev_snap,
                                     opening_ng, fb["minute"])
                SCANNER_STATS["snapshots_saved"] += 1

                # Rules
                opening = {
                    "next_goal": float(opening_ng) if opening_ng else None,
                    "draw_ml": parsed.get("draw_ml"),
                    "home_ml": parsed.get("home_ml"),
                    "away_ml": parsed.get("away_ml"),
                }
                signals = evaluate_rules(snap, prev_snap, opening)
                if signals and fb["minute"] is not None:
                    fire_signals(cur, match_id, snap, parsed, fb["minute"],
                                 opening, signals)

                cur.execute("RELEASE SAVEPOINT row_sp")

            except Exception as e:
                cur.execute("ROLLBACK TO SAVEPOINT row_sp")
                SCANNER_STATS["errors"] += 1
                # Include event_id + home/away so we can pinpoint problem rows
                eid = (parsed or {}).get("event_id", "?") if 'parsed' in dir() else "?"
                hm = (parsed or {}).get("home", "?") if 'parsed' in dir() else "?"
                aw = (parsed or {}).get("away", "?") if 'parsed' in dir() else "?"
                log.warning(f"row failed [{eid} {hm} vs {aw}]: {e}")

        try:
            settle_paper_trades(cur)
            update_goal_followups(cur)
        except Exception as e:
            log.warning(f"post-scan tasks failed: {e}")


def scanner_loop():
    log.info(f"Scanner thread started (interval={SCAN_INTERVAL_SEC}s)")
    while True:
        try:
            scan_once()
        except Exception as e:
            SCANNER_STATS["errors"] += 1
            log.error(f"scan_once crashed: {e}\n{traceback.format_exc()}")
        time.sleep(SCAN_INTERVAL_SEC)


# ============================================================================
# CLAUDE AI
# ============================================================================

# This is the master system prompt. It defines who Claude is, what PapaGoal's
# philosophy is, and how Claude must reason about football odds patterns.
# It is sent with EVERY call to Claude in this app.
PAPAGOAL_SYSTEM_PROMPT = """You are PapaGoal AI.

Your main job is to discover football goal/no-goal patterns based on the relationship between:
- match minute
- score
- Over/Under odds
- odds movement over time
- opening odds vs current odds
- expected odds for that minute
- goals that happened or did not happen afterward

Core idea:
PapaGoal does not analyze football teams.
PapaGoal analyzes whether the odds are too low or too high for the current minute.

The most important concept is:

Expected Odds vs Real Odds

Example:
If minute is 88 and the odds for another goal are 2.80-3.30,
this may be low for that minute.
Normally late-minute goal odds should be much higher.
If odds are still relatively low, the market may still believe a goal is coming.

Another example:
Opening:
Over 0.5 first half = 1.25
Over 1.5 first half = 2.10

Minute 25, score 0-0:
Over 0.5 first half is now 1.70-1.80

This may indicate accumulated pressure:
The market expected an early goal, no goal has happened yet, but the odds are still not high enough for the time remaining.
This can suggest a first-half goal is still likely.

Your job is to find these types of patterns from data.

Analyze:
- odds snapshots
- goals detected
- paper trades
- failed signals
- successful signals
- pattern_stats

Look for patterns such as:

1. Odds too low for the minute
   Minute 80-90 with Over/Next Goal odds 2.70-3.50.
   Did a goal happen within 2/5/10 minutes?

2. First-half accumulated pressure
   Opening Over 0.5 HT very low, like 1.20-1.35.
   By minute 20-30, current Over 0.5 HT rises to 1.60-1.90.
   Did a first-half goal happen before HT?

3. Over 1.5 HT pressure
   Opening Over 1.5 HT around 2.00-2.30.
   Minute 20-30 with 0 or 1 goals.
   Current odds still indicate the market expects another goal.
   Did another goal happen before HT?

4. Market rejection / no-goal patterns
   Odds jump too high too fast. Over odds rise sharply.
   Market stops believing in a goal.
   Did a goal NOT happen within 5/10 minutes?

5. Trap patterns
   Odds look attractive but goal does not happen.
   Find what was different: minute too late, pressure too low, odds movement too fast,
   reversal, score state, market frozen.

For every pattern, compare:
- minute range
- score state
- market name
- opening odds
- current odds
- expected odds at that minute
- difference between expected and current odds
- pressure score
- held seconds
- movement direction
- goals within 2 / 5 / 10 minutes
- no goal cases
- false positives
- paper trade result

Your output must focus on learning rules like:
"When current odds are lower than expected for this minute by X%, and this happens
 between minutes Y-Z, goals occurred within 5/10 minutes in N% of similar cases."

DO NOT make betting instructions.
DO NOT use the words: bet, enter, stake, guaranteed, sure goal.

Only produce: patterns discovered, rules to test, rules to narrow,
no-goal patterns, trap patterns, missing data warnings.

If there is not enough data, say: "Not enough data yet to confirm this pattern."

Important:
- Focus mainly on Over/Under goal odds compared to the minute.
- Learn from both goals and no-goal cases.
- New rules are hypotheses only.
- Do not invent statistics if sample size is too small.
- Reply in Hebrew inside all JSON text fields."""


def claude_call(prompt: str, system: str = "", max_tokens: int = 1500) -> str:
    """Generic Claude wrapper. Defaults to PapaGoal's master system prompt."""
    if not _anthropic_client:
        return ""
    try:
        msg = _anthropic_client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=max_tokens,
            system=system or PAPAGOAL_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": prompt}],
        )
        return msg.content[0].text if msg.content else ""
    except Exception as e:
        log.warning(f"claude_call failed: {e}")
        return ""


def claude_review_signal(signal_row: dict) -> str:
    """One-off review of a single live signal. Outputs short Hebrew prose
    consistent with PapaGoal's philosophy (no betting language)."""
    if not _anthropic_client:
        return ""

    # Compute the gap vs the expected curve so Claude can reason about it
    minute = signal_row.get("minute") or 0
    over   = signal_row.get("next_goal_odd")
    o05    = signal_row.get("over_05_ht_odd")
    o15    = signal_row.get("over_15_ht_odd")
    exp_25  = get_expected_odd(EXPECTED_NEXT_GOAL, minute)
    exp_05  = get_expected_odd(EXPECTED_OVER05_HT, minute) if minute <= 45 else None
    exp_15  = get_expected_odd(EXPECTED_OVER15_HT, minute) if minute <= 45 else None

    def _gap(actual, expected):
        if not actual or not expected:
            return "n/a"
        return f"{round((float(expected) - float(actual)) / float(expected) * 100, 1)}%"

    prompt = f"""A live signal just fired. Analyze it through the PapaGoal lens
(expected odds vs real odds for this minute). Reply in 3-5 short Hebrew sentences.

MATCH: {signal_row.get('home')} vs {signal_row.get('away')}
MINUTE: {minute}
RULE: {signal_row.get('rule_name')} -> {signal_row.get('verdict')}
PRESSURE SCORE: {signal_row.get('pressure_score')}
RULE CONFIDENCE: {signal_row.get('confidence')}%

ODDS SNAPSHOT:
- Live Next Goal:    {over}    (expected at this minute: {exp_25}, gap: {_gap(over, exp_25)})
- Over 0.5 HT:      {o05}     (expected: {exp_05}, gap: {_gap(o05, exp_05)})
- Over 1.5 HT:      {o15}     (expected: {exp_15}, gap: {_gap(o15, exp_15)})
- Opening Next Goal: {signal_row.get('opening_ng')}
- Opening Draw:     {signal_row.get('opening_draw')}

YOUR TASK:
Tell me, in Hebrew, what the market is saying here. Specifically:
1. Does the gap between expected and real odds support the rule's verdict?
2. Is the minute-range appropriate for this kind of signal?
3. What's the main risk factor that could make this a trap?
4. Confidence assessment: low / medium / high (and why).

Do NOT use the words: bet, stake, guaranteed, sure goal, "go for it".
Talk in terms of "the market is signaling X" / "the pattern suggests Y" / "trap risk: Z"."""

    return claude_call(prompt, max_tokens=600)


def _gather_pattern_data() -> dict:
    """Pull a comprehensive analysis snapshot from the DB for Claude to learn from.

    Returns dict with: goals (last 7 days), failed_signals, successful_signals,
    paper_trade_summary, no_goal_examples (snapshots that LOOKED like signals
    but didn't lead to goals)."""
    out = {
        "goals": [],
        "successful_signals": [],
        "failed_signals": [],
        "paper_trade_summary": {},
        "no_goal_examples": [],
        "rule_performance": [],
    }
    try:
        with db_cursor(dict_rows=True) as cur:
            # Goals + the odds snapshots that preceded them
            cur.execute("""
                SELECT g.minute, g.score_before, g.score_after, g.goal_time,
                       g.odds_30s, g.odds_60s, g.odds_120s, g.odds_300s,
                       m.league
                  FROM goals g
                  JOIN matches m ON m.id = g.match_id
                 WHERE g.goal_time > NOW() - INTERVAL '7 days'
                 ORDER BY g.goal_time DESC
                 LIMIT 100
            """)
            out["goals"] = [dict(r) for r in cur.fetchall()]

            # Successful paper trades (signals that hit)
            cur.execute("""
                SELECT pt.minute_entry, pt.entry_odd, pt.verdict, pt.rule_name,
                       pt.profit_loss, s.next_goal_odd, s.over_05_ht_odd, s.over_15_ht_odd,
                       s.opening_ng, s.pressure_score, m.league
                  FROM paper_trades pt
                  JOIN signals s ON s.id = pt.signal_id
                  JOIN matches m ON m.id = pt.match_id
                 WHERE pt.result = 'success'
                   AND pt.opened_at > NOW() - INTERVAL '7 days'
                 ORDER BY pt.opened_at DESC
                 LIMIT 50
            """)
            out["successful_signals"] = [dict(r) for r in cur.fetchall()]

            # Failed paper trades (signals that missed)
            cur.execute("""
                SELECT pt.minute_entry, pt.entry_odd, pt.verdict, pt.rule_name,
                       pt.profit_loss, s.next_goal_odd, s.over_05_ht_odd, s.over_15_ht_odd,
                       s.opening_ng, s.pressure_score, m.league
                  FROM paper_trades pt
                  JOIN signals s ON s.id = pt.signal_id
                  JOIN matches m ON m.id = pt.match_id
                 WHERE pt.result = 'miss'
                   AND pt.opened_at > NOW() - INTERVAL '7 days'
                 ORDER BY pt.opened_at DESC
                 LIMIT 50
            """)
            out["failed_signals"] = [dict(r) for r in cur.fetchall()]

            # Aggregated rule performance
            cur.execute("""
                SELECT rule_name, action, total_signals, success_count,
                       fail_count, success_rate
                  FROM rules
                 WHERE total_signals > 0
                 ORDER BY total_signals DESC
            """)
            out["rule_performance"] = [dict(r) for r in cur.fetchall()]

            # No-goal examples: snapshots that had high pressure but no goal
            # within the next 5 minutes. These are "almost-traps".
            cur.execute("""
                SELECT s.minute, s.next_goal, s.over_05_ht, s.over_15_ht,
                       s.opening_next_goal, s.expected_next_goal, s.pressure,
                       s.direction, s.held_seconds, s.captured_at, m.league
                  FROM odds_snapshots s
                  JOIN matches m ON m.id = s.match_id
                 WHERE s.pressure >= 50
                   AND s.goal_300s = FALSE
                   AND s.captured_at > NOW() - INTERVAL '7 days'
                 ORDER BY s.captured_at DESC
                 LIMIT 50
            """)
            out["no_goal_examples"] = [dict(r) for r in cur.fetchall()]

            # Paper trade summary
            cur.execute("""
                SELECT result, COUNT(*) AS n,
                       COALESCE(SUM(profit_loss), 0) AS pl
                  FROM paper_trades
                 WHERE opened_at > NOW() - INTERVAL '7 days'
                 GROUP BY result
            """)
            for r in cur.fetchall():
                out["paper_trade_summary"][r["result"]] = {
                    "count": r["n"], "pl": float(r["pl"] or 0)
                }
    except Exception as e:
        log.warning(f"_gather_pattern_data failed: {e}")
    return out


def claude_suggest_rules():
    """Run a deep pattern-discovery analysis. Asks Claude to follow PapaGoal's
    full system prompt and return structured JSON of patterns + new rules."""
    if not _anthropic_client:
        return None

    data = _gather_pattern_data()

    if not data["goals"] and not data["successful_signals"] and not data["failed_signals"]:
        log.info("claude_suggest_rules: not enough data yet")
        return None

    # Trim to keep prompt size reasonable
    sample = {
        "goals_count": len(data["goals"]),
        "goals_sample": data["goals"][:30],
        "successful_signals_count": len(data["successful_signals"]),
        "successful_signals_sample": data["successful_signals"][:20],
        "failed_signals_count": len(data["failed_signals"]),
        "failed_signals_sample": data["failed_signals"][:20],
        "no_goal_examples_count": len(data["no_goal_examples"]),
        "no_goal_examples_sample": data["no_goal_examples"][:15],
        "rule_performance": data["rule_performance"],
        "paper_trade_summary": data["paper_trade_summary"],
    }
    sample_json = json.dumps(sample, default=str, indent=1)[:14000]

    prompt = f"""Analyze the following PapaGoal data dump and produce structured JSON
with discovered patterns, new rules to test, and improvements.

The dataset includes:
- {len(data['goals'])} goals from the last 7 days, each with odds snapshots at -30s / -60s / -2m / -5m before the goal
- {len(data['successful_signals'])} signals that successfully predicted (paper-trade hit)
- {len(data['failed_signals'])} signals that failed (paper-trade miss)
- {len(data['no_goal_examples'])} high-pressure snapshots that did NOT lead to a goal within 5 minutes
- Performance breakdown of every active rule

DATA:
{sample_json}

Reply with ONLY a JSON object in this EXACT schema. No prose, no markdown fences:

{{
  "summary": "Hebrew summary of what was learned",
  "goal_patterns": [
    {{
      "pattern_name": "",
      "market": "",
      "minute_range": "",
      "score_state": "",
      "opening_odds_range": "",
      "current_odds_range": "",
      "expected_odds_range": "",
      "expected_vs_real_gap": "",
      "pressure_condition": "",
      "movement_condition": "",
      "sample_size": 0,
      "goal_within_2m_rate": 0,
      "goal_within_5m_rate": 0,
      "goal_within_10m_rate": 0,
      "confidence": "low / medium / high",
      "reason": ""
    }}
  ],
  "no_goal_patterns": [
    {{
      "pattern_name": "",
      "market": "",
      "minute_range": "",
      "odds_condition": "",
      "movement_condition": "",
      "sample_size": 0,
      "no_goal_rate_10m": 0,
      "confidence": "low / medium / high",
      "reason": ""
    }}
  ],
  "trap_patterns": [
    {{
      "pattern_name": "",
      "market": "",
      "minute_range": "",
      "why_it_looked_good": "",
      "why_it_failed": "",
      "sample_size": 0,
      "trap_rate": 0,
      "confidence": "low / medium / high"
    }}
  ],
  "new_rules_to_test": [
    {{
      "rule_name": "",
      "signal_type": "goal / no_goal / trap",
      "market": "",
      "minute_min": 0,
      "minute_max": 0,
      "score_condition": "",
      "opening_odds_min": 0,
      "opening_odds_max": 0,
      "current_odds_min": 0,
      "current_odds_max": 0,
      "expected_gap_min": 0,
      "pressure_min": 0,
      "held_seconds_min": 0,
      "movement_condition": "",
      "reason": "",
      "confidence_estimate": 0
    }}
  ],
  "rules_to_improve": [
    {{
      "existing_rule": "",
      "problem": "",
      "suggested_change": "",
      "reason": ""
    }}
  ],
  "missing_data": [
    {{
      "issue": "",
      "why_it_matters": "",
      "fix": ""
    }}
  ]
}}

All Hebrew text fields must be in Hebrew. All numeric fields must be numbers, not strings.
If a section has no findings due to insufficient data, return an empty array for it
and add an entry under missing_data explaining what's needed."""

    text = claude_call(prompt, max_tokens=4000)

    # Persist for the /insights page
    try:
        with db_cursor() as cur:
            cur.execute("""
                INSERT INTO ai_insights (insight_type, content, goals_analyzed)
                VALUES ('papagoal_analysis', %s, %s)
            """, (text, len(data["goals"])))
    except Exception as e:
        log.warning(f"failed to persist insight: {e}")
    return text


# ============================================================================
# FLASK APP  +  TEMPLATES
# ============================================================================
app = Flask(__name__)


# ----- shared layout -----
BASE_CSS = """
:root{
  --bg:#0a0e14; --bg2:#0f141c; --card:#141a23; --card2:#1a2230;
  --line:#222b3a; --txt:#e6edf3; --mute:#7d8a9c; --dim:#4a5566;
  --accent:#ffb800; --hot:#ff5c4d; --goal:#00ff9d; --trap:#ff3860;
  --info:#4ea1ff; --noentry:#a888ff;
}
*{box-sizing:border-box}
html,body{margin:0;padding:0;background:var(--bg);color:var(--txt);
  font-family:'JetBrains Mono','SF Mono',Menlo,monospace;font-size:13px}
body{background:radial-gradient(ellipse at top,#10161f 0%,#0a0e14 60%) fixed}
a{color:var(--info);text-decoration:none}
a:hover{color:#fff}
.wrap{max-width:1500px;margin:0 auto;padding:18px}
.nav{display:flex;gap:2px;background:var(--card);border:1px solid var(--line);
  border-radius:10px;padding:5px;margin-bottom:18px;flex-wrap:wrap}
.nav a{padding:10px 14px;color:var(--mute);border-radius:6px;
  font-weight:600;letter-spacing:.04em;text-transform:uppercase;font-size:11px}
.nav a:hover{color:var(--txt);background:var(--card2)}
.nav a.active{color:var(--bg);background:var(--accent)}
.brand{font-family:'Bricolage Grotesque',Georgia,serif;font-weight:700;
  font-size:22px;letter-spacing:-.02em;color:var(--accent);
  padding:8px 14px;margin-right:8px}
.brand span{color:var(--txt);font-weight:400;font-size:14px;margin-left:8px}
.row{display:grid;gap:14px}
.row.cols-3{grid-template-columns:repeat(3,1fr)}
.row.cols-4{grid-template-columns:repeat(4,1fr)}
.row.cols-2{grid-template-columns:repeat(2,1fr)}
@media(max-width:900px){.row.cols-3,.row.cols-4{grid-template-columns:1fr}}
.card{background:var(--card);border:1px solid var(--line);border-radius:12px;
  padding:18px}
.card h2{font-family:'Bricolage Grotesque',Georgia,serif;margin:0 0 12px;
  font-size:14px;letter-spacing:.08em;text-transform:uppercase;color:var(--mute);
  font-weight:600;display:flex;justify-content:space-between;align-items:baseline}
.kpi{font-size:32px;font-weight:700;color:var(--accent);
  font-family:'Bricolage Grotesque',Georgia,serif;line-height:1}
.kpi small{font-size:13px;color:var(--mute);font-family:inherit;
  margin-left:6px;font-weight:400}
table{width:100%;border-collapse:collapse;font-size:12px}
th{text-align:left;padding:8px 10px;color:var(--mute);font-weight:600;
  letter-spacing:.05em;text-transform:uppercase;font-size:10px;
  border-bottom:1px solid var(--line)}
td{padding:9px 10px;border-bottom:1px solid var(--line)}
tr:hover td{background:var(--card2)}
.tag{display:inline-block;padding:3px 8px;border-radius:4px;font-size:10px;
  font-weight:700;letter-spacing:.05em;text-transform:uppercase}
.tag.GOAL{background:rgba(0,255,157,.12);color:var(--goal)}
.tag.STRONG_GOAL{background:var(--goal);color:#000;box-shadow:0 0 12px rgba(0,255,157,.4)}
.tag.WEAK_GOAL{background:rgba(0,255,157,.18);color:var(--goal);border:1px dashed var(--goal)}
.tag.NO_GOAL{background:rgba(255,56,96,.15);color:var(--trap)}
.tag.TRAP{background:rgba(255,56,96,.15);color:var(--trap)}
.tag.DRAW_UNDER{background:rgba(168,136,255,.18);color:var(--noentry)}
.tag.NO_ENTRY{background:rgba(125,138,156,.18);color:var(--mute)}
.tag.HOT{background:var(--hot);color:#fff}
.tag.LIVE{background:var(--goal);color:#000}
.tag.PATTERN{background:rgba(78,161,255,.15);color:var(--info);font-size:9px}
.bar{height:8px;background:var(--card2);border-radius:4px;overflow:hidden;
  margin-top:6px;border:1px solid var(--line)}
.bar>span{display:block;height:100%;background:linear-gradient(90deg,var(--info),var(--accent),var(--hot));
  transition:width .4s}
.muted{color:var(--mute)}
.dim{color:var(--dim)}
.up{color:var(--trap)}      /* odds going up = goal further away = bad for over */
.down{color:var(--goal)}    /* odds going down = pressure */
.flat{color:var(--mute)}
.btn{display:inline-block;padding:7px 14px;border:1px solid var(--line);
  background:var(--card2);color:var(--txt);border-radius:6px;cursor:pointer;
  font-family:inherit;font-size:11px;font-weight:600;text-transform:uppercase;
  letter-spacing:.05em}
.btn:hover{border-color:var(--accent);color:var(--accent)}
.btn.primary{background:var(--accent);color:var(--bg);border-color:var(--accent)}
.btn.primary:hover{background:#fff}
.right{text-align:right}
.center{text-align:center}
.empty{padding:40px;text-align:center;color:var(--mute)}
.statline{display:flex;gap:18px;flex-wrap:wrap;font-size:11px;
  color:var(--mute);padding:8px 14px;background:var(--card);
  border:1px solid var(--line);border-radius:8px;margin-bottom:14px}
.statline b{color:var(--accent);margin-right:4px}
.live-dot{display:inline-block;width:8px;height:8px;border-radius:50%;
  background:var(--goal);margin-right:6px;animation:pulse 1.5s infinite}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.4}}
"""

def layout(title: str, active: str, body: str, footer: str = "") -> str:
    nav_items = [
        ("/",            "live",      "Live"),
        ("/goals",       "goals",     "Goals"),
        ("/simulation",  "sim",       "Simulation"),
        ("/signals",     "signals",   "Signals"),
        ("/rules",       "rules",     "Rules"),
        ("/analytics",   "analytics", "Analytics"),
        ("/insights",    "insights",  "AI Insights"),
    ]
    nav_html = "".join(
        f'<a href="{href}" class="{"active" if k==active else ""}">{label}</a>'
        for href, k, label in nav_items
    )
    return f"""<!doctype html>
<html lang="he" dir="ltr">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>PapaGoal · {title}</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Bricolage+Grotesque:wght@500;600;700&family=JetBrains+Mono:wght@400;500;600;700&display=swap" rel="stylesheet">
<style>{BASE_CSS}</style>
</head>
<body>
<div class="wrap">
  <nav class="nav">
    <span class="brand">PapaGoal<span>Market Recorder · v5</span></span>
    {nav_html}
  </nav>
  {body}
  {footer}
</div>
</body>
</html>"""


# ---------- helpers for templates ----------
def _direction_class(d):
    return {"UP": "up", "DOWN": "down", "FLAT": "flat"}.get(d, "muted")

def _direction_arrow(d):
    return {"UP": "▲", "DOWN": "▼", "FLAT": "—"}.get(d, "·")


# ============================================================================
# PAGE 1 -- LIVE
# ============================================================================
@app.route("/")
def page_live():
    rows = []
    hot = []
    try:
        with db_cursor(dict_rows=True) as cur:
            cur.execute("""
                SELECT m.id, m.home, m.away, m.league, m.minute,
                       m.score_home, m.score_away, m.opening_next_goal,
                       s.next_goal, s.draw_ml, s.over_05_ht, s.over_15_ht,
                       s.direction, s.held_seconds, s.pressure,
                       s.expected_next_goal, s.captured_at,
                       s.movement_pattern, s.drop_type, s.movement_score
                  FROM matches m
             LEFT JOIN LATERAL (
                       SELECT *
                         FROM odds_snapshots
                        WHERE match_id = m.id
                        ORDER BY captured_at DESC
                        LIMIT 1
                       ) s ON TRUE
                 WHERE m.last_updated_at > NOW() - INTERVAL '5 minutes'
                 ORDER BY s.pressure DESC NULLS LAST, m.last_updated_at DESC
                 LIMIT 80
            """)
            rows = cur.fetchall()
            cur.execute("""
                SELECT s.*, m.home, m.away
                  FROM signals s
                  JOIN matches m ON m.id = s.match_id
                 WHERE s.triggered_at > NOW() - INTERVAL '15 minutes'
                   AND s.confidence >= 65
                 ORDER BY s.confidence DESC, s.triggered_at DESC
                 LIMIT 12
            """)
            hot = cur.fetchall()
    except Exception as e:
        log.warning(f"page_live db error: {e}")

    # Hot signals
    if hot:
        hot_rows = "".join(f"""
        <tr>
          <td><span class="tag HOT">HOT</span></td>
          <td><b>{h['home']}</b> vs <b>{h['away']}</b></td>
          <td class="muted">{h['minute']}'</td>
          <td>{h['rule_name']}</td>
          <td><span class="tag {h['verdict']}">{h['verdict']}</span></td>
          <td class="right"><b>{h['confidence']}%</b></td>
          <td class="right">{h['next_goal_odd'] or '-'}</td>
          <td class="right">{round(float(h['pressure_score']),1) if h['pressure_score'] else '-'}%</td>
          <td><a class="btn" href="/signals?id={h['id']}">analyze</a></td>
        </tr>""" for h in hot)
    else:
        hot_rows = '<tr><td colspan="9" class="empty">No hot signals in the last 15 minutes</td></tr>'

    # Live matches
    if rows:
        match_rows = []
        for r in rows:
            over = r['next_goal']
            exp = r['expected_next_goal']
            press = float(r['pressure'] or 0)
            press_pct = max(0, min(100, press))
            score = f"{r['score_home']}-{r['score_away']}"
            dir_html = f'<span class="{_direction_class(r["direction"])}">{_direction_arrow(r["direction"])} {r["direction"] or "-"}</span>'
            held = r['held_seconds'] or 0

            # Movement column
            mp = r.get('movement_pattern') or 'UNKNOWN'
            dt = r.get('drop_type') or 'NONE'
            ms = r.get('movement_score') or 0
            # Color the pattern by goodness
            pattern_color = {
                'STEADY_DROP': 'down',  # green
                'SHARP_DROP':  'down',
                'FAKE_DROP':   'up',    # red
                'SPIKE_UP':    'up',
                'CHAOTIC':     'up',
                'FLAT':        'muted',
                'UNKNOWN':     'dim',
            }.get(mp, 'muted')
            # Bold REAL_DROP since it's the gold-standard signal
            dt_html = f'<b style="color:var(--goal)">{dt}</b>' if dt == 'REAL_DROP' else \
                      f'<span class="muted">{dt}</span>'
            movement_html = (f'<span class="tag PATTERN {pattern_color}">{mp.replace("_", " ")}</span>'
                             f'<br>{dt_html} <span class="muted">· {ms}</span>')

            match_rows.append(f"""
            <tr>
              <td><span class="live-dot"></span><b>{r['home']}</b><br><b>{r['away']}</b></td>
              <td class="muted">{(r['league'] or '')[:24]}</td>
              <td class="center"><b>{r['minute'] or '-'}'</b><br><span class="muted">{score}</span></td>
              <td class="right"><b>{over or '-'}</b><br><span class="muted">exp {round(float(exp),2) if exp else '-'}</span></td>
              <td class="right">{r['opening_next_goal'] or '-'}</td>
              <td class="right">{r['draw_ml'] or '-'}</td>
              <td class="right">{r['over_05_ht'] or '-'}</td>
              <td class="right">{r['over_15_ht'] or '-'}</td>
              <td>{dir_html}<br><span class="muted">held {held}s</span></td>
              <td style="min-width:130px">{movement_html}</td>
              <td style="min-width:140px">
                <b class="{'up' if press>60 else ('down' if press>30 else 'muted')}">{round(press,1)}%</b>
                <div class="bar"><span style="width:{press_pct}%"></span></div>
              </td>
            </tr>""")
        match_html = "".join(match_rows)
    else:
        match_html = '<tr><td colspan="11" class="empty">Scanner is warming up… (waiting for first live odds)</td></tr>'

    body = f"""
    <div class="statline">
      <span><b>{SCANNER_STATS['matches_seen']}</b>matches seen</span>
      <span><b>{SCANNER_STATS['snapshots_saved']}</b>snapshots</span>
      <span><b>{SCANNER_STATS['signals_fired']}</b>signals</span>
      <span><b>{SCANNER_STATS['goals_recorded']}</b>goals recorded</span>
      <span><b>{SCANNER_STATS['loops']}</b>loops</span>
      <span><b>{SCANNER_STATS['errors']}</b>errors</span>
      <span class="dim">last run: {SCANNER_STATS['last_run'] or 'pending'}</span>
    </div>

    <div class="card" style="margin-bottom:14px">
      <h2>🔥 Hot Signals <span class="muted">last 15 min · confidence ≥ 65%</span></h2>
      <table>
        <tr><th></th><th>Match</th><th>Min</th><th>Rule</th><th>Verdict</th>
            <th class="right">Conf.</th><th class="right">Next Goal</th>
            <th class="right">Pressure</th><th></th></tr>
        {hot_rows}
      </table>
    </div>

    <div class="card">
      <h2>📡 Live Matches <span class="muted">sorted by pressure</span></h2>
      <table>
        <tr><th>Match</th><th>League</th><th class="center">Min · Score</th>
            <th class="right">Next Goal</th><th class="right">Open</th>
            <th class="right">Draw</th><th class="right">Over 0.5 HT</th>
            <th class="right">Over 1.5 HT</th>
            <th>Direction</th><th>Movement</th><th>Pressure</th></tr>
        {match_html}
      </table>
    </div>

    <script>setTimeout(()=>location.reload(), 30000);</script>
    """
    return layout("Live", "live", body)


# ============================================================================
# PAGE 2 -- GOALS
# ============================================================================
@app.route("/goals")
def page_goals():
    goals = []
    try:
        with db_cursor(dict_rows=True) as cur:
            cur.execute("""
                SELECT g.*, m.home, m.away, m.league
                  FROM goals g
                  JOIN matches m ON m.id = g.match_id
                 ORDER BY g.goal_time DESC
                 LIMIT 80
            """)
            goals = cur.fetchall()
    except Exception as e:
        log.warning(f"page_goals db: {e}")

    rows_html = []
    for g in goals:
        def fmt(window):
            d = g.get(window)
            if not d:
                return "<td colspan='3' class='dim center'>—</td>"
            ov = d.get("next_goal")
            press = d.get("pressure")
            direction = d.get("direction") or "-"
            return (f"<td class='right'>{ov if ov else '-'}</td>"
                    f"<td class='right muted'>{round(press,1) if press else '-'}%</td>"
                    f"<td class='{_direction_class(direction)}'>{_direction_arrow(direction)}</td>")

        rows_html.append(f"""
        <tr>
          <td class="muted">{g['goal_time'].strftime('%H:%M:%S') if g.get('goal_time') else ''}</td>
          <td><b>{g['home']}</b> vs <b>{g['away']}</b><br>
              <span class="muted">{(g['league'] or '')[:30]}</span></td>
          <td class="center"><b>{g['minute']}'</b><br>
              <span class="muted">{g['score_before']} → {g['score_after']}</span></td>
          {fmt('odds_30s')}
          {fmt('odds_60s')}
          {fmt('odds_120s')}
          {fmt('odds_300s')}
          <td class="muted right">{g['had_snapshots']}/4</td>
        </tr>""")

    table = "".join(rows_html) or '<tr><td colspan="14" class="empty">No goals recorded yet</td></tr>'
    body = f"""
    <div class="card">
      <h2>⚽ Goals <span class="muted">odds at -30s / -60s / -2m / -5m before each goal</span></h2>
      <table>
        <tr>
          <th>Time</th><th>Match</th><th class="center">Min · Score</th>
          <th class="right" colspan="3">-30s</th>
          <th class="right" colspan="3">-60s</th>
          <th class="right" colspan="3">-2m</th>
          <th class="right" colspan="3">-5m</th>
          <th class="right">Coverage</th>
        </tr>
        <tr style="font-size:9px;color:var(--dim)">
          <th></th><th></th><th></th>
          <th class="right">Over</th><th class="right">Press</th><th class="center">Dir</th>
          <th class="right">Over</th><th class="right">Press</th><th class="center">Dir</th>
          <th class="right">Over</th><th class="right">Press</th><th class="center">Dir</th>
          <th class="right">Over</th><th class="right">Press</th><th class="center">Dir</th>
          <th></th>
        </tr>
        {table}
      </table>
    </div>"""
    return layout("Goals", "goals", body)


# ============================================================================
# PAGE 3 -- SIMULATION (Paper Trades)
# ============================================================================
@app.route("/simulation")
def page_simulation():
    trades = []
    summary = {"pending": 0, "success": 0, "miss": 0, "pl": 0.0}
    try:
        with db_cursor(dict_rows=True) as cur:
            cur.execute("""
                SELECT pt.*, m.home, m.away
                  FROM paper_trades pt
                  JOIN matches m ON m.id = pt.match_id
                 ORDER BY pt.opened_at DESC
                 LIMIT 200
            """)
            trades = cur.fetchall()
            cur.execute("""
                SELECT result, COUNT(*), COALESCE(SUM(profit_loss),0)
                  FROM paper_trades
                 GROUP BY result
            """)
            for r, c, pl in cur.fetchall():
                summary[r] = c
                if r != "pending":
                    summary["pl"] += float(pl or 0)
    except Exception as e:
        log.warning(f"page_sim db: {e}")

    total_settled = summary["success"] + summary["miss"]
    win_rate = round(100 * summary["success"] / total_settled, 1) if total_settled else 0.0

    trade_rows = "".join(f"""
      <tr>
        <td class="muted">{(t['opened_at'].strftime('%H:%M:%S') if t.get('opened_at') else '')}</td>
        <td><b>{t['home']}</b> vs <b>{t['away']}</b></td>
        <td class="center">{t['minute_entry'] or '-'}'</td>
        <td>{t['rule_name']}</td>
        <td><span class="tag {t['verdict']}">{t['verdict']}</span></td>
        <td class="right">{t['entry_odd'] or '-'}</td>
        <td><span class="tag {'GOAL' if t['result']=='success' else ('NO_GOAL' if t['result']=='miss' else 'NO_ENTRY')}">{(t['result'] or 'pending').upper()}</span></td>
        <td class="right {'down' if (t['profit_loss'] or 0) > 0 else ('up' if (t['profit_loss'] or 0) < 0 else 'muted')}">
            {('+' if (t['profit_loss'] or 0)>0 else '')}{t['profit_loss'] or 0}
        </td>
      </tr>""" for t in trades) or '<tr><td colspan="8" class="empty">No paper trades yet</td></tr>'

    body = f"""
    <div class="row cols-4" style="margin-bottom:14px">
      <div class="card"><h2>Pending</h2><div class="kpi">{summary['pending']}</div></div>
      <div class="card"><h2>Success</h2><div class="kpi" style="color:var(--goal)">{summary['success']}</div></div>
      <div class="card"><h2>Miss</h2><div class="kpi" style="color:var(--trap)">{summary['miss']}</div></div>
      <div class="card"><h2>Win rate · P/L</h2>
        <div class="kpi">{win_rate}<small>%</small></div>
        <div class="muted" style="margin-top:6px">P/L: <b style="color:{'var(--goal)' if summary['pl']>=0 else 'var(--trap)'}">{round(summary['pl'],2)}u</b></div>
      </div>
    </div>
    <div class="card">
      <h2>📈 Paper Trades</h2>
      <table>
        <tr><th>Opened</th><th>Match</th><th class="center">Min</th><th>Rule</th>
            <th>Verdict</th><th class="right">Entry odd</th><th>Result</th>
            <th class="right">P/L</th></tr>
        {trade_rows}
      </table>
    </div>"""
    return layout("Simulation", "sim", body)


# ============================================================================
# PAGE 4 -- SIGNALS
# ============================================================================
@app.route("/signals")
def page_signals():
    sig_id = request.args.get("id", type=int)
    detail_html = ""
    rows = []
    try:
        with db_cursor(dict_rows=True) as cur:
            cur.execute("""
                SELECT s.*, m.home, m.away, m.league
                  FROM signals s
                  JOIN matches m ON m.id = s.match_id
                 WHERE s.triggered_at > NOW() - INTERVAL '3 hours'
                 ORDER BY s.triggered_at DESC
                 LIMIT 200
            """)
            rows = cur.fetchall()

            if sig_id:
                cur.execute("""
                    SELECT s.*, m.home, m.away, m.league
                      FROM signals s
                      JOIN matches m ON m.id = s.match_id
                     WHERE s.id = %s
                """, (sig_id,))
                row = cur.fetchone()
                if row:
                    review = claude_review_signal(row) if _anthropic_client else "(Anthropic key not configured)"
                    detail_html = f"""
                    <div class="card" style="margin-bottom:14px;border-color:var(--accent)">
                      <h2>🤖 Claude Review · signal #{sig_id}</h2>
                      <p><b>{row['home']}</b> vs <b>{row['away']}</b> — minute {row['minute']}'<br>
                         Rule: <b>{row['rule_name']}</b> ({row['verdict']}, conf {row['confidence']}%)</p>
                      <pre style="white-space:pre-wrap;background:var(--bg2);padding:14px;border-radius:8px;color:var(--accent);font-family:inherit">{review}</pre>
                    </div>"""
    except Exception as e:
        log.warning(f"page_signals db: {e}")

    sig_rows = "".join(f"""
      <tr>
        <td class="muted">{r['triggered_at'].strftime('%H:%M:%S') if r.get('triggered_at') else ''}</td>
        <td><b>{r['home']}</b> vs <b>{r['away']}</b></td>
        <td class="center">{r['minute'] or '-'}'</td>
        <td>{r['rule_name']}</td>
        <td><span class="tag {r['verdict']}">{r['verdict']}</span></td>
        <td class="right"><b>{r['confidence']}%</b></td>
        <td class="right">{r['next_goal_odd'] or '-'}</td>
        <td class="right">{round(float(r['pressure_score']),1) if r['pressure_score'] else '-'}%</td>
        <td><a class="btn" href="/signals?id={r['id']}">Claude</a></td>
      </tr>""" for r in rows) or '<tr><td colspan="9" class="empty">No signals in the last 3 hours</td></tr>'

    body = f"""
    {detail_html}
    <div class="card">
      <h2>🔥 Signals <span class="muted">last 3 hours</span></h2>
      <table>
        <tr><th>Time</th><th>Match</th><th class="center">Min</th>
            <th>Rule</th><th>Verdict</th><th class="right">Conf.</th>
            <th class="right">Next Goal</th><th class="right">Pressure</th><th></th></tr>
        {sig_rows}
      </table>
    </div>"""
    return layout("Signals", "signals", body)


# ============================================================================
# PAGE 5 -- RULES
# ============================================================================
@app.route("/rules")
def page_rules():
    rules = []
    try:
        with db_cursor(dict_rows=True) as cur:
            cur.execute("""
                SELECT * FROM rules
                 ORDER BY is_active DESC, success_rate DESC, total_signals DESC
            """)
            rules = cur.fetchall()
    except Exception as e:
        log.warning(f"page_rules db: {e}")

    rule_rows = "".join(f"""
      <tr>
        <td class="muted right">{r['rule_num']}</td>
        <td><b>{r['rule_name']}</b><br><span class="muted">{r['description'] or ''}</span></td>
        <td><span class="tag {r['action']}">{r['action']}</span></td>
        <td class="muted">{r['source']}</td>
        <td class="right">{r['total_signals']}</td>
        <td class="right" style="color:var(--goal)">{r['success_count']}</td>
        <td class="right" style="color:var(--trap)">{r['fail_count']}</td>
        <td class="right"><b>{r['success_rate']}%</b></td>
        <td>
          <form method="post" action="/rules/{r['id']}/toggle" style="display:inline">
            <button class="btn {'primary' if r['is_active'] else ''}">
              {'ON' if r['is_active'] else 'OFF'}
            </button>
          </form>
        </td>
      </tr>""" for r in rules) or '<tr><td colspan="9" class="empty">No rules</td></tr>'

    body = f"""
    <div class="card" style="margin-bottom:14px">
      <h2>📋 Rules Engine</h2>
      <p class="muted">Toggle a rule OFF to silence it (signals already fired stay).
      Use <b>Improve</b> to ask Claude to suggest tightenings.</p>
      <form method="post" action="/rules/improve" style="display:inline-block">
        <button class="btn primary">🤖 Ask Claude for new rules</button>
      </form>
    </div>
    <div class="card">
      <table>
        <tr><th class="right">#</th><th>Name · Description</th><th>Action</th><th>Source</th>
            <th class="right">Total</th><th class="right">Success</th>
            <th class="right">Fail</th><th class="right">Rate</th><th>Status</th></tr>
        {rule_rows}
      </table>
    </div>"""
    return layout("Rules", "rules", body)


@app.route("/rules/<int:rid>/toggle", methods=["POST"])
def rule_toggle(rid):
    try:
        with db_cursor() as cur:
            cur.execute("UPDATE rules SET is_active = NOT is_active WHERE id = %s", (rid,))
    except Exception as e:
        log.warning(f"rule_toggle: {e}")
    return ("", 302, {"Location": "/rules"})


@app.route("/rules/improve", methods=["POST"])
def rule_improve():
    try:
        text = claude_suggest_rules()
        log.info(f"Claude suggested rules: {text[:200] if text else 'none'}")
    except Exception as e:
        log.warning(f"rule_improve: {e}")
    return ("", 302, {"Location": "/insights"})


# ============================================================================
# PAGE 6 -- ANALYTICS
# ============================================================================
@app.route("/analytics")
def page_analytics():
    data = {"total_matches": 0, "total_snaps": 0, "total_goals": 0,
            "total_signals": 0, "top_rules": []}
    try:
        with db_cursor(dict_rows=True) as cur:
            cur.execute("SELECT COUNT(*) AS c FROM matches")
            data["total_matches"] = cur.fetchone()["c"]
            cur.execute("SELECT COUNT(*) AS c FROM odds_snapshots")
            data["total_snaps"] = cur.fetchone()["c"]
            cur.execute("SELECT COUNT(*) AS c FROM goals")
            data["total_goals"] = cur.fetchone()["c"]
            cur.execute("SELECT COUNT(*) AS c FROM signals")
            data["total_signals"] = cur.fetchone()["c"]
            cur.execute("""
                SELECT rule_name, total_signals, success_count, fail_count, success_rate
                  FROM rules WHERE total_signals > 0
                 ORDER BY success_rate DESC, total_signals DESC LIMIT 12
            """)
            data["top_rules"] = cur.fetchall()
    except Exception as e:
        log.warning(f"page_analytics db: {e}")

    top_html = "".join(f"""
      <tr><td><b>{r['rule_name']}</b></td>
          <td class="right">{r['total_signals']}</td>
          <td class="right" style="color:var(--goal)">{r['success_count']}</td>
          <td class="right" style="color:var(--trap)">{r['fail_count']}</td>
          <td class="right"><b>{r['success_rate']}%</b></td>
      </tr>""" for r in data["top_rules"]) or '<tr><td colspan="5" class="empty">Not enough trades yet</td></tr>'

    body = f"""
    <div class="row cols-4" style="margin-bottom:14px">
      <div class="card"><h2>Matches</h2><div class="kpi">{data['total_matches']}</div></div>
      <div class="card"><h2>Snapshots</h2><div class="kpi">{data['total_snaps']}</div></div>
      <div class="card"><h2>Goals</h2><div class="kpi" style="color:var(--goal)">{data['total_goals']}</div></div>
      <div class="card"><h2>Signals</h2><div class="kpi" style="color:var(--hot)">{data['total_signals']}</div></div>
    </div>
    <div class="card">
      <h2>🏆 Top Rules <span class="muted">by win rate</span></h2>
      <table>
        <tr><th>Rule</th><th class="right">Total</th><th class="right">Hit</th>
            <th class="right">Miss</th><th class="right">Rate</th></tr>
        {top_html}
      </table>
    </div>"""
    return layout("Analytics", "analytics", body)


# ============================================================================
# PAGE 7 -- AI INSIGHTS
# ============================================================================
@app.route("/insights")
def page_insights():
    rows = []
    try:
        with db_cursor(dict_rows=True) as cur:
            cur.execute("""
                SELECT * FROM ai_insights
                 ORDER BY created_at DESC LIMIT 30
            """)
            rows = cur.fetchall()
    except Exception as e:
        log.warning(f"page_insights db: {e}")

    items = "".join(f"""
      <div class="card" style="margin-bottom:12px">
        <h2>{r['insight_type']}
          <span class="muted">{r['created_at'].strftime('%Y-%m-%d %H:%M') if r.get('created_at') else ''}
            · analyzed {r['goals_analyzed']} goals</span>
        </h2>
        <pre style="white-space:pre-wrap;color:var(--txt);font-family:inherit;margin:0">{r['content']}</pre>
      </div>""" for r in rows) or '<div class="card empty">No AI insights yet. Click <b>Ask Claude for new rules</b> on the Rules page.</div>'

    body = f"""
    <div class="card" style="margin-bottom:14px">
      <h2>🤖 AI Insights</h2>
      <p class="muted">Claude reads the recent goals + the snapshots that preceded them, and proposes rule patterns.</p>
      <form method="post" action="/rules/improve" style="display:inline-block">
        <button class="btn primary">Generate fresh insights now</button>
      </form>
    </div>
    {items}"""
    return layout("AI Insights", "insights", body)


# ============================================================================
# JSON / OPS
# ============================================================================
@app.route("/health")
def health():
    return jsonify({
        "ok": True,
        "scanner": SCANNER_STATS,
        "anthropic": _anthropic_client is not None,
        "db": bool(DATABASE_URL),
        "oddsapi": bool(ODDSPAPI_KEY),
        "markets_seen": sorted(_SEEN_MARKETS),
        "now": datetime.now(timezone.utc).isoformat(),
    })


@app.route("/debug/markets")
def debug_markets():
    """Show raw OddsAPI response for the first live event with actual markets.
    Useful for figuring out what markets and lines OddsAPI actually ships."""
    if not ODDSPAPI_KEY:
        return jsonify({"error": "ODDSPAPI_KEY not set"}), 500

    events = fetch_oddsapi_events()
    if not events:
        return jsonify({"error": "no live events right now"}), 200

    # Try to find a match with non-empty bookmaker data. We try up to 10
    # events because the first ones are often pre-match with no quotes yet.
    event_ids_to_try = []
    for ev in events[:30]:
        eid = ev.get("id") or ev.get("eventId")
        if eid:
            event_ids_to_try.append(str(eid))

    if not event_ids_to_try:
        return jsonify({"error": "no event ids found"}), 200

    # Fetch odds for the first batch
    odds_data = fetch_oddsapi_odds(event_ids_to_try)

    # Phase 1: try to find a live match (minute != null) with markets.
    # Phase 2: fall back to any match with markets if no live ones exist.
    chosen = None
    rejected_summary = []
    pre_match_with_markets = None

    for raw in odds_data:
        bookmakers = raw.get("bookmakers") or {}
        bet365 = bookmakers.get("Bet365") or bookmakers.get("bet365") or []
        market_count = len(bet365) if isinstance(bet365, list) else 0

        eid = raw.get("id") or raw.get("eventId")
        home = raw.get("home", "?")
        away = raw.get("away", "?")
        minute = raw.get("minute")
        score = raw.get("score")

        if market_count > 0:
            if minute is not None:
                # This is a LIVE match -- use it
                chosen = raw
                break
            elif pre_match_with_markets is None:
                # Remember as fallback
                pre_match_with_markets = raw

        rejected_summary.append({
            "id": eid,
            "match": f"{home} vs {away}",
            "minute": minute,
            "score": score,
            "market_count": market_count,
        })

    # Use pre-match fallback if no live match was found
    if chosen is None and pre_match_with_markets is not None:
        chosen = pre_match_with_markets

    if not chosen:
        return jsonify({
            "error": "couldn't find a live event with markets",
            "tried_count": len(odds_data),
            "rejected_examples": rejected_summary[:10],
        }), 200

    # Build a clean inventory of every market name + every line
    bookmakers = chosen.get("bookmakers") or {}
    bet365 = bookmakers.get("Bet365") or bookmakers.get("bet365") or []
    inventory = []
    for market in bet365 if isinstance(bet365, list) else []:
        if not isinstance(market, dict):
            continue
        name = market.get("name", "?")
        odds_list = market.get("odds", [])
        if not isinstance(odds_list, list):
            continue
        entries = []
        for entry in odds_list:
            if isinstance(entry, dict):
                entries.append(entry)
        inventory.append({"market_name": name, "odds_entries": entries})

    league_raw = chosen.get("league")
    league_name = league_raw.get("name") if isinstance(league_raw, dict) else league_raw

    return jsonify({
        "event": {
            "id": chosen.get("id"),
            "home": chosen.get("home"),
            "away": chosen.get("away"),
            "league": league_name,
            "minute": chosen.get("minute"),
            "score": chosen.get("score"),
        },
        "markets_inventory": inventory,
        "total_markets_in_response": len(inventory),
        "events_skipped_before_finding_one": len(rejected_summary),
    })


@app.route("/api/scan_now", methods=["POST"])
def api_scan_now():
    threading.Thread(target=scan_once, daemon=True).start()
    return jsonify({"started": True})


@app.errorhandler(404)
def nf(_):
    return layout("404", "", '<div class="card empty">Page not found</div>'), 404


# ============================================================================
# BOOT
# ============================================================================
def main():
    log.info("=" * 60)
    log.info("PapaGoal Market Recorder · v5 starting")
    log.info(f"  ODDSAPI:    {'set' if ODDSPAPI_KEY else 'MISSING'}")
    log.info(f"  FOOTBALL:   {'set' if FOOTBALL_API_KEY else 'MISSING'}")
    log.info(f"  ANTHROPIC:  {'set' if ANTHROPIC_API_KEY else 'MISSING'}")
    log.info(f"  DB:         {'set' if DATABASE_URL else 'MISSING'}")
    log.info(f"  PORT:       {PORT}")
    log.info("=" * 60)

    try:
        init_db()
    except Exception as e:
        log.error(f"init_db crashed: {e}\n{traceback.format_exc()}")

    t = threading.Thread(target=scanner_loop, daemon=True, name="scanner")
    t.start()

    app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False)


if __name__ == "__main__":
    main()
