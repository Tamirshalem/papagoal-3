import os, time, json, logging, threading, re
from datetime import datetime, timezone
from urllib.parse import urlparse
from flask import Flask, jsonify, render_template_string, request
import pg8000.native
import requests

# ─── Config ───────────────────────────────────────────────────────────────────
ODDSAPI_KEY       = os.environ.get("ODDSAPI_KEY", "")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
DATABASE_URL      = os.environ.get("DATABASE_URL", "")
PORT              = int(os.environ.get("PORT", 8080))
POLL_INTERVAL     = 30

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("papagoal")
app = Flask(__name__)

# ─── Expected Curves ──────────────────────────────────────────────────────────
EXPECTED = {
    "H1_0.5": {0:1.25,5:1.28,10:1.32,15:1.38,20:1.45,25:1.55,30:1.68,35:1.85,40:2.10,45:2.50},
    "H1_1.5": {0:2.10,5:2.15,10:2.22,15:2.32,20:2.45,25:2.65,30:2.90,35:3.20,40:3.60,45:4.20},
    "H1_2.5": {0:3.50,5:3.60,10:3.75,15:3.95,20:4.20,25:4.60,30:5.20,35:6.00,40:7.50,45:10.0},
    "FT_0.5": {0:1.10,10:1.12,20:1.15,30:1.20,40:1.28,50:1.38,60:1.55,70:1.85,80:2.50,88:4.00},
    "FT_1.5": {0:1.85,10:1.88,20:1.92,30:2.05,40:2.25,50:2.55,60:3.00,70:3.80,80:5.50,88:9.00},
    "FT_2.5": {0:2.80,10:2.85,20:2.95,30:3.15,40:3.50,50:4.00,60:4.80,70:6.50,80:10.0,88:18.0},
    "FT_3.5": {0:5.50,10:5.60,20:5.80,30:6.20,40:7.00,50:8.50,60:11.0,70:16.0,80:28.0,88:55.0},
}

def get_expected(market_type, line, minute):
    key = f"{market_type}_{line}"
    curve = EXPECTED.get(key)
    if not curve:
        # Fallback: scale from nearest curve
        return None
    keys = sorted(curve.keys())
    m = min(max(minute, 0), keys[-1])
    for i, k in enumerate(keys):
        if m <= k:
            if i == 0: return curve[k]
            prev_k = keys[i-1]
            r = (m - prev_k) / (k - prev_k)
            return round(curve[prev_k] + r * (curve[k] - curve[prev_k]), 3)
    return curve[keys[-1]]

def calc_pressure(real_odd, opening_odd, expected_odd):
    if not opening_odd or not real_odd or not expected_odd: return 0
    rise = real_odd / opening_odd
    exp_rise = expected_odd / opening_odd
    if exp_rise <= 0: return 0
    return max(0, min(100, int((1 - rise / exp_rise) * 100)))


# ─── Safe DB Migrations ───────────────────────────────────────────────────────
def _safe_migrate(conn):
    """Add missing columns to existing tables — safe to run on every startup."""
    migrations = [
        # odds_snapshots — columns added in v6
        ("odds_snapshots", "market_type",       "TEXT"),
        ("odds_snapshots", "line",               "FLOAT"),
        ("odds_snapshots", "bookmaker",          "TEXT DEFAULT 'Bet365'"),
        ("odds_snapshots", "over_odd",           "FLOAT"),
        ("odds_snapshots", "under_odd",          "FLOAT"),
        ("odds_snapshots", "opening_over",       "FLOAT"),
        ("odds_snapshots", "opening_under",      "FLOAT"),
        ("odds_snapshots", "prev_over",          "FLOAT"),
        ("odds_snapshots", "prev_under",         "FLOAT"),
        ("odds_snapshots", "delta_over",         "FLOAT DEFAULT 0"),
        ("odds_snapshots", "delta_under",        "FLOAT DEFAULT 0"),
        ("odds_snapshots", "direction",          "TEXT DEFAULT 'stable'"),
        ("odds_snapshots", "held_seconds",       "INT DEFAULT 0"),
        ("odds_snapshots", "expected_over",      "FLOAT"),
        ("odds_snapshots", "gap_over",           "FLOAT DEFAULT 0"),
        ("odds_snapshots", "gap_ratio_over",     "FLOAT DEFAULT 1"),
        ("odds_snapshots", "pressure_score",     "INT DEFAULT 0"),
        ("odds_snapshots", "movement_type",      "TEXT DEFAULT 'stable'"),
        ("odds_snapshots", "reversal_detected",  "BOOLEAN DEFAULT FALSE"),
        ("odds_snapshots", "frozen_market",      "BOOLEAN DEFAULT FALSE"),
        ("odds_snapshots", "is_live",            "BOOLEAN DEFAULT FALSE"),
        ("odds_snapshots", "goal_2m",            "BOOLEAN DEFAULT FALSE"),
        ("odds_snapshots", "goal_5m",            "BOOLEAN DEFAULT FALSE"),
        ("odds_snapshots", "goal_10m",           "BOOLEAN DEFAULT FALSE"),
        # matches — extra columns
        ("matches", "event_id",                  "TEXT"),
        ("matches", "fixture_id",                "TEXT"),
        ("matches", "total_goals",               "INT DEFAULT 0"),
        # rules — extra columns
        ("rules", "avg_entry_odd",               "FLOAT DEFAULT 0"),
        ("rules", "false_positive_rate",         "FLOAT DEFAULT 0"),
        ("rules", "pressure_min",                "INT DEFAULT 0"),
        ("rules", "held_seconds_min",            "INT DEFAULT 0"),
        ("rules", "movement_condition",          "TEXT"),
        ("rules", "score_condition",             "TEXT"),
        # paper_trades
        ("paper_trades", "created_at",          "TIMESTAMPTZ DEFAULT NOW()"),
        ("paper_trades", "resolved_at",         "TIMESTAMPTZ"),
        ("paper_trades", "result",               "TEXT DEFAULT 'pending'"),
        ("paper_trades", "rule_id",              "INT DEFAULT 0"),
        ("paper_trades", "rule_name",            "TEXT DEFAULT ''"),
        ("paper_trades", "confidence_estimate",  "INT DEFAULT 50"),
        ("paper_trades", "movement_type",        "TEXT"),
        ("paper_trades", "failure_reason",       "TEXT"),
        ("paper_trades", "entry_minute",         "INT DEFAULT 0"),
        ("paper_trades", "entry_score_home",     "INT DEFAULT 0"),
        ("paper_trades", "entry_score_away",     "INT DEFAULT 0"),
        ("paper_trades", "entry_total_goals",    "INT DEFAULT 0"),
        ("paper_trades", "score_entry",          "TEXT DEFAULT '0-0'"),
        ("paper_trades", "home_team",            "TEXT DEFAULT ''"),
        ("paper_trades", "away_team",            "TEXT DEFAULT ''"),
        ("paper_trades", "league",               "TEXT DEFAULT ''"),
        ("paper_trades", "market_type",          "TEXT DEFAULT 'FT'"),
        ("paper_trades", "line",                 "FLOAT DEFAULT 2.5"),
        ("paper_trades", "selected_side",        "TEXT DEFAULT 'over'"),
        ("paper_trades", "action_type",          "TEXT DEFAULT ''"),
        ("paper_trades", "entry_odd",            "FLOAT DEFAULT 0"),
        ("paper_trades", "expected_odd",         "FLOAT"),
        ("paper_trades", "gap",                  "FLOAT DEFAULT 0"),
        ("paper_trades", "pressure_score",       "INT DEFAULT 0"),
        ("paper_trades", "validation_window",    "TEXT DEFAULT '10m'"),
        ("paper_trades", "dummy_stake",          "FLOAT DEFAULT 100"),
        ("paper_trades", "dummy_profit_loss",    "FLOAT DEFAULT 0"),
        # matches
        ("matches", "match_id",                  "TEXT"),
        ("matches", "event_id",                  "TEXT"),
        ("matches", "total_goals",               "INT DEFAULT 0"),
        ("matches", "period",                    "TEXT DEFAULT 'FT'"),
    ]
    for table, col, col_type in migrations:
        try:
            conn.run(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {col} {col_type}")
        except Exception as e:
            log.debug(f"Migration skip {table}.{col}: {e}")

# ─── DB ───────────────────────────────────────────────────────────────────────
def parse_db(url):
    p = urlparse(url)
    return {"host":p.hostname,"port":p.port or 5432,"database":p.path.lstrip("/"),
            "user":p.username,"password":p.password,"ssl_context":True}

def get_db():
    return pg8000.native.Connection(**parse_db(DATABASE_URL))

def init_db():
    conn = get_db()
    try:
        conn.run("""CREATE TABLE IF NOT EXISTS matches (
            id SERIAL PRIMARY KEY, match_id TEXT UNIQUE,
            event_id TEXT, league TEXT, home_team TEXT, away_team TEXT,
            minute INT DEFAULT 0, score_home INT DEFAULT 0, score_away INT DEFAULT 0,
            total_goals INT DEFAULT 0, period TEXT DEFAULT 'FT',
            status TEXT DEFAULT 'upcoming', last_updated TIMESTAMPTZ DEFAULT NOW()
        )""")
        conn.run("""CREATE TABLE IF NOT EXISTS odds_snapshots (
            id SERIAL PRIMARY KEY, match_id TEXT,
            captured_at TIMESTAMPTZ DEFAULT NOW(),
            minute INT DEFAULT 0, score_home INT DEFAULT 0, score_away INT DEFAULT 0,
            total_goals INT DEFAULT 0, period TEXT DEFAULT 'FT',
            market_type TEXT, line FLOAT, bookmaker TEXT DEFAULT 'Bet365',
            over_odd FLOAT, under_odd FLOAT,
            opening_over FLOAT, opening_under FLOAT,
            prev_over FLOAT, prev_under FLOAT,
            delta_over FLOAT DEFAULT 0, delta_under FLOAT DEFAULT 0,
            direction TEXT DEFAULT 'stable', held_seconds INT DEFAULT 0,
            expected_over FLOAT, expected_under FLOAT,
            gap_over FLOAT DEFAULT 0, gap_under FLOAT DEFAULT 0,
            gap_ratio_over FLOAT DEFAULT 1, gap_ratio_under FLOAT DEFAULT 1,
            pressure_score INT DEFAULT 0,
            movement_type TEXT DEFAULT 'stable',
            reversal_detected BOOLEAN DEFAULT FALSE,
            frozen_market BOOLEAN DEFAULT FALSE,
            is_live BOOLEAN DEFAULT FALSE,
            goal_2m BOOLEAN DEFAULT FALSE, goal_5m BOOLEAN DEFAULT FALSE,
            goal_10m BOOLEAN DEFAULT FALSE
        )""")
        # Safe migrations — add missing columns to existing tables
        _safe_migrate(conn)

        # Indexes — each wrapped so one failure doesn't block the rest
        for _idx in [
            "CREATE INDEX IF NOT EXISTS idx_snap_match   ON odds_snapshots(match_id)",
            "CREATE INDEX IF NOT EXISTS idx_snap_time    ON odds_snapshots(captured_at)",
            "CREATE INDEX IF NOT EXISTS idx_snap_market  ON odds_snapshots(market_type, line)",
        ]:
            try: conn.run(_idx)
            except Exception as _e: log.warning(f"Index skip: {_e}")
        conn.run("""CREATE TABLE IF NOT EXISTS goals (
            id SERIAL PRIMARY KEY, match_id TEXT,
            minute INT, goal_time TIMESTAMPTZ DEFAULT NOW(),
            score_before TEXT, score_after TEXT,
            period TEXT, auto_detected BOOLEAN DEFAULT TRUE,
            had_snapshots BOOLEAN DEFAULT FALSE,
            odds_10s JSONB DEFAULT '{}', odds_30s JSONB DEFAULT '{}',
            odds_60s JSONB DEFAULT '{}', odds_120s JSONB DEFAULT '{}',
            odds_300s JSONB DEFAULT '{}'
        )""")
        conn.run("""CREATE TABLE IF NOT EXISTS observations (
            id SERIAL PRIMARY KEY, match_id TEXT,
            detected_at TIMESTAMPTZ DEFAULT NOW(),
            home_team TEXT, away_team TEXT, league TEXT,
            rule_id INT, rule_name TEXT, source TEXT DEFAULT 'system',
            minute INT DEFAULT 0, score TEXT DEFAULT '0-0',
            market_type TEXT, line FLOAT,
            over_odd FLOAT, under_odd FLOAT,
            expected_odd FLOAT, gap FLOAT DEFAULT 0,
            gap_ratio FLOAT DEFAULT 1, pressure_score INT DEFAULT 0,
            movement_type TEXT DEFAULT 'stable',
            confidence_estimate INT DEFAULT 50,
            action_type TEXT, reason TEXT
        )""")
        conn.run("""CREATE TABLE IF NOT EXISTS paper_trades (
            id SERIAL PRIMARY KEY, observation_id INT,
            match_id TEXT, rule_id INT, rule_name TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            home_team TEXT, away_team TEXT, league TEXT,
            market_type TEXT, line FLOAT,
            selected_side TEXT, action_type TEXT,
            entry_odd FLOAT, expected_odd FLOAT,
            gap FLOAT DEFAULT 0, pressure_score INT DEFAULT 0,
            movement_type TEXT, confidence_estimate INT DEFAULT 50,
            validation_window TEXT,
            result TEXT DEFAULT 'pending',
            resolved_at TIMESTAMPTZ,
            dummy_stake FLOAT DEFAULT 100,
            dummy_profit_loss FLOAT DEFAULT 0,
            failure_reason TEXT,
            UNIQUE(match_id, rule_id, validation_window)
        )""")
        conn.run("""CREATE TABLE IF NOT EXISTS rules (
            id SERIAL PRIMARY KEY, rule_name TEXT UNIQUE,
            description TEXT, source TEXT DEFAULT 'manual',
            market_type TEXT, line_min FLOAT, line_max FLOAT,
            minute_min INT, minute_max INT,
            over_odd_min FLOAT, over_odd_max FLOAT,
            under_odd_min FLOAT, under_odd_max FLOAT,
            expected_gap_min FLOAT DEFAULT 0,
            pressure_min INT DEFAULT 0,
            held_seconds_min INT DEFAULT 0,
            movement_condition TEXT,
            action_type TEXT,
            selected_side TEXT DEFAULT 'over',
            validation_window TEXT DEFAULT '10m',
            status TEXT DEFAULT 'ACTIVE',
            is_active BOOLEAN DEFAULT TRUE,
            total_signals INT DEFAULT 0,
            win_count INT DEFAULT 0, lose_count INT DEFAULT 0,
            win_rate FLOAT DEFAULT 0, false_positive_rate FLOAT DEFAULT 0,
            dummy_profit FLOAT DEFAULT 0, avg_entry_odd FLOAT DEFAULT 0,
            created_at TIMESTAMPTZ DEFAULT NOW(), last_updated TIMESTAMPTZ DEFAULT NOW()
        )""")
        conn.run("""CREATE TABLE IF NOT EXISTS pattern_stats (
            id SERIAL PRIMARY KEY,
            market_type TEXT, line FLOAT, minute_bucket TEXT,
            score_state TEXT, odds_bucket TEXT,
            gap_bucket TEXT, pressure_bucket TEXT, movement_type TEXT,
            total_cases INT DEFAULT 0, wins INT DEFAULT 0, losses INT DEFAULT 0,
            win_rate FLOAT DEFAULT 0,
            goals_2m INT DEFAULT 0, goals_5m INT DEFAULT 0, goals_10m INT DEFAULT 0,
            false_positive_cases INT DEFAULT 0, trap_rate FLOAT DEFAULT 0,
            avg_entry_odd FLOAT DEFAULT 0, avg_gap FLOAT DEFAULT 0,
            dummy_profit FLOAT DEFAULT 0, confidence_level TEXT DEFAULT 'low',
            last_updated TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(market_type, line, minute_bucket, score_state, odds_bucket, gap_bucket)
        )""")
        conn.run("""CREATE TABLE IF NOT EXISTS ai_insights (
            id SERIAL PRIMARY KEY, created_at TIMESTAMPTZ DEFAULT NOW(),
            insight_type TEXT, content TEXT,
            goals_analyzed INT DEFAULT 0, rules_analyzed INT DEFAULT 0
        )""")

        # Seed 5 default rules if empty
        existing = conn.run("SELECT COUNT(*) FROM rules")[0][0]
        if existing == 0:
            _seed_rules(conn)

        log.info("✅ DB ready")
    except Exception as e:
        log.error(f"DB init: {e}")
    finally:
        conn.close()

def _seed_rules(conn):
    rules = [
        # ── 1. Market Shut ─────────────────────────────────────────────────────
        # 39-40 historical signals, 100% success. VALIDATED.
        # Market believes no more goals late in the game.
        {
            "rule_name": "Market Shut",
            "description": "Over FT ≥2.80 after min 82 → market believes no more goals",
            "market_type": "FT", "line_min": 1.5, "line_max": 5.5,
            "minute_min": 82, "minute_max": 95,
            "over_odd_min": 2.80, "over_odd_max": 99.0,
            "under_odd_min": None, "under_odd_max": None,
            "action_type": "UNDER_HOLDS_10M", "selected_side": "under",
            "validation_window": "10m", "status": "VALIDATED",
            "pressure_min": 0, "held_seconds_min": 0,
            "score_condition": None,
        },
        # ── 2. Early Drop ──────────────────────────────────────────────────────
        # 9 historical signals, 100% success. PROMISING (small sample).
        # Sharp drop in FT over odd early → goal coming soon.
        {
            "rule_name": "Early Drop Signal",
            "description": "Over FT drops to 1.50-1.57 at min 17-20 → goal very soon",
            "market_type": "FT", "line_min": 0.5, "line_max": 1.5,
            "minute_min": 17, "minute_max": 20,
            "over_odd_min": 1.50, "over_odd_max": 1.57,
            "under_odd_min": None, "under_odd_max": None,
            "action_type": "OVER_LINE_WITHIN_10M", "selected_side": "over",
            "validation_window": "10m", "status": "PROMISING",
            "pressure_min": 0, "held_seconds_min": 0,
            "score_condition": None,
        },
        # ── 3. H1 Goal Pressure Minute 18 ─────────────────────────────────────
        # H1 market still believes another goal before HT.
        {
            "rule_name": "H1 Minute 18 Goal Pressure",
            "description": "Over H1 next line 1.40-1.60 at min 15-22 → H1 goal before HT",
            "market_type": "H1", "line_min": 0.5, "line_max": 3.5,
            "minute_min": 15, "minute_max": 22,
            "over_odd_min": 1.40, "over_odd_max": 1.60,
            "under_odd_min": None, "under_odd_max": None,
            "action_type": "H1_OVER_LINE_BEFORE_HT", "selected_side": "over",
            "validation_window": "HT", "status": "TESTING",
            "pressure_min": 0, "held_seconds_min": 0,
            "score_condition": None,
        },
        # ── 4. H1 Under 1.66 Trap ─────────────────────────────────────────────
        # Market expects H1 line to hold until HT.
        {
            "rule_name": "H1 Under 1.66 Minute 34",
            "description": "Under H1 ~1.60-1.72 at min 30-38 → line holds to HT",
            "market_type": "H1", "line_min": 0.5, "line_max": 3.5,
            "minute_min": 30, "minute_max": 38,
            "over_odd_min": None, "over_odd_max": None,
            "under_odd_min": 1.60, "under_odd_max": 1.72,
            "action_type": "UNDER_HOLDS_TO_HT", "selected_side": "under",
            "validation_window": "HT", "status": "TESTING",
            "pressure_min": 0, "held_seconds_min": 0,
            "score_condition": None,
        },
        # ── 5. Late FT Goal Hold ───────────────────────────────────────────────
        # Late game, over ~2.50 held 60s+ → market still alive.
        {
            "rule_name": "Late FT Goal Hold",
            "description": "Over FT ~2.50 at min 86+ held 60s → goal before FT",
            "market_type": "FT", "line_min": 1.5, "line_max": 4.5,
            "minute_min": 86, "minute_max": 95,
            "over_odd_min": 2.20, "over_odd_max": 2.80,
            "under_odd_min": None, "under_odd_max": None,
            "action_type": "OVER_LINE_BEFORE_FT", "selected_side": "over",
            "validation_window": "FT", "status": "TESTING",
            "pressure_min": 0, "held_seconds_min": 60,
            "score_condition": None,
        },
    ]
    for r in rules:
        try:
            conn.run("""INSERT INTO rules
                (rule_name,description,market_type,line_min,line_max,
                 minute_min,minute_max,over_odd_min,over_odd_max,
                 under_odd_min,under_odd_max,action_type,selected_side,
                 validation_window,status,pressure_min,held_seconds_min)
                VALUES (:a,:b,:c,:d,:e,:f,:g,:h,:i,:j,:k,:l,:m,:n,:o,:p,:q)
                ON CONFLICT DO NOTHING""",
                a=r["rule_name"],b=r["description"],c=r["market_type"],
                d=r["line_min"],e=r["line_max"],f=r["minute_min"],g=r["minute_max"],
                h=r.get("over_odd_min"),i=r.get("over_odd_max"),
                j=r.get("under_odd_min"),k=r.get("under_odd_max"),
                l=r["action_type"],m=r["selected_side"],n=r["validation_window"],
                o=r["status"],p=r["pressure_min"],q=r["held_seconds_min"])
        except Exception as e:
            log.error(f"Seed rule error: {e}")

# ─── Price Cache ───────────────────────────────────────────────────────────────
price_cache  = {}   # match_id+market+line -> {over, under, since, prev_over, prev_under}
opening_cache= {}   # match_id+market+line -> {over, under}
last_scores  = {}   # match_id -> total_goals

def cache_key(mid, mtype, line):
    return f"{mid}_{mtype}_{line}"

def get_opening(mid, mtype, line):
    return opening_cache.get(cache_key(mid, mtype, line))

def set_opening(mid, mtype, line, over, under):
    k = cache_key(mid, mtype, line)
    if k not in opening_cache:
        opening_cache[k] = {"over": over, "under": under}

# ─── OddsAPI.io ───────────────────────────────────────────────────────────────
def fetch_events():
    if not ODDSAPI_KEY:
        log.warning("⚠️ ODDSAPI_KEY not set!")
        return []
    try:
        r = requests.get("https://api.odds-api.io/v3/events",
            params={"apiKey": ODDSAPI_KEY, "sport": "football", "status": "live", "limit": 50},
            timeout=15)
        if r.status_code != 200:
            log.warning(f"OddsAPI events HTTP {r.status_code}: {r.text[:300]}")
            return []
        raw = r.json()
        events = raw if isinstance(raw, list) else (
            raw.get("data") or raw.get("events") or raw.get("results") or [])
        log.info(f"📡 OddsAPI: {len(events)} live events")
        return events
    except Exception as e:
        log.error(f"OddsAPI events: {e}")
        return []

def fetch_odds_multi(event_ids):
    """Try multiple odds endpoint patterns until one works"""
    if not event_ids or not ODDSAPI_KEY: return []
    results = []
    for eid in event_ids[:30]:
        data = None
        # Try endpoint patterns in order
        endpoints = [
            f"https://api.odds-api.io/v3/events/{eid}/odds",
            f"https://api.odds-api.io/v3/odds?eventId={eid}",
        ]
        for url in endpoints:
            try:
                r = requests.get(url, params={"apiKey": ODDSAPI_KEY}, timeout=8)
                if r.status_code == 200:
                    d = r.json()
                    if d and not d.get("error"):
                        d["_event_id"] = str(eid)
                        data = d
                        break
                elif r.status_code not in (404, 400):
                    log.debug(f"Odds {eid} {url}: {r.status_code} {r.text[:80]}")
            except: pass
        if data:
            results.append(data)
    if results:
        log.info(f"📊 Odds loaded: {len(results)}/{len(event_ids[:30])} events")
    return results

def parse_event(event, odds_data):
    """Parse event + odds into structured format"""
    # League can be string or object
    league_raw = event.get("league") or ""
    league = league_raw.get("name", "") if isinstance(league_raw, dict) else str(league_raw)

    result = {
        "event_id": str(event.get("id") or ""),
        "home": event.get("home") or event.get("homeTeam") or "",
        "away": event.get("away") or event.get("awayTeam") or "",
        "league": league,
        "minute": 0, "score_home": 0, "score_away": 0, "period": "H1",
        "markets": []
    }

    # scores: {"home": 1, "away": 2, "periods": {"p1": {...}}}
    scores = event.get("scores") or event.get("score") or {}
    if isinstance(scores, dict):
        result["score_home"] = int(scores.get("home") or 0)
        result["score_away"] = int(scores.get("away") or 0)
        # Determine period from periods data
        periods = scores.get("periods") or {}
        if "p2" in periods or (isinstance(periods, dict) and len(periods) >= 2):
            result["period"] = "H2"
        elif "p1" in periods:
            result["period"] = "H1"

    # minute from liveData or elapsed
    live = event.get("liveData") or event.get("status_data") or {}
    if isinstance(live, dict):
        result["minute"] = int(live.get("minute") or live.get("elapsed") or 0)
    # fallback: estimate from time if no minute
    if result["minute"] == 0:
        try:
            from datetime import datetime, timezone
            start = datetime.fromisoformat(event.get("date","").replace("Z","+00:00"))
            elapsed = (datetime.now(timezone.utc) - start).total_seconds() / 60
            result["minute"] = max(0, min(90, int(elapsed)))
        except: pass

    # Parse odds from odds_data
    if not odds_data:
        return result

    def _parse_bookmaker_markets(bk_markets):
        """Parse a list of market dicts from one bookmaker into result['markets']"""
        if not isinstance(bk_markets, list):
            return
        for mkt in bk_markets:
            mname = (mkt.get("name") or mkt.get("market") or mkt.get("type") or "").upper().strip()
            is_ou = ("OVER" in mname or "UNDER" in mname or "O/U" in mname
                     or "TOTAL" in mname or "GOALS" in mname)
            if not is_ou:
                continue
            market_type = "FT"
            if "HALF" in mname or "HT" in mname or "H1" in mname or "1ST" in mname or "FIRST" in mname:
                market_type = "H1"

            # odds-api.io returns outcomes as list OR flat keys
            outcomes = mkt.get("outcomes") or mkt.get("odds") or []
            over_odd = under_odd = None
            line_val = None

            if isinstance(outcomes, list):
                for oc in outcomes:
                    nm = (oc.get("name") or oc.get("label") or oc.get("type") or "").upper()
                    val = oc.get("price") or oc.get("odd") or oc.get("value") or 0
                    try:
                        val = float(val)
                    except:
                        val = 0
                    hdp = oc.get("handicap") or oc.get("line") or oc.get("total") or oc.get("points")
                    if hdp is not None and line_val is None:
                        try: line_val = float(hdp)
                        except: pass
                    if "OVER" in nm and val > 1:
                        over_odd = val
                    elif "UNDER" in nm and val > 1:
                        under_odd = val
            elif isinstance(outcomes, dict):
                # flat dict: {"over": 1.85, "under": 1.95, "line": 2.5}
                try: over_odd = float(outcomes.get("over") or 0) or None
                except: pass
                try: under_odd = float(outcomes.get("under") or 0) or None
                except: pass
                try: line_val = float(outcomes.get("line") or outcomes.get("total") or 0) or None
                except: pass

            # Try flat keys on the market itself
            if over_odd is None:
                try: over_odd = float(mkt.get("over") or 0) or None
                except: pass
            if under_odd is None:
                try: under_odd = float(mkt.get("under") or 0) or None
                except: pass
            if line_val is None:
                try: line_val = float(mkt.get("line") or mkt.get("total") or mkt.get("handicap") or mkt.get("points") or 2.5)
                except: line_val = 2.5

            if line_val is None:
                line_val = 2.5

            if over_odd and over_odd > 1:
                result["markets"].append({
                    "market_type": market_type,
                    "line": round(line_val, 1),
                    "over_odd": round(over_odd, 3),
                    "under_odd": round(under_odd, 3) if under_odd and under_odd > 1 else None
                })

    # odds_data.bookmakers can be a dict {bkName: [...]} or a list [{name, markets:[]}]
    bookmakers_raw = odds_data.get("bookmakers") or {}
    if isinstance(bookmakers_raw, dict):
        for _bk_name, bk_data in bookmakers_raw.items():
            if isinstance(bk_data, list):
                _parse_bookmaker_markets(bk_data)
            elif isinstance(bk_data, dict):
                _parse_bookmaker_markets(bk_data.get("markets") or [])
    elif isinstance(bookmakers_raw, list):
        for bk in bookmakers_raw:
            if isinstance(bk, dict):
                _parse_bookmaker_markets(bk.get("markets") or bk.get("odds") or [])

    # Also check top-level markets key (some API responses flatten it)
    if not result["markets"]:
        _parse_bookmaker_markets(odds_data.get("markets") or [])

    # Deduplicate: keep best (most complete) entry per market_type+line
    seen = {}
    for m in result["markets"]:
        k = (m["market_type"], m["line"])
        if k not in seen or (m["under_odd"] and not seen[k].get("under_odd")):
            seen[k] = m
    result["markets"] = list(seen.values())

    return result

# ─── Rules Engine ─────────────────────────────────────────────────────────────
def check_rules(conn, match_id, home, away, league, minute, score_h, score_a, period, markets, held_map):
    """Check all active rules against current market data"""
    try:
        rules = conn.run("""SELECT id,rule_name,market_type,line_min,line_max,
            minute_min,minute_max,over_odd_min,over_odd_max,under_odd_min,under_odd_max,
            expected_gap_min,pressure_min,held_seconds_min,movement_condition,
            action_type,selected_side,validation_window,status
            FROM rules WHERE is_active=TRUE ORDER BY id""")
    except Exception as e:
        log.error(f"Rules fetch: {e}")
        return

    for rule in rules:
        (rid, rname, mtype, lmin, lmax, mmin, mmax,
         ovmin, ovmax, unmin, unmax, gap_min, pmin, held_min,
         mov_cond, action_type, side, val_window, status) = rule

        # Check minute range
        if mmin and minute < mmin: continue
        if mmax and minute > mmax: continue

        # Find matching market
        for mkt in markets:
            if mkt["market_type"] != mtype: continue
            line = mkt["line"]
            if lmin and line < lmin: continue
            if lmax and line > lmax: continue

            over = mkt.get("over_odd")
            under = mkt.get("under_odd")

            # Check over odd range
            if side == "over" or ovmin or ovmax:
                if over is None: continue
                if ovmin and over < ovmin: continue
                if ovmax and over > ovmax: continue

            # Check under odd range
            if side == "under" or unmin or unmax:
                if under is None: continue
                if unmin and under < unmin: continue
                if unmax and under > unmax: continue

            # Check held seconds
            hkey = cache_key(match_id, mtype, line)
            held = held_map.get(hkey, 0)
            if held_min and held < held_min: continue

            # Check pressure
            op = get_opening(match_id, mtype, line)
            exp = get_expected(mtype, str(line), minute)
            odd_to_check = over if side == "over" else under
            pres = 0
            if op and exp and odd_to_check:
                op_side = op.get("over") if side == "over" else op.get("under")
                pres = calc_pressure(odd_to_check, op_side, exp)
            if pmin and pres < pmin: continue

            # Calculate gap
            gap = 0
            if exp and odd_to_check:
                gap = round(exp - odd_to_check, 3) if side == "over" else round(odd_to_check - exp, 3)
            if gap_min and gap < gap_min: continue

            # Rule triggered! Create observation + paper trade
            score_str = f"{score_h}-{score_a}"
            entry_odd = over if side == "over" else under
            confidence = min(95, 50 + pres // 3 + (20 if status == "VALIDATED" else 10 if status == "PROMISING" else 0))

            try:
                # Check no duplicate
                existing = conn.run("""SELECT COUNT(*) FROM paper_trades
                    WHERE match_id=:a AND rule_id=:b AND validation_window=:c AND result='pending'""",
                    a=match_id, b=rid, c=val_window)
                if existing[0][0] > 0: continue

                # Create observation
                obs = conn.run("""INSERT INTO observations
                    (match_id,home_team,away_team,league,rule_id,rule_name,source,
                     minute,score,market_type,line,over_odd,under_odd,expected_odd,
                     gap,pressure_score,movement_type,confidence_estimate,action_type,reason)
                    VALUES (:a,:b,:c,:d,:e,:f,'system',:g,:h,:i,:j,:k,:l,:m,:n,:o,'stable',:p,:q,:r)
                    RETURNING id""",
                    a=match_id,b=home,c=away,d=league,e=rid,f=rname,
                    g=minute,h=score_str,i=mtype,j=line,k=over,l=under,
                    m=exp,n=gap,o=pres,p=confidence,q=action_type,
                    r=f"Rule {rname} triggered: {side} {line} @ {entry_odd} | gap={gap:.2f} | pressure={pres}%")
                obs_id = obs[0][0] if obs else None

                # Create paper trade
                conn.run("""INSERT INTO paper_trades
                    (observation_id,match_id,rule_id,rule_name,home_team,away_team,league,
                     market_type,line,selected_side,action_type,entry_odd,expected_odd,
                     gap,pressure_score,confidence_estimate,validation_window,dummy_stake)
                    VALUES (:a,:b,:c,:d,:e,:f,:g,:h,:i,:j,:k,:l,:m,:n,:o,:p,:q,100)""",
                    a=obs_id,b=match_id,c=rid,d=rname,e=home,f=away,g=league,
                    h=mtype,i=line,j=side,k=action_type,l=entry_odd,m=exp,
                    n=gap,o=pres,p=confidence,q=val_window)

                # Update rule total_signals
                conn.run("UPDATE rules SET total_signals=total_signals+1,last_updated=NOW() WHERE id=:a", a=rid)

                log.info(f"🎯 SIGNAL: {rname} | {home} vs {away} | {mtype} {line} {side} @ {entry_odd} min:{minute}")
            except Exception as e:
                log.debug(f"Signal create: {e}")

# ─── Validation Engine ────────────────────────────────────────────────────────
def validate_trades(conn):
    """Resolve pending paper trades"""
    try:
        try:
            pending = conn.run("""SELECT id, match_id,
                COALESCE(rule_id, 0), COALESCE(rule_name, ''),
                COALESCE(action_type, ''), COALESCE(validation_window, '10m'),
                COALESCE(entry_odd, 1.0), COALESCE(selected_side, 'over'),
                COALESCE(market_type, 'FT'), COALESCE(line, 2.5), created_at,
                COALESCE(entry_score_home, 0), COALESCE(entry_score_away, 0),
                COALESCE(entry_total_goals, 0)
                FROM paper_trades WHERE result='pending'""")
        except Exception as eq:
            log.warning(f"Validate trades SELECT failed (skipping): {eq}")
            return

        for p in pending:
            (tid, mid, rid, rname, action_type, val_window, entry_odd, side,
             mtype, line, created_at, entry_h, entry_a, entry_total) = p

            # Fetch current match state separately
            try:
                mrow = conn.run("""SELECT score_home, score_away, minute, period, total_goals
                    FROM matches WHERE match_id=:a LIMIT 1""", a=mid)
                if mrow:
                    cur_h, cur_a, cur_min, cur_period, cur_goals = mrow[0]
                else:
                    cur_h, cur_a, cur_min, cur_period, cur_goals = 0, 0, 0, "FT", 0
            except:
                cur_h, cur_a, cur_min, cur_period, cur_goals = 0, 0, 0, "FT", 0

            now = datetime.now(timezone.utc)
            created = created_at if created_at.tzinfo else created_at.replace(tzinfo=timezone.utc)
            elapsed_min = (now - created).total_seconds() / 60

            result = None
            failure_reason = None
            profit = 0

            total_now = (cur_h or 0) + (cur_a or 0)
            entry_total = int(entry_total or 0)
            goals_since = max(0, total_now - entry_total)
            line_crossed = total_now > float(line or 0)

            if action_type == "OVER_LINE_WITHIN_10M":
                if goals_since > 0 and line_crossed:
                    result = "win"
                elif elapsed_min > 12:
                    result = "lose"; failure_reason = "No goal / line not crossed in 10min"

            elif action_type == "UNDER_HOLDS_10M":
                if goals_since > 0 and line_crossed:
                    result = "lose"; failure_reason = "Line crossed"
                elif elapsed_min > 12:
                    result = "win"

            elif action_type in ("H1_OVER_LINE_BEFORE_HT", "H1_GOAL_BEFORE_HT"):
                ht_done = cur_period in ("H2","FT") or (cur_period=="H1" and (cur_min or 0)>=46)
                if ht_done:
                    result = "win" if line_crossed else "lose"
                    if result=="lose": failure_reason = "Line not crossed by HT"
                elif elapsed_min > 65:
                    result = "lose"; failure_reason = "HT timeout"

            elif action_type == "UNDER_HOLDS_TO_HT":
                ht_done = cur_period in ("H2","FT") or (cur_period=="H1" and (cur_min or 0)>=46)
                if ht_done:
                    result = "win" if goals_since==0 else "lose"
                    if result=="lose": failure_reason = "Goal scored before HT"
                elif elapsed_min > 65:
                    result = "lose"; failure_reason = "HT timeout"

            elif action_type in ("OVER_LINE_BEFORE_FT","GOAL_BY_FT"):
                if cur_period == "FT":
                    result = "win" if line_crossed else "lose"
                    if result=="lose": failure_reason = "Line not crossed by FT"
                elif elapsed_min > 35:
                    result = "lose"; failure_reason = "FT timeout"

            if result:
                profit = round((float(entry_odd or 1)-1)*100, 2) if result=="win" else -100.0
                conn.run("""UPDATE paper_trades SET result=:a, resolved_at=NOW(),
                    dummy_profit_loss=:b, failure_reason=:c WHERE id=:d""",
                    a=result, b=profit, c=failure_reason, d=tid)
                try:
                    if result=="win":
                        conn.run("""UPDATE rules SET win_count=win_count+1,
                            win_rate=ROUND((win_count+1)::float/(win_count+lose_count+1)*100,1),
                            dummy_profit=dummy_profit+:a, last_updated=NOW() WHERE id=:b""",
                            a=profit, b=rid)
                    else:
                        conn.run("""UPDATE rules SET lose_count=lose_count+1,
                            win_rate=CASE WHEN (win_count+lose_count+1)>0
                                THEN ROUND(win_count::float/(win_count+lose_count+1)*100,1) ELSE 0 END,
                            dummy_profit=dummy_profit+:a, last_updated=NOW() WHERE id=:b""",
                            a=profit, b=rid)
                except: pass
                log.info(f"{'✅' if result=='win' else '❌'} {rname} {result} | profit:{profit}")
    except Exception as e:
        log.error(f"Validate trades: {e}")

        for p in pending:
            (tid, mid, rid, rname, action_type, val_window, entry_odd, side,
             mtype, line, created_at, cur_h, cur_a, cur_min, cur_period, cur_goals) = p

            now = datetime.now(timezone.utc)
            created = created_at if created_at.tzinfo else created_at.replace(tzinfo=timezone.utc)
            elapsed_min = (now - created).total_seconds() / 60

            result = None
            failure_reason = None
            profit = 0

            # Goals at trade entry — use closest snapshot before trade was created
            try:
                snap_at_entry = conn.run("""SELECT total_goals FROM odds_snapshots
                    WHERE match_id=:a AND captured_at <= :b
                    ORDER BY captured_at DESC LIMIT 1""",
                    a=mid, b=str(created_at))
                goals_at_entry = snap_at_entry[0][0] if snap_at_entry else 0
            except:
                goals_at_entry = 0

            total_now = (cur_h or 0) + (cur_a or 0)
            goals_since_entry = max(0, total_now - goals_at_entry)
            line_crossed_now = total_now > line

            # ── Validate by action type ─────────────────────────────────────
            if action_type == "OVER_LINE_WITHIN_10M":
                if goals_since_entry > 0 and line_crossed_now:
                    result = "win"
                elif elapsed_min > 12:
                    result = "lose"
                    failure_reason = "No goal / line not crossed in 10min"

            elif action_type == "UNDER_HOLDS_10M":
                if goals_since_entry > 0 and line_crossed_now:
                    result = "lose"; failure_reason = "Line crossed – goal scored"
                elif elapsed_min > 12:
                    result = "win"

            elif action_type in ("H1_OVER_LINE_BEFORE_HT", "H1_GOAL_BEFORE_HT"):
                h1_done = cur_period in ("H2", "FT") or (cur_period == "H1" and cur_min >= 46)
                if h1_done:
                    result = "win" if line_crossed_now else "lose"
                    if result == "lose": failure_reason = "Line not crossed by HT"
                elif elapsed_min > 65:
                    result = "lose"; failure_reason = "HT timeout"

            elif action_type == "UNDER_HOLDS_TO_HT":
                h1_done = cur_period in ("H2", "FT") or (cur_period == "H1" and cur_min >= 46)
                if h1_done:
                    result = "win" if goals_since_entry == 0 else "lose"
                    if result == "lose": failure_reason = "Goal scored before HT"
                elif elapsed_min > 65:
                    result = "lose"; failure_reason = "HT timeout"

            elif action_type in ("OVER_LINE_BEFORE_FT", "GOAL_BY_FT"):
                if cur_period == "FT":
                    result = "win" if line_crossed_now else "lose"
                    if result == "lose": failure_reason = "Line not crossed by FT"
                elif elapsed_min > 35:
                    result = "lose"; failure_reason = "FT timeout – no data"

            if result:
                profit = round((entry_odd - 1) * 100, 2) if result == "win" else -100.0
                conn.run("""UPDATE paper_trades SET result=:a, resolved_at=NOW(),
                    dummy_profit_loss=:b, failure_reason=:c WHERE id=:d""",
                    a=result, b=profit, c=failure_reason, d=tid)

                # Update rule win/lose + win_rate
                try:
                    if result == "win":
                        conn.run("""UPDATE rules SET win_count=win_count+1,
                            win_rate=ROUND((win_count+1)::float/(win_count+lose_count+1)*100,1),
                            dummy_profit=dummy_profit+:a, last_updated=NOW()
                            WHERE id=:b""", a=profit, b=rid)
                    else:
                        conn.run("""UPDATE rules SET lose_count=lose_count+1,
                            win_rate=CASE WHEN (win_count+lose_count+1)>0
                                THEN ROUND(win_count::float/(win_count+lose_count+1)*100,1)
                                ELSE 0 END,
                            dummy_profit=dummy_profit+:a, last_updated=NOW()
                            WHERE id=:b""", a=profit, b=rid)
                except: pass

                log.info(f"{'✅' if result=='win' else '❌'} Trade {result}: {rname} | profit:{profit}")
    except Exception as e:
        log.error(f"Validate trades: {e}")

# ─── Main Collector ───────────────────────────────────────────────────────────
def collect():
    try:
        events = fetch_events()
        if not events:
            return

        # Fetch odds in batches of 10
        all_ids = [str(e.get("id") or e.get("eventId") or "") for e in events if e.get("id") or e.get("eventId")]
        all_odds = {}
        for i in range(0, len(all_ids), 10):
            batch = all_ids[i:i+10]
            odds_batch = fetch_odds_multi(batch)
            for o in odds_batch:
                eid = str(o.get("id") or o.get("eventId") or "")
                if eid: all_odds[eid] = o

        conn = get_db()
        try:
            live_cnt = 0
            held_map = {}  # for rules engine

            for event in events:
                eid = str(event.get("id") or event.get("eventId") or "")
                odds_data = all_odds.get(eid)
                parsed = parse_event(event, odds_data)

                home = parsed["home"]; away = parsed["away"]
                if not home or not away: continue

                league   = parsed["league"]
                minute   = parsed["minute"]
                score_h  = parsed["score_home"]
                score_a  = parsed["score_away"]
                period   = parsed["period"]
                markets  = parsed["markets"]
                total    = score_h + score_a
                is_live = True  # all events from fetch_events() are live by definition
                if is_live: live_cnt += 1

                # Debug: log first event structure once to understand API format
                if live_cnt == 1 and len(markets) == 0:
                    log.info(f"🔍 Event sample: {json.dumps(event)[:500]}")

                match_id = f"oa_{eid}"

                # Upsert match
                try:
                    conn.run("""INSERT INTO matches
                        (match_id,event_id,league,home_team,away_team,minute,
                         score_home,score_away,total_goals,period,status,last_updated)
                        VALUES (:a,:b,:c,:d,:e,:f,:g,:h,:i,:j,:k,NOW())
                        ON CONFLICT (match_id) DO UPDATE SET
                        league=:c,home_team=:d,away_team=:e,
                        minute=:f,score_home=:g,score_away=:h,
                        total_goals=:i,period=:j,status=:k,last_updated=NOW()""",
                        a=match_id,b=eid,c=league,d=home,e=away,f=minute,
                        g=score_h,h=score_a,i=total,
                        j=period,k='live')
                except: pass

                # Detect goal
                prev_goals = last_scores.get(match_id)
                if prev_goals is not None and total > prev_goals and is_live:
                    log.info(f"⚽ GOAL: {home} vs {away} {score_h}-{score_a} min:{minute}")
                    goal_time = datetime.now(timezone.utc)
                    # Fetch recent snapshots for odds history
                    try:
                        snap = conn.run("""SELECT market_type, line, over_odd, under_odd
                            FROM odds_snapshots WHERE match_id=:a
                            AND captured_at > NOW()-INTERVAL '5 minutes'
                            ORDER BY captured_at DESC LIMIT 30""", a=match_id)
                        snap_dict = {}
                        for s in snap:
                            k = f"{s[0]}_{s[1]}"
                            if k not in snap_dict:
                                snap_dict[k] = {"over": s[2], "under": s[3]}
                        conn.run("""INSERT INTO goals
                            (match_id,minute,goal_time,score_before,score_after,period,
                             auto_detected,had_snapshots,odds_30s,odds_60s)
                            VALUES (:a,:b,:c,:d,:e,:f,TRUE,:g,:h,:i)""",
                            a=match_id, b=minute,
                            c=str(goal_time),
                            d=str(prev_goals), e=f"{score_h}-{score_a}",
                            f=period, g=bool(snap_dict),
                            h=json.dumps(snap_dict), i=json.dumps(snap_dict))
                        # Mark recent snapshots
                        for t, col in [(2,"goal_2m"),(5,"goal_5m"),(10,"goal_10m")]:
                            try:
                                conn.run(f"UPDATE odds_snapshots SET {col}=TRUE WHERE match_id=:a AND captured_at>NOW()-INTERVAL '{t} minutes'", a=match_id)
                            except: pass
                    except Exception as e:
                        log.error(f"Goal save: {e}")

                last_scores[match_id] = total

                # Process each market
                for mkt in markets:
                    mtype = mkt["market_type"]
                    line  = mkt["line"]
                    over  = mkt.get("over_odd")
                    under = mkt.get("under_odd")
                    if not over and not under: continue

                    hkey = cache_key(match_id, mtype, line)
                    now  = time.time()

                    # Track price changes
                    prev_over = prev_under = None
                    held = 0; direction = "stable"
                    delta_o = delta_u = 0.0

                    if hkey in price_cache:
                        pc = price_cache[hkey]
                        prev_over  = pc.get("over")
                        prev_under = pc.get("under")
                        if over and prev_over:
                            delta_o = round(over - prev_over, 3)
                        if under and prev_under:
                            delta_u = round(under - prev_under, 3)
                        # Direction based on over movement
                        if abs(delta_o) < 0.005:
                            held = int(now - pc.get("since", now))
                            direction = "stable"
                        else:
                            direction = "down" if delta_o < 0 else "up"
                            price_cache[hkey] = {"over":over,"under":under,"since":now}
                    else:
                        price_cache[hkey] = {"over":over,"under":under,"since":now}
                    held = int(now - price_cache[hkey]["since"])
                    held_map[hkey] = held

                    # Opening odds
                    set_opening(match_id, mtype, str(line), over, under)
                    op = get_opening(match_id, mtype, str(line))
                    opening_over  = op.get("over")  if op else over
                    opening_under = op.get("under") if op else under

                    # Expected + gap
                    exp_over  = get_expected(mtype, str(line), minute)
                    exp_under = None
                    if exp_over and over: exp_under = None  # we'll compute if needed
                    gap_over = round(exp_over - over, 3) if exp_over and over else 0
                    gap_ratio = round(over / exp_over, 3) if exp_over and over else 1
                    pressure  = calc_pressure(over, opening_over, exp_over) if over and opening_over and exp_over else 0

                    # Movement type
                    movement = "stable"
                    if direction == "up" and abs(delta_o) > 0.10: movement = "spike_up"
                    elif direction == "down" and abs(delta_o) > 0.10: movement = "sharp_drop"
                    elif held > 120 and direction == "stable": movement = "frozen"
                    elif direction == "up": movement = "slow_rise"
                    elif direction == "down": movement = "slow_drop"

                    # Save snapshot
                    try:
                        conn.run("""INSERT INTO odds_snapshots
                            (match_id,minute,score_home,score_away,total_goals,period,
                             market_type,line,bookmaker,over_odd,under_odd,
                             opening_over,opening_under,prev_over,prev_under,
                             delta_over,delta_under,direction,held_seconds,
                             expected_over,gap_over,gap_ratio_over,
                             pressure_score,movement_type,is_live)
                            VALUES (:a,:b,:c,:d,:e,:f,:g,:h,'Bet365',
                                    :i,:j,:k,:l,:m,:n,:o,:p,:q,:r,:s,:t,:u,:v,:w,:x)""",
                            a=match_id,b=minute,c=score_h,d=score_a,e=total,f=period,
                            g=mtype,h=line,i=over,j=under,
                            k=opening_over,l=opening_under,m=prev_over,n=prev_under,
                            o=delta_o,p=delta_u,q=direction,r=held,
                            s=exp_over,t=gap_over,u=gap_ratio,
                            v=pressure,w=movement,x=is_live)
                    except Exception as e:
                        log.debug(f"Snapshot save: {e}")

                # Check rules only if we have market data
                if markets:
                    check_rules(conn, match_id, home, away, league,
                               minute, score_h, score_a, period, markets, held_map)

            # Validate pending paper trades
            validate_trades(conn)
            log.info(f"✅ Saved | live:{live_cnt}/{len(events)}")
        finally:
            conn.close()
    except Exception as e:
        log.error(f"Collect error: {e}")

def collector_loop():
    time.sleep(5)
    while True:
        collect()
        time.sleep(POLL_INTERVAL)

# ─── Dashboard HTML ───────────────────────────────────────────────────────────
HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>PapaGoal</title>
<link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;700&family=Inter:wght@300;400;600;700;900&display=swap" rel="stylesheet">
<style>
:root{--bg:#0A0F1E;--bg2:#0F172A;--card:#131929;--card2:#1a2235;--border:#1e2d45;--border2:#243452;--blue:#3B82F6;--green:#10B981;--red:#EF4444;--yellow:#F59E0B;--purple:#8B5CF6;--text:#E2E8F0;--muted:#64748B}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--text);font-family:'Inter',sans-serif;min-height:100vh;display:flex}
.sidebar{width:220px;min-height:100vh;background:var(--bg2);border-right:1px solid var(--border);display:flex;flex-direction:column;position:fixed;top:0;left:0;bottom:0;z-index:100}
.logo{padding:20px 16px;border-bottom:1px solid var(--border)}
.logo-main{font-family:'JetBrains Mono',monospace;font-size:17px;font-weight:700;color:#fff;letter-spacing:2px}
.logo-main span{color:var(--blue)}
.logo-sub{font-size:10px;color:var(--muted);margin-top:2px}
.nav{flex:1;padding:12px 8px}
.nav-item{display:flex;align-items:center;gap:10px;padding:9px 12px;border-radius:8px;font-size:13px;color:var(--muted);cursor:pointer;transition:all 0.15s;margin-bottom:2px;border:none;background:none;width:100%;text-align:left;font-family:'Inter',sans-serif}
.nav-item:hover{background:var(--card);color:var(--text)}
.nav-item.active{background:rgba(59,130,246,0.15);color:var(--blue)}
.main{margin-left:220px;flex:1}
.page{display:none;padding:24px;max-width:1300px}
.page.active{display:block}
.ph{margin-bottom:20px;display:flex;justify-content:space-between;align-items:flex-start;flex-wrap:wrap;gap:10px}
.pt{font-size:22px;font-weight:700}
.ps{font-size:12px;color:var(--muted);font-family:'JetBrains Mono',monospace;margin-top:4px}
.sr{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-bottom:20px}
.sc{background:var(--card);border:1px solid var(--border);border-radius:10px;padding:14px}
.sn{font-size:26px;font-weight:900;font-family:'JetBrains Mono',monospace}
.sl{font-size:11px;color:var(--muted);margin-top:4px}
.stit{font-size:11px;letter-spacing:3px;color:var(--muted);text-transform:uppercase;margin-bottom:12px;padding-bottom:8px;border-bottom:1px solid var(--border)}
.card{background:var(--card);border:1px solid var(--border);border-radius:12px;padding:16px;margin-bottom:10px;transition:border-color 0.2s}
.card.goal{border-color:rgba(59,130,246,0.5);background:linear-gradient(135deg,rgba(59,130,246,0.05),var(--card))}
.card.win{border-color:rgba(16,185,129,0.5)}
.card.lose{border-color:rgba(239,68,68,0.4)}
.card.hot{border-color:rgba(59,130,246,0.8);box-shadow:0 0 15px rgba(59,130,246,0.15)}
.ctop{display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:10px;gap:8px}
.mn{font-size:15px;font-weight:700}
.ml{font-size:11px;color:var(--muted);margin-top:2px}
.bgs{display:flex;gap:5px;align-items:center;flex-wrap:wrap}
.bg{padding:3px 8px;border-radius:5px;font-size:11px;font-weight:600;font-family:'JetBrains Mono',monospace}
.bgb{background:rgba(59,130,246,0.15);color:var(--blue);border:1px solid rgba(59,130,246,0.3)}
.bgg{background:rgba(16,185,129,0.12);color:var(--green);border:1px solid rgba(16,185,129,0.3)}
.bgr{background:rgba(239,68,68,0.12);color:var(--red);border:1px solid rgba(239,68,68,0.3)}
.bgy{background:rgba(245,158,11,0.12);color:var(--yellow);border:1px solid rgba(245,158,11,0.3)}
.bgp{background:rgba(139,92,246,0.12);color:var(--purple);border:1px solid rgba(139,92,246,0.3)}
.or{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:10px}
.ot{background:var(--card2);border:1px solid var(--border2);border-radius:6px;padding:5px 10px;font-family:'JetBrains Mono',monospace;font-size:12px;display:flex;flex-direction:column;align-items:center;gap:1px;min-width:60px}
.ol{font-size:9px;color:var(--muted);letter-spacing:1px}
.ov{font-size:13px;font-weight:700}
.rec-box{background:rgba(59,130,246,0.08);border:1px solid rgba(59,130,246,0.3);border-radius:8px;padding:12px;margin-bottom:8px}
.rec-title{font-size:13px;font-weight:700;color:var(--blue);margin-bottom:6px}
.rec-row{display:flex;gap:16px;font-size:12px;color:var(--muted);flex-wrap:wrap}
.rec-val{color:var(--text);font-weight:600;font-family:'JetBrains Mono',monospace}
.pbar{height:4px;background:var(--border2);border-radius:2px;margin:6px 0;overflow:hidden}
.pfill{height:100%;border-radius:2px}
.pb6{height:6px;background:var(--card2);border-radius:3px;overflow:hidden;margin:3px 0}
.pf6{height:100%;border-radius:3px}
.status-badge{padding:2px 8px;border-radius:4px;font-size:10px;font-weight:700;font-family:'JetBrains Mono',monospace}
.s-validated{background:rgba(16,185,129,0.15);color:var(--green)}
.s-promising{background:rgba(59,130,246,0.15);color:var(--blue)}
.s-testing{background:rgba(245,158,11,0.15);color:var(--yellow)}
.s-rejected{background:rgba(239,68,68,0.15);color:var(--red)}
.s-dangerous{background:rgba(239,68,68,0.2);color:var(--red)}
.s-active{background:rgba(139,92,246,0.15);color:var(--purple)}
.toggle{padding:4px 12px;border-radius:6px;font-size:11px;font-weight:700;cursor:pointer;border:1px solid transparent;font-family:'JetBrains Mono',monospace;transition:all 0.2s}
.ton{background:rgba(16,185,129,0.15);color:var(--green);border-color:rgba(16,185,129,0.3)!important}
.toff{background:rgba(255,255,255,0.05);color:var(--muted);border-color:var(--border)!important}
.abtn{background:rgba(139,92,246,0.1);border:1px solid rgba(139,92,246,0.3);color:var(--purple);border-radius:8px;padding:9px 18px;font-size:13px;font-family:'Inter',sans-serif;font-weight:600;cursor:pointer;transition:all 0.2s}
.abtn:hover{background:rgba(139,92,246,0.2)}
.abtn:disabled{opacity:0.5;cursor:not-allowed}
.otg{display:grid;grid-template-columns:repeat(5,1fr);gap:6px;margin-top:10px}
.otc{background:var(--card2);border-radius:6px;padding:6px;text-align:center}
.empty{text-align:center;padding:60px 20px;color:var(--muted)}
.ldot{width:8px;height:8px;border-radius:50%;background:var(--blue);animation:blink 1.2s infinite;display:inline-block;margin-right:6px}
.upd{font-size:11px;color:var(--muted);font-family:'JetBrains Mono',monospace}
.tc{display:grid;grid-template-columns:1fr 1fr;gap:16px}
@keyframes blink{0%,100%{opacity:1}50%{opacity:0.2}}
::-webkit-scrollbar{width:4px}::-webkit-scrollbar-track{background:var(--bg)}::-webkit-scrollbar-thumb{background:var(--border2);border-radius:2px}
@media(max-width:900px){.sidebar{width:52px}.main{margin-left:52px}.sidebar .nav-item span:last-child,.logo-sub,.logo-main{display:none}.sr{grid-template-columns:repeat(2,1fr)}.tc{grid-template-columns:1fr}.otg{grid-template-columns:repeat(3,1fr)}}
</style></head>
<body>
<div class="sidebar">
  <div class="logo"><div class="logo-main">PAPA<span>GOAL</span></div><div class="logo-sub">READ THE MARKET</div></div>
  <nav class="nav">
    <button class="nav-item active" onclick="show('live',this)"><span>📡</span><span>Live Dashboard</span></button>
    <button class="nav-item" onclick="show('goals',this)"><span>⚽</span><span>Goals</span></button>
    <button class="nav-item" onclick="show('trades',this)"><span>📈</span><span>Simulation</span></button>
    <button class="nav-item" onclick="show('obs',this)"><span>🔥</span><span>Observations</span></button>
    <button class="nav-item" onclick="show('rules',this)"><span>📋</span><span>Rules Engine</span></button>
    <button class="nav-item" onclick="show('analytics',this)"><span>📊</span><span>Analytics</span></button>
    <button class="nav-item" onclick="show('ai',this)"><span>🤖</span><span>AI Insights</span></button>
    <button class="nav-item" onclick="show('debug',this)"><span>🔧</span><span>API Debug</span></button>
  </nav>
</div>
<div class="main">

<div class="page active" id="p-live">
  <div class="ph"><div><div class="pt"><span class="ldot"></span>Live Dashboard</div><div class="ps">Don't predict football. Read the market.</div></div><div class="upd" id="upd">Updating...</div></div>
  <div class="sr">
    <div class="sc"><div class="sn" style="color:var(--blue)" id="sl">—</div><div class="sl">Live Matches</div></div>
    <div class="sc"><div class="sn" style="color:var(--green)" id="sh">—</div><div class="sl">Active Signals</div></div>
    <div class="sc"><div class="sn" style="color:var(--yellow)" id="sg">—</div><div class="sl">Goals Today</div></div>
    <div class="sc"><div class="sn" style="color:var(--purple)" id="st">—</div><div class="sl">Open Trades</div></div>
  </div>
  <div class="stit">🎯 Active Recommendations</div>
  <div id="live-cards"><div class="empty"><div style="font-size:42px">📡</div><div>Scanning live matches...</div></div></div>
  <div class="stit" style="margin-top:20px">📡 All Live Matches</div>
  <div id="all-matches"><div class="empty" style="padding:20px">Loading matches...</div></div>
</div>

<div class="page" id="p-goals">
  <div class="ph"><div><div class="pt">⚽ Goals Detected</div><div class="ps">Odds before each goal – core learning data</div></div></div>
  <div id="goals-list"><div class="empty"><div style="font-size:42px">⚽</div><div>Loading goals...</div></div></div>
</div>

<div class="page" id="p-trades">
  <div class="ph"><div><div class="pt">📈 Simulation</div><div class="ps">Paper Trading – measuring rule accuracy</div></div></div>
  <div id="trades-content"><div class="empty"><div style="font-size:42px">📈</div><div>Loading...</div></div></div>
</div>

<div class="page" id="p-obs">
  <div class="ph"><div><div class="pt">🔥 Observations</div><div class="ps">All signals from last 3 hours</div></div></div>
  <div id="obs-list"><div class="empty"><div style="font-size:42px">🔥</div><div>Loading...</div></div></div>
</div>

<div class="page" id="p-rules">
  <div class="ph">
    <div><div class="pt">📋 Rules Engine</div><div class="ps">Rule lifecycle · hit rates · AI suggestions</div></div>
    <button class="abtn" onclick="runAIRules()" id="ai-rules-btn">🤖 AI: Improve Rules</button>
  </div>
  <div class="sr">
    <div class="sc"><div class="sn" style="color:var(--green)" id="ra">—</div><div class="sl">Active Rules</div></div>
    <div class="sc"><div class="sn" style="color:var(--blue)" id="rv">—</div><div class="sl">Validated</div></div>
    <div class="sc"><div class="sn" style="color:var(--yellow)" id="rt">—</div><div class="sl">Total Signals</div></div>
    <div class="sc"><div class="sn" style="color:var(--purple)" id="rp">—</div><div class="sl">Dummy Profit</div></div>
  </div>
  <div id="rules-list"><div class="empty"><div style="font-size:42px">📋</div><div>Loading...</div></div></div>
</div>

<div class="page" id="p-analytics">
  <div class="ph"><div><div class="pt">📊 Analytics</div><div class="ps">Pattern analysis & performance metrics</div></div></div>
  <div id="analytics-content"><div class="empty"><div style="font-size:42px">📊</div><div>Loading...</div></div></div>
</div>

<div class="page" id="p-ai">
  <div class="ph">
    <div><div class="pt">🤖 AI Insights</div><div class="ps">Claude analyzes patterns & suggests rules</div></div>
    <button class="abtn" onclick="runAI()" id="ai-btn">🤖 Run Analysis</button>
  </div>
  <div id="ai-content"><div class="empty"><div style="font-size:42px">🤖</div><div>Click Run Analysis to get insights</div></div></div>
</div>

<div class="page" id="p-debug">
  <div class="ph">
    <div><div class="pt">🔧 API Debug</div><div class="ps">Raw response from odds-api.io — diagnose parsing issues</div></div>
    <button class="abtn" onclick="loadDebug()">🔄 Fetch Now</button>
  </div>
  <div id="debug-content"><div class="empty"><div style="font-size:42px">🔧</div><div>Click Fetch Now to inspect the API response</div></div></div>
</div>

</div>
<script>
let cur='live';
const statusClass={'VALIDATED':'s-validated','PROMISING':'s-promising','TESTING':'s-testing','ACTIVE':'s-active','REJECTED':'s-rejected','DANGEROUS':'s-dangerous'};

function show(p,btn){
  document.querySelectorAll('.page').forEach(x=>x.classList.remove('active'));
  document.querySelectorAll('.nav-item').forEach(x=>x.classList.remove('active'));
  document.getElementById('p-'+p).classList.add('active');
  if(btn) btn.classList.add('active');
  cur=p;
  const fn={goals:loadGoals,trades:loadTrades,obs:loadObs,rules:loadRules,analytics:loadAnalytics,ai:loadAI,debug:loadDebug};
  if(fn[p]) fn[p]();
}

async function loadLive(){
  try{
    const[st,obs,ai,matches]=await Promise.all([
      fetch('/api/stats').then(r=>r.json()),
      fetch('/api/signals').then(r=>r.json()),
      fetch('/api/ai_live').then(r=>r.json()),
      fetch('/api/matches').then(r=>r.json())
    ]);
    document.getElementById('sl').textContent=st.live||0;
    document.getElementById('sh').textContent=st.signals||0;
    document.getElementById('sg').textContent=st.goals_today||0;
    document.getElementById('st').textContent=st.open_trades||0;
    document.getElementById('upd').textContent='Updated: '+new Date().toLocaleTimeString();
    const aiMap={};ai.forEach(a=>aiMap[a.match_id]=a.analysis);
    const el=document.getElementById('live-cards');
    if(!obs.length){
      el.innerHTML='<div class="empty"><div style="font-size:36px">✅</div><div>No active signals yet</div></div>';
    } else {
      const bm={};
      obs.forEach(o=>{if(!bm[o.match_id]) bm[o.match_id]={...o,signals:[]};bm[o.match_id].signals.push(o);});
      el.innerHTML=Object.values(bm).map(m=>{
        const ai=aiMap[m.match_id]?`<div style="background:rgba(59,130,246,0.06);border:1px solid rgba(59,130,246,0.2);border-radius:8px;padding:10px;margin-top:8px;font-size:13px;line-height:1.6;color:#94a3b8"><div style="font-size:10px;letter-spacing:2px;color:var(--blue);margin-bottom:4px">🤖 CLAUDE AI</div>${aiMap[m.match_id]}</div>`:'';
        const sigs=m.signals.map(s=>`<div class="rec-box">
          <div class="rec-title">🎯 ${s.action_type} · ${s.rule_name}</div>
          <div class="rec-row">
            <span>Market: <span class="rec-val">${s.market_type} ${s.line}</span></span>
            <span>Side: <span class="rec-val">${(s.selected_side||'').toUpperCase()}</span></span>
            <span>Odd: <span class="rec-val" style="color:var(--yellow)">${s.entry_odd||'—'}</span></span>
            <span>Gap: <span class="rec-val" style="color:${(s.gap||0)>0?'var(--green)':'var(--red)'}">${s.gap||0}</span></span>
            <span>Pressure: <span class="rec-val">${s.pressure||0}%</span></span>
            <span style="color:var(--green)">📈 Paper Trade Created</span>
          </div>
        </div>`).join('');
        return `<div class="card hot">
          <div class="ctop">
            <div><div class="mn">${m.home_team} vs ${m.away_team}</div><div class="ml">${m.league||''}</div></div>
            <div class="bgs">
              ${m.minute>0?`<span class="bg bgb">⏱ ${m.minute}'</span>`:''}
              ${m.score&&m.score!='0-0'?`<span class="bg bgy">${m.score}</span>`:''}
              <span class="bg bgg">🎯 SIGNAL</span>
            </div>
          </div>${sigs}${ai}</div>`;
      }).join('');
    }
    // Show all matches
    const mel=document.getElementById('all-matches');
    if(!matches.length){
      mel.innerHTML='<div style="color:var(--muted);font-size:12px;padding:10px">No matches in DB yet — waiting for first collection cycle</div>';
    } else {
      mel.innerHTML='<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:8px">'+
        matches.map(m=>`<div class="card" style="padding:12px">
          <div style="font-size:13px;font-weight:700;margin-bottom:4px">${m.home_team} vs ${m.away_team}</div>
          <div style="font-size:10px;color:var(--muted);margin-bottom:6px">${m.league||'Unknown League'}</div>
          <div class="bgs">
            <span class="bg bgb">⏱ ${m.minute}'</span>
            <span class="bg bgy">${m.score_home}-${m.score_away}</span>
            <span class="bg ${m.period==='H1'?'bgb':m.period==='H2'?'bgp':'bgg'}">${m.period}</span>
          </div>
        </div>`).join('')+'</div>';
    }
  }catch(e){console.error(e);}
}

async function loadGoals(){
  const goals=await fetch('/api/goals').then(r=>r.json()).catch(()=>[]);
  const el=document.getElementById('goals-list');
  if(!goals.length){el.innerHTML='<div class="empty"><div style="font-size:42px">⚽</div><div>No goals yet</div></div>';return;}
  el.innerHTML=goals.map(g=>{
    const snap=g.odds_30s||{};
    const getOdd=k=>Object.entries(snap).find(([key])=>key.includes(k))?.[1]?.over?.toFixed(2)||'—';
    return `<div class="card win">
      <div class="ctop">
        <div><div class="mn">${g.home_team||''} vs ${g.away_team||''}</div><div class="ml">${g.league||''} · ${g.period||'FT'}</div></div>
        <div style="font-size:16px;font-weight:700;font-family:'JetBrains Mono',monospace;color:var(--green)">⚽ Min ${g.minute}</div>
      </div>
      <div style="font-size:12px;color:var(--muted);margin-bottom:6px">${g.score_before||'?'} → ${g.score_after||'?'} ${g.had_snapshots?'✅ has odds data':'⚠️ no odds yet'}</div>
      <div class="stit" style="margin-bottom:8px">Over odds before goal</div>
      <div class="otg">
        <div class="otc"><div class="ol">30s before</div><div class="ov" style="color:var(--green)">${getOdd('FT')}</div></div>
        <div class="otc"><div class="ol">60s before</div><div class="ov" style="color:var(--green)">${getOdd('H1')}</div></div>
      </div>
    </div>`;
  }).join('');
}

async function loadTrades(){
  const trades=await fetch('/api/trades').then(r=>r.json()).catch(()=>[]);
  const el=document.getElementById('trades-content');
  const pend=trades.filter(t=>t.result==='pending');
  const wins=trades.filter(t=>t.result==='win');
  const lose=trades.filter(t=>t.result==='lose');
  const total=wins.length+lose.length;
  const pct=total>0?Math.round(wins.length/total*100):0;
  const profit=trades.reduce((s,t)=>s+(t.dummy_profit_loss||0),0);
  el.innerHTML=`
    <div class="sr">
      <div class="sc"><div class="sn" style="color:var(--yellow)">${pend.length}</div><div class="sl">⏳ Pending</div></div>
      <div class="sc"><div class="sn" style="color:var(--green)">${wins.length}</div><div class="sl">✅ Win</div></div>
      <div class="sc"><div class="sn" style="color:var(--red)">${lose.length}</div><div class="sl">❌ Lose</div></div>
      <div class="sc"><div class="sn" style="color:${profit>=0?'var(--green)':'var(--red)'}">${pct}% · €${profit.toFixed(0)}</div><div class="sl">Hit Rate · P&L</div></div>
    </div>
    <div class="stit">All Trades (${trades.length})</div>
    ${!trades.length?'<div class="empty"><div style="font-size:42px">📈</div><div>No trades yet</div></div>':
      trades.map(t=>{
        const rc=t.result==='pending'?'bgy':t.result==='win'?'bgg':'bgr';
        const rl=t.result==='pending'?'⏳ PENDING':t.result==='win'?'✅ WIN':'❌ LOSE';
        const bc=t.result==='pending'?'var(--yellow)':t.result==='win'?'var(--green)':'var(--red)';
        return `<div class="card" style="border-color:${bc}33">
          <div class="ctop">
            <div><div class="mn">${t.home_team} vs ${t.away_team}</div>
            <div class="ml">${t.rule_name} · ${t.market_type} ${t.line} ${t.selected_side?.toUpperCase()}</div></div>
            <div class="bgs">
              ${t.minute_entry>0?`<span class="bg bgb">⏱ ${t.minute_entry}'</span>`:''}
              <span class="bg ${rc}">${rl}</span>
            </div>
          </div>
          <div class="or">
            <div class="ot"><div class="ol">ENTRY ODD</div><div class="ov" style="color:var(--yellow)">${t.entry_odd||'—'}</div></div>
            <div class="ot"><div class="ol">EXPECTED</div><div class="ov">${t.expected_odd||'—'}</div></div>
            <div class="ot"><div class="ol">GAP</div><div class="ov" style="color:var(--blue)">${t.gap||0}</div></div>
            <div class="ot"><div class="ol">PRESSURE</div><div class="ov">${t.pressure_score||0}%</div></div>
            ${t.result!=='pending'?`<div class="ot"><div class="ol">P&L</div><div class="ov" style="color:${(t.dummy_profit_loss||0)>=0?'var(--green)':'var(--red)'}">€${(t.dummy_profit_loss||0).toFixed(0)}</div></div>`:''}
          </div>
          <div style="font-size:11px;color:var(--muted)">${t.action_type} · ${t.validation_window} window · Score: ${t.score_entry||'?'}</div>
          ${t.failure_reason?`<div style="font-size:11px;color:var(--red);margin-top:4px">Reason: ${t.failure_reason}</div>`:''}
        </div>`;
      }).join('')}`;
}

async function loadObs(){
  const obs=await fetch('/api/observations').then(r=>r.json()).catch(()=>[]);
  const el=document.getElementById('obs-list');
  if(!obs.length){el.innerHTML='<div class="empty"><div style="font-size:42px">🔥</div><div>No observations</div></div>';return;}
  el.innerHTML=obs.map(o=>`
    <div class="card">
      <div class="ctop">
        <div><div class="mn">${o.home_team} vs ${o.away_team}</div>
        <div class="ml">${o.rule_name} · ${o.league||''}</div></div>
        <div class="bgs">
          ${o.minute>0?`<span class="bg bgb">⏱ ${o.minute}'</span>`:''}
          <span class="bg bgy">${o.market_type} ${o.line}</span>
          <span class="bg bgp">${o.action_type}</span>
        </div>
      </div>
      <div class="or">
        <div class="ot"><div class="ol">OVER</div><div class="ov">${o.over_odd||'—'}</div></div>
        <div class="ot"><div class="ol">EXPECTED</div><div class="ov">${o.expected_odd||'—'}</div></div>
        <div class="ot"><div class="ol">GAP</div><div class="ov" style="color:var(--blue)">${o.gap||0}</div></div>
        <div class="ot"><div class="ol">PRESSURE</div><div class="ov">${o.pressure_score||0}%</div></div>
        <div class="ot"><div class="ol">CONF</div><div class="ov">${o.confidence_estimate||50}%</div></div>
      </div>
      <div style="font-size:12px;color:var(--muted)">${o.reason||''}</div>
    </div>`).join('');
}

async function loadRules(){
  const rules=await fetch('/api/rules').then(r=>r.json()).catch(()=>[]);
  const el=document.getElementById('rules-list');
  document.getElementById('ra').textContent=rules.filter(r=>r.is_active).length;
  document.getElementById('rv').textContent=rules.filter(r=>r.status==='VALIDATED').length;
  document.getElementById('rt').textContent=rules.reduce((s,r)=>s+(r.total_signals||0),0);
  const prof=rules.reduce((s,r)=>s+(r.dummy_profit||0),0);
  document.getElementById('rp').textContent=(prof>=0?'+':'')+'€'+prof.toFixed(0);
  if(!rules.length){el.innerHTML='<div class="empty">No rules</div>';return;}
  el.innerHTML=rules.map(r=>{
    const wr=parseFloat(r.win_rate||0);
    const wc=wr>=60?'var(--green)':wr>=45?'var(--yellow)':'var(--red)';
    const resolved=(r.win_count||0)+(r.lose_count||0);
    const pending=(r.total_signals||0)-resolved;
    const prof=r.dummy_profit||0;
    const sideLabel=r.selected_side==='under'?'⬇ UNDER':'⬆ OVER';
    const sideColor=r.selected_side==='under'?'var(--purple)':'var(--green)';
    // Conditions summary
    const oddRange=r.selected_side==='under'
      ?`Under ${r.under_odd_min||'?'}–${r.under_odd_max||'?'}`
      :`Over ${r.over_odd_min||'?'}–${r.over_odd_max||'?'}`;
    return `<div class="card" style="border-color:${r.is_active?'var(--border2)':'var(--border)'}">
      <div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:10px;gap:8px">
        <div style="flex:1">
          <div style="display:flex;align-items:center;gap:8px;margin-bottom:4px;flex-wrap:wrap">
            <span style="font-size:14px;font-weight:700;color:${r.is_active?'var(--text)':'var(--muted)'}">${r.source==='ai'?'🤖 ':'📋 '}${r.rule_name}</span>
            <span class="status-badge ${statusClass[r.status]||'s-active'}">${r.status}</span>
            <span style="font-size:11px;font-weight:700;color:${sideColor};font-family:'JetBrains Mono',monospace">${sideLabel}</span>
          </div>
          <div style="font-size:11px;color:var(--muted);margin-bottom:6px">${r.description||''}</div>
          <div style="font-size:10px;color:var(--muted);font-family:'JetBrains Mono',monospace;display:flex;gap:12px;flex-wrap:wrap">
            <span>📅 min ${r.minute_min}–${r.minute_max}</span>
            <span>📊 ${r.market_type} ${r.line_min}–${r.line_max}</span>
            <span>💰 ${oddRange}</span>
            <span>⏱ window: ${r.validation_window}</span>
            <span>🎯 ${r.action_type}</span>
          </div>
        </div>
        <button class="toggle ${r.is_active?'ton':'toff'}" onclick="toggleRule('${r.rule_name}',${!r.is_active})">${r.is_active?'ON':'OFF'}</button>
      </div>
      <div style="background:var(--bg2);border-radius:8px;padding:10px;margin-top:8px">
        <div style="display:grid;grid-template-columns:repeat(5,1fr);gap:8px;text-align:center;margin-bottom:8px">
          <div><div style="font-size:18px;font-weight:900;font-family:'JetBrains Mono',monospace;color:var(--blue)">${r.total_signals||0}</div><div style="font-size:10px;color:var(--muted)">SIGNALS</div></div>
          <div><div style="font-size:18px;font-weight:900;font-family:'JetBrains Mono',monospace;color:var(--green)">${r.win_count||0}</div><div style="font-size:10px;color:var(--muted)">WON</div></div>
          <div><div style="font-size:18px;font-weight:900;font-family:'JetBrains Mono',monospace;color:var(--red)">${r.lose_count||0}</div><div style="font-size:10px;color:var(--muted)">LOST</div></div>
          <div><div style="font-size:18px;font-weight:900;font-family:'JetBrains Mono',monospace;color:var(--yellow)">${pending}</div><div style="font-size:10px;color:var(--muted)">PENDING</div></div>
          <div><div style="font-size:18px;font-weight:900;font-family:'JetBrains Mono',monospace;color:${prof>=0?'var(--green)':'var(--red)'}">${prof>=0?'+':''}€${prof.toFixed(0)}</div><div style="font-size:10px;color:var(--muted)">P&L</div></div>
        </div>
        <div style="display:flex;align-items:center;gap:10px">
          <div style="flex:1"><div class="pb6"><div class="pf6" style="width:${Math.min(100,wr)}%;background:${wc}"></div></div></div>
          <span style="font-size:13px;font-family:'JetBrains Mono',monospace;font-weight:700;color:${wc};width:44px;text-align:right">${wr.toFixed(1)}%</span>
          <span style="font-size:10px;color:var(--muted)">${resolved} resolved</span>
        </div>
      </div>
    </div>`;
  }).join('');
}

async function toggleRule(name,state){
  try{await fetch('/api/rules/toggle',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({rule_name:name,is_active:state})});loadRules();}catch(e){console.error(e);}
}

async function runAIRules(){
  const btn=document.getElementById('ai-rules-btn');
  btn.disabled=true;btn.textContent='🤖 Analyzing...';
  try{
    const r=await fetch('/api/ai_rules',{method:'POST'});
    const d=await r.json();
    if(d.error)alert('Error: '+d.error);
    else{alert(`AI done! ${d.new_rules||0} new rule suggestions added.`);loadRules();}
  }catch(e){alert('Error');}
  btn.disabled=false;btn.textContent='🤖 AI: Improve Rules';
}

async function loadAnalytics(){
  const data=await fetch('/api/analytics').then(r=>r.json()).catch(()=>({}));
  const el=document.getElementById('analytics-content');
  const targets=[
    {l:"Goals collected",v:data.total_goals,t:500,c:"var(--green)"},
    {l:"Snapshots saved",v:data.total_snapshots,t:50000,c:"var(--blue)"},
    {l:"Paper trades",v:data.total_trades,t:200,c:"var(--purple)"},
    {l:"Observations",v:data.total_obs,t:1000,c:"var(--yellow)"}
  ];
  el.innerHTML=`
    <div class="sr">
      <div class="sc"><div class="sn" style="color:var(--green)">${data.total_goals||0}</div><div class="sl">Goals</div></div>
      <div class="sc"><div class="sn" style="color:var(--blue)">${(data.total_snapshots||0).toLocaleString()}</div><div class="sl">Snapshots</div></div>
      <div class="sc"><div class="sn" style="color:var(--yellow)">${data.total_obs||0}</div><div class="sl">Observations</div></div>
      <div class="sc"><div class="sn" style="color:${(data.success_rate||0)>=55?'var(--green)':'var(--red)'}">${data.success_rate||0}%</div><div class="sl">Hit Rate</div></div>
    </div>
    <div class="tc">
      <div class="card">
        <div class="stit">Collection Progress</div>
        ${targets.map(t=>`
          <div style="display:flex;justify-content:space-between;margin-top:12px;font-size:12px">
            <span style="color:var(--muted)">${t.l}</span>
            <span style="color:${t.c};font-family:'JetBrains Mono',monospace">${t.v||0} / ${t.t}</span>
          </div>
          <div class="pb6"><div class="pf6" style="width:${Math.min(100,(t.v||0)/t.t*100)}%;background:${t.c}"></div></div>
        `).join('')}
      </div>
      <div class="card">
        <div class="stit">Top Rules by Signals</div>
        ${(data.top_rules||[]).map(r=>`
          <div style="display:flex;justify-content:space-between;padding:8px 0;border-bottom:1px solid var(--border);font-size:13px">
            <span style="color:var(--muted)">${r.rule_name}</span>
            <span style="color:var(--blue);font-family:'JetBrains Mono',monospace">${r.cnt} signals</span>
          </div>`).join('')}
      </div>
    </div>`;
}

async function loadAI(){
  const ins=await fetch('/api/insights').then(r=>r.json()).catch(()=>[]);
  const el=document.getElementById('ai-content');
  if(!ins.length){el.innerHTML='<div class="empty"><div style="font-size:42px">🤖</div><div>Click Run Analysis to get insights</div></div>';return;}
  el.innerHTML=ins.map(i=>`<div class="card">
    <div style="font-size:13px;font-weight:700;color:var(--purple);margin-bottom:4px">🧠 Market Analysis</div>
    <div style="font-size:11px;color:var(--muted);margin-bottom:10px;font-family:'JetBrains Mono',monospace">${new Date(i.created_at).toLocaleString()} · ${i.goals_analyzed||0} goals · ${i.rules_analyzed||0} rules</div>
    <div style="font-size:13px;line-height:1.7;color:#94a3b8;white-space:pre-line">${i.content}</div>
  </div>`).join('');
}

async function runAI(){
  const btn=document.getElementById('ai-btn');btn.disabled=true;btn.textContent='⏳ Analyzing...';
  try{
    const r=await fetch('/api/run_ai',{method:'POST'});
    const d=await r.json();
    if(d.error)btn.textContent='❌ '+d.error;
    else{await loadAI();btn.textContent='✅ Done';}
  }catch(e){btn.textContent='❌ Error';}
  setTimeout(()=>{btn.disabled=false;btn.textContent='🤖 Run Analysis';},3000);
}

async function loadDebug(){
  const el=document.getElementById('debug-content');
  el.innerHTML='<div class="empty">⏳ Fetching...</div>';
  try{
    const d=await fetch('/api/debug_odds').then(r=>r.json());
    const mkts=(d.parsed?.markets||[]).map(m=>`
      <div style="background:var(--bg2);border-radius:6px;padding:8px 12px;margin:4px 0;font-family:'JetBrains Mono',monospace;font-size:12px;display:flex;gap:16px;flex-wrap:wrap">
        <span style="color:var(--blue)">${m.market_type}</span>
        <span>Line: <b>${m.line}</b></span>
        <span style="color:var(--green)">Over: ${m.over_odd||'—'}</span>
        <span style="color:var(--red)">Under: ${m.under_odd||'—'}</span>
      </div>`).join('');
    el.innerHTML=`
      <div class="card">
        <div class="stit">Parsed Markets (${d.markets_found||0} found) — ${d.total_events||0} live events</div>
        ${d.markets_found>0?mkts:'<div style="color:var(--red);padding:12px">⚠️ NO MARKETS PARSED — check raw JSON below</div>'}
      </div>
      <div class="card" style="margin-top:10px">
        <div class="stit">Raw Odds Response</div>
        <pre style="font-size:10px;color:var(--muted);overflow:auto;max-height:300px;white-space:pre-wrap;font-family:monospace">${JSON.stringify(d.odds_raw,null,2)}</pre>
      </div>
      <div class="card" style="margin-top:10px">
        <div class="stit">Raw Event</div>
        <pre style="font-size:10px;color:var(--muted);overflow:auto;max-height:200px;white-space:pre-wrap;font-family:monospace">${JSON.stringify(d.event_raw,null,2)}</pre>
      </div>`;
  }catch(e){el.innerHTML=`<div class="empty">Error: ${e.message}</div>`;}
}

async function auto(){if(cur==='live') await loadLive();}
loadLive();setInterval(auto,20000);
</script></body></html>"""

# ─── API Routes ───────────────────────────────────────────────────────────────
@app.route("/")
def index(): return render_template_string(HTML)

@app.route("/api/matches")
def api_matches():
    try:
        conn=get_db()
        try:
            # Try live first, fallback to all recent
            rows=conn.run("""SELECT match_id,home_team,away_team,league,
                minute,score_home,score_away,total_goals,period,status,last_updated
                FROM matches WHERE last_updated>NOW()-INTERVAL '10 minutes'
                ORDER BY last_updated DESC LIMIT 100""")
            return jsonify([{
                "match_id":r[0],"home_team":r[1]or"","away_team":r[2]or"",
                "league":r[3]or"","minute":r[4]or 0,
                "score_home":r[5]or 0,"score_away":r[6]or 0,
                "total_goals":r[7]or 0,"period":r[8]or"FT",
                "status":r[9]or"","last_updated":str(r[10])
            } for r in rows])
        finally: conn.close()
    except Exception as e:
        log.error(f"api_matches: {e}")
        return jsonify([])

@app.route("/api/stats")
def api_stats():
    try:
        conn=get_db()
        try:
            r1=conn.run("SELECT COUNT(*) FROM matches WHERE last_updated>NOW()-INTERVAL '3 minutes'")
            r2=conn.run("SELECT COUNT(*) FROM observations WHERE detected_at>NOW()-INTERVAL '30 minutes'")
            r3=conn.run("SELECT COUNT(*) FROM goals WHERE goal_time>NOW()-INTERVAL '24 hours'")
            r4=conn.run("SELECT COUNT(*) FROM paper_trades WHERE result='pending'")
            return jsonify({"live":r1[0][0],"signals":r2[0][0],"goals_today":r3[0][0],"open_trades":r4[0][0]})
        finally: conn.close()
    except: return jsonify({"live":0,"signals":0,"goals_today":0,"open_trades":0})

@app.route("/api/signals")
def api_signals():
    try:
        conn=get_db()
        try:
            rows=conn.run("""SELECT DISTINCT ON (o.match_id, o.rule_id)
                o.id, o.match_id, o.home_team, o.away_team, o.league,
                o.rule_name, o.minute, o.score, o.market_type, o.line,
                o.over_odd, o.under_odd, o.expected_odd, o.gap, o.pressure_score,
                o.confidence_estimate, o.action_type, o.reason,
                pt.selected_side, pt.entry_odd, pt.validation_window
                FROM observations o
                LEFT JOIN paper_trades pt ON o.id=pt.observation_id
                WHERE o.detected_at>NOW()-INTERVAL '30 minutes'
                ORDER BY o.match_id, o.rule_id, o.detected_at DESC LIMIT 50""")
            cols=["id","match_id","home_team","away_team","league","rule_name","minute","score",
                  "market_type","line","over_odd","under_odd","expected_odd","gap","pressure",
                  "confidence","action_type","reason","selected_side","entry_odd","validation_window"]
            return jsonify([dict(zip(cols,r)) for r in rows])
        finally: conn.close()
    except: return jsonify([])

@app.route("/api/goals")
def api_goals():
    try:
        conn=get_db()
        try:
            rows=conn.run("""SELECT g.match_id,g.minute,g.score_before,g.score_after,
                g.had_snapshots,g.odds_30s,g.odds_60s,g.goal_time,g.period,
                m.home_team,m.away_team,m.league
                FROM goals g LEFT JOIN matches m ON g.match_id=m.match_id
                ORDER BY g.goal_time DESC LIMIT 50""")
            result=[]
            for r in rows:
                result.append({"match_id":r[0],"minute":r[1],"score_before":r[2],
                               "score_after":r[3],"had_snapshots":r[4],
                               "odds_30s":r[5]or{},"odds_60s":r[6]or{},
                               "goal_time":str(r[7]),"period":r[8],
                               "home_team":r[9]or"","away_team":r[10]or"","league":r[11]or""})
            return jsonify(result)
        finally: conn.close()
    except: return jsonify([])

@app.route("/api/trades")
def api_trades():
    try:
        conn=get_db()
        try:
            rows=conn.run("""SELECT home_team,away_team,league,rule_name,market_type,line,
                selected_side,action_type,entry_odd,expected_odd,gap,pressure_score,
                confidence_estimate,validation_window,result,dummy_profit_loss,
                failure_reason,created_at,
                EXTRACT(EPOCH FROM (created_at - NOW()))/60 as mins_ago
                FROM paper_trades ORDER BY created_at DESC LIMIT 100""")
            cols=["home_team","away_team","league","rule_name","market_type","line",
                  "selected_side","action_type","entry_odd","expected_odd","gap","pressure_score",
                  "confidence_estimate","validation_window","result","dummy_profit_loss",
                  "failure_reason","created_at","minute_entry"]
            result=[dict(zip(cols,r)) for r in rows]
            for r in result:
                r["created_at"]=str(r["created_at"])
                # Extract score from observation
                r["score_entry"]=r.get("score_entry","—")
            return jsonify(result)
        finally: conn.close()
    except: return jsonify([])

@app.route("/api/observations")
def api_observations():
    try:
        conn=get_db()
        try:
            rows=conn.run("""SELECT match_id,home_team,away_team,league,rule_name,
                minute,score,market_type,line,over_odd,under_odd,expected_odd,
                gap,pressure_score,confidence_estimate,action_type,reason,detected_at
                FROM observations WHERE detected_at>NOW()-INTERVAL '3 hours'
                ORDER BY detected_at DESC LIMIT 100""")
            cols=["match_id","home_team","away_team","league","rule_name","minute","score",
                  "market_type","line","over_odd","under_odd","expected_odd","gap",
                  "pressure_score","confidence_estimate","action_type","reason","detected_at"]
            result=[dict(zip(cols,r)) for r in rows]
            for r in result: r["detected_at"]=str(r["detected_at"])
            return jsonify(result)
        finally: conn.close()
    except: return jsonify([])

@app.route("/api/rules")
def api_rules():
    try:
        conn=get_db()
        try:
            rows=conn.run("""SELECT id,rule_name,description,source,market_type,
                line_min,line_max,minute_min,minute_max,
                over_odd_min,over_odd_max,under_odd_min,under_odd_max,
                action_type,selected_side,validation_window,status,is_active,
                total_signals,win_count,lose_count,win_rate,dummy_profit,
                avg_entry_odd,created_at
                FROM rules ORDER BY total_signals DESC""")
            cols=["id","rule_name","description","source","market_type",
                  "line_min","line_max","minute_min","minute_max",
                  "over_odd_min","over_odd_max","under_odd_min","under_odd_max",
                  "action_type","selected_side","validation_window","status","is_active",
                  "total_signals","win_count","lose_count","win_rate","dummy_profit",
                  "avg_entry_odd","created_at"]
            result=[dict(zip(cols,r)) for r in rows]
            for r in result: r["created_at"]=str(r["created_at"])
            return jsonify(result)
        finally: conn.close()
    except: return jsonify([])

@app.route("/api/rules/toggle", methods=["POST"])
def api_rules_toggle():
    try:
        data=request.json
        conn=get_db()
        try:
            conn.run("UPDATE rules SET is_active=:a,last_updated=NOW() WHERE rule_name=:b",
                a=data["is_active"],b=data["rule_name"])
            return jsonify({"status":"ok"})
        finally: conn.close()
    except Exception as e: return jsonify({"error":str(e)}),500

@app.route("/api/ai_live")
def api_ai_live():
    try:
        conn=get_db()
        try:
            rows=conn.run("""SELECT content FROM ai_insights
                WHERE insight_type='live_signal' AND created_at>NOW()-INTERVAL '30 minutes'
                ORDER BY created_at DESC LIMIT 20""")
            seen={}
            for r in rows:
                parts=(r[0]or"").split("|||",1)
                if len(parts)==2 and parts[0] not in seen:
                    seen[parts[0]]={"match_id":parts[0],"analysis":parts[1]}
            return jsonify(list(seen.values()))
        finally: conn.close()
    except: return jsonify([])

@app.route("/api/analytics")
def api_analytics():
    try:
        conn=get_db()
        try:
            r1=conn.run("SELECT COUNT(*) FROM goals")[0][0]
            r2=conn.run("SELECT COUNT(*) FROM odds_snapshots")[0][0]
            r3=conn.run("SELECT COUNT(*) FROM paper_trades")[0][0]
            r4=conn.run("SELECT COUNT(*) FROM observations")[0][0]
            wins=conn.run("SELECT COUNT(*) FROM paper_trades WHERE result='win'")[0][0]
            tot=conn.run("SELECT COUNT(*) FROM paper_trades WHERE result!='pending'")[0][0]
            rate=round(wins/tot*100) if tot>0 else 0
            top=conn.run("SELECT rule_name,COUNT(*) cnt FROM observations GROUP BY rule_name ORDER BY cnt DESC LIMIT 8")
            return jsonify({"total_goals":r1,"total_snapshots":r2,"total_trades":r3,
                           "total_obs":r4,"success_rate":rate,
                           "top_rules":[{"rule_name":r[0],"cnt":r[1]} for r in top]})
        finally: conn.close()
    except: return jsonify({"total_goals":0,"total_snapshots":0,"total_trades":0,"total_obs":0,"success_rate":0})

@app.route("/api/insights")
def api_insights():
    try:
        conn=get_db()
        try:
            rows=conn.run("""SELECT insight_type,content,goals_analyzed,rules_analyzed,created_at
                FROM ai_insights WHERE insight_type='market_analysis'
                ORDER BY created_at DESC LIMIT 10""")
            return jsonify([{"insight_type":r[0],"content":r[1],"goals_analyzed":r[2],
                            "rules_analyzed":r[3],"created_at":str(r[4])} for r in rows])
        finally: conn.close()
    except: return jsonify([])

@app.route("/api/run_ai", methods=["POST"])
def api_run_ai():
    if not ANTHROPIC_API_KEY: return jsonify({"error":"No API key"}),400
    try:
        conn=get_db()
        try:
            goals=conn.run("""SELECT g.minute,g.score_before,g.period,g.odds_30s,m.league
                FROM goals g LEFT JOIN matches m ON g.match_id=m.match_id
                ORDER BY g.goal_time DESC LIMIT 200""")
            rules=conn.run("SELECT rule_name,status,total_signals,win_count,lose_count,win_rate,dummy_profit FROM rules ORDER BY total_signals DESC")
            trades=conn.run("SELECT result,COUNT(*) FROM paper_trades WHERE result!='pending' GROUP BY result")
            snaps=conn.run("SELECT COUNT(*) FROM odds_snapshots")[0][0]

            goals_txt=f"Total {len(goals)} goals\n"
            for g in goals[:30]:
                snap=g[3]or{}
                goods={}
                for k,v in snap.items():
                    if isinstance(v,dict): goods[k]=v.get("over","?")
                goals_txt+=f"min {g[0]} | {g[1]} | {g[2]} | odds:{goods}\n"

            rules_txt="Rules:\n"+"\n".join([f"{r[0]}({r[1]}): {r[2]} signals, {r[5]or 0}% win, €{r[6]or 0:.0f}" for r in rules])
            trades_txt=", ".join([f"{t[0]}:{t[1]}" for t in trades])

            prompt=f"""You are PapaGoal AI – betting market analyst. Analyze this data:

{snaps:,} snapshots | {len(goals)} goals
Paper Trading: {trades_txt}

{goals_txt}

{rules_txt}

Answer in English:
1. Which Over/Under lines & minutes show the clearest edge?
2. What odds patterns appear before goals? (be specific)
3. Which rules are performing well and which are failing?
4. What new rule would you recommend based on this data?
5. At what odds level is the best entry point (80% rule)?
6. What patterns should we watch for?

Be specific with numbers. Say 'insufficient data' if sample is too small (<20 cases)."""

            resp=requests.post("https://api.anthropic.com/v1/messages",
                headers={"x-api-key":ANTHROPIC_API_KEY,"anthropic-version":"2023-06-01","content-type":"application/json"},
                json={"model":"claude-sonnet-4-20250514","max_tokens":1500,"messages":[{"role":"user","content":prompt}]},timeout=30)
            if resp.status_code==200:
                analysis=resp.json()["content"][0]["text"]
                conn.run("INSERT INTO ai_insights (insight_type,content,goals_analyzed,rules_analyzed) VALUES ('market_analysis',:a,:b,:c)",
                    a=analysis,b=len(goals),c=len(rules))
                return jsonify({"status":"ok"})
            return jsonify({"error":f"Claude: {resp.status_code}"}),500
        finally: conn.close()
    except Exception as e: return jsonify({"error":str(e)}),500

@app.route("/api/ai_rules", methods=["POST"])
def api_ai_rules():
    if not ANTHROPIC_API_KEY: return jsonify({"error":"No API key"}),400
    try:
        conn=get_db()
        try:
            goals=conn.run("""SELECT g.minute,g.period,g.score_before,g.odds_30s
                FROM goals g ORDER BY g.goal_time DESC LIMIT 200""")
            rules=conn.run("SELECT rule_name,status,total_signals,win_rate,dummy_profit FROM rules ORDER BY total_signals DESC")

            goals_txt=f"{len(goals)} goals:\n"
            for g in goals[:50]:
                snap=g[3]or{}
                goods={}
                for k,v in snap.items():
                    if isinstance(v,dict): goods[k]=v.get("over","?")
                goals_txt+=f"min:{g[0]} {g[1]} {g[2]} odds:{goods}\n"

            rules_txt="Current rules:\n"+"\n".join([f"{r[0]}({r[1]}): {r[2]} signals, {r[3]or 0}% win" for r in rules])

            prompt=f"""You are PapaGoal AI. Analyze football betting market data and suggest rule improvements.

{goals_txt}
{rules_txt}

Return ONLY valid JSON (no markdown, no explanation outside JSON):
{{
  "new_rules": [
    {{
      "rule_name": "unique_snake_case_name",
      "description": "clear description under 100 chars",
      "market_type": "H1 or FT",
      "line_min": 0.5,
      "line_max": 2.5,
      "minute_min": 17,
      "minute_max": 20,
      "over_odd_min": 1.50,
      "over_odd_max": 1.60,
      "action_type": "OVER_LINE_WITHIN_10M",
      "selected_side": "over",
      "validation_window": "10m"
    }}
  ],
  "disable_rules": ["rule names to disable due to poor performance"],
  "insights": "2-3 sentence summary"
}}

Only suggest rules with clear data support. Max 3 new rules."""

            resp=requests.post("https://api.anthropic.com/v1/messages",
                headers={"x-api-key":ANTHROPIC_API_KEY,"anthropic-version":"2023-06-01","content-type":"application/json"},
                json={"model":"claude-sonnet-4-20250514","max_tokens":1000,"messages":[{"role":"user","content":prompt}]},timeout=30)

            if resp.status_code==200:
                text=resp.json()["content"][0]["text"]
                m=re.search(r'\{.*\}',text,re.DOTALL)
                if m:
                    data=json.loads(m.group())
                    new_count=0
                    for nr in data.get("new_rules",[]):
                        try:
                            conn.run("""INSERT INTO rules (rule_name,description,source,market_type,
                                line_min,line_max,minute_min,minute_max,
                                over_odd_min,over_odd_max,action_type,selected_side,
                                validation_window,status)
                                VALUES (:a,:b,'ai',:c,:d,:e,:f,:g,:h,:i,:j,:k,:l,'TESTING')
                                ON CONFLICT (rule_name) DO NOTHING""",
                                a=nr["rule_name"],b=nr.get("description",""),
                                c=nr.get("market_type","FT"),
                                d=nr.get("line_min",0.5),e=nr.get("line_max",3.5),
                                f=nr.get("minute_min",0),g=nr.get("minute_max",90),
                                h=nr.get("over_odd_min"),i=nr.get("over_odd_max"),
                                j=nr.get("action_type","OVER_LINE_WITHIN_10M"),
                                k=nr.get("selected_side","over"),
                                l=nr.get("validation_window","10m"))
                            new_count+=1
                        except: pass
                    # Disable poor rules
                    for rname in data.get("disable_rules",[]):
                        try: conn.run("UPDATE rules SET is_active=FALSE WHERE rule_name=:a",a=rname)
                        except: pass
                    conn.run("INSERT INTO ai_insights (insight_type,content,goals_analyzed,rules_analyzed) VALUES ('rule_improvement',:a,:b,:c)",
                        a=data.get("insights",""),b=len(goals),c=len(rules))
                    return jsonify({"status":"ok","new_rules":new_count})
            return jsonify({"error":f"Claude: {resp.status_code}"}),500
        finally: conn.close()
    except Exception as e: return jsonify({"error":str(e)}),500

@app.route("/health")
def health():
    return jsonify({"status":"ok","version":"v6","time":datetime.now(timezone.utc).isoformat()})

@app.route("/api/debug_odds")
def api_debug_odds():
    """Show raw API response — for diagnosing parse/auth issues"""
    out = {"api_key_set": bool(ODDSAPI_KEY), "api_key_prefix": ODDSAPI_KEY[:8]+"..." if ODDSAPI_KEY else "MISSING"}
    try:
        # Step 0: get available sports
        try:
            rs = requests.get("https://api.odds-api.io/v3/sports",
                params={"apiKey": ODDSAPI_KEY}, timeout=10)
            out["sports_status"] = rs.status_code
            try: out["sports_list"] = rs.json()
            except: out["sports_text"] = rs.text[:1000]
        except Exception as se:
            out["sports_error"] = str(se)

        # Step 1: try to get events
        r = requests.get("https://api.odds-api.io/v3/events",
            params={"apiKey": ODDSAPI_KEY, "sport": "football", "status": "live", "limit": 10},
            timeout=15)
        out["events_status"] = r.status_code
        out["events_url"] = r.url
        try: out["events_raw"] = r.json()
        except: out["events_raw_text"] = r.text[:2000]

        events_list = []
        raw = out["events_raw"] if "events_raw" in out else {}
        if isinstance(raw, list): events_list = raw
        elif isinstance(raw, dict):
            events_list = raw.get("data") or raw.get("events") or raw.get("results") or []

        out["events_count"] = len(events_list)

        # Step 2: if we have events, get odds for first one
        if events_list:
            eid = str(events_list[0].get("id") or events_list[0].get("eventId") or "")
            out["first_event"] = events_list[0]
            if eid:
                r2 = requests.get(f"https://api.odds-api.io/v3/events/{eid}/odds",
                    params={"apiKey": ODDSAPI_KEY},
                    timeout=15)
                out["odds_status"] = r2.status_code
                out["odds_url"] = r2.url
                try: out["odds_raw"] = r2.json()
                except: out["odds_raw_text"] = r2.text[:2000]

                parsed = parse_event(events_list[0], out.get("odds_raw", {}))
                out["parsed"] = parsed
                out["markets_found"] = len(parsed.get("markets", []))
        else:
            out["note"] = "No events — trying without status filter"
            r3 = requests.get("https://api.odds-api.io/v3/events",
                params={"apiKey": ODDSAPI_KEY, "sport": "soccer", "limit": 5},
                timeout=15)
            out["no_filter_status"] = r3.status_code
            try: out["no_filter_raw"] = r3.json()
            except: out["no_filter_text"] = r3.text[:2000]

    except Exception as e:
        out["error"] = str(e)
    return jsonify(out)

# ─── Start ────────────────────────────────────────────────────────────────────
init_db()
threading.Thread(target=collector_loop, daemon=True).start()
log.info("🚀 PapaGoal v6 started")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT, debug=False)
