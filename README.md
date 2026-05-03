# PapaGoal v6

**Read the market. Don't predict football.**

A live football betting market analysis system that learns whether Over/Under odds are correctly priced relative to the minute, score, and line — and surfaces opportunities when the market misprices.

## What it does

- Polls live football matches every 30 seconds via odds-api.io
- Saves snapshots of every Over/Under market (H1 + FT, all lines)
- Detects goals and records odds history around each goal
- Runs 5 hand-crafted rules against every live snapshot
- Creates Paper Trades (simulated bets) for every rule trigger
- Validates each trade at HT / FT / within time window
- Tracks win rate, P&L, and signals per rule
- AI analysis via Claude (Anthropic) to suggest new rules

## Rules

| Rule | Market | Condition | Action |
|------|--------|-----------|--------|
| Market Shut | FT | min ≥82, Over ≥2.80 | UNDER holds |
| Early Drop Signal | FT | min 17-20, Over 1.50-1.57 | OVER within 10m |
| H1 Minute 18 Pressure | H1 | min 15-22, Over 1.40-1.60 | H1 goal before HT |
| H1 Under 1.66 Minute 34 | H1 | min 30-38, Under 1.60-1.72 | Line holds to HT |
| Late FT Goal Hold | FT | min ≥86, Over 2.20-2.80, held 60s | Goal before FT |

## Environment Variables

| Variable | Description |
|----------|-------------|
| `ODDSAPI_KEY` | odds-api.io API key |
| `ANTHROPIC_API_KEY` | Claude API key (for AI analysis) |
| `DATABASE_URL` | PostgreSQL connection string |
| `PORT` | Port (set automatically by Railway) |

## Stack

- Python + Flask
- PostgreSQL (via pg8000)
- odds-api.io (live football odds)
- Anthropic Claude API (AI rule engine)
- Deployed on Railway

## Pages

- **Live Dashboard** — active signals and recommendations
- **Goals** — detected goals with odds history
- **Simulation** — all paper trades (pending / won / lost)
- **Observations** — raw rule triggers from last 3 hours
- **Rules Engine** — rule stats: signals / won / lost / win rate / P&L
- **Analytics** — collection progress and top rules
- **AI Insights** — Claude market analysis and rule suggestions
- **API Debug** — raw odds-api.io response for diagnosing parsing issues

## Debug

Visit `/api/debug_odds` or the **🔧 API Debug** tab to inspect the raw API response and verify markets are being parsed correctly.
