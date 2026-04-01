# Deriv ExpiryRange Bot — Python Edition

| | |
|---|---|
| Symbol | `1HZ10V` |
| Contract | `EXPIRYRANGE` (Ends In) |
| Barriers | `±1.7` |
| Duration | `2 minutes` |

## Files

| File | Purpose |
|---|---|
| `bot.py` | Complete bot — all logic in one file |
| `requirements.txt` | `websockets>=12.0` only |
| `Procfile` | `worker: python bot.py` |
| `railway.json` | Railway deploy config |
| `nixpacks.toml` | Python 3.11 build config |
| `.env.example` | All environment variables |

## Deploy to Railway

1. Push this folder to a GitHub repo
2. Railway → New Project → Deploy from GitHub
3. Add variables from `.env.example` in Railway → Variables tab
4. Bot starts automatically

## Signal Layers (6 votes, need 3+ for trade)

| Layer | Condition for TRADE vote |
|---|---|
| ATR Regime | ATR(20)/ATR(100) < 0.80 (low volatility) |
| BB Width | BB width < 2% of price (consolidating) |
| RSI Gate | RSI between 35–65 (neutral) |
| EMA Distance | Price within 1.2× ATR of EMA midpoint |
| Candle Body | Body ratio < 0.50 (indecision candle) |
| Tick Streak | Directional streak ≤ 4 (oscillating) |
