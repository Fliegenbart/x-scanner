# Social scanner for Reddit + X

This Python CLI scans the last _N_ days of Reddit and/or X and writes normalized output to:

- `records.csv`
- `records.jsonl`
- `scan_meta.json`

## What it supports

### Reddit

- Scan one or more subreddits for recent posts
- Optionally apply a Reddit search query inside each subreddit
- Or run a broader search across `r/all`

### X

Two modes:

1. **Platform-wide search** with `--x-query`
   - Uses X full-archive search
   - Needed for 30-day platform-wide searches
2. **Specific-account scan** with `--x-usernames`
   - Uses user timelines
   - Works for older-than-7-day windows on specific public accounts

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements_social_scanner.txt
```

Set credentials:

```bash
export REDDIT_CLIENT_ID="..."
export REDDIT_CLIENT_SECRET="..."
export REDDIT_USER_AGENT="myapp:scanner:v1.0 (by u/myusername)"
export X_BEARER_TOKEN="..."
```

Or copy `.env.example` to `.env` and load it before running the script.

## Usage examples

### Reddit: scan subreddits with a query

```bash
python social_scanner.py \
  --days 30 \
  --reddit-subreddits MachineLearning,OpenAI \
  --reddit-query 'openai OR gpt' \
  --outdir ./out
```

### Reddit: scan recent posts from a subreddit without a query

```bash
python social_scanner.py \
  --days 30 \
  --reddit-subreddits startups \
  --outdir ./out
```

### X: 30-day platform-wide search

```bash
python social_scanner.py \
  --days 30 \
  --x-query '"OpenAI" lang:en -is:retweet' \
  --outdir ./out
```

### X: 30-day scan of specific accounts

```bash
python social_scanner.py \
  --days 30 \
  --x-usernames openai,sama \
  --x-exclude-replies \
  --x-exclude-retweets \
  --outdir ./out
```

### Combined scan

```bash
python social_scanner.py \
  --days 30 \
  --reddit-subreddits MachineLearning,OpenAI \
  --reddit-query 'openai OR gpt' \
  --x-query '"OpenAI" lang:en -is:retweet' \
  --outdir ./out
```

## Important caveats

- Reddit listings/searches commonly top out around 1000 items. On very busy subreddits or broad queries, older posts from the requested window may be truncated.
- X platform-wide 30-day scanning requires full-archive search access. If that is not enabled, use `--x-usernames` for specific accounts or reduce the request to the recent-search window.
- The tool currently scans Reddit submissions and X posts. It does **not** scan Reddit comments.

## Output schema

The CSV/JSONL outputs use normalized columns such as:

- `platform`
- `source_mode`
- `source_label`
- `platform_id`
- `created_at`
- `author`
- `title`
- `text`
- `url`
- `subreddit`
- engagement metrics where available

## Quick validation

You can inspect CLI help without credentials:

```bash
python social_scanner.py --help
```
