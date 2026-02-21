# Kata 3 — API Consumption

Foundation 2, Sprint Week 1

## Project Summary

This assignment implements a FRED API client in Python using:
- `requests` for the core required API flow
- `aiohttp` for the stretch async/concurrent flow

The script file is:
- `fetch_fred_data.py`

## Setup

### 1) Install dependencies

```powershell
pip install requests aiohttp
```

### 2) Set your FRED API key (PowerShell)

```powershell
$env:FRED_API_KEY="your_key_here"
```

## Usage

### Required path (uses `requests`)

```powershell
python fetch_fred_data.py --series-id UNRATE --limit 10
```

### Save output

```powershell
python fetch_fred_data.py --series-id GDP --start 2020-01-01 --end 2024-12-31 --out gdp.json
python fetch_fred_data.py --series-id GDP --all --out gdp.csv
```

### Stretch path (async concurrent requests with `aiohttp`)

```powershell
python fetch_fred_data.py --series-id GDP --all --async-fetch --concurrency 5
```

## Sample Run & Output

### Example command

```powershell
python fetch_fred_data.py --series-id UNRATE --limit 5 --out unrate.json
```

### Example console output

```text
Series: UNRATE
Returned observations: 5
- 2026-01-01: 4.1
- 2025-12-01: 4.0
- 2025-11-01: 4.1
- 2025-10-01: 4.0
- 2025-09-01: 4.2
Saved full response to: unrate.json
```

Note: values/dates will vary based on current FRED data at runtime.

## Requirement Checklist

### Exercise requirements

- [x] Fetches data from a public API (FRED)
- [x] Handles pagination (`--all` or large limits)
- [x] Implements retry logic with exponential backoff
- [x] Saves results locally to JSON or CSV (`--out`)

### Technical requirements

- [x] Uses `requests` library (default fetch path)
- [x] Handles HTTP errors gracefully (4xx/5xx + network failures)
- [x] Respects rate limits (`--delay`, plus 429 handling with `Retry-After` / backoff)
- [x] Uses environment variable for API key (`FRED_API_KEY`)

### Git tasks

- [x] Two branches from `main` modified same retry logic:
  - `branch-a`
  - `branch-b`
- [x] Merge conflict practiced and resolved in `main`
- [x] Conflict resolution learning documented in commit message

Recent relevant commits:
- `b5b56a9` — branch-a retry logic change
- `ea5240b` — branch-b retry logic change
- `98f9f9c` — merge conflict resolution commit to main

### Stretch

- [x] Implemented async requests with `aiohttp` for concurrent API calls (`--async-fetch`)

## Notes

- Default mode intentionally uses `requests` to satisfy the core requirement.
- Async mode is optional and enabled with `--async-fetch`.
