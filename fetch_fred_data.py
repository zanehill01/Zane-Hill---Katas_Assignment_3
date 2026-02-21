"""Fetch time-series data from the FRED public API.

Usage (PowerShell):
    $env:FRED_API_KEY="your_key_here"
    python fetch_fred_data.py --series-id UNRATE --limit 10

Optional:
    python fetch_fred_data.py --series-id GDP --start 2020-01-01 --end 2024-12-31 --out gdp.json
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen

BASE_URL = "https://api.stlouisfed.org/fred/series/observations"


def fetch_fred_observations(
    api_key: str,
    series_id: str,
    start: str | None = None,
    end: str | None = None,
    limit: int = 10,
) -> dict[str, Any]:
    """Fetch observations for a FRED series."""
    params: dict[str, str | int] = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "limit": limit,
        "sort_order": "desc",
    }

    if start:
        params["observation_start"] = start
    if end:
        params["observation_end"] = end

    url = f"{BASE_URL}?{urlencode(params)}"

    with urlopen(url, timeout=15) as response:
        return json.loads(response.read().decode("utf-8"))


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch data from the FRED API")
    parser.add_argument("--series-id", default="UNRATE", help="FRED series id, e.g. UNRATE, GDP, FEDFUNDS")
    parser.add_argument("--start", default=None, help="Observation start date (YYYY-MM-DD)")
    parser.add_argument("--end", default=None, help="Observation end date (YYYY-MM-DD)")
    parser.add_argument("--limit", type=int, default=10, help="Max observations to return")
    parser.add_argument("--out", default=None, help="Optional path to save full JSON response")
    args = parser.parse_args()

    api_key = os.getenv("FRED_API_KEY")
    if not api_key:
        raise SystemExit(
            "FRED_API_KEY is not set.\n"
            "PowerShell: $env:FRED_API_KEY=\"your_key_here\""
        )

    try:
        payload = fetch_fred_observations(
            api_key=api_key,
            series_id=args.series_id,
            start=args.start,
            end=args.end,
            limit=args.limit,
        )
    except HTTPError as error:
        raise SystemExit(f"FRED HTTP error {error.code}: {error.reason}") from error
    except URLError as error:
        raise SystemExit(f"Network error: {error.reason}") from error

    observations = payload.get("observations", [])
    print(f"Series: {args.series_id}")
    print(f"Returned observations: {len(observations)}")

    for obs in observations[: min(5, len(observations))]:
        print(f"- {obs['date']}: {obs['value']}")

    if args.out:
        out_path = Path(args.out)
        out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        print(f"Saved full response to: {out_path}")


if __name__ == "__main__":
    main()
