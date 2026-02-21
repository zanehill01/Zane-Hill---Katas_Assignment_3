"""Fetch time-series data from the FRED public API.

Usage (PowerShell):
    $env:FRED_API_KEY="your_key_here"
    python fetch_fred_data.py --series-id UNRATE --limit 10

Optional:
    python fetch_fred_data.py --series-id GDP --start 2020-01-01 --end 2024-12-31 --out gdp.json
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import time
from pathlib import Path
from typing import Any

import requests

BASE_URL = "https://api.stlouisfed.org/fred/series/observations"


def exponential_backoff(attempt: int, base_delay: float) -> float:
    """Calculate exponential backoff delay for a retry attempt."""
    return min(base_delay * (2 ** (attempt - 1)), 8.0)


def save_results(payload: dict[str, Any], out_path: Path, series_id: str) -> None:
    """Save fetched data to JSON or CSV based on file extension."""
    suffix = out_path.suffix.lower()

    if suffix == ".json":
        out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return

    if suffix == ".csv":
        observations = payload.get("observations", [])
        with out_path.open("w", newline="", encoding="utf-8") as csv_file:
            writer = csv.DictWriter(
                csv_file,
                fieldnames=["series_id", "date", "value", "realtime_start", "realtime_end"],
            )
            writer.writeheader()
            for obs in observations:
                writer.writerow(
                    {
                        "series_id": series_id,
                        "date": obs.get("date"),
                        "value": obs.get("value"),
                        "realtime_start": obs.get("realtime_start"),
                        "realtime_end": obs.get("realtime_end"),
                    }
                )
        return

    raise ValueError("Unsupported output format. Use .json or .csv")


def fetch_fred_observations(
    api_key: str,
    series_id: str,
    start: str | None = None,
    end: str | None = None,
    limit: int | None = 10,
    delay_seconds: float = 0.25,
    max_retries: int = 3,
) -> dict[str, Any]:
    """Fetch observations for a FRED series, handling paginated responses."""
    params: dict[str, str | int] = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "sort_order": "desc",
    }

    if start:
        params["observation_start"] = start
    if end:
        params["observation_end"] = end

    requested_total = limit
    page_limit = 1000 if requested_total is None else max(1, min(requested_total, 1000))
    offset = 0
    collected: list[dict[str, Any]] = []
    first_payload: dict[str, Any] | None = None

    while True:
        page_params = dict(params)
        page_params["limit"] = page_limit
        page_params["offset"] = offset

        payload: dict[str, Any] | None = None
        for attempt in range(1, max_retries + 1):
            try:
                response = requests.get(BASE_URL, params=page_params, timeout=15)

                if response.status_code == 429:
                    retry_after = response.headers.get("Retry-After")
                    backoff = (
                        float(retry_after)
                        if retry_after
                        else exponential_backoff(attempt, delay_seconds)
                    )
                    if attempt == max_retries:
                        raise RuntimeError("Rate limit exceeded after retries (HTTP 429).")
                    time.sleep(backoff)
                    continue

                if 400 <= response.status_code < 500:
                    raise RuntimeError(
                        f"FRED client error {response.status_code}: {response.text}"
                    )
                if 500 <= response.status_code < 600:
                    if attempt == max_retries:
                        raise RuntimeError(
                            f"FRED server error {response.status_code} after retries."
                        )
                    time.sleep(exponential_backoff(attempt, delay_seconds))
                    continue

                response.raise_for_status()
                payload = response.json()
                break
            except requests.RequestException as error:
                if attempt == max_retries:
                    raise RuntimeError(f"Network/request error after retries: {error}") from error
                time.sleep(exponential_backoff(attempt, delay_seconds))

        if payload is None:
            raise RuntimeError("Failed to fetch FRED data after retries.")

        if first_payload is None:
            first_payload = payload

        page_observations = payload.get("observations", [])
        if not page_observations:
            break

        collected.extend(page_observations)
        total_available = int(payload.get("count", len(collected)))

        if requested_total is not None and len(collected) >= requested_total:
            collected = collected[:requested_total]
            break

        if len(collected) >= total_available:
            break

        offset += len(page_observations)
        if requested_total is not None:
            remaining = requested_total - len(collected)
            page_limit = max(1, min(remaining, 1000))

        if delay_seconds > 0:
            time.sleep(delay_seconds)

    result = dict(first_payload or {})
    result["observations"] = collected
    result["offset"] = 0
    result["limit"] = len(collected)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch data from the FRED API")
    parser.add_argument("--series-id", default="UNRATE", help="FRED series id, e.g. UNRATE, GDP, FEDFUNDS")
    parser.add_argument("--start", default=None, help="Observation start date (YYYY-MM-DD)")
    parser.add_argument("--end", default=None, help="Observation end date (YYYY-MM-DD)")
    parser.add_argument("--limit", type=int, default=10, help="Max observations to return")
    parser.add_argument("--all", action="store_true", help="Fetch all available observations (uses pagination)")
    parser.add_argument("--delay", type=float, default=0.25, help="Delay between paginated API requests in seconds")
    parser.add_argument("--retries", type=int, default=3, help="Max retries per request")
    parser.add_argument("--out", default=None, help="Optional path to save response (.json or .csv)")
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
            limit=None if args.all else args.limit,
            delay_seconds=args.delay,
            max_retries=args.retries,
        )
    except RuntimeError as error:
        raise SystemExit(str(error)) from error

    observations = payload.get("observations", [])
    print(f"Series: {args.series_id}")
    print(f"Returned observations: {len(observations)}")

    for obs in observations[: min(5, len(observations))]:
        print(f"- {obs['date']}: {obs['value']}")

    if args.out:
        out_path = Path(args.out)
        try:
            save_results(payload, out_path, args.series_id)
            print(f"Saved full response to: {out_path}")
        except ValueError as error:
            raise SystemExit(str(error)) from error


if __name__ == "__main__":
    main()
