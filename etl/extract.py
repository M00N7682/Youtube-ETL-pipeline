"""extract.py
YouTube Data API v3 extractor module
-----------------------------------
This script provides reusable helper functions for the Extract phase of the ETL
pipeline. It now supports multiple search queries in one run.

    python extract.py --query "music,kpop,hiphop" --max_results 50

Dependencies:
    pip install google-api-python-client python-dotenv
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from googleapiclient.discovery import build  # type: ignore
from googleapiclient.errors import HttpError  # type: ignore

DEFAULT_MAX_RESULTS_PER_QUERY: int = 50
RAW_DATA_DIR: Path = Path(__file__).resolve().parent.parent / "data" / "raw"
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

def get_youtube_client(api_key: str):
    return build("youtube", "v3", developerKey=api_key, cache_discovery=False)

def save_response(data: List[Dict[str, Any]], query: str) -> Path:
    date_str = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%SZ")
    filename = RAW_DATA_DIR / f"{date_str}_{query.replace(' ', '_')}.json"
    with filename.open("w", encoding="utf-8") as fp:
        json.dump(data, fp, ensure_ascii=False, indent=2)
    logger.info("Saved %d items to %s", len(data), filename)
    return filename

def extract_videos_for_query(
    query: str,
    max_total: int = 100,
    api_key: Optional[str] = None,
    sleep_between_pages: float = 1.0,
) -> List[Dict[str, Any]]:
    api_key = api_key or os.getenv("YT_API_KEY")
    if not api_key:
        raise RuntimeError("YouTube API key not set. Provide --api_key or set YT_API_KEY env var.")

    youtube = get_youtube_client(api_key)
    collected: List[Dict[str, Any]] = []
    next_page_token: Optional[str] = None

    while len(collected) < max_total:
        try:
            request = youtube.search().list(
                q=query,
                part="snippet",
                type="video",
                maxResults=min(DEFAULT_MAX_RESULTS_PER_QUERY, max_total - len(collected)),
                pageToken=next_page_token,
                order="date",
            )
            response = request.execute()
        except HttpError as e:
            logger.error("YouTube API error: %s", e)
            logger.info("Sleeping 60 seconds before retry...")
            time.sleep(60)
            continue

        items = response.get("items", [])
        if not items:
            logger.warning("No more items returned before reaching max_total (%d).", max_total)
            break

        collected.extend(items)
        next_page_token = response.get("nextPageToken")
        logger.info("Fetched %d/%d items for query '%s'", len(collected), max_total, query)

        if not next_page_token:
            break

        time.sleep(sleep_between_pages)

    return collected[:max_total]

def run_extract(**context):
    params = context.get("params", {})
    raw_queries = params.get("query", "music")
    query_list = [q.strip() for q in raw_queries.split(",") if q.strip()]
    max_total: int = int(params.get("max_total", 100))
    api_key = params.get("api_key") or os.getenv("YT_API_KEY")

    paths = []
    for query in query_list:
        items = extract_videos_for_query(query, max_total=max_total, api_key=api_key)
        output_path = save_response(items, query)
        paths.append(str(output_path))

    context["ti"].xcom_push(key="raw_paths", value=paths)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract YouTube search results as raw JSON")
    parser.add_argument("--query", required=True, help="Comma-separated keywords, e.g., 'music,kpop' ")
    parser.add_argument("--max_results", type=int, default=100, help="Number of videos per keyword")
    parser.add_argument("--api_key", help="YouTube Data API key; overrides YT_API_KEY env var")

    args = parser.parse_args()
    query_list = [q.strip() for q in args.query.split(",") if q.strip()]

    try:
        for query in query_list:
            videos = extract_videos_for_query(
                query=query,
                max_total=args.max_results,
                api_key=args.api_key,
            )
            save_response(videos, query)
    except Exception as exc:
        logger.exception("Extraction failed: %s", exc)
        sys.exit(1)
    else:
        logger.info("Extraction finished successfully")
