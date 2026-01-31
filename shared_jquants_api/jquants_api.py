from __future__ import annotations

import datetime as dt
import logging
import os
import random
import time
from functools import lru_cache
from typing import Any

import requests
from requests.exceptions import HTTPError, ConnectionError, Timeout, RequestException


class JQuantsAPI:
    def __init__(self, api_key: str | None = None):
        self.api_key = (api_key or os.getenv("JQUANTS_API_KEY") or "").strip()
        self.base_url = (os.getenv("JQUANTS_BASE_URL") or "https://api.jquants.com/v2").rstrip("/")
        self.master_date_override = (os.getenv("JQUANTS_MASTER_DATE") or "").strip()
        self.api_sleep_sec = float(os.getenv("API_SLEEP_SEC", "0.6"))
        self.api_max_retries = int(os.getenv("API_MAX_RETRIES", "6") or 6)
        self.api_backoff_base = float(os.getenv("API_BACKOFF_BASE", "1.0") or 1.0)

    def _headers(self) -> dict[str, str]:
        if not self.api_key:
            raise ValueError("JQUANTS_API_KEY is not set.")
        return {"x-api-key": self.api_key}

    def _normalize_date(self, date: str | None) -> str:
        if self.master_date_override:
            return self.master_date_override
        if date:
            return date.replace("-", "")
        return dt.date.today().strftime("%Y%m%d")

    @lru_cache(maxsize=8192)
    def fetch_equities_master(self, code: str, date: str | None = None) -> dict[str, Any] | None:
        if not code:
            return None
        params = {"code": code, "date": self._normalize_date(date)}
        url = f"{self.base_url}/equities/master"

        payload = self._get_with_retry(url, params=params, timeout=15)
        if payload is None:
            return None

        data = payload.get("data") if isinstance(payload, dict) else None
        if not data:
            return None
        return data[0]

    def fetch_equities_master_all(self, date: str | None = None) -> list[dict[str, Any]]:
        params = {"date": self._normalize_date(date)}
        url = f"{self.base_url}/equities/master"

        data: list[dict[str, Any]] = []
        payload = self._get_with_retry(url, params=params, timeout=30)
        if payload is None:
            raise RequestException("Failed to fetch equities master after retries.")
        data += payload.get("data", [])

        while "pagination_key" in payload or "paginationKey" in payload:
            pagination_key = payload.get("pagination_key") or payload.get("paginationKey")
            params["pagination_key"] = pagination_key
            payload = self._get_with_retry(url, params=params, timeout=30)
            if payload is None:
                raise RequestException("Failed to fetch equities master pagination after retries.")
            data += payload.get("data", [])

        logging.info("fetch_equities_master_all fetched %d records.", len(data))
        return data

    def fetch_statements(self, code: str | None = None, date: str | None = None) -> dict[str, list]:
        if not code and not date:
            raise ValueError("Either code or date must be provided.")

        params: dict[str, str] = {}
        if code:
            params["code"] = code
        if date:
            params["date"] = date

        url = f"{self.base_url}/fins/summary"
        headers = self._headers()

        data: list = []
        page_count = 0
        logging.info("fetch_statements using API_SLEEP_SEC=%s", self.api_sleep_sec)
        payload = self._get_with_retry(url, params=params, timeout=30)
        if payload is None:
            raise RequestException("Failed to fetch statements after retries.")
        data += payload.get("data", [])
        page_count += 1

        while "pagination_key" in payload or "paginationKey" in payload:
            pagination_key = payload.get("pagination_key") or payload.get("paginationKey")
            params["pagination_key"] = pagination_key
            logging.info("fetch_statements pagination_key=%s", pagination_key)
            payload = self._get_with_retry(url, params=params, timeout=30)
            if payload is None:
                raise RequestException("Failed to fetch statements pagination after retries.")
            data += payload.get("data", [])
            page_count += 1

        logging.info("fetch_statements fetched %d pages, %d records.", page_count, len(data))
        return {"data": data}

    def _get_with_retry(self, url: str, params: dict[str, str], timeout: int) -> dict[str, Any] | None:
        headers = self._headers()
        for attempt in range(self.api_max_retries):
            if self.api_sleep_sec > 0:
                time.sleep(self.api_sleep_sec)
            try:
                response = requests.get(url, headers=headers, params=params, timeout=timeout)
                if response.status_code == 429:
                    retry_after = response.headers.get("Retry-After")
                    if retry_after:
                        wait = float(retry_after)
                    else:
                        wait = self.api_backoff_base * (2 ** attempt) + random.uniform(0, 1)
                    logging.warning(
                        "Rate limited (429). Retry in %.1fs (attempt %d/%d).",
                        wait,
                        attempt + 1,
                        self.api_max_retries,
                    )
                    time.sleep(wait)
                    continue
                response.raise_for_status()
                return response.json()
            except (HTTPError, ConnectionError, Timeout, RequestException) as e:
                if attempt >= self.api_max_retries - 1:
                    logging.error("Request failed after %d attempts: %s", self.api_max_retries, e)
                    return None
                wait = self.api_backoff_base
                logging.warning("Request error: %s. Retry in %.1fs (attempt %d/%d).", e, wait, attempt + 1, self.api_max_retries)
                time.sleep(wait)
        return None
