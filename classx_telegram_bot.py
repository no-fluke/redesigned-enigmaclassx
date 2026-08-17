"""
ClassX Multi-Site Test Series Scraper — Telegram Bot (v2)
==========================================================

NEW in v2:
  • MongoDB backend  — site/API profiles are stored in MongoDB, not hardcoded.
  • /addapi command  — add a new ClassX site via the bot (step-by-step conversation).
  • /listapis        — list all saved APIs from DB.
  • /deleteapi       — remove an API by number.
  • Numbered selection flow (NO inline buttons for scraping):
      /sites  → bot sends a numbered list of sites → user replies with a number
              → bot fetches test series → sends numbered list
              → user replies with a number → bot fetches quizzes → sends numbered list
              → user replies with their selection:
                  "3"       → single quiz
                  "1&3&5"   → multiple specific quizzes
                  "2-6"     → a range of quizzes
                  "all"     → every quiz in the series
              → bot scrapes each selected quiz and sends a separate JSON file per quiz

INSTALL:
  pip install python-telegram-bot pymongo requests

MONGODB:
  Set MONGO_URI below (e.g. "mongodb://localhost:27017" or Atlas URI).
  The bot uses database "classxbot", collection "apis".

  Each document shape:
  {
    "key":         "parmar",           # unique short key
    "label":       "Parmar Academy",
    "base_url":    "https://parmaracademyapi.classx.co.in",
    "origin":      "https://www.parmaracademy.in",
    "referer":     "https://www.parmaracademy.in/",
    "user_id":     "391142",
    "auth_token":  "eyJ...",
    "output_name": "parmar_academy_data.json"
  }

  On first run the 5 built-in sites are seeded automatically if the collection
  is empty. You can delete them via /deleteapi if you want.

BOT SETUP:
  1. Get a token from @BotFather → set BOT_TOKEN below.
  2. Set MONGO_URI.
  3. pip install python-telegram-bot pymongo requests
  4. python classx_telegram_bot.py

COMMANDS:
  /start        — Welcome
  /help         — Full help text
  /sites        — Pick a site (numbered list)
  /addapi       — Start add-API wizard (multi-step conversation)
  /listapis     — Show all saved APIs
  /deleteapi    — Delete an API by number
  /cancel       — Cancel any running wizard or scrape
"""

import asyncio
import json
import logging
import os
import re
import time
from copy import deepcopy
from datetime import datetime
from io import BytesIO

import aiohttp
from aiohttp import web
import requests
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

# ─── CONFIG ───────────────────────────────────────────────────────────────────

BOT_TOKEN   = os.environ.get("BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
MONGO_URI   = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
PORT        = int(os.environ.get("PORT", "8080"))
RENDER_URL  = os.environ.get("RENDER_EXTERNAL_URL", "")  # injected by Render automatically

DELAY       = 0.5   # seconds between ClassX API requests
PING_INTERVAL = 4 * 60  # self-ping every 4 minutes

# ─── LOGGING ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ─── MONGODB ──────────────────────────────────────────────────────────────────

_mongo_client: MongoClient | None = None
_apis_col = None   # collection handle


def get_apis_col():
    global _mongo_client, _apis_col
    if _apis_col is not None:
        return _apis_col
    _mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    # Ping to verify connection
    _mongo_client.admin.command("ping")
    db = _mongo_client["classxbot"]
    _apis_col = db["apis"]
    # Seed built-in sites only if collection is empty
    if _apis_col.count_documents({}) == 0:
        _apis_col.insert_many(BUILTIN_SITES)
        logger.info("Seeded %d built-in sites into MongoDB.", len(BUILTIN_SITES))
    return _apis_col


def load_all_apis() -> list[dict]:
    """Return all API profiles from MongoDB as a list (sorted by label)."""
    col = get_apis_col()
    return list(col.find({}, {"_id": 0}).sort("label", 1))


def get_api_by_key(key: str) -> dict | None:
    col = get_apis_col()
    return col.find_one({"key": key}, {"_id": 0})


def upsert_api(profile: dict):
    col = get_apis_col()
    col.update_one({"key": profile["key"]}, {"$set": profile}, upsert=True)


def delete_api(key: str) -> bool:
    col = get_apis_col()
    result = col.delete_one({"key": key})
    return result.deleted_count > 0


# ─── BUILT-IN SEED DATA ───────────────────────────────────────────────────────

BUILTIN_SITES = [
    {
        "key":         "parmar",
        "label":       "Parmar Academy",
        "base_url":    "https://parmaracademyapi.classx.co.in",
        "origin":      "https://www.parmaracademy.in",
        "referer":     "https://www.parmaracademy.in/",
        "user_id":     "391142",
        "auth_token":  (
            "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpZCI6IjM5MTE0MiIsInRpbWVzdGFtcCI6"
            "MTc4MTcxMDc3NSwiaXZfdmVyIjoyMCwic2Vzc2lvbiI6ImV5SjBlWEFpT2lKS1YxUWlMQ0poY"
            "kdjaU9pSklVekkxTmlKOS5leUpwWkNJNklqTTVNVEUwTWlJc0ltVnRZV2xzSWpvaVltaHlhVz"
            "VuWVd4emFXNW5hRGsxUUdkdFlXbHNMbU52YlNJc0ltNWhiV1VpT2lKQ2FISnBibWRoYkNCVGF"
            "XNW5hQ0lzSW5SbGJtRnVkRlI1Y0dVaU9pSjFjMlZ5SWl3aWRHVnVZVzUwVG1GdFpTSTZJbkJ"
            "oY20xaGNtRmpZV1JsYlhsZlpHSWlMQ0owWlc1aGJuUkpaQ0k2SWlJc0ltUnBjM0J2YzJGaWJ"
            "HVWlPbVpoYkhObGZRLkw1aHAyaDhrSmkxNlc5YzdFZGNQanh2U1N5OTNHLWRjX1hCaGxVQzF6"
            "MU0ifQ.BJj_tp7An9A-89IgNPXx-tT_7eQKmgp2Nk8M2P5D2uc"
        ),
        "output_name": "parmar_academy_data.json",
    },
    {
        "key":         "achievecap",
        "label":       "AchieveCap",
        "base_url":    "https://achievecapfapi.classx.co.in",
        "origin":      "https://www.achievecap.in",
        "referer":     "https://www.achievecap.in/",
        "user_id":     "391142",
        "auth_token":  (
            "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpZCI6IjM5MTE0MiIsInRpbWVzdGFtcCI6"
            "MTc4MTcxMDc3NSwiaXZfdmVyIjoyMCwic2Vzc2lvbiI6ImV5SjBlWEFpT2lKS1YxUWlMQ0poY"
            "kdjaU9pSklVekkxTmlKOS5leUpwWkNJNklqTTVNVEUwTWlJc0ltVnRZV2xzSWpvaVltaHlhVz"
            "VuWVd4emFXNW5hRGsxUUdkdFlXbHNMbU52YlNJc0ltNWhiV1VpT2lKQ2FISnBibWRoYkNCVGF"
            "XNW5hQ0lzSW5SbGJtRnVkRlI1Y0dVaU9pSjFjMlZ5SWl3aWRHVnVZVzUwVG1GdFpTSTZJbkJ"
            "oY20xaGNtRmpZV1JsYlhsZlpHSWlMQ0owWlc1aGJuUkpaQ0k2SWlJc0ltUnBjM0J2YzJGaWJ"
            "HVWlPbVpoYkhObGZRLkw1aHAyaDhrSmkxNlc5YzdFZGNQanh2U1N5OTNHLWRjX1hCaGxVQzF6"
            "MU0ifQ.BJj_tp7An9A-89IgNPXx-tT_7eQKmgp2Nk8M2P5D2uc"
        ),
        "output_name": "achievecap_data.json",
    },
    {
        "key":         "capfacmentors",
        "label":       "CAPF AC Mentors",
        "base_url":    "https://capfacmentorsapi.classx.co.in",
        "origin":      "https://www.capfacmentors.in",
        "referer":     "https://www.capfacmentors.in/",
        "user_id":     "391142",
        "auth_token":  (
            "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpZCI6IjM5MTE0MiIsInRpbWVzdGFtcCI6"
            "MTc4MTcxMDc3NSwiaXZfdmVyIjoyMCwic2Vzc2lvbiI6ImV5SjBlWEFpT2lKS1YxUWlMQ0poY"
            "kdjaU9pSklVekkxTmlKOS5leUpwWkNJNklqTTVNVEUwTWlJc0ltVnRZV2xzSWpvaVltaHlhVz"
            "VuWVd4emFXNW5hRGsxUUdkdFlXbHNMbU52YlNJc0ltNWhiV1VpT2lKQ2FISnBibWRoYkNCVGF"
            "XNW5hQ0lzSW5SbGJtRnVkRlI1Y0dVaU9pSjFjMlZ5SWl3aWRHVnVZVzUwVG1GdFpTSTZJbkJ"
            "oY20xaGNtRmpZV1JsYlhsZlpHSWlMQ0owWlc1aGJuUkpaQ0k2SWlJc0ltUnBjM0J2YzJGaWJ"
            "HVWlPbVpoYkhObGZRLkw1aHAyaDhrSmkxNlc5YzdFZGNQanh2U1N5OTNHLWRjX1hCaGxVQzF6"
            "MU0ifQ.BJj_tp7An9A-89IgNPXx-tT_7eQKmgp2Nk8M2P5D2uc"
        ),
        "output_name": "capfacmentors_data.json",
    },
    {
        "key":         "avksa",
        "label":       "AVKS Academy CAPF",
        "base_url":    "https://avksacademycapfapi.akamai.net.in",
        "origin":      "https://www.avksacademycapf.in",
        "referer":     "https://www.avksacademycapf.in/",
        "user_id":     "391142",
        "auth_token":  (
            "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpZCI6IjM5MTE0MiIsInRpbWVzdGFtcCI6"
            "MTc4MTcxMDc3NSwiaXZfdmVyIjoyMCwic2Vzc2lvbiI6ImV5SjBlWEFpT2lKS1YxUWlMQ0poY"
            "kdjaU9pSklVekkxTmlKOS5leUpwWkNJNklqTTVNVEUwTWlJc0ltVnRZV2xzSWpvaVltaHlhVz"
            "VuWVd4emFXNW5hRGsxUUdkdFlXbHNMbU52YlNJc0ltNWhiV1VpT2lKQ2FISnBibWRoYkNCVGF"
            "XNW5hQ0lzSW5SbGJtRnVkRlI1Y0dVaU9pSjFjMlZ5SWl3aWRHVnVZVzUwVG1GdFpTSTZJbkJ"
            "oY20xaGNtRmpZV1JsYlhsZlpHSWlMQ0owWlc1aGJuUkpaQ0k2SWlJc0ltUnBjM0J2YzJGaWJ"
            "HVWlPbVpoYkhObGZRLkw1aHAyaDhrSmkxNlc5YzdFZGNQanh2U1N5OTNHLWRjX1hCaGxVQzF6"
            "MU0ifQ.BJj_tp7An9A-89IgNPXx-tT_7eQKmgp2Nk8M2P5D2uc"
        ),
        "output_name": "avksa_capf_data.json",
    },
    {
        "key":         "revolutioneducation",
        "label":       "Revolution Education",
        "base_url":    "https://revolutioneducationapi.teachx.in",
        "origin":      "https://revolutioneducationapi.teachx.in",
        "referer":     "https://revolutioneducationapi.teachx.in/",
        "user_id":     "391142",
        "auth_token":  (
            "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpZCI6IjM5MTE0MiIsInRpbWVzdGFtcCI6"
            "MTc4MTcxMDc3NSwiaXZfdmVyIjoyMCwic2Vzc2lvbiI6ImV5SjBlWEFpT2lKS1YxUWlMQ0poY"
            "kdjaU9pSklVekkxTmlKOS5leUpwWkNJNklqTTVNVEUwTWlJc0ltVnRZV2xzSWpvaVltaHlhVz"
            "VuWVd4emFXNW5hRGsxUUdkdFlXbHNMbU52YlNJc0ltNWhiV1VpT2lKQ2FISnBibWRoYkNCVGF"
            "XNW5hQ0lzSW5SbGJtRnVkRlI1Y0dVaU9pSjFjMlZ5SWl3aWRHVnVZVzUwVG1GdFpTSTZJbkJ"
            "oY20xaGNtRmpZV1JsYlhsZlpHSWlMQ0owWlc1aGJuUkpaQ0k2SWlJc0ltUnBjM0J2YzJGaWJ"
            "HVWlPbVpoYkhObGZRLkw1aHAyaDhrSmkxNlc5YzdFZGNQanh2U1N5OTNHLWRjX1hCaGxVQzF6"
            "MU0ifQ.BJj_tp7An9A-89IgNPXx-tT_7eQKmgp2Nk8M2P5D2uc"
        ),
        "output_name": "revolution_education_data.json",
    },
]

# ─── SCRAPER CLASS  (all extractor functions identical to original) ────────────

class ClassXScraper:
    """
    All extraction logic is 100% identical to the original Termux script.
    The only addition is a `cancelled` flag for mid-run stops.
    """

    def __init__(self, profile: dict):
        self.profile   = profile
        self.base_url  = profile["base_url"]
        self.cancelled = False
        self.headers   = {
            "accept":           "*/*",
            "accept-language":  "en-US,en;q=0.9,en-IN;q=0.8",
            "auth-key":         "appxapi",
            "authorization":    profile["auth_token"],
            "client-service":   "Appx",
            "device-type":      "website",
            "origin":           profile["origin"],
            "referer":          profile["referer"],
            "source":           "website",
            "user-agent":       (
                "Mozilla/5.0 (Linux; Android 15; Pixel 9) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/150.0.0.0 Mobile Safari/537.36"
            ),
            "user-id":          profile["user_id"],
        }

    # ── Helpers (identical to original) ──────────────────────────────────────

    def safe_get(self, path, params=None):
        url = f"{self.base_url}{path}"
        try:
            r = requests.get(url, params=params, headers=self.headers, timeout=15)
            r.raise_for_status()
            return r.json()
        except requests.HTTPError as e:
            if e.response.status_code == 401:
                raise RuntimeError("Token expired! Update auth_token via /addapi.")
            logger.warning("HTTP %s — %s", e.response.status_code, url)
            return None
        except Exception as exc:
            logger.warning("safe_get error: %s", exc)
            return None

    def extract_list(self, data, keys=("data", "result", "items", "tests",
                                       "test_titles", "test_series", "subjects")):
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            for k in keys:
                if k in data and isinstance(data[k], list):
                    return data[k]
        return []

    # ── Series / Subjects / Tests (identical to original) ────────────────────

    def get_all_test_series(self) -> list:
        all_series, start = [], 0
        while not self.cancelled:
            data = self.safe_get(
                "/get/test_series",
                params={"start": start, "search": "",
                        "client_api_url": "", "exam_id": ""},
            )
            if data is None:
                break
            items = self.extract_list(data)
            if not items:
                break
            all_series.extend(items)
            start += len(items)
            time.sleep(DELAY)
        return all_series

    def get_subjects(self, series_id) -> list:
        data = self.safe_get(
            "/get/testseries_subjects",
            params={"testseries_id": series_id},
        )
        if data is None:
            return []
        return self.extract_list(
            data, keys=("data", "subjects", "result", "items", "test_subjects")
        )

    def get_tests(self, series_id, subject_id) -> list:
        all_tests, start = [], 0
        while not self.cancelled:
            data = self.safe_get(
                "/get/test_titlev2",
                params={
                    "testseriesid": series_id,
                    "subject_id":   subject_id,
                    "chapter_id":   -1,
                    "userid":       self.profile["user_id"],
                    "search":       "",
                    "start":        start,
                },
            )
            if data is None:
                break
            items = self.extract_list(
                data, keys=("data", "tests", "test_titles", "result", "items")
            )
            if not items:
                break
            all_tests.extend(items)
            start += len(items)
            time.sleep(DELAY)
        return all_tests

    # ── Question parsing (identical to original) ──────────────────────────────

    def parse_questions(self, raw: list) -> list:
        out = []
        for q in raw:
            q_en = (q.get("question") or q.get("question_en")
                    or q.get("title")  or q.get("question_text") or "")
            q_hi = (q.get("question_hi") or q.get("question_hindi") or "")

            raw_opts = q.get("options") or q.get("answers") or []
            options  = []

            if isinstance(raw_opts, list):
                for o in raw_opts:
                    if isinstance(o, dict):
                        options.append({
                            "id":         o.get("id")  or o.get("key")    or "",
                            "text":       (o.get("option") or o.get("text")
                                           or o.get("value") or ""),
                            "text_hindi": (o.get("option_hi")
                                           or o.get("option_hindi") or ""),
                            "is_correct": bool(
                                o.get("is_correct") or o.get("correct") or False
                            ),
                        })
                    else:
                        options.append({"text": str(o)})
            elif isinstance(raw_opts, dict):
                for k, v in raw_opts.items():
                    options.append({"id": k, "text": str(v)})

            # Parmar-style: option_1 … option_10
            if not options:
                for num in range(1, 11):
                    if q.get(f"option_{num}"):
                        options.append({
                            "id":         str(num),
                            "text":       q.get(f"option_{num}", ""),
                            "text_hindi": "",
                            "image":      q.get(f"option_image_{num}", ""),
                        })
                # Legacy letter-style: option_a … option_e
                if not options:
                    for letter in "abcde":
                        if f"option_{letter}" in q:
                            options.append({
                                "id":         letter.upper(),
                                "text":       q.get(f"option_{letter}", ""),
                                "text_hindi": q.get(f"option_{letter}_hi", ""),
                            })

            out.append({
                "id":                q.get("id") or q.get("question_id") or "",
                "question":          q_en,
                "question_hindi":    q_hi,
                "options":           options,
                "correct_answer":    (q.get("correct_answer") or q.get("answer")
                                      or q.get("correct") or ""),
                "explanation":       (q.get("explanation") or q.get("solution_text")
                                      or q.get("solution")
                                      or q.get("explanation_en") or ""),
                "explanation_hindi": (q.get("explanation_hi")
                                      or q.get("explanation_hindi")
                                      or q.get("solution_hindi") or ""),
                "subject":           q.get("subject") or q.get("section") or "",
                "topic":             q.get("topic")   or q.get("chapter") or "",
                "difficulty":        q.get("difficulty") or q.get("level") or "",
            })
        return out

    def fetch_cdn(self, url: str) -> list:
        if not url:
            return []
        try:
            r = requests.get(url, headers=self.headers, timeout=30)
            r.raise_for_status()
            data = r.json()
        except Exception as exc:
            logger.warning("CDN fetch error: %s", exc)
            return []
        raw = self.extract_list(data, keys=("questions", "data", "result", "items"))
        if not raw and isinstance(data, list):
            raw = data
        return self.parse_questions(raw)

    def get_questions(self, test: dict) -> list:
        """Identical to original — fetches EN + HI, merges bilingual data."""
        url_en = test.get("test_questions_url") or ""
        url_hi = test.get("test_questions_url_2") or ""
        if not url_en and not url_hi:
            return []

        qs = self.fetch_cdn(url_en) if url_en else []

        if url_hi and url_hi != url_en:
            hi_list = self.fetch_cdn(url_hi)
            hi = {str(q["id"]): q for q in hi_list}

            if not qs and hi_list:
                qs = hi_list
                for q in qs:
                    q["question_hindi"]    = q.pop("question", "")
                    q["question"]          = ""
                    q["explanation_hindi"] = q.pop("explanation", "")
                    q["explanation"]       = ""
                    for opt in q.get("options", []):
                        opt["text_hindi"] = opt.pop("text", "")
                        opt["text"]       = ""
            else:
                for q in qs:
                    h = hi.get(str(q["id"]), {})
                    if h.get("question") and not q.get("question_hindi"):
                        q["question_hindi"] = h["question"]
                    if h.get("explanation") and not q.get("explanation_hindi"):
                        q["explanation_hindi"] = h["explanation"]
                    for i, opt in enumerate(q.get("options", [])):
                        ho = h.get("options", [])
                        if i < len(ho) and not opt.get("text_hindi"):
                            opt["text_hindi"] = ho[i].get("text", "")
                        if i < len(ho) and not opt.get("image") and ho[i].get("image"):
                            opt["image"] = ho[i].get("image", "")
        return qs

    # ── Convenience: scrape a single test object ──────────────────────────────

    def scrape_single_test(self, test: dict) -> dict:
        """Scrape one test and return a complete test entry dict with questions."""
        t_id   = test.get("id") or test.get("test_id") or ""
        t_name = test.get("title") or test.get("name") or f"Test #{t_id}"
        qs = self.get_questions(test)
        return {
            "id":              t_id,
            "name":            t_name,
            "duration_mins":   test.get("time")  or "",
            "total_marks":     test.get("marks") or "",
            "metadata":        test,
            "questions":       qs,
            "total_questions": len(qs),
        }


# ─── USER SESSION STATE ───────────────────────────────────────────────────────
# Stores per-chat state for the numbered-selection flow and addapi wizard.
# chat_id → dict

user_state: dict[int, dict] = {}

# ConversationHandler states for /addapi wizard
(
    ADD_KEY, ADD_LABEL, ADD_BASE_URL,
    ADD_ORIGIN, ADD_REFERER,
    ADD_USER_ID, ADD_AUTH_TOKEN,
) = range(7)

# ConversationHandler states for /deleteapi
DEL_CONFIRM = 10

# ─── HELPERS ──────────────────────────────────────────────────────────────────

def chunk_message(text: str, limit: int = 4000) -> list[str]:
    """Split a long message into chunks ≤ limit characters."""
    lines   = text.split("\n")
    chunks  = []
    current = ""
    for line in lines:
        if len(current) + len(line) + 1 > limit:
            chunks.append(current)
            current = ""
        current += line + "\n"
    if current.strip():
        chunks.append(current)
    return chunks


def build_numbered_list(items: list, label_fn) -> str:
    """Return a newline-separated numbered list string."""
    return "\n".join(f"{i+1}. {label_fn(item)}" for i, item in enumerate(items))


def parse_number(text: str, max_val: int) -> int | None:
    """Parse a single integer 1..max_val. Returns None on invalid input."""
    text = text.strip()
    if re.fullmatch(r"\d+", text):
        n = int(text)
        if 1 <= n <= max_val:
            return n
    return None


def parse_multi_selection(text: str, max_val: int) -> list[int] | None:
    """
    Parse a multi-quiz selection from user text.

    Accepts:
      • "all"          → [1, 2, ..., max_val]
      • "1"            → [1]
      • "1&3&5"        → [1, 3, 5]
      • "1, 3, 5"      → [1, 3, 5]   (comma-separated also accepted)
      • "1-5"          → [1, 2, 3, 4, 5]  (range)

    Returns a sorted unique list of 1-based indices, or None if input is invalid.
    """
    text = text.strip().lower()

    if text == "all":
        return list(range(1, max_val + 1))

    # Normalise separators: & , space → comma
    normalised = re.sub(r"[&,\s]+", ",", text)
    parts = [p.strip() for p in normalised.split(",") if p.strip()]

    indices = set()
    for part in parts:
        # Range: e.g. "2-5"
        range_match = re.fullmatch(r"(\d+)-(\d+)", part)
        if range_match:
            lo, hi = int(range_match.group(1)), int(range_match.group(2))
            if lo > hi or lo < 1 or hi > max_val:
                return None
            indices.update(range(lo, hi + 1))
        elif re.fullmatch(r"\d+", part):
            n = int(part)
            if n < 1 or n > max_val:
                return None
            indices.add(n)
        else:
            return None

    if not indices:
        return None
    return sorted(indices)


# ─── SEND HELPERS ─────────────────────────────────────────────────────────────

async def send_chunked(message, text: str, **kwargs):
    """Send possibly-long text in chunks, respecting Telegram's 4096-char limit."""
    for chunk in chunk_message(text, 4000):
        await message.reply_text(chunk, **kwargs)


# ─── COMMAND: /start ──────────────────────────────────────────────────────────

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 *ClassX Scraper Bot v2*\n\n"
        "I scrape test series from ClassX-powered sites and send you the JSON.\n\n"
        "*Commands:*\n"
        "• /sites — Pick a site, then a test series, then a quiz to extract\n"
        "• /addapi — Add a new ClassX site (saved to MongoDB)\n"
        "• /listapis — View all saved sites\n"
        "• /deleteapi — Remove a saved site\n"
        "• /cancel — Cancel any active operation\n"
        "• /help — Full help\n",
        parse_mode="Markdown",
    )


# ─── COMMAND: /help ───────────────────────────────────────────────────────────

async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📖 *ClassX Scraper Bot — Help*\n\n"
        "*Scraping flow (no buttons, all text-based):*\n"
        "1️⃣ /sites → I show a numbered list of sites.\n"
        "   Reply with the number of the site you want.\n"
        "2️⃣ I fetch that site's test series and show them numbered.\n"
        "   Reply with the number of the series you want.\n"
        "3️⃣ I show all quizzes inside that series, numbered.\n"
        "   Reply with your selection:\n"
        "   • Single quiz → `3`\n"
        "   • Multiple → `1&3&5`\n"
        "   • Range → `2-6`\n"
        "   • All → `all`\n"
        "4️⃣ Each selected quiz is scraped and sent as a separate JSON file. ✅\n\n"
        "*Managing sites (MongoDB):*\n"
        "• /addapi — Walk through adding a new ClassX site step-by-step.\n"
        "• /listapis — See all saved sites.\n"
        "• /deleteapi — Delete a site by picking its number.\n\n"
        "• /cancel — Cancel any in-progress wizard or selection.\n\n"
        "⚠️ If a token expires, delete the site via /deleteapi and re-add it with /addapi.",
        parse_mode="Markdown",
    )


# ─── COMMAND: /listapis ───────────────────────────────────────────────────────

async def cmd_listapis(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        apis = load_all_apis()
    except Exception as e:
        await update.message.reply_text(f"❌ MongoDB error: {e}")
        return

    if not apis:
        await update.message.reply_text("No APIs saved yet. Use /addapi to add one.")
        return

    lines = ["📋 *Saved APIs:*\n"]
    for i, api in enumerate(apis, 1):
        lines.append(
            f"{i}. *{api['label']}*\n"
            f"   Key: `{api['key']}`\n"
            f"   Base URL: `{api['base_url']}`\n"
            f"   User ID: `{api['user_id']}`\n"
        )
    await send_chunked(update.message, "\n".join(lines), parse_mode="Markdown")


# ─── COMMAND: /deleteapi  (simple ConversationHandler) ────────────────────────

async def cmd_deleteapi(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        apis = load_all_apis()
    except Exception as e:
        await update.message.reply_text(f"❌ MongoDB error: {e}")
        return ConversationHandler.END

    if not apis:
        await update.message.reply_text("No APIs saved yet.")
        return ConversationHandler.END

    ctx.user_data["_del_apis"] = apis
    lines = ["🗑 *Which API do you want to delete?*\n"]
    for i, api in enumerate(apis, 1):
        lines.append(f"{i}. {api['label']} (`{api['key']}`)")
    lines.append("\nReply with the number, or /cancel to abort.")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")
    return DEL_CONFIRM


async def del_confirm(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    apis = ctx.user_data.get("_del_apis", [])
    n = parse_number(update.message.text, len(apis))
    if n is None:
        await update.message.reply_text(
            f"Please send a number between 1 and {len(apis)}, or /cancel."
        )
        return DEL_CONFIRM

    target = apis[n - 1]
    deleted = delete_api(target["key"])
    if deleted:
        await update.message.reply_text(
            f"✅ Deleted *{target['label']}* (`{target['key']}`).",
            parse_mode="Markdown",
        )
    else:
        await update.message.reply_text("⚠️ Could not delete — it may have already been removed.")
    ctx.user_data.pop("_del_apis", None)
    return ConversationHandler.END


# ─── /addapi  ConversationHandler ─────────────────────────────────────────────

async def addapi_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data["_new_api"] = {}
    await update.message.reply_text(
        "➕ *Add a new ClassX API — Step 1/7*\n\n"
        "Send a short *unique key* for this site (e.g. `parmar`, `mysite`).\n"
        "Lowercase letters and underscores only. /cancel to abort.",
        parse_mode="Markdown",
    )
    return ADD_KEY


async def addapi_key(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    key = update.message.text.strip().lower()
    if not re.fullmatch(r"[a-z0-9_]+", key):
        await update.message.reply_text("Invalid key. Use only lowercase letters, digits, underscores.")
        return ADD_KEY
    # Check duplicate
    existing = get_api_by_key(key)
    if existing:
        await update.message.reply_text(
            f"⚠️ Key `{key}` already exists ({existing['label']}). Choose a different key.",
            parse_mode="Markdown",
        )
        return ADD_KEY
    ctx.user_data["_new_api"]["key"] = key
    await update.message.reply_text(
        "✅ Key set.\n\n*Step 2/7* — Send the *display label* (e.g. `Parmar Academy`):",
        parse_mode="Markdown",
    )
    return ADD_LABEL


async def addapi_label(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data["_new_api"]["label"] = update.message.text.strip()
    await update.message.reply_text(
        "✅ Label set.\n\n*Step 3/7* — Send the *base API URL*.\n"
        "Example: `https://parmaracademyapi.classx.co.in`",
        parse_mode="Markdown",
    )
    return ADD_BASE_URL


async def addapi_base_url(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    url = update.message.text.strip().rstrip("/")
    ctx.user_data["_new_api"]["base_url"] = url
    await update.message.reply_text(
        "✅ Base URL set.\n\n*Step 4/7* — Send the *origin URL*.\n"
        "Example: `https://www.parmaracademy.in`",
        parse_mode="Markdown",
    )
    return ADD_ORIGIN


async def addapi_origin(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data["_new_api"]["origin"] = update.message.text.strip().rstrip("/")
    await update.message.reply_text(
        "✅ Origin set.\n\n*Step 5/7* — Send the *referer URL*.\n"
        "Usually the origin with a trailing slash. Example: `https://www.parmaracademy.in/`",
        parse_mode="Markdown",
    )
    return ADD_REFERER


async def addapi_referer(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ref = update.message.text.strip()
    if not ref.endswith("/"):
        ref += "/"
    ctx.user_data["_new_api"]["referer"] = ref
    await update.message.reply_text(
        "✅ Referer set.\n\n*Step 6/7* — Send your *ClassX User ID*.\n"
        "Example: `391142`",
        parse_mode="Markdown",
    )
    return ADD_USER_ID


async def addapi_user_id(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.message.text.strip()
    ctx.user_data["_new_api"]["user_id"] = uid
    await update.message.reply_text(
        "✅ User ID set.\n\n*Step 7/7* — Send your *ClassX Auth Token* (the JWT string).\n"
        "This is the long `eyJ...` string from your browser/app.",
        parse_mode="Markdown",
    )
    return ADD_AUTH_TOKEN


async def addapi_auth_token(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    token = update.message.text.strip()
    new_api = ctx.user_data["_new_api"]
    new_api["auth_token"]  = token
    key = new_api["key"]
    new_api["output_name"] = f"{key}_data.json"

    try:
        upsert_api(new_api)
    except Exception as e:
        await update.message.reply_text(f"❌ MongoDB save error: {e}")
        return ConversationHandler.END

    await update.message.reply_text(
        f"✅ *{new_api['label']}* saved to MongoDB!\n\n"
        f"Key: `{key}`\n"
        f"Use /sites to start scraping it.",
        parse_mode="Markdown",
    )
    ctx.user_data.pop("_new_api", None)
    return ConversationHandler.END


async def conv_cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data.pop("_new_api", None)
    ctx.user_data.pop("_del_apis", None)
    # Also clear selection state
    user_state.pop(update.effective_chat.id, None)
    await update.message.reply_text("❌ Cancelled.")
    return ConversationHandler.END


# ─── COMMAND: /sites → numbered selection flow ────────────────────────────────

async def cmd_sites(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    # Clear any previous state for this user
    user_state.pop(chat_id, None)

    try:
        apis = load_all_apis()
    except Exception as e:
        await update.message.reply_text(f"❌ MongoDB error: {e}")
        return

    if not apis:
        await update.message.reply_text(
            "No APIs saved yet. Use /addapi to add a ClassX site."
        )
        return

    user_state[chat_id] = {"step": "site_select", "apis": apis}

    numbered = build_numbered_list(apis, lambda a: f"*{a['label']}*  (`{a['key']}`)")
    await send_chunked(
        update.message,
        f"🌐 *Available Sites* — reply with a number:\n\n{numbered}\n\n"
        "Reply with the number of the site you want to scrape.",
        parse_mode="Markdown",
    )


# ─── TEXT HANDLER — drives the numbered-selection flow ────────────────────────

async def handle_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """
    Handles plain text messages that drive the numbered selection flow:
      site_select → series_select → quiz_select → (scrape)
    """
    chat_id = update.effective_chat.id
    state   = user_state.get(chat_id)

    if not state:
        # No active flow — ignore (don't confuse with addapi wizard)
        return

    step = state.get("step")
    text = update.message.text.strip()

    # ── STEP 1: User selects a site ──────────────────────────────────────────
    if step == "site_select":
        apis = state["apis"]
        n = parse_number(text, len(apis))
        if n is None:
            await update.message.reply_text(
                f"Please reply with a number between 1 and {len(apis)}."
            )
            return

        profile = apis[n - 1]
        state["profile"] = profile

        await update.message.reply_text(
            f"⏳ Fetching test series from *{profile['label']}*…\nThis may take a moment.",
            parse_mode="Markdown",
        )

        loop = asyncio.get_event_loop()
        scraper = ClassXScraper(profile)
        state["scraper"] = scraper

        try:
            series_list = await loop.run_in_executor(
                None, scraper.get_all_test_series
            )
        except RuntimeError as e:
            user_state.pop(chat_id, None)
            await update.message.reply_text(f"❌ {e}")
            return

        if not series_list:
            user_state.pop(chat_id, None)
            await update.message.reply_text(
                "❌ No test series found. Token may have expired.\n"
                "Update it via /deleteapi then /addapi."
            )
            return

        state["series_list"] = series_list
        state["step"]        = "series_select"

        numbered = build_numbered_list(
            series_list,
            lambda s: (s.get("title") or s.get("name") or f"ID:{s.get('id','')}"),
        )
        await send_chunked(
            update.message,
            f"📚 *{profile['label']}* — {len(series_list)} test series found.\n"
            f"Reply with the number of the series you want:\n\n{numbered}",
            parse_mode="Markdown",
        )

    # ── STEP 2: User selects a test series ───────────────────────────────────
    elif step == "series_select":
        series_list = state["series_list"]
        profile     = state["profile"]
        n = parse_number(text, len(series_list))
        if n is None:
            await update.message.reply_text(
                f"Please reply with a number between 1 and {len(series_list)}."
            )
            return

        series = series_list[n - 1]
        state["series"] = series
        s_id   = series.get("id") or series.get("series_id") or ""
        s_name = series.get("title") or series.get("name") or f"Series #{s_id}"

        await update.message.reply_text(
            f"⏳ Fetching quizzes inside *{s_name}*…",
            parse_mode="Markdown",
        )

        loop    = asyncio.get_event_loop()
        scraper: ClassXScraper = state["scraper"]

        # Gather subjects, then tests under each subject
        subjects = await loop.run_in_executor(None, scraper.get_subjects, s_id)
        if not subjects:
            subjects = [{"subjectid": 0, "subject_name": "All Tests"}]

        all_tests = []
        for subj in subjects:
            subj_id = (subj.get("subjectid") or subj.get("id")
                       or subj.get("subject_id") or 0)
            tests = await loop.run_in_executor(
                None, scraper.get_tests, s_id, subj_id
            )
            # Tag each test with its subject name for display
            subj_name = (subj.get("subject_name") or subj.get("name")
                         or subj.get("title") or "")
            for t in tests:
                t["_subject_name"] = subj_name
            all_tests.extend(tests)

        if not all_tests:
            await update.message.reply_text(
                f"⚠️ No quizzes found inside *{s_name}*.\n"
                "Try a different series.",
                parse_mode="Markdown",
            )
            state["step"] = "series_select"
            return

        state["all_tests"] = all_tests
        state["step"]      = "quiz_select"

        def test_label(t):
            name = t.get("title") or t.get("name") or f"ID:{t.get('id','')}"
            subj = t.get("_subject_name", "")
            marks = t.get("marks", "")
            mins  = t.get("time", "")
            extras = []
            if subj:  extras.append(subj)
            if marks: extras.append(f"{marks} marks")
            if mins:  extras.append(f"{mins} min")
            suffix = f"  [{', '.join(extras)}]" if extras else ""
            return f"{name}{suffix}"

        numbered = build_numbered_list(all_tests, test_label)
        await send_chunked(
            update.message,
            f"📝 *{s_name}* — {len(all_tests)} quiz(zes) found.\n\n"
            f"{numbered}\n\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"Reply with your selection:\n"
            f"• Single quiz → `3`\n"
            f"• Multiple quizzes → `1&3&5`\n"
            f"• A range → `2-6`\n"
            f"• Everything → `all`",
            parse_mode="Markdown",
        )

    # ── STEP 3: User selects quiz(zes) → scrape them ─────────────────────────
    elif step == "quiz_select":
        all_tests = state["all_tests"]
        profile   = state["profile"]
        series    = state["series"]
        scraper: ClassXScraper = state["scraper"]
        s_name = series.get("title") or series.get("name") or ""

        # Parse multi-selection: "all", "1", "1&3&5", "2-6", etc.
        indices = parse_multi_selection(text, len(all_tests))
        if indices is None:
            await update.message.reply_text(
                f"⚠️ Invalid selection. Examples:\n"
                f"• Single: `3`\n"
                f"• Multiple: `1&3&5`\n"
                f"• Range: `2-6`\n"
                f"• All: `all`\n\n"
                f"Numbers must be between 1 and {len(all_tests)}.",
                parse_mode="Markdown",
            )
            return

        selected_tests = [all_tests[i - 1] for i in indices]
        total_selected = len(selected_tests)

        # Clear state — scraping begins now
        user_state.pop(chat_id, None)

        await update.message.reply_text(
            f"⏳ Starting extraction of *{total_selected}* quiz(zes)…\n"
            f"Each quiz will be sent as a separate JSON file.",
            parse_mode="Markdown",
        )

        loop = asyncio.get_event_loop()
        success_count = 0
        fail_count    = 0

        for idx, (orig_num, test) in enumerate(zip(indices, selected_tests), 1):
            t_name = test.get("title") or test.get("name") or f"ID:{test.get('id','')}"

            # Progress ping every quiz
            progress_msg = await ctx.bot.send_message(
                chat_id,
                f"⏳ [{idx}/{total_selected}] Scraping *{t_name}*…",
                parse_mode="Markdown",
            )

            try:
                t_entry = await loop.run_in_executor(
                    None, scraper.scrape_single_test, test
                )
            except Exception as e:
                await ctx.bot.edit_message_text(
                    f"❌ [{idx}/{total_selected}] Failed — *{t_name}*\n`{e}`",
                    chat_id=chat_id,
                    message_id=progress_msg.message_id,
                    parse_mode="Markdown",
                )
                fail_count += 1
                continue

            total_q = t_entry["total_questions"]
            output  = {
                "site":       profile["label"],
                "fetched_at": datetime.now().isoformat(),
                "series":     s_name,
                "test":       t_entry,
            }

            json_bytes = json.dumps(output, indent=2, ensure_ascii=False).encode("utf-8")
            size_kb    = len(json_bytes) / 1024
            safe_name  = re.sub(r"[^\w\- ]", "_", t_name)[:60]
            filename   = f"{safe_name}.json"

            # Update progress message to show done
            await ctx.bot.edit_message_text(
                f"✅ [{idx}/{total_selected}] *{t_name}*\n"
                f"❓ {total_q} questions  •  📦 {size_kb:.1f} KB",
                chat_id=chat_id,
                message_id=progress_msg.message_id,
                parse_mode="Markdown",
            )

            if len(json_bytes) > 50 * 1024 * 1024:
                await ctx.bot.send_message(
                    chat_id,
                    f"⚠️ *{t_name}* is over Telegram's 50 MB limit and was skipped.",
                    parse_mode="Markdown",
                )
                fail_count += 1
            else:
                file_obj = BytesIO(json_bytes)
                await ctx.bot.send_document(
                    chat_id=chat_id,
                    document=file_obj,
                    filename=filename,
                    caption=f"📄 {t_name}\n❓ {total_q} questions",
                )
                success_count += 1

        # Final summary
        summary = (
            f"🎉 *All done!*\n"
            f"✅ Sent: {success_count}/{total_selected}\n"
        )
        if fail_count:
            summary += f"❌ Failed: {fail_count}/{total_selected}\n"
        summary += "\nUse /sites to scrape more."

        await ctx.bot.send_message(chat_id, summary, parse_mode="Markdown")


# ─── SELF-PING / WEB SERVER ───────────────────────────────────────────────────

async def health_handler(request: web.Request) -> web.Response:
    """
    Simple HTTP endpoint.
    Render uses /health to mark the service as Live.
    The self-ping loop hits / every 4 min to prevent free-plan spin-down.
    """
    return web.Response(text="OK ✅", status=200)


async def start_web_server():
    """Start the aiohttp health-check server on PORT."""
    web_app = web.Application()
    web_app.router.add_get("/",       health_handler)
    web_app.router.add_get("/health", health_handler)

    runner = web.AppRunner(web_app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    logger.info("Health server listening on port %d", PORT)


async def self_ping_loop():
    """
    Pings the bot's own public URL every PING_INTERVAL seconds so Render's
    free plan never spins the container down after 15 min of inactivity.

    Falls back to localhost if RENDER_EXTERNAL_URL is not set (local dev).
    """
    await asyncio.sleep(20)  # give the server a moment to start

    target = (RENDER_URL.rstrip("/") if RENDER_URL else f"http://localhost:{PORT}")
    logger.info("Self-ping loop started → %s  (every %ds)", target, PING_INTERVAL)

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                async with session.get(
                    target, timeout=aiohttp.ClientTimeout(total=10)
                ) as resp:
                    logger.debug("Self-ping %s → %s", target, resp.status)
            except Exception as e:
                logger.warning("Self-ping failed: %s", e)
            await asyncio.sleep(PING_INTERVAL)


# ─── MAIN ─────────────────────────────────────────────────────────────────────

async def main():
    if BOT_TOKEN == "YOUR_BOT_TOKEN_HERE":
        print("ERROR: Set BOT_TOKEN before running.")
        print("  export BOT_TOKEN='123456:ABC-...'")
        return

    # Verify MongoDB on startup
    try:
        get_apis_col()
        logger.info("MongoDB connected ✅")
    except Exception as e:
        logger.error("MongoDB connection failed: %s", e)
        print(f"ERROR: Cannot connect to MongoDB — {e}")
        print(f"  Make sure MongoDB is running at: {MONGO_URI}")
        return

    # ── Build PTB app ─────────────────────────────────────────────────────────
    app = Application.builder().token(BOT_TOKEN).build()

    # /addapi wizard
    add_conv = ConversationHandler(
        entry_points=[CommandHandler("addapi", addapi_start)],
        states={
            ADD_KEY:        [MessageHandler(filters.TEXT & ~filters.COMMAND, addapi_key)],
            ADD_LABEL:      [MessageHandler(filters.TEXT & ~filters.COMMAND, addapi_label)],
            ADD_BASE_URL:   [MessageHandler(filters.TEXT & ~filters.COMMAND, addapi_base_url)],
            ADD_ORIGIN:     [MessageHandler(filters.TEXT & ~filters.COMMAND, addapi_origin)],
            ADD_REFERER:    [MessageHandler(filters.TEXT & ~filters.COMMAND, addapi_referer)],
            ADD_USER_ID:    [MessageHandler(filters.TEXT & ~filters.COMMAND, addapi_user_id)],
            ADD_AUTH_TOKEN: [MessageHandler(filters.TEXT & ~filters.COMMAND, addapi_auth_token)],
        },
        fallbacks=[CommandHandler("cancel", conv_cancel)],
    )

    # /deleteapi wizard
    del_conv = ConversationHandler(
        entry_points=[CommandHandler("deleteapi", cmd_deleteapi)],
        states={
            DEL_CONFIRM: [MessageHandler(filters.TEXT & ~filters.COMMAND, del_confirm)],
        },
        fallbacks=[CommandHandler("cancel", conv_cancel)],
    )

    app.add_handler(CommandHandler("start",     cmd_start))
    app.add_handler(CommandHandler("help",      cmd_help))
    app.add_handler(CommandHandler("listapis",  cmd_listapis))
    app.add_handler(CommandHandler("sites",     cmd_sites))
    app.add_handler(CommandHandler("cancel",    conv_cancel))
    app.add_handler(add_conv)
    app.add_handler(del_conv)

    # Plain text drives the numbered selection flow
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    # ── Start everything together ──────────────────────────────────────────────
    await start_web_server()                      # aiohttp health server
    asyncio.create_task(self_ping_loop())         # self-ping to prevent spin-down

    await app.initialize()
    await app.start()
    await app.updater.start_polling(allowed_updates=Update.ALL_TYPES)

    logger.info("✅ ClassX Telegram Bot v2 running.")

    # Block forever (until Ctrl+C / SIGTERM)
    try:
        await asyncio.Event().wait()
    finally:
        await app.updater.stop()
        await app.stop()
        await app.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
