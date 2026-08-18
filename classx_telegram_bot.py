"""
ClassX Multi-Site Test Series Scraper — Telegram Bot (v2)
==========================================================

NEW in v2:
  • MongoDB backend  — site/API profiles are stored in MongoDB, not hardcoded.
  • /addapi command  — add a new ClassX site by pasting the API URL (1 step).
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

  • /manual command  — skip the series list; enter a test series ID directly.
      /manual → numbered list of sites → reply with number
              → bot asks for test series ID → user types the ID
              → bot fetches quizzes for that series ID → same quiz selection flow
                  "3" / "1&3&5" / "2-6" / "all"
              → bot scrapes and sends JSON files

  • /search command  — probe a range of series IDs and show which ones exist.
      /search → numbered list of sites → reply with number
              → bot asks for an ID range, e.g. "100-150"  (max 200 IDs at once)
              → bot silently probes every ID in that range
              → sends a summary table: found IDs with quiz counts & series name
              → user can then use /manual with any of those IDs to scrape them

INSTALL:
  pip install python-telegram-bot pymongo requests aiohttp

MONGODB:
  Set MONGO_URI below (e.g. "mongodb://localhost:27017" or Atlas URI).
  The bot uses database "classxbot", collection "apis".

BOT SETUP:
  1. Get a token from @BotFather → set BOT_TOKEN below.
  2. Set MONGO_URI.
  3. pip install python-telegram-bot pymongo requests aiohttp
  4. python classx_bot.py

COMMANDS:
  /start        — Welcome
  /help         — Full help text
  /sites        — Pick a site (numbered list)
  /manual       — Enter a test series ID manually (skip series list)
  /search       — Probe a range of series IDs to discover what exists
  /addapi       — Paste a ClassX API URL to add it (1 step)
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
from datetime import datetime
from io import BytesIO
from urllib.parse import urlparse

import aiohttp
from aiohttp import web
import requests
from pymongo import MongoClient
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

BOT_TOKEN      = os.environ.get("BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
MONGO_URI      = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
PORT           = int(os.environ.get("PORT", "8080"))
RENDER_URL     = os.environ.get("RENDER_EXTERNAL_URL", "")

DELAY          = 0.5        # seconds between ClassX API requests
PING_INTERVAL  = 4 * 60    # self-ping every 4 minutes

# ─── LOGGING ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ─── MONGODB ──────────────────────────────────────────────────────────────────

_mongo_client = None
_apis_col     = None


def get_apis_col():
    global _mongo_client, _apis_col
    if _apis_col is not None:
        return _apis_col
    _mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    _mongo_client.admin.command("ping")
    db        = _mongo_client["classxbot"]
    _apis_col = db["apis"]
    if _apis_col.count_documents({}) == 0:
        _apis_col.insert_many(BUILTIN_SITES)
        logger.info("Seeded %d built-in sites into MongoDB.", len(BUILTIN_SITES))
    return _apis_col


def load_all_apis() -> list:
    col = get_apis_col()
    return list(col.find({}, {"_id": 0}).sort("label", 1))


def get_api_by_key(key: str):
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

# ─── /addapi — single step: paste URL ─────────────────────────────────────────

ADD_URL = 0   # only one ConversationHandler state needed


def _shared_auth() -> tuple:
    """Return (auth_token, user_id) from the first saved API, or seed fallback."""
    try:
        apis = load_all_apis()
        if apis:
            return apis[0]["auth_token"], apis[0]["user_id"]
    except Exception:
        pass
    seed = BUILTIN_SITES[0]
    return seed["auth_token"], seed["user_id"]


def _parse_url_to_profile(raw_url: str):
    """
    Derive a full site profile from a bare API URL.

    classx.co.in pattern:
      https://parmaracademyapi.classx.co.in
        → origin  = https://www.parmaracademy.in
        → label   = Parmar Academy

    teachx / akamai / anything else:
      https://revolutioneducationapi.teachx.in
        → origin  = https://revolutioneducationapi.teachx.in  (same)
        → label   = Revolution Education

    Returns None if URL cannot be parsed.
    """
    url = raw_url.strip().rstrip("/")
    if not url.startswith("http"):
        url = "https://" + url

    parsed    = urlparse(url)
    hostname  = parsed.hostname or ""
    parts     = hostname.split(".")       # ['parmaracademyapi','classx','co','in']
    if len(parts) < 2:
        return None

    subdomain = parts[0]                  # e.g. 'parmaracademyapi'
    clean     = subdomain[:-3] if subdomain.endswith("api") else subdomain

    # Human label: split camelCase / compound words and title-case
    label = re.sub(r"([a-z])([A-Z])", r"\1 \2", clean).title()

    # Key: lowercase alphanumeric slug
    key = re.sub(r"[^a-z0-9]", "", clean.lower())[:30]

    # Origin/referer
    tld_domain = ".".join(parts[1:])
    if "classx" in tld_domain:
        tld_parts = parts[2:]             # skip 'classx'
        origin    = f"https://www.{clean}.{'.'.join(tld_parts)}"
    else:
        origin    = url                   # teachx, akamai, etc.

    auth_token, user_id = _shared_auth()

    return {
        "key":         key,
        "label":       label,
        "base_url":    url,
        "origin":      origin,
        "referer":     origin.rstrip("/") + "/",
        "user_id":     user_id,
        "auth_token":  auth_token,
        "output_name": f"{key}_data.json",
    }


async def addapi_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "➕ Paste the ClassX / TeachX API URL:\n\n"
        "`https://revolutioneducationapi.teachx.in`\n\n"
        "_/cancel to abort_",
        parse_mode="Markdown",
    )
    return ADD_URL


async def addapi_url(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    raw     = update.message.text.strip()
    profile = _parse_url_to_profile(raw)

    if not profile:
        await update.message.reply_text(
            "⚠️ Couldn't parse that URL. It should look like:\n"
            "`https://somethingapi.classx.co.in`\n"
            "Try again or /cancel.",
            parse_mode="Markdown",
        )
        return ADD_URL

    # Ensure unique key
    col          = get_apis_col()
    original_key = profile["key"]
    counter      = 2
    while col.find_one({"key": profile["key"]}):
        profile["key"] = f"{original_key}{counter}"
        counter += 1

    try:
        upsert_api(profile)
    except Exception as e:
        await update.message.reply_text(f"❌ MongoDB error: {e}")
        return ConversationHandler.END

    await update.message.reply_text(
        f"✅ *{profile['label']}* added!\n\n"
        f"Key: `{profile['key']}`\n"
        f"Base URL: `{profile['base_url']}`\n"
        f"Origin: `{profile['origin']}`\n\n"
        "Use /sites to scrape it.",
        parse_mode="Markdown",
    )
    return ConversationHandler.END


# ─── /deleteapi ConversationHandler ───────────────────────────────────────────

DEL_CONFIRM = 10


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
    n    = parse_number(update.message.text, len(apis))
    if n is None:
        await update.message.reply_text(
            f"Please send a number between 1 and {len(apis)}, or /cancel."
        )
        return DEL_CONFIRM

    target  = apis[n - 1]
    deleted = delete_api(target["key"])
    if deleted:
        await update.message.reply_text(
            f"✅ Deleted *{target['label']}* (`{target['key']}`).",
            parse_mode="Markdown",
        )
    else:
        await update.message.reply_text("⚠️ Could not delete — may already be removed.")
    ctx.user_data.pop("_del_apis", None)
    return ConversationHandler.END


# ─── SCRAPER ──────────────────────────────────────────────────────────────────

class ClassXScraper:

    def __init__(self, profile: dict):
        self.profile   = profile
        self.base_url  = profile["base_url"]
        self.cancelled = False
        self.headers   = {
            "accept":          "*/*",
            "accept-language": "en-US,en;q=0.9,en-IN;q=0.8",
            "auth-key":        "appxapi",
            "authorization":   profile["auth_token"],
            "client-service":  "Appx",
            "device-type":     "website",
            "origin":          profile["origin"],
            "referer":         profile["referer"],
            "source":          "website",
            "user-agent":      (
                "Mozilla/5.0 (Linux; Android 15; Pixel 9) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/150.0.0.0 Mobile Safari/537.36"
            ),
            "user-id":         profile["user_id"],
        }

    def safe_get(self, path, params=None):
        url = f"{self.base_url}{path}"
        try:
            r = requests.get(url, params=params, headers=self.headers, timeout=15)
            r.raise_for_status()
            return r.json()
        except requests.HTTPError as e:
            if e.response.status_code == 401:
                raise RuntimeError("Token expired! Update auth_token via /deleteapi + /addapi.")
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

            if not options:
                for num in range(1, 11):
                    if q.get(f"option_{num}"):
                        options.append({
                            "id":         str(num),
                            "text":       q.get(f"option_{num}", ""),
                            "text_hindi": "",
                            "image":      q.get(f"option_image_{num}", ""),
                        })
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
        url_en = test.get("test_questions_url") or ""
        url_hi = test.get("test_questions_url_2") or ""
        if not url_en and not url_hi:
            return []

        qs = self.fetch_cdn(url_en) if url_en else []

        if url_hi and url_hi != url_en:
            hi_list = self.fetch_cdn(url_hi)
            hi      = {str(q["id"]): q for q in hi_list}

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

    def probe_series_id(self, series_id: str) -> dict | None:
        """
        Lightweight probe: fetch subjects + test titles for a given series ID.
        Returns a dict  {"id": series_id, "quiz_count": N, "subjects": [...],
                         "sample_names": [...up to 3 quiz titles...]}
        or None if the series ID yields nothing.
        Does NOT fetch questions — purely for discovery.
        """
        subjects = self.get_subjects(series_id)
        if not subjects:
            subjects = [{"subjectid": 0, "subject_name": "All Tests"}]

        total_tests  = 0
        sample_names = []

        for subj in subjects:
            subj_id = (subj.get("subjectid") or subj.get("id")
                       or subj.get("subject_id") or 0)
            tests = self.get_tests(series_id, subj_id)
            total_tests += len(tests)
            for t in tests:
                name = t.get("title") or t.get("name") or ""
                if name and len(sample_names) < 3:
                    sample_names.append(name)
            if self.cancelled:
                break

        if total_tests == 0:
            return None

        return {
            "id":           series_id,
            "quiz_count":   total_tests,
            "sample_names": sample_names,
        }

    def scrape_single_test(self, test: dict) -> dict:
        t_id   = test.get("id") or test.get("test_id") or ""
        t_name = test.get("title") or test.get("name") or f"Test #{t_id}"
        qs     = self.get_questions(test)
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

user_state: dict = {}

# ─── HELPERS ──────────────────────────────────────────────────────────────────

def chunk_message(text: str, limit: int = 4000) -> list:
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
    return "\n".join(f"{i+1}. {label_fn(item)}" for i, item in enumerate(items))


def parse_number(text: str, max_val: int):
    text = text.strip()
    if re.fullmatch(r"\d+", text):
        n = int(text)
        if 1 <= n <= max_val:
            return n
    return None


def parse_multi_selection(text: str, max_val: int):
    text = text.strip().lower()

    if text == "all":
        return list(range(1, max_val + 1))

    normalised = re.sub(r"[&,\s]+", ",", text)
    parts      = [p.strip() for p in normalised.split(",") if p.strip()]

    indices = set()
    for part in parts:
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

    return sorted(indices) if indices else None


async def send_chunked(message, text: str, **kwargs):
    for chunk in chunk_message(text, 4000):
        await message.reply_text(chunk, **kwargs)


# ─── SHARED QUIZ SCRAPE LOGIC (used by both /sites and /manual) ───────────────

async def run_quiz_scrape(update: Update, ctx: ContextTypes.DEFAULT_TYPE,
                          chat_id: int, all_tests: list, profile: dict,
                          series_name: str, scraper: ClassXScraper,
                          indices: list):
    """
    Given a resolved list of 1-based indices into all_tests, scrape each quiz
    and send it as a JSON file. Shared between the /sites and /manual flows.
    """
    selected_tests = [all_tests[i - 1] for i in indices]
    total_selected = len(selected_tests)

    await ctx.bot.send_message(
        chat_id,
        f"⏳ Starting extraction of *{total_selected}* quiz(zes)…\n"
        "Each quiz will be sent as a separate JSON file.",
        parse_mode="Markdown",
    )

    loop          = asyncio.get_event_loop()
    success_count = 0
    fail_count    = 0

    for idx, (orig_num, test) in enumerate(zip(indices, selected_tests), 1):
        t_name = test.get("title") or test.get("name") or f"ID:{test.get('id','')}"

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

        total_q   = t_entry["total_questions"]
        output    = {
            "site":       profile["label"],
            "fetched_at": datetime.now().isoformat(),
            "series":     series_name,
            "test":       t_entry,
        }
        json_bytes = json.dumps(output, indent=2, ensure_ascii=False).encode("utf-8")
        size_kb    = len(json_bytes) / 1024
        safe_name  = re.sub(r"[^\w\- ]", "_", t_name)[:60]
        filename   = f"{safe_name}.json"

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
                f"⚠️ *{t_name}* exceeds Telegram's 50 MB limit — skipped.",
                parse_mode="Markdown",
            )
            fail_count += 1
        else:
            await ctx.bot.send_document(
                chat_id=chat_id,
                document=BytesIO(json_bytes),
                filename=filename,
                caption=f"📄 {t_name}\n❓ {total_q} questions",
            )
            success_count += 1

    summary = (
        f"🎉 *All done!*\n"
        f"✅ Sent: {success_count}/{total_selected}\n"
    )
    if fail_count:
        summary += f"❌ Failed: {fail_count}/{total_selected}\n"
    summary += "\nUse /sites or /manual to scrape more."
    await ctx.bot.send_message(chat_id, summary, parse_mode="Markdown")


# ─── COMMANDS ─────────────────────────────────────────────────────────────────

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 *ClassX Scraper Bot v2*\n\n"
        "I scrape test series from ClassX-powered sites and send you the JSON.\n\n"
        "*Commands:*\n"
        "• /sites — Pick a site, series and quiz to extract\n"
        "• /manual — Enter a test series ID directly (skip the series list)\n"
        "• /search — Probe a range of IDs to discover what series exist\n"
        "• /addapi — Add a new site by pasting its API URL\n"
        "• /listapis — View all saved sites\n"
        "• /deleteapi — Remove a saved site\n"
        "• /cancel — Cancel any active operation\n"
        "• /help — Full help\n",
        parse_mode="Markdown",
    )


async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📖 *ClassX Scraper Bot — Help*\n\n"
        "*Normal scraping flow (/sites):*\n"
        "1️⃣ /sites → numbered list of sites → reply with number\n"
        "2️⃣ Numbered list of test series → reply with number\n"
        "3️⃣ Numbered list of quizzes → reply with selection:\n"
        "   • Single → `3`\n"
        "   • Multiple → `1&3&5`\n"
        "   • Range → `2-6`\n"
        "   • All → `all`\n"
        "4️⃣ Each quiz sent as a separate JSON file ✅\n\n"
        "*Manual mode (/manual):*\n"
        "1️⃣ /manual → numbered list of sites → reply with number\n"
        "2️⃣ Bot asks for a test series ID → type any numeric ID\n"
        "   (You can enter multiple IDs separated by spaces to batch-fetch)\n"
        "3️⃣ Same quiz selection flow as above\n\n"
        "*Search / discover mode (/search):*\n"
        "1️⃣ /search → pick a site\n"
        "2️⃣ Enter an ID range → `100-150`  (max 200 IDs)\n"
        "3️⃣ Bot probes every ID and shows a table:\n"
        "   `ID | Quiz count | Sample names`\n"
        "4️⃣ Use /manual with any discovered ID to scrape it\n\n"
        "*Managing sites:*\n"
        "• /addapi — Paste one URL, done.\n"
        "• /listapis — See all saved sites.\n"
        "• /deleteapi — Delete a site by number.\n"
        "• /cancel — Cancel any wizard.\n\n"
        "⚠️ If a token expires, delete the site and re-add it.",
        parse_mode="Markdown",
    )


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


async def cmd_sites(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    user_state.pop(chat_id, None)
    ctx.user_data.pop("_new_api", None)
    ctx.user_data.pop("_del_apis", None)

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


# ─── /manual COMMAND ──────────────────────────────────────────────────────────

async def cmd_manual(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """
    Manual mode: user picks a site, then types a test series ID directly
    instead of browsing the series list.
    """
    chat_id = update.effective_chat.id
    user_state.pop(chat_id, None)
    ctx.user_data.pop("_new_api", None)
    ctx.user_data.pop("_del_apis", None)

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

    user_state[chat_id] = {"step": "manual_site_select", "apis": apis}

    numbered = build_numbered_list(apis, lambda a: f"*{a['label']}*  (`{a['key']}`)")
    await send_chunked(
        update.message,
        f"🔧 *Manual Mode* — pick a site:\n\n{numbered}\n\n"
        "Reply with the site number.",
        parse_mode="Markdown",
    )


# ─── /search COMMAND ──────────────────────────────────────────────────────────

SEARCH_MAX_RANGE = 200   # hard cap so no one accidentally fires 10 000 requests

async def cmd_search(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """
    Discovery mode: ask user for a site + an ID range, probe every ID in that
    range, and report back which series IDs exist along with their quiz count
    and a few sample quiz names.
    """
    chat_id = update.effective_chat.id
    user_state.pop(chat_id, None)
    ctx.user_data.pop("_new_api", None)
    ctx.user_data.pop("_del_apis", None)

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

    user_state[chat_id] = {"step": "search_site_select", "apis": apis}

    numbered = build_numbered_list(apis, lambda a: f"*{a['label']}*  (`{a['key']}`)")
    await send_chunked(
        update.message,
        f"🔍 *Search / Discover Series IDs*\n\n"
        f"Pick a site first:\n\n{numbered}\n\n"
        "Reply with the site number.",
        parse_mode="Markdown",
    )


async def conv_cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data.pop("_new_api", None)
    ctx.user_data.pop("_del_apis", None)
    user_state.pop(update.effective_chat.id, None)

    cmd = ""
    if update.message and update.message.text:
        cmd = update.message.text.strip().lstrip("/").split()[0].lower()

    if cmd == "sites":
        return await cmd_sites(update, ctx)
    if cmd == "manual":
        return await cmd_manual(update, ctx)
    if cmd == "search":
        return await cmd_search(update, ctx)
    if cmd == "deleteapi":
        return await cmd_deleteapi(update, ctx)
    if cmd == "addapi":
        return await addapi_start(update, ctx)

    await update.message.reply_text("❌ Cancelled.")
    return ConversationHandler.END


# ─── TEXT HANDLER — numbered selection flow ───────────────────────────────────

async def handle_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    state   = user_state.get(chat_id)
    if not state:
        return

    step = state.get("step")
    text = update.message.text.strip()

    # ── /sites flow ───────────────────────────────────────────────────────────

    # Step 1: select site
    if step == "site_select":
        apis = state["apis"]
        n    = parse_number(text, len(apis))
        if n is None:
            await update.message.reply_text(
                f"Please reply with a number between 1 and {len(apis)}."
            )
            return

        profile = apis[n - 1]
        state["profile"] = profile

        await update.message.reply_text(
            f"⏳ Fetching test series from *{profile['label']}*…",
            parse_mode="Markdown",
        )

        loop    = asyncio.get_event_loop()
        scraper = ClassXScraper(profile)
        state["scraper"] = scraper

        try:
            series_list = await loop.run_in_executor(None, scraper.get_all_test_series)
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

    # Step 2: select test series
    elif step == "series_select":
        await _handle_series_select(update, ctx, chat_id, state, text)

    # Step 3: select quiz(zes) and scrape
    elif step == "quiz_select":
        await _handle_quiz_select(update, ctx, chat_id, state, text)

    # ── /manual flow ──────────────────────────────────────────────────────────

    # Manual step 1: select site
    elif step == "manual_site_select":
        apis = state["apis"]
        n    = parse_number(text, len(apis))
        if n is None:
            await update.message.reply_text(
                f"Please reply with a number between 1 and {len(apis)}."
            )
            return

        profile = apis[n - 1]
        scraper = ClassXScraper(profile)
        state["profile"] = profile
        state["scraper"] = scraper
        state["step"]    = "manual_series_id"

        await update.message.reply_text(
            f"✅ Site: *{profile['label']}*\n\n"
            "🔢 Now enter the *test series ID* (numeric).\n\n"
            "You can also enter *multiple IDs* separated by spaces to fetch "
            "quizzes from several series at once:\n"
            "`123` or `123 456 789`",
            parse_mode="Markdown",
        )

    # Manual step 2: receive series ID(s) and fetch quizzes
    elif step == "manual_series_id":
        await _handle_manual_series_id(update, ctx, chat_id, state, text)

    # Manual step 3: quiz selection (reuses the same handler as /sites)
    elif step == "manual_quiz_select":
        await _handle_quiz_select(update, ctx, chat_id, state, text)

    # ── /search flow ──────────────────────────────────────────────────────────

    # Search step 1: select site
    elif step == "search_site_select":
        apis = state["apis"]
        n    = parse_number(text, len(apis))
        if n is None:
            await update.message.reply_text(
                f"Please reply with a number between 1 and {len(apis)}."
            )
            return

        profile = apis[n - 1]
        scraper = ClassXScraper(profile)
        state["profile"] = profile
        state["scraper"] = scraper
        state["step"]    = "search_range_input"

        await update.message.reply_text(
            f"✅ Site: *{profile['label']}*\n\n"
            f"🔢 Enter the *ID range* to probe:\n\n"
            f"`100-150`  — probe IDs 100 through 150\n"
            f"`500-520`  — probe IDs 500 through 520\n\n"
            f"⚠️ Max {SEARCH_MAX_RANGE} IDs per search.\n"
            f"Each ID takes ~0.5 s, so 100 IDs ≈ 1 min.\n\n"
            f"Or /cancel to abort.",
            parse_mode="Markdown",
        )

    # Search step 2: receive range and probe
    elif step == "search_range_input":
        await _handle_search_range(update, ctx, chat_id, state, text)


# ─── SHARED SUB-HANDLERS ──────────────────────────────────────────────────────

async def _handle_series_select(update, ctx, chat_id, state, text):
    """Handle series selection in the normal /sites flow."""
    series_list = state["series_list"]
    profile     = state["profile"]
    n           = parse_number(text, len(series_list))
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
    scraper = state["scraper"]

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
        subj_name = (subj.get("subject_name") or subj.get("name")
                     or subj.get("title") or "")
        for t in tests:
            t["_subject_name"] = subj_name
        all_tests.extend(tests)

    if not all_tests:
        await update.message.reply_text(
            f"⚠️ No quizzes found inside *{s_name}*. Try a different series.",
            parse_mode="Markdown",
        )
        state["step"] = "series_select"
        return

    state["all_tests"]   = all_tests
    state["series_name"] = s_name
    state["step"]        = "quiz_select"

    await _send_quiz_list(update.message, all_tests, s_name)


async def _handle_manual_series_id(update, ctx, chat_id, state, text):
    """
    Receive one or more space-separated series IDs from the user,
    fetch quizzes from all of them, and present a combined numbered list.
    """
    raw_ids = text.split()
    # Validate: all tokens must be numeric
    if not raw_ids or not all(re.fullmatch(r"\d+", sid) for sid in raw_ids):
        await update.message.reply_text(
            "⚠️ Please enter one or more *numeric* series IDs separated by spaces.\n"
            "Example: `123` or `123 456 789`\n\n"
            "Or /cancel to abort.",
            parse_mode="Markdown",
        )
        return

    profile = state["profile"]
    scraper = state["scraper"]
    loop    = asyncio.get_event_loop()

    # Deduplicate while preserving order
    seen    = set()
    series_ids = []
    for sid in raw_ids:
        if sid not in seen:
            seen.add(sid)
            series_ids.append(sid)

    id_display = ", ".join(f"`{sid}`" for sid in series_ids)
    await update.message.reply_text(
        f"⏳ Fetching quizzes for series ID(s): {id_display} …",
        parse_mode="Markdown",
    )

    all_tests  = []
    found_ids  = []
    missed_ids = []

    for sid in series_ids:
        # Fetch subjects for this series ID
        subjects = await loop.run_in_executor(None, scraper.get_subjects, sid)
        if not subjects:
            subjects = [{"subjectid": 0, "subject_name": "All Tests"}]

        series_tests = []
        for subj in subjects:
            subj_id = (subj.get("subjectid") or subj.get("id")
                       or subj.get("subject_id") or 0)
            tests = await loop.run_in_executor(
                None, scraper.get_tests, sid, subj_id
            )
            subj_name = (subj.get("subject_name") or subj.get("name")
                         or subj.get("title") or "")
            for t in tests:
                t["_subject_name"] = subj_name
                t["_series_id"]    = sid   # tag so we know which series it came from
            series_tests.extend(tests)

        if series_tests:
            all_tests.extend(series_tests)
            found_ids.append(sid)
        else:
            missed_ids.append(sid)

    if not all_tests:
        ids_str = ", ".join(series_ids)
        await update.message.reply_text(
            f"❌ No quizzes found for series ID(s): {ids_str}\n\n"
            "Check the IDs and try again, or /cancel.",
        )
        # Stay in the same step so the user can retry
        return

    # Build a human-readable series name for the output JSON
    if len(found_ids) == 1:
        series_name = f"Manual Series #{found_ids[0]}"
    else:
        series_name = "Manual Series #" + "+".join(found_ids)

    state["all_tests"]   = all_tests
    state["series_name"] = series_name
    state["step"]        = "manual_quiz_select"

    # Warn about any IDs that returned nothing
    if missed_ids:
        await update.message.reply_text(
            f"⚠️ No quizzes found for series ID(s): {', '.join(missed_ids)} — skipped.",
        )

    await _send_quiz_list(update.message, all_tests, series_name)


async def _send_quiz_list(message, all_tests: list, series_name: str):
    """Send the numbered quiz list and selection instructions."""
    def test_label(t):
        name   = t.get("title") or t.get("name") or f"ID:{t.get('id','')}"
        subj   = t.get("_subject_name", "")
        sid    = t.get("_series_id", "")    # shown in manual mode
        marks  = t.get("marks", "")
        mins   = t.get("time", "")
        extras = []
        if sid:   extras.append(f"series {sid}")
        if subj:  extras.append(subj)
        if marks: extras.append(f"{marks} marks")
        if mins:  extras.append(f"{mins} min")
        suffix = f"  [{', '.join(extras)}]" if extras else ""
        return f"{name}{suffix}"

    numbered = build_numbered_list(all_tests, test_label)
    await send_chunked(
        message,
        f"📝 *{series_name}* — {len(all_tests)} quiz(zes) found.\n\n"
        f"{numbered}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"Reply with your selection:\n"
        f"• Single quiz → `3`\n"
        f"• Multiple → `1&3&5`\n"
        f"• Range → `2-6`\n"
        f"• Everything → `all`",
        parse_mode="Markdown",
    )


async def _handle_quiz_select(update, ctx, chat_id, state, text):
    """Handle quiz selection and kick off scraping (shared by /sites and /manual)."""
    all_tests   = state["all_tests"]
    profile     = state["profile"]
    series_name = state.get("series_name") or (
        state.get("series", {}).get("title")
        or state.get("series", {}).get("name", "")
    )
    scraper     = state["scraper"]

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

    user_state.pop(chat_id, None)

    await run_quiz_scrape(
        update, ctx,
        chat_id, all_tests, profile, series_name, scraper, indices,
    )


async def _handle_search_range(update, ctx, chat_id, state, text):
    """
    Parse a range like '100-150', probe every ID, and report what was found.
    Sends a live progress message that is edited as IDs are checked, then
    sends the final results table.
    """
    profile = state["profile"]
    scraper = state["scraper"]

    # ── Parse the range ───────────────────────────────────────────────────────
    m = re.fullmatch(r"(\d+)\s*[-–]\s*(\d+)", text.strip())
    if not m:
        await update.message.reply_text(
            "⚠️ Please enter a range like `100-150`.\n"
            "Or /cancel to abort.",
            parse_mode="Markdown",
        )
        return

    lo, hi = int(m.group(1)), int(m.group(2))
    if lo > hi:
        lo, hi = hi, lo          # swap silently

    total = hi - lo + 1
    if total > SEARCH_MAX_RANGE:
        await update.message.reply_text(
            f"⚠️ That range has *{total}* IDs — the limit is *{SEARCH_MAX_RANGE}*.\n"
            f"Please use a smaller range, e.g. `{lo}-{lo + SEARCH_MAX_RANGE - 1}`.",
            parse_mode="Markdown",
        )
        return

    # ── Clear state now so /cancel doesn't interfere mid-search ──────────────
    user_state.pop(chat_id, None)

    # ── Live progress message ─────────────────────────────────────────────────
    progress_msg = await update.message.reply_text(
        f"🔍 Probing IDs *{lo}* → *{hi}* on *{profile['label']}*…\n"
        f"0 / {total} checked  •  0 found",
        parse_mode="Markdown",
    )

    loop        = asyncio.get_event_loop()
    found       = []          # list of probe result dicts
    checked     = 0
    EDIT_EVERY  = 10          # edit the progress message every N IDs

    for sid_int in range(lo, hi + 1):
        sid    = str(sid_int)
        result = await loop.run_in_executor(None, scraper.probe_series_id, sid)
        checked += 1

        if result:
            found.append(result)

        # Update progress periodically so the user sees live feedback
        if checked % EDIT_EVERY == 0 or checked == total:
            try:
                await ctx.bot.edit_message_text(
                    f"🔍 Probing IDs *{lo}* → *{hi}* on *{profile['label']}*…\n"
                    f"{checked} / {total} checked  •  {len(found)} found",
                    chat_id=chat_id,
                    message_id=progress_msg.message_id,
                    parse_mode="Markdown",
                )
            except Exception:
                pass   # ignore edit failures (rate-limit, no change, etc.)

    # ── Final results ─────────────────────────────────────────────────────────
    if not found:
        await ctx.bot.send_message(
            chat_id,
            f"😕 No series found in the range *{lo}–{hi}* on *{profile['label']}*.\n\n"
            "Try a different range or use /sites to browse the full list.",
            parse_mode="Markdown",
        )
        return

    # Build results table
    lines = [
        f"✅ *Search complete — {len(found)} series found in range {lo}–{hi}*",
        f"Site: *{profile['label']}*\n",
        "```",
        f"{'ID':<8} {'Quizzes':<9} Sample quiz names",
        "─" * 60,
    ]
    for r in found:
        sid        = r["id"]
        qcount     = r["quiz_count"]
        samples    = r["sample_names"]
        # Truncate long names so the table stays readable
        sample_str = " / ".join(s[:35] + "…" if len(s) > 35 else s
                                for s in samples[:2])
        lines.append(f"{sid:<8} {qcount:<9} {sample_str}")
    lines.append("```")

    # Collect just the IDs for a quick-copy hint
    id_list = " ".join(r["id"] for r in found)
    lines.append(
        f"\n💡 Use `/manual` and paste any of these IDs to scrape them:\n"
        f"`{id_list}`"
    )

    for chunk in chunk_message("\n".join(lines), 4000):
        await ctx.bot.send_message(chat_id, chunk, parse_mode="Markdown")


# ─── HEALTH SERVER + SELF-PING ────────────────────────────────────────────────

async def health_handler(request: web.Request) -> web.Response:
    return web.Response(text="OK ✅", status=200)


async def start_web_server():
    web_app = web.Application()
    web_app.router.add_get("/",       health_handler)
    web_app.router.add_get("/health", health_handler)
    runner = web.AppRunner(web_app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()
    logger.info("Health server listening on port %d", PORT)


async def self_ping_loop():
    await asyncio.sleep(20)
    target = RENDER_URL.rstrip("/") if RENDER_URL else f"http://localhost:{PORT}"
    logger.info("Self-ping → %s every %ds", target, PING_INTERVAL)
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                async with session.get(target, timeout=aiohttp.ClientTimeout(total=10)) as r:
                    logger.debug("Self-ping %s → %s", target, r.status)
            except Exception as e:
                logger.warning("Self-ping failed: %s", e)
            await asyncio.sleep(PING_INTERVAL)


# ─── MAIN ─────────────────────────────────────────────────────────────────────

async def main():
    if BOT_TOKEN == "YOUR_BOT_TOKEN_HERE":
        print("ERROR: Set BOT_TOKEN before running.")
        print("  export BOT_TOKEN='123456:ABC-...'")
        return

    try:
        get_apis_col()
        logger.info("MongoDB connected ✅")
    except Exception as e:
        logger.error("MongoDB connection failed: %s", e)
        print(f"ERROR: Cannot connect to MongoDB — {e}")
        return

    app = Application.builder().token(BOT_TOKEN).build()

    # /addapi — single-step wizard
    add_conv = ConversationHandler(
        entry_points=[CommandHandler("addapi", addapi_start)],
        states={
            ADD_URL: [MessageHandler(filters.TEXT & ~filters.COMMAND, addapi_url)],
        },
        fallbacks=[
            CommandHandler("cancel",    conv_cancel),
            CommandHandler("sites",     conv_cancel),
            CommandHandler("manual",    conv_cancel),
            CommandHandler("search",    conv_cancel),
            CommandHandler("deleteapi", conv_cancel),
        ],
    )

    # /deleteapi wizard
    del_conv = ConversationHandler(
        entry_points=[CommandHandler("deleteapi", cmd_deleteapi)],
        states={
            DEL_CONFIRM: [MessageHandler(filters.TEXT & ~filters.COMMAND, del_confirm)],
        },
        fallbacks=[
            CommandHandler("cancel",  conv_cancel),
            CommandHandler("sites",   conv_cancel),
            CommandHandler("manual",  conv_cancel),
            CommandHandler("search",  conv_cancel),
            CommandHandler("addapi",  conv_cancel),
        ],
    )

    app.add_handler(CommandHandler("start",    cmd_start))
    app.add_handler(CommandHandler("help",     cmd_help))
    app.add_handler(CommandHandler("listapis", cmd_listapis))
    app.add_handler(CommandHandler("sites",    cmd_sites))
    app.add_handler(CommandHandler("manual",   cmd_manual))
    app.add_handler(CommandHandler("search",   cmd_search))
    app.add_handler(CommandHandler("cancel",   conv_cancel))
    app.add_handler(add_conv)
    app.add_handler(del_conv)
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    await start_web_server()
    asyncio.create_task(self_ping_loop())

    await app.initialize()
    await app.start()
    await app.updater.start_polling(allowed_updates=Update.ALL_TYPES)

    logger.info("✅ ClassX Telegram Bot v2 running.")

    try:
        await asyncio.Event().wait()
    finally:
        await app.updater.stop()
        await app.stop()
        await app.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
