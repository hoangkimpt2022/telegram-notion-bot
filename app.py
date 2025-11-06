#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
app.py - Complete webhook bot (merged + DAO relation fix)
Behavior:
 - "name đáo" -> search in TARGET_NOTION_DATABASE_ID (dao data)
 - preview total/per_day/dates -> wait 'ok'
 - on 'ok' -> create pages in NOTION_DATABASE_ID (calendar DB)
   with properties:
     - Title = "{name} — đáo YYYY-MM-DD"
     - DATE_PROP (Ngày Góp) = date
     - CHECKBOX_PROP (Đã Góp) = True
     - "Lịch G" (relation) -> link to source page id (source in TARGET_NOTION_DATABASE_ID)
     - "Source Page" (rich_text) for trace
 - idempotency: skip if page with same name token + date exists in NOTION_DATABASE_ID
 - robust Notion retries, logging, and Telegram confirmation flow
"""
import os
import time
import json
import math
import requests
import random
import traceback
import threading
import re
import unicodedata
from flask import Flask, request, Response
from typing import List, Tuple, Optional, Dict, Any
from pathlib import Path
from datetime import datetime, timezone, timedelta

app = Flask(__name__)

# ---------------- CONFIG (from env) ----------------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")  # optional restrict
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID")  # calendar DB (where to create pages)
TARGET_NOTION_DATABASE_ID = os.getenv("TARGET_NOTION_DATABASE_ID")  # dao data DB (where to search)
SEARCH_PROPERTY = ""  # "" means search title (Name)
CHECKBOX_PROP = os.getenv("CHECKBOX_PROP", "Đã Góp")
DATE_PROP_NAME = os.getenv("DATE_PROP_NAME", "Ngày Góp")
# DAO config
DAO_CONFIRM_TIMEOUT = int(os.getenv("DAO_CONFIRM_TIMEOUT", 120))
DAO_MAX_DAYS = int(os.getenv("DAO_MAX_DAYS", 30))
DAO_TOTAL_FIELD_CANDIDATES = os.getenv("DAO_TOTAL_FIELDS", "✅Đáo/thối,total,pre,tong,Σ").split(",")
DAO_CALC_TOTAL_FIELDS = ["trước", "pre"]
DAO_PERDAY_FIELD_CANDIDATES = os.getenv("DAO_PERDAY_FIELDS", "G ngày,per_day,perday,trước /ngày").split(",")
DAO_CHECKFIELD_CANDIDATES = os.getenv("DAO_CHECK_FIELDS", "Đáo/Thối,Đáo,Đáo Thối,dao,daothoi").split(",")
# Operational settings
WAIT_CONFIRM = int(os.getenv("WAIT_CONFIRM", 120))
NOTION_PAGE_SIZE = int(os.getenv("NOTION_PAGE_SIZE", 100))
MAX_PREVIEW = int(os.getenv("MAX_PREVIEW", 100))
PATCH_DELAY = float(os.getenv("PATCH_DELAY", 0.45))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", 3))
RETRY_SLEEP = float(os.getenv("RETRY_SLEEP", 1.0))
LOG_PATH = Path(os.getenv("LOG_PATH", "notion_assistant_actions.log"))
NOTION_CACHE_TTL = int(os.getenv("NOTION_CACHE_TTL", 20))

if TELEGRAM_TOKEN:
    BASE_TELE_URL = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"
else:
    BASE_TELE_URL = None

NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}" if NOTION_TOKEN else "",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}

# state
pending_confirm: Dict[str, Dict[str, Any]] = {}

# simple cache for source (calendar) DB (used by many functions)
_NOTION_CACHE = {"ts": 0.0, "pages": [], "tokens_map": {}, "preview_map": {}, "props_map": {}}
_cache_lock = threading.Lock()

# ---------------- Helpers ----------------
def normalize_notion_id(maybe_id: Optional[str]) -> Optional[str]:
    if not maybe_id:
        return None
    s = (maybe_id or "").strip().replace("-", "")
    if len(s) == 32:
        return f"{s[0:8]}-{s[8:12]}-{s[12:16]}-{s[16:20]}-{s[20:32]}"
    return maybe_id

NOTION_DATABASE_ID = normalize_notion_id(NOTION_DATABASE_ID)
TARGET_NOTION_DATABASE_ID = normalize_notion_id(TARGET_NOTION_DATABASE_ID) if TARGET_NOTION_DATABASE_ID else None

def log_action(entry: dict):
    try:
        LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with LOG_PATH.open("a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception as e:
        print("Log write error:", e)

def now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat()

def send_telegram(chat_id: str, text: str) -> bool:
    if not BASE_TELE_URL:
        print("No TELEGRAM_TOKEN set; cannot send message.")
        return False
    try:
        r = requests.post(BASE_TELE_URL + "/sendMessage", json={"chat_id": chat_id, "text": text}, timeout=15)
        r.raise_for_status()
        return True
    except Exception as e:
        print("Telegram send error:", e)
        return False

def send_long_text(chat_id: str, text: str):
    limit = 3800
    parts = []
    cur = ""
    for line in text.splitlines(keepends=True):
        if len(cur) + len(line) > limit:
            parts.append(cur)
            cur = ""
        cur += line
    if cur:
        parts.append(cur)
    for p in parts:
        send_telegram(chat_id, p)
        time.sleep(0.2)

# text utils
def strip_accents(s: str) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFD", s)
    return "".join(ch for ch in s if unicodedata.category(ch) != "Mn")

def normalize_and_tokenize(s: str) -> List[str]:
    if not s:
        return []
    t = strip_accents(s).lower()
    return re.findall(r"\w+", t)

# ---------------- Notion API helpers ----------------
def notion_query_all_raw(db_id: str) -> List[dict]:
    if not db_id:
        return []
    url = f"https://api.notion.com/v1/databases/{db_id}/query"
    results = []
    payload: Dict[str, Any] = {"page_size": NOTION_PAGE_SIZE}
    next_cursor = None
    while True:
        if next_cursor:
            payload["start_cursor"] = next_cursor
        resp = requests.post(url, headers=NOTION_HEADERS, json=payload, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        results.extend(data.get("results", []))
        next_cursor = data.get("next_cursor")
        if not next_cursor:
            break
    return results

def _refresh_notion_cache_if_needed():
    """Cache for NOTION_DATABASE_ID (calendar DB) used by many existing flows."""
    with _cache_lock:
        now = time.time()
        if now - _NOTION_CACHE["ts"] <= NOTION_CACHE_TTL and _NOTION_CACHE["pages"]:
            return
        pages = notion_query_all_raw(NOTION_DATABASE_ID) if NOTION_DATABASE_ID else []
        _NOTION_CACHE["pages"] = pages
        _NOTION_CACHE["tokens_map"].clear()
        _NOTION_CACHE["preview_map"].clear()
        _NOTION_CACHE["props_map"].clear()
        for p in pages:
            pid = p.get("id")
            props = p.get("properties", {})
            title = extract_title_from_props(props)
            auto_text = extract_prop_text(props, SEARCH_PROPERTY) if SEARCH_PROPERTY else ""
            combined = " | ".join([x for x in (title, auto_text) if x])
            _NOTION_CACHE["tokens_map"][pid] = normalize_and_tokenize(combined)
            _NOTION_CACHE["preview_map"][pid] = title or auto_text or pid
            _NOTION_CACHE["props_map"][pid] = props
        _NOTION_CACHE["ts"] = now

def notion_get_page(page_id: str) -> dict:
    url = f"https://api.notion.com/v1/pages/{page_id}"
    r = requests.get(url, headers=NOTION_HEADERS, timeout=15)
    r.raise_for_status()
    page = r.json()
    # update cache entry if it's in calendar db
    with _cache_lock:
        pid = page.get("id")
        props = page.get("properties", {})
        title = extract_title_from_props(props)
        auto_text = extract_prop_text(props, SEARCH_PROPERTY) if SEARCH_PROPERTY else ""
        combined = " | ".join([x for x in (title, auto_text) if x])
        _NOTION_CACHE["tokens_map"][pid] = normalize_and_tokenize(combined)
        _NOTION_CACHE["preview_map"][pid] = title or auto_text or pid
        _NOTION_CACHE["props_map"][pid] = props
    return page

def notion_patch_page_properties(page_id: str, properties: dict) -> Tuple[bool, str]:
    url = f"https://api.notion.com/v1/pages/{page_id}"
    body = {"properties": properties}
    err = "unknown"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.patch(url, headers=NOTION_HEADERS, json=body, timeout=15)
            if r.status_code == 200:
                return True, "OK"
            else:
                err = f"{r.status_code}: {r.text}"
                print("Notion patch error:", err)
            time.sleep(RETRY_SLEEP * attempt)
        except Exception as e:
            err = str(e)
            print("Notion patch exception:", err)
            time.sleep(RETRY_SLEEP * attempt)
    return False, err

def notion_archive_page(page_id: str) -> Tuple[bool, str]:
    url = f"https://api.notion.com/v1/pages/{page_id}"
    body = {"archived": True}
    err = "unknown"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.patch(url, headers=NOTION_HEADERS, json=body, timeout=15)
            if r.status_code == 200:
                return True, "OK"
            else:
                err = f"{r.status_code}: {r.text}"
                print("Notion archive error:", err)
            time.sleep(RETRY_SLEEP * attempt)
        except Exception as e:
            err = str(e)
            print("Notion archive exception:", err)
            time.sleep(RETRY_SLEEP * attempt)
    return False, err

def notion_archive_page_revert(page_id: str) -> Tuple[bool, str]:
    url = f"https://api.notion.com/v1/pages/{page_id}"
    body = {"archived": False}
    err = "unknown"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.patch(url, headers=NOTION_HEADERS, json=body, timeout=15)
            if r.status_code == 200:
                return True, "OK"
            else:
                err = f"{r.status_code}: {r.text}"
            time.sleep(RETRY_SLEEP * attempt)
        except Exception as e:
            err = str(e)
            time.sleep(RETRY_SLEEP * attempt)
    return False, err

def notion_create_page_in_db(db_id: str, properties: dict) -> Tuple[bool, dict]:
    url = "https://api.notion.com/v1/pages"
    body = {"parent": {"database_id": db_id}, "properties": properties}
    err: Dict[str, Any] = {}
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.post(url, headers=NOTION_HEADERS, json=body, timeout=20)
            if r.status_code == 200:
                return True, r.json()
            else:
                err = {"status": r.status_code, "text": r.text}
                print("Notion create error:", err)
            time.sleep(RETRY_SLEEP * attempt)
        except Exception as e:
            err = {"exception": str(e)}
            print("Notion create exception:", err)
            time.sleep(RETRY_SLEEP * attempt)
    return False, err

# ---------------- Extractors ----------------
def _join_plain_text_array(arr: List[dict]) -> str:
    if not arr:
        return ""
    return "".join([x.get("plain_text", "") for x in arr if isinstance(x, dict)]).strip()

def extract_prop_text(props: dict, name: str) -> str:
    if not props or not name:
        return ""
    key = next((k for k in props if k.lower() == name.lower()), None)
    if key is None:
        return ""
    prop = props.get(key, {})
    ptype = prop.get("type", "")
    try:
        if ptype in ("title", "rich_text"):
            return _join_plain_text_array(prop.get(ptype, []))
        if ptype == "formula":
            form = prop.get("formula", {})
            return (form.get("string") or "").strip()
        if ptype == "url":
            return (prop.get("url") or "").strip()
        if ptype == "select":
            sel = prop.get("select")
            return (sel.get("name", "") if isinstance(sel, dict) else "").strip()
        if ptype == "multi_select":
            arr = prop.get("multi_select", [])
            if isinstance(arr, list):
                return ", ".join([a.get("name", "") for a in arr]).strip()
        for c in ("plain_text", "text", "name", "value"):
            v = prop.get(c)
            if isinstance(v, str) and v.strip():
                return v.strip()
            if isinstance(v, list):
                return " ".join([x.get("plain_text", "") for x in v if x.get("plain_text")]).strip()
    except Exception:
        pass
    return ""

def extract_title_from_props(props: dict) -> str:
    for k, v in props.items():
        if isinstance(v, dict) and v.get("type") == "title":
            return _join_plain_text_array(v.get("title", []))
    return extract_prop_text(props, "Name") or extract_prop_text(props, "Title") or ""

def find_date_for_page(p: dict) -> Optional[str]:
    props = p.get("properties", {})
    if DATE_PROP_NAME:
        date_text = extract_date_from_prop(props, DATE_PROP_NAME)
        if date_text:
            return date_text
    for k, v in props.items():
        if isinstance(v, dict) and v.get("type") == "date":
            dt = v.get("date", {})
            if isinstance(dt, dict):
                start = dt.get("start")
                if start:
                    return start
    return p.get("created_time")

def extract_date_from_prop(props: dict, prop_name: str) -> Optional[str]:
    key = next((k for k in props if k.lower() == prop_name.lower()), None)
    if key is None:
        return None
    v = props.get(key, {})
    if isinstance(v, dict) and v.get("type") == "date":
        dt = v.get("date", {})
        if isinstance(dt, dict):
            return dt.get("start")
    return None

def is_checked(props: dict, checkbox_name: str) -> Optional[bool]:
    key = next((k for k in props if k.lower() == checkbox_name.lower()), None)
    if key is None:
        return None
    v = props.get(key, {})
    if isinstance(v, dict) and v.get("type") == "checkbox":
        return bool(v.get("checkbox"))
    return None

# ---------------- Matching helpers ----------------
def parse_date_or_max(s: str) -> datetime:
    if not s:
        return datetime.max.replace(tzinfo=timezone.utc)
    try:
        ds = s.replace("Z", "+00:00") if s.endswith("Z") else s
        return datetime.fromisoformat(ds)
    except Exception:
        try:
            return datetime.fromtimestamp(float(s))
        except Exception:
            return datetime.max.replace(tzinfo=timezone.utc)

def _get_cached_pages_and_maps():
    _refresh_notion_cache_if_needed()
    with _cache_lock:
        return _NOTION_CACHE["pages"], _NOTION_CACHE["tokens_map"], _NOTION_CACHE["preview_map"], _NOTION_CACHE["props_map"]

def find_matching_unchecked_pages(db_id: str, keyword: str, limit: Optional[int] = None) -> List[Tuple[str, str, str]]:
    keyword = keyword.strip()
    if not keyword or not db_id:
        return []
    keyword_norm = strip_accents(keyword).lower()
    pages, tokens_map, preview_map, props_map = _get_cached_pages_and_maps()
    matches = []
    for p in pages:
        pid = p.get("id")
        tokens = tokens_map.get(pid, [])
        if keyword_norm in tokens:
            props = props_map.get(pid, {})
            cb = is_checked(props, CHECKBOX_PROP)
            if cb is True:
                continue
            date_iso = find_date_for_page(p) or ""
            preview = preview_map.get(pid, pid)
            matches.append((pid, preview, date_iso))
    matches_sorted = sorted(matches, key=lambda it: (parse_date_or_max(it[2]), it[1].lower()))
    return matches_sorted[:limit] if limit else matches_sorted

def find_matching_all_pages_in_db(db_id: str, keyword: str, limit: Optional[int] = None) -> List[Tuple[str, str, str]]:
    """
    Query a given db_id (no cache) and return tuples (page_id, title, date_iso)
    Used for DAO search in TARGET_NOTION_DATABASE_ID.
    """
    keyword = keyword.strip()
    if not keyword or not db_id:
        return []
    pages = notion_query_all_raw(db_id)
    keyword_norm = strip_accents(keyword).lower()
    matches = []
    for p in pages:
        pid = p.get("id")
        props = p.get("properties", {})
        title = extract_title_from_props(props) or pid
        combined = " ".join([title, extract_prop_text(props, SEARCH_PROPERTY) or ""])
        tokens = normalize_and_tokenize(combined)
        if keyword_norm in tokens:
            date_iso = find_date_for_page(p) or ""
            matches.append((pid, title, date_iso))
    matches_sorted = sorted(matches, key=lambda it: (parse_date_or_max(it[2]), it[1].lower()))
    return matches_sorted[:limit] if limit else matches_sorted

def find_matching_all_pages(db_id: str, keyword: str, limit: Optional[int] = None) -> List[Tuple[str, str, str]]:
    # default behavior uses cached calendar DB (NOTION_DATABASE_ID)
    keyword = keyword.strip()
    if not keyword or not db_id:
        return []
    keyword_norm = strip_accents(keyword).lower()
    pages, tokens_map, preview_map, props_map = _get_cached_pages_and_maps()
    matches = []
    for p in pages:
        pid = p.get("id")
        tokens = tokens_map.get(pid, [])
        if keyword_norm in tokens:
            date_iso = find_date_for_page(p) or ""
            preview = preview_map.get(pid, pid)
            matches.append((pid, preview, date_iso))
    matches_sorted = sorted(matches, key=lambda it: (parse_date_or_max(it[2]), it[1].lower()))
    return matches_sorted[:limit] if limit else matches_sorted

def find_matching_pages_counts(db_id: str, keyword: str) -> Tuple[int, int]:
    keyword = keyword.strip()
    if not keyword or not db_id:
        return 0, 0
    keyword_norm = strip_accents(keyword).lower()
    pages, tokens_map, _, props_map = _get_cached_pages_and_maps()
    unchecked = 0
    checked = 0
    for p in pages:
        pid = p.get("id")
        tokens = tokens_map.get(pid, [])
        if keyword_norm in tokens:
            props = props_map.get(pid, {})
            cb = is_checked(props, CHECKBOX_PROP)
            if cb is True:
                checked += 1
            else:
                unchecked += 1
    return unchecked, checked

# ---------------- DAO helpers & create with relation ----------------
def extract_number_from_prop(props: dict, candidate_names: List[str]) -> Optional[float]:
    for name in candidate_names:
        val = extract_prop_text(props, name)
        if val:
            v = val.replace(",", "").strip()
            m = re.findall(r"-?\d+\.?\d*", v)
            if m:
                try:
                    return float(m[0])
                except ValueError:
                    continue
    # fallback: scan all props
    for k in props:
        text = extract_prop_text(props, k)
        if text:
            v = text.replace(",", "")
            m = re.findall(r"-?\d+\.?\d*", v)
            if m:
                try:
                    return float(m[0])
                except ValueError:
                    continue
    return None

def check_checkfield_has_check(props: dict, candidates: List[str]) -> bool:
    for name in candidates:
        txt = extract_prop_text(props, name)
        if txt and "✅" in txt:
            return True
    for k in props:
        txt = extract_prop_text(props, k)
        if txt and "✅" in txt:
            return True
    return False

def build_dao_preview_text(name: str, display_total: Optional[float], per_day: Optional[float], days: int, start_date: datetime, calc_total: Optional[float]) -> str:
    lines = []
    lines.append(f"🔔 đáo lại cho: {name} - Tổng đáo: ✅ {int(display_total) if display_total is not None else display_total}")
    lines.append(f"Lấy trước: {days} ngày {int(per_day) if per_day is not None else per_day} | {int(calc_total) if calc_total is not None else calc_total}")
    lines.append("")
    lines.append("Danh sách ngày dự kiến tạo (bắt đầu từ hôm nay):")
    for i in range(days):
        dt = start_date.date() + timedelta(days=i)
        lines.append(f"{i+1}. {dt.isoformat()}")
    lines.append("")
    lines.append(f"Gửi 'ok' để tạo {days} page, hoặc 'cancel' để hủy.")
    return "\n".join(lines)

def notion_find_pages_by_name_and_date_in_db(db_id: str, name_token: str, date_iso: str) -> List[dict]:
    """
    Check existence in a given db (used for idempotency); not cached for reliability.
    """
    pages = notion_query_all_raw(db_id)
    nt = strip_accents(name_token).lower()
    res = []
    for p in pages:
        props = p.get("properties", {})
        title = extract_title_from_props(props) or ""
        tokens = normalize_and_tokenize(title)
        if nt in tokens:
            d = extract_date_from_prop(props, DATE_PROP_NAME)
            if d and d.startswith(date_iso):
                res.append(p)
    return res

def create_pages_for_dates(user_chat: str, name: str, source_page_id: str, dates: List[datetime]) -> Tuple[List[dict], List[dict]]:
    """
    Create pages in NOTION_DATABASE_ID (calendar DB).
    Set 'Lịch G' relation to source_page_id (which lives in TARGET_NOTION_DATABASE_ID).
    """
    created = []
    skipped = []
    for dt in dates:
        date_iso = dt.date().isoformat()
        existing = notion_find_pages_by_name_and_date_in_db(NOTION_DATABASE_ID, name, date_iso)
        if existing:
            skipped.append({"date": date_iso, "reason": "exists", "page_id": existing[0].get("id")})
            continue
        title_prop_key = "Name"
        date_prop_key = DATE_PROP_NAME or "Ngày Góp"
        checkbox_key = CHECKBOX_PROP
        source_text = f"source_page_id: {source_page_id}"
        # Build properties including relation for "Lịch G" -> link to source page id
        properties = {
            title_prop_key: {"title": [{"text": {"content": f"{name} — đáo {date_iso}"}}]},
            date_prop_key: {"date": {"start": date_iso}},
            checkbox_key: {"checkbox": True},
            "Source Page": {"rich_text": [{"text": {"content": source_text}}]},
        }
        # Add relation "Lịch G" if TARGET_NOTION_DATABASE_ID/source_page_id provided
        if source_page_id:
            properties["Lịch G"] = {"relation": [{"id": source_page_id}]}
        ok, created_obj = notion_create_page_in_db(NOTION_DATABASE_ID, properties)
        if ok:
            created.append(created_obj)
        else:
            skipped.append({"date": date_iso, "reason": "create_failed", "error": created_obj})
        time.sleep(0.25)
    return created, skipped

# ---------------- Command parsing & handlers ----------------
def parse_user_command(text: str) -> Tuple[str, Optional[int], str]:
    text = text.strip()
    if not text:
        return "", None, "mark"
    parts = text.lower().split()
    if len(parts) >= 2 and parts[-1] in ("đáo", "dao", "da o", "dao"):
        return " ".join(parts[:-1]).strip(), None, "dao"
    if parts[-1] == "undo":
        return " ".join(parts[:-1]).strip(), None, "undo"
    action = "mark"
    last = parts[-1]
    n = None
    try:
        n = int(last)
        parts = parts[:-1]
    except ValueError:
        n = None
    if parts and parts[-1] in ("xóa", "xoa", "delete", "del"):
        action = "archive"
        parts = parts[:-1]
    keyword = " ".join(parts).strip()
    return keyword, n, action

def build_preview_lines(matches: List[Tuple[str, str, str]]) -> List[str]:
    lines = []
    for i, (pid, pre, d) in enumerate(matches, start=1):
        date_part = d[:10] if d else "-"
        lines.append(f"{i}. [{date_part}] {pre}")
    return lines

def parse_selection_text(sel_text: str, total: int) -> List[int]:
    s = sel_text.strip().lower()
    if not s:
        return []
    if s in ("all", "tất cả", "tat ca"):
        return list(range(1, total + 1))
    if s in ("none", "0"):
        return []
    s = s.replace(".", " ").replace(",", " ")
    parts = [p.strip() for p in s.split() if p.strip()]
    res = set()
    for p in parts:
        if "-" in p:
            try:
                a, b = map(int, p.split("-", 1))
                for i in range(min(a, b), max(a, b) + 1):
                    if 1 <= i <= total:
                        res.add(i)
            except ValueError:
                continue
        else:
            try:
                n = int(p)
                if 1 <= n <= total:
                    res.add(n)
            except ValueError:
                continue
    return sorted(res)

def find_prop_key_case_insensitive(props: dict, name: str) -> Optional[str]:
    return next((k for k in props if k.lower() == name.lower()), None)

def handle_command_mark(chat_id: str, keyword: str, count: Optional[int], orig_cmd: str):
    try:
        unchecked_count, checked_count = find_matching_pages_counts(NOTION_DATABASE_ID, keyword)
        if count and count > 0:
            matches = find_matching_unchecked_pages(NOTION_DATABASE_ID, keyword, limit=count)
            if len(matches) < count:
                send_telegram(chat_id, f"⚠️ Chỉ có {len(matches)} mục chưa tích cho '{keyword}', không đủ {count}.")
                return
            to_apply = matches[:count]
            succeeded, failed = [], []
            op_id = f"op-{int(time.time())}-{random.randint(1000,9999)}"
            for idx, (pid, pre, d) in enumerate(to_apply, start=1):
                try:
                    page = notion_get_page(pid)
                    props = page.get("properties", {})
                    cb = is_checked(props, CHECKBOX_PROP)
                    if cb is True:
                        failed.append((pid, pre, d, "Already checked"))
                        continue
                except Exception as e:
                    failed.append((pid, pre, d, f"fetch error {str(e)}"))
                    continue
                prop_key = find_prop_key_case_insensitive(props, CHECKBOX_PROP)
                prop_obj = {prop_key if prop_key else CHECKBOX_PROP: {"checkbox": True}}
                ok, msg = notion_patch_page_properties(pid, prop_obj)
                if ok:
                    time.sleep(0.6)
                    try:
                        new_page = notion_get_page(pid)
                        new_props = new_page.get("properties", {})
                        if is_checked(new_props, CHECKBOX_PROP):
                            succeeded.append((pid, pre, d))
                        else:
                            failed.append((pid, pre, d, "verify failed"))
                    except Exception as e:
                        failed.append((pid, pre, d, f"verify error {str(e)}"))
                else:
                    failed.append((pid, pre, d, msg))
                time.sleep(PATCH_DELAY)
            log_action({
                "ts": now_iso(), "type": "mark_auto", "op_id": op_id, "user_chat": chat_id,
                "command": orig_cmd, "keyword": keyword, "requested_count": count,
                "succeeded": [{"page_id": p, "preview": pr, "date": dt} for p, pr, dt in succeeded],
                "failed": failed
            })
            res_lines = []
            if succeeded:
                res_lines.append(f"✅ Đã đánh dấu {len(succeeded)} mục cho '{keyword}':")
                for i, (p, pr, dt) in enumerate(succeeded, start=1):
                    res_lines.append(f"{i}. [{dt[:10] if dt else '-'}] {pr}")
            if failed:
                res_lines.append("\n⚠️ Một vài mục không cập nhật:")
                for i, item in enumerate(failed, start=1):
                    res_lines.append(f"{i}. {item[1]} ({item[3]})")
            send_long_text(chat_id, "\n".join(res_lines))
            return
        matches_full = find_matching_unchecked_pages(NOTION_DATABASE_ID, keyword, limit=MAX_PREVIEW)
        header = f"🔎 : '{keyword}'\n" \
                 f"✅ Đã tích: {checked_count}\n" \
                 f"🟡 Chưa tích: {unchecked_count}\n\n"
        if not matches_full:
            send_telegram(chat_id, header + "Không còn mục chưa tích để hiển thị.")
            return
        header += f"📤 Gửi số ( ví dụ 1 hoặc 1-3 ) trong {WAIT_CONFIRM}s để chọn, hoặc /cancel.\n"
        preview_lines = build_preview_lines(matches_full)
        send_long_text(chat_id, header + "\n".join(preview_lines))
        pending_confirm[str(chat_id)] = {
            "type": "mark",
            "keyword": keyword,
            "matches": matches_full,
            "expires": time.time() + WAIT_CONFIRM,
            "orig_command": orig_cmd
        }
    except Exception as e:
        print("handle_command_mark exception:", e)
        traceback.print_exc()
        send_telegram(chat_id, f"❌ Lỗi xử lý mark: {str(e)}")

def handle_command_archive(chat_id: str, keyword: str, count: Optional[int], orig_cmd: str):
    try:
        unchecked_count, checked_count = find_matching_pages_counts(NOTION_DATABASE_ID, keyword)
        limit = count if count and count > 0 else MAX_PREVIEW
        matches = find_matching_all_pages(NOTION_DATABASE_ID, keyword, limit=limit if count else MAX_PREVIEW)
        if not matches:
            send_telegram(chat_id, f"⚠️ Không tìm thấy mục chứa '{keyword}'.")
            return
        if count and count > 0:
            to_apply = matches[:count]
            succeeded, failed = [], []
            op_id = f"op-archive-{int(time.time())}-{random.randint(1000,9999)}"
            for idx, (pid, pre, d) in enumerate(to_apply, start=1):
                ok, msg = notion_archive_page(pid)
                if ok:
                    succeeded.append((pid, pre, d))
                else:
                    failed.append((pid, pre, d, msg))
                time.sleep(PATCH_DELAY)
            log_action({
                "ts": now_iso(), "type": "archive_auto", "op_id": op_id, "user_chat": chat_id,
                "command": orig_cmd, "keyword": keyword, "requested_count": count,
                "succeeded": [{"page_id": p, "preview": pr, "date": dt} for p, pr, dt in succeeded],
                "failed": failed
            })
            res_lines = []
            if succeeded:
                res_lines.append(f"🗑️ Đã archive {len(succeeded)} mục cho '{keyword}':")
                for i, (p, pr, dt) in enumerate(succeeded, start=1):
                    res_lines.append(f"{i}. [{dt[:10] if dt else '-'}] {pr}")
            if failed:
                res_lines.append("\n⚠️ Một vài mục không archive:")
                for i, item in enumerate(failed, start=1):
                    res_lines.append(f"{i}. {item[1]} ({item[3]})")
            send_long_text(chat_id, "\n".join(res_lines))
            return
        preview_lines = build_preview_lines(matches)
        header = f"🔎 Khách hàng: '{keyword}'\n" \
                 f"✅ Đã tích: {checked_count}\n" \
                 f"🟡 Chưa tích: {unchecked_count}\n\n" \
                 f"⚠️ CHÚ Ý: Bạn sắp archive {len(matches)} mục chứa '{keyword}'. Gửi số (ví dụ 1-7) trong {WAIT_CONFIRM}s để chọn, hoặc 'all' để archive tất cả, hoặc /cancel.\n"
        send_long_text(chat_id, header + "\n".join(preview_lines))
        pending_confirm[str(chat_id)] = {
            "type": "archive",
            "keyword": keyword,
            "matches": matches,
            "expires": time.time() + WAIT_CONFIRM,
            "orig_command": orig_cmd
        }
    except Exception as e:
        print("handle_command_archive exception:", e)
        traceback.print_exc()
        send_telegram(chat_id, f"❌ Lỗi xử lý archive: {str(e)}")

def process_pending_selection(chat_id: str, text: str):
    pc = pending_confirm.get(str(chat_id))
    if not pc:
        send_telegram(chat_id, "Không có thao tác đang chờ lựa chọn.")
        return
    if time.time() > pc.get("expires", 0):
        del pending_confirm[str(chat_id)]
        send_telegram(chat_id, "⏳ Hết thời gian chọn. Yêu cầu đã bị hủy.")
        return
    typ = pc.get("type")
    if typ in ("dao_choose", "dao_confirm"):
        process_pending_selection_for_dao(chat_id, text)
        return
    matches = pc.get("matches", [])
    total = len(matches)
    sel_indices = parse_selection_text(text, total)
    if not sel_indices:
        send_telegram(chat_id, "Không nhận được lựa chọn hợp lệ. Yêu cầu đã bị hủy.")
        del pending_confirm[str(chat_id)]
        return
    selected = [matches[i - 1] for i in sel_indices if i - 1 < len(matches)]
    if typ == "mark":
        succeeded, failed = [], []
        op_id = f"op-{int(time.time())}-{random.randint(1000,9999)}"
        for (pid, pre, d) in selected:
            try:
                page = notion_get_page(pid)
                props = page.get("properties", {})
                cb = is_checked(props, CHECKBOX_PROP)
                if cb is True:
                    failed.append((pid, pre, d, "Already checked"))
                    continue
            except Exception as e:
                failed.append((pid, pre, d, f"fetch error {str(e)}"))
                continue
            prop_key = find_prop_key_case_insensitive(props, CHECKBOX_PROP)
            prop_obj = {prop_key if prop_key else CHECKBOX_PROP: {"checkbox": True}}
            ok, msg = notion_patch_page_properties(pid, prop_obj)
            if ok:
                time.sleep(0.6)
                try:
                    new_page = notion_get_page(pid)
                    if is_checked(new_page.get("properties", {}), CHECKBOX_PROP):
                        succeeded.append((pid, pre, d))
                    else:
                        failed.append((pid, pre, d, "verify failed"))
                except Exception as e:
                    failed.append((pid, pre, d, f"verify error {str(e)}"))
            else:
                failed.append((pid, pre, d, msg))
            time.sleep(PATCH_DELAY)
        log_action({
            "ts": now_iso(), "type": "mark_manual", "op_id": op_id, "user_chat": chat_id,
            "command": pc.get("orig_command"), "keyword": pc.get("keyword"),
            "selected": [{"page_id": p, "preview": pr, "date": dt} for p, pr, dt in succeeded],
            "failed": failed
        })
        out = []
        if succeeded:
            out.append(f"✅ Đã đánh dấu {len(succeeded)} mục:")
            for i, (p, pr, dt) in enumerate(succeeded, start=1):
                out.append(f"{i}. [{dt[:10] if dt else '-'}] {pr}")
        if failed:
            out.append("\n⚠️ Một vài mục không cập nhật:")
            for i, item in enumerate(failed, start=1):
                out.append(f"{i}. {item[1]} ({item[3]})")
        send_long_text(chat_id, "\n".join(out))
        del pending_confirm[str(chat_id)]
        return
    elif typ == "archive":
        succeeded, failed = [], []
        op_id = f"op-archive-{int(time.time())}-{random.randint(1000,9999)}"
        for (pid, pre, d) in selected:
            ok, msg = notion_archive_page(pid)
            if ok:
                succeeded.append((pid, pre, d))
            else:
                failed.append((pid, pre, d, msg))
            time.sleep(PATCH_DELAY)
        log_action({
            "ts": now_iso(), "type": "archive_manual", "op_id": op_id, "user_chat": chat_id,
            "command": pc.get("orig_command"), "keyword": pc.get("keyword"),
            "selected": [{"page_id": p, "preview": pr, "date": dt} for p, pr, dt in succeeded],
            "failed": failed
        })
        out = []
        if succeeded:
            out.append(f"🗑️ Đã archive {len(succeeded)} mục:")
            for i, (p, pr, dt) in enumerate(succeeded, start=1):
                out.append(f"{i}. [{dt[:10] if dt else '-'}] {pr}")
        if failed:
            out.append("\n⚠️ Một vài mục không archive:")
            for i, item in enumerate(failed, start=1):
                out.append(f"{i}. {item[1]} ({item[3]})")
        send_long_text(chat_id, "\n".join(out))
        del pending_confirm[str(chat_id)]
        return
    else:
        send_telegram(chat_id, "Pending type không nhận diện.")
        del pending_confirm[str(chat_id)]

def undo_last(chat_id: str, op_id: Optional[str], keyword: Optional[str] = None):
    if not LOG_PATH.exists():
        send_telegram(chat_id, "Không có log để undo.")
        return
    lines = LOG_PATH.read_text(encoding="utf-8").strip().splitlines()
    if not lines:
        send_telegram(chat_id, "Log rỗng.")
        return
    found = None
    for ln in reversed(lines):
        try:
            entry = json.loads(ln)
        except json.JSONDecodeError:
            continue
        if str(entry.get("user_chat")) != str(chat_id):
            continue
        if keyword and entry.get("keyword", "").lower() != keyword.lower():
            continue
        if op_id:
            if entry.get("op_id") == op_id:
                found = entry
                break
        else:
            if entry.get("type", "").startswith("mark") or entry.get("type", "").startswith("archive"):
                found = entry
                break
    if not found:
        send_telegram(chat_id, "Không tìm thấy op để undo.")
        return
    typ = found.get("type", "")
    reverted = []
    failed = []
    if typ.startswith("mark"):
        items = found.get("succeeded", []) or found.get("selected", [])
        for it in items:
            pid = it.get("page_id")
            try:
                prop_key = find_prop_key_case_insensitive(notion_get_page(pid).get("properties", {}), CHECKBOX_PROP)
                ok, msg = notion_patch_page_properties(pid, {prop_key or CHECKBOX_PROP: {"checkbox": False}})
                if ok:
                    reverted.append(pid)
                else:
                    failed.append((pid, msg))
                time.sleep(PATCH_DELAY)
            except Exception as e:
                failed.append((pid, str(e)))
        send_telegram(chat_id, f"♻️ Undo done. Reverted {len(reverted)} items. Failed: {len(failed)}")
        log_action({"ts": now_iso(), "type": "undo", "user_chat": chat_id, "original_op": found.get("op_id"), "reverted": reverted, "failed": failed})
        return
    elif typ.startswith("archive"):
        items = found.get("succeeded", []) or found.get("selected", [])
        for it in items:
            pid = it.get("page_id")
            try:
                ok, msg = notion_archive_page_revert(pid)
                if ok:
                    reverted.append(pid)
                else:
                    failed.append((pid, msg))
                time.sleep(PATCH_DELAY)
            except Exception as e:
                failed.append((pid, str(e)))
        send_telegram(chat_id, f"♻️ Undo archive done. Reverted {len(reverted)} items. Failed: {len(failed)}")
        log_action({"ts": now_iso(), "type": "undo_archive", "user_chat": chat_id, "original_op": found.get("op_id"), "reverted": reverted, "failed": failed})
        return
    else:
        send_telegram(chat_id, "Không thể undo cho loại op này.")

# ---------------- DAO flow ----------------
def handle_command_dao(chat_id: str, keyword: str, orig_cmd: str):
    try:
        if not keyword:
            send_telegram(chat_id, "Vui lòng cung cấp tên (ví dụ: 'Trâm đáo').")
            return
        # SEARCH in TARGET_NOTION_DATABASE_ID (dao data)
        if not TARGET_NOTION_DATABASE_ID:
            send_telegram(chat_id, "⚠️ TARGET_NOTION_DATABASE_ID chưa cấu hình.")
            return
        matches = find_matching_all_pages_in_db(TARGET_NOTION_DATABASE_ID, keyword, limit=MAX_PREVIEW)
        if not matches:
            send_telegram(chat_id, f"⚠️ Không tìm thấy '{keyword}' trong DB đáo.")
            return
        if len(matches) > 1:
            header = f"Tìm thấy {len(matches)} kết quả cho '{keyword}'. Chọn index để tiếp tục hoặc gửi SĐT để match chính xác."
            preview_lines = build_preview_lines(matches)
            send_long_text(chat_id, header + "\n" + "\n".join(preview_lines))
            pending_confirm[str(chat_id)] = {
                "type": "dao_choose",
                "keyword": keyword,
                "matches": matches,
                "expires": time.time() + DAO_CONFIRM_TIMEOUT,
                "orig_command": orig_cmd
            }
            return
        # single match: use the page as source
        pid, preview, date_iso = matches[0]
        page = notion_get_page(pid)
        props = page.get("properties", {})
        ok_check = check_checkfield_has_check(props, DAO_CHECKFIELD_CANDIDATES)
        if not ok_check:
            send_telegram(chat_id, f"🔴 chưa thể đáo cho {preview}.")
            return
        display_total = extract_number_from_prop(props, DAO_TOTAL_FIELD_CANDIDATES)
        per_day = extract_number_from_prop(props, DAO_PERDAY_FIELD_CANDIDATES)
        calc_total = extract_number_from_prop(props, DAO_CALC_TOTAL_FIELDS) or display_total
        if per_day is None or per_day == 0:
            send_telegram(chat_id, f"⚠️ Không tìm thấy hoặc per_day = 0. Kiểm tra cột phần/ngày trên page {preview}.")
            return
        if calc_total is None:
            send_telegram(chat_id, f"⚠️ Không tìm thấy total trên page {preview}.")
            return
        days = int(math.ceil(calc_total / per_day))
        if days <= 0:
            send_telegram(chat_id, f"⚠️ Kết quả days không hợp lệ: {days}.")
            return
        if days > DAO_MAX_DAYS:
            send_telegram(chat_id, f"⚠️ Số ngày ({days}) vượt mức tối đa ({DAO_MAX_DAYS}). Hãy giảm hoặc thay đổi per_day.")
            return
        preview_text = build_dao_preview_text(preview, display_total, per_day, days, datetime.now(), calc_total)
        pending_confirm[str(chat_id)] = {
            "type": "dao_confirm",
            "keyword": keyword,
            "source_page_id": pid,
            "source_preview": preview,
            "display_total": display_total,
            "per_day": per_day,
            "calc_total": calc_total,
            "days": days,
            "start_date": datetime.now().date().isoformat(),
            "expires": time.time() + DAO_CONFIRM_TIMEOUT,
            "orig_command": orig_cmd
        }
        send_long_text(chat_id, preview_text)
    except Exception as e:
        print("handle_command_dao exception:", e)
        traceback.print_exc()
        send_telegram(chat_id, f"❌ Lỗi xử lý dao: {str(e)}")

def process_pending_selection_for_dao(chat_id: str, text: str):
    pc = pending_confirm.get(str(chat_id))
    if not pc:
        send_telegram(chat_id, "Không có thao tác đang chờ lựa chọn.")
        return
    if time.time() > pc.get("expires", 0):
        del pending_confirm[str(chat_id)]
        send_telegram(chat_id, "⏳ Hết thời gian chọn. Yêu cầu đã bị hủy.")
        return
    typ = pc.get("type")
    if typ == "dao_choose":
        matches = pc.get("matches", [])
        total = len(matches)
        sel_indices = parse_selection_text(text, total)
        if not sel_indices:
            send_telegram(chat_id, "Không nhận được lựa chọn hợp lệ. Yêu cầu đã bị hủy.")
            del pending_confirm[str(chat_id)]
            return
        selected = [matches[i - 1] for i in sel_indices]
        pid, preview, date_iso = selected[0]
        try:
            page = notion_get_page(pid)
            props = page.get("properties", {})
            ok_check = check_checkfield_has_check(props, DAO_CHECKFIELD_CANDIDATES)
            if not ok_check:
                send_telegram(chat_id, f"🔴 chưa thể đáo cho {preview}.")
                del pending_confirm[str(chat_id)]
                return
            display_total = extract_number_from_prop(props, DAO_TOTAL_FIELD_CANDIDATES)
            per_day = extract_number_from_prop(props, DAO_PERDAY_FIELD_CANDIDATES)
            calc_total = extract_number_from_prop(props, DAO_CALC_TOTAL_FIELDS) or display_total
            if per_day is None or per_day == 0:
                send_telegram(chat_id, f"⚠️ per_day không hợp lệ trên page {preview}.")
                del pending_confirm[str(chat_id)]
                return
            if calc_total is None:
                send_telegram(chat_id, f"⚠️ total không tìm được trên page {preview}.")
                del pending_confirm[str(chat_id)]
                return
            days = int(math.ceil(calc_total / per_day))
            if days > DAO_MAX_DAYS:
                send_telegram(chat_id, f"⚠️ Số ngày ({days}) vượt mức tối đa ({DAO_MAX_DAYS}). Hủy.")
                del pending_confirm[str(chat_id)]
                return
            pending_confirm[str(chat_id)] = {
                "type": "dao_confirm",
                "keyword": pc.get("keyword"),
                "source_page_id": pid,
                "source_preview": preview,
                "display_total": display_total,
                "per_day": per_day,
                "calc_total": calc_total,
                "days": days,
                "start_date": datetime.now().date().isoformat(),
                "expires": time.time() + DAO_CONFIRM_TIMEOUT,
                "orig_command": pc.get("orig_command")
            }
            preview_text = build_dao_preview_text(preview, display_total, per_day, days, datetime.now(), calc_total)
            send_long_text(chat_id, preview_text)
            return
        except Exception as e:
            send_telegram(chat_id, f"❌ Lỗi khi lấy page: {str(e)}")
            del pending_confirm[str(chat_id)]
            return
    if typ == "dao_confirm":
        if text.strip().lower() in ("cancel", "/cancel", "hủy", "huy"):
            del pending_confirm[str(chat_id)]
            send_telegram(chat_id, "Đã hủy thao tác đáo.")
            return
        if text.strip().lower() not in ("ok", "yes", "đồng ý", "dong y"):
            send_telegram(chat_id, "Gửi 'ok' để xác nhận tạo pages, hoặc 'cancel' để hủy.")
            return
        try:
            pcdata = pc
            days = int(pcdata.get("days", 0))
            if days <= 0:
                send_telegram(chat_id, "⚠️ Số ngày tính toán không hợp lệ.")
                del pending_confirm[str(chat_id)]
                return
            if days > DAO_MAX_DAYS:
                send_telegram(chat_id, f"⚠️ Số ngày vượt mức ({DAO_MAX_DAYS}). Hủy.")
                del pending_confirm[str(chat_id)]
                return
            src_pid = pcdata.get("source_page_id")
            name_preview = pcdata.get("source_preview")
            start_date = datetime.fromisoformat(pcdata.get("start_date"))
            dates = [start_date + timedelta(days=i) for i in range(days)]
            created, skipped = create_pages_for_dates(chat_id, name_preview, src_pid, dates)
            op_id = f"dao-{int(time.time())}-{random.randint(1000,9999)}"
            log_action({
                "ts": now_iso(),
                "type": "dao_create",
                "op_id": op_id,
                "user_chat": chat_id,
                "source_page": src_pid,
                "created_ids": [c.get("id") for c in created],
                "skipped": skipped,
                "params": {"days": days, "per_day": pcdata.get("per_day"), "calc_total": pcdata.get("calc_total")}
            })
            lines = []
            if created:
                lines.append(f"✅ Đã tạo {len(created)} page cho 🔎 {name_preview}:")
                for i, c in enumerate(created, start=1):
                    dateval = c.get("properties", {}).get(DATE_PROP_NAME, {}).get("date", {}).get("start", "") if c.get("properties") else ""
                    lines.append(f"{i}.[{dateval}] page_id: {c.get('id')}")
            if skipped:
                lines.append("")
                lines.append(f"⚠️ Skip/Failed: {len(skipped)}")
                for s in skipped:
                    if s.get("reason") == "exists":
                        lines.append(f"- [ {s.get('date')} ] skipped (exists) page_id: {s.get('page_id')}")
                    else:
                        lines.append(f"- [ {s.get('date')} ] failed: {s.get('reason')} {s.get('error')}")
            send_long_text(chat_id, "\n".join(lines))
            del pending_confirm[str(chat_id)]
            return
        except Exception as e:
            print("dao execution error:", e)
            traceback.print_exc()
            send_telegram(chat_id, f"❌ Lỗi khi tạo pages: {str(e)}")
            del pending_confirm[str(chat_id)]
            return
    send_telegram(chat_id, "Pending type không nhận diện (expecting dao flow or mark/archive).")
    del pending_confirm[str(chat_id)]

# ---------------- Message handler ----------------
def handle_incoming_message(chat_id: str, text: str):
    try:
        if TELEGRAM_CHAT_ID and str(chat_id) != str(TELEGRAM_CHAT_ID):
            send_telegram(chat_id, "⚠️ Bot chưa được phép nhận lệnh từ chat này.")
            return
        raw = text.strip()
        if not raw:
            send_telegram(chat_id, "Vui lòng gửi lệnh hoặc từ khoá.")
            return
        low = raw.lower()
        # Pending flow
        if str(chat_id) in pending_confirm:
            if low in ("/cancel", "cancel", "hủy", "huy"):
                del pending_confirm[str(chat_id)]
                send_telegram(chat_id, "Đã hủy thao tác đang chờ.")
                return
            if any(ch.isdigit() for ch in low) or low in ("all", "tất cả", "tat ca", "none") or low in ("ok", "yes", "đồng ý", "dong y"):
                send_telegram(chat_id, "Đang xử lý lựa chọn...")
                threading.Thread(target=process_pending_selection, args=(chat_id, raw), daemon=True).start()
                return
            del pending_confirm[str(chat_id)]
        if low in ("/cancel", "cancel", "hủy", "huy"):
            send_telegram(chat_id, "Không có thao tác đang chờ. /cancel ignored.")
            return
        keyword, count, action = parse_user_command(raw)
        if action == "undo":
            send_telegram(chat_id, "Đang tìm và undo...")
            threading.Thread(target=undo_last, args=(chat_id, None, keyword if keyword else None), daemon=True).start()
            return
        if action == "archive":
            send_telegram(chat_id, "Đang xử lý archive...")
            threading.Thread(target=handle_command_archive, args=(chat_id, keyword, count, raw), daemon=True).start()
            return
        if action == "dao":
            send_telegram(chat_id, "Đang xử lý đáo...")
            threading.Thread(target=handle_command_dao, args=(chat_id, keyword, raw), daemon=True).start()
            return
        # default mark
        send_telegram(chat_id, "Đang xử lý...")
        threading.Thread(target=handle_command_mark, args=(chat_id, keyword, count, raw), daemon=True).start()
    except Exception as e:
        print("handle_incoming_message exception:", e)
        traceback.print_exc()
        try:
            send_telegram(chat_id, f"❌ Lỗi xử lý: {str(e)}")
        except:
            pass

# ---------------- Webhook / health ----------------
@app.route('/webhook', methods=['POST'])
def webhook():
    update = request.get_json(silent=True)
    if update and 'message' in update:
        try:
            chat_id = str(update['message']['chat']['id'])
            text = update['message'].get('text', '')
            threading.Thread(target=handle_incoming_message, args=(chat_id, text), daemon=True).start()
        except Exception as e:
            print("Webhook handling exception:", e)
            traceback.print_exc()
    return Response('OK', status=200)

@app.route('/')
def home():
    return "OK", 200

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
