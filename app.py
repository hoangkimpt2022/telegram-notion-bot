#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
app.py - Telegram <-> Notion assistant (mark / archive / undo / dao flow)
Config via env:
  TELEGRAM_TOKEN, NOTION_TOKEN, NOTION_DATABASE_ID, TARGET_NOTION_DATABASE_ID (optional)
Behavior:
  - 'keyword' => show preview of unchecked items (mark flow)
  - 'keyword N' => mark N items as checked
  - 'keyword xóa' => archive flow
  - 'keyword đáo' => dao flow (preview) -> send 'ok' to create pages -> creates pages and ticks checkbox
Start on Render:
  - Add gunicorn in requirements and use: gunicorn app:app --bind 0.0.0.0:$PORT --workers 4
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
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")  # optional restrict to single chat id
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID")  # database to create new pages (calendar)
TARGET_NOTION_DATABASE_ID = os.getenv("TARGET_NOTION_DATABASE_ID")  # database where DAO records live
SEARCH_PROPERTY = ""  # optional property name to include for text search
CHECKBOX_PROP = os.getenv("CHECKBOX_PROP", "Đã Góp")
DATE_PROP_NAME = os.getenv("DATE_PROP_NAME", "Ngày Góp")

# DAO config & candidate column names (comma separated env vars allowed)
DAO_CONFIRM_TIMEOUT = int(os.getenv("DAO_CONFIRM_TIMEOUT", 120))
DAO_MAX_DAYS = int(os.getenv("DAO_MAX_DAYS", 40))
DAO_TOTAL_FIELD_CANDIDATES = os.getenv("DAO_TOTAL_FIELDS", "✅Đáo/thối,total,pre,tong,Σ").split(",")
DAO_CALC_TOTAL_FIELDS = os.getenv("DAO_CALC_TOTAL_FIELDS", "trước,prev_total,pre,# trước").split(",")
DAO_PERDAY_FIELD_CANDIDATES = os.getenv("DAO_PERDAY_FIELDS", "G ngày,per_day,perday,trước /ngày,Q G ngày").split(",")
DAO_CHECKFIELD_CANDIDATES = os.getenv("DAO_CHECK_FIELDS", "Đáo/Thối,Đáo,Đáo Thối,dao,daothoi,✅Đáo/thối").split(",")

# Additional candidates for prev total/days
DAO_PREV_TOTAL_CANDIDATES = ["Trước", "trước", "truoc", "pre", "prev", "prev_total", "for_pre", "forua"]
DAO_PREV_DAYS_CANDIDATES  = ["ngày trước", "ngày_trước", "ngay trước", "ngay_truoc", "days_before", "prev_days"]

# Operational settings
WAIT_CONFIRM = int(os.getenv("WAIT_CONFIRM", 120))
NOTION_PAGE_SIZE = int(os.getenv("NOTION_PAGE_SIZE", 100))
MAX_PREVIEW = int(os.getenv("MAX_PREVIEW", 100))
PATCH_DELAY = float(os.getenv("PATCH_DELAY", 0.45))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", 3))
RETRY_SLEEP = float(os.getenv("RETRY_SLEEP", 1.0))
LOG_PATH = Path(os.getenv("LOG_PATH", "notion_assistant_actions.log"))
NOTION_CACHE_TTL = int(os.getenv("NOTION_CACHE_TTL", 20))

# Behavior choices from user: create pages and tick checkbox (you chose "Tạo thật" + "Có tick")
AUTO_CREATE_ON_OK = True
AUTO_TICK_CREATED = True

if TELEGRAM_TOKEN:
    BASE_TELE_URL = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"
else:
    BASE_TELE_URL = None

NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}" if NOTION_TOKEN else "",
    # Keep a reasonably modern Notion-Version
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}

# state
pending_confirm: Dict[str, Dict[str, Any]] = {}
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
            if r.status_code in (200, 201):
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
        if ptype in ("number", "rich_text", "select", "multi_select"):
            # number/select types may store values differently; try to stringify
            if ptype == "number":
                return str(prop.get("number"))
            if ptype in ("select", "multi_select"):
                v = prop.get(ptype)
                if isinstance(v, dict):
                    return v.get("name", "")
                if isinstance(v, list):
                    return ", ".join([x.get("name", "") for x in v])
            return _join_plain_text_array(prop.get("rich_text", []))
        if ptype == "date":
            v = prop.get("date", {})
            return v.get("start", "") or ""
    except Exception:
        pass
    return ""

def extract_title_from_props(props: dict) -> str:
    if not props:
        return ""
    # try common title keys
    for k in props:
        if props[k].get("type") == "title":
            return _join_plain_text_array(props[k].get("title", []))
    # fallback: first text-like prop
    for k in props:
        if props[k].get("type") in ("rich_text", "select", "multi_select"):
            val = extract_prop_text(props, k)
            if val:
                return val
    return ""

def extract_number_from_prop(props: dict, name: str) -> Optional[float]:
    if not props:
        return None
    key = find_prop_key_case_insensitive(props, name)
    if not key:
        return None
    try:
        prop = props.get(key, {})
        ptype = prop.get("type", "")
        if ptype == "number":
            return prop.get("number")
        if ptype in ("formula",):
            # formula result might be number in 'number' field
            v = prop.get("number")
            if v is not None:
                return v
            # sometimes formula in Notion returns as string in 'rich_text'
            text = extract_prop_text(props, key)
            try:
                return float(re.sub(r"[^\d.-]", "", text)) if text else None
            except:
                return None
        # try to parse from text
        text = extract_prop_text(props, key)
        if not text:
            return None
        s = re.sub(r"[^\d\.\-]", "", text)
        if not s:
            return None
        return float(s)
    except Exception:
        return None

def find_prop_key_case_insensitive(props: dict, want: str) -> Optional[str]:
    if not props or not want:
        return None
    want_l = want.lower()
    for k in props:
        if k.lower() == want_l:
            return k
    # fuzzy match by substring
    for k in props:
        if want_l in k.lower():
            return k
    return None

def check_checkfield_has_check(props: dict, candidates: List[str]) -> bool:
    for c in candidates:
        key = find_prop_key_case_insensitive(props, c)
        if key:
            p = props.get(key, {})
            if p.get("type") == "checkbox" and p.get("checkbox"):
                return True
    return False

# ---------------- Query & match helpers ----------------
def find_matching_unchecked_pages(db_id: str, keyword: str, limit: int = 100) -> List[Tuple[str, str, Optional[str]]]:
    """
    Return list of (page_id, preview_text, date_iso) for pages whose title or SEARCH_PROPERTY contains keyword
    and have checkbox CHECKBOX_PROP == False (or missing)
    """
    _refresh_notion_cache_if_needed()
    kw_tokens = normalize_and_tokenize(keyword)
    matches = []
    for pid, tokens in _NOTION_CACHE["tokens_map"].items():
        if len(matches) >= limit:
            break
        if all(t in tokens for t in kw_tokens):
            props = _NOTION_CACHE["props_map"].get(pid, {})
            # check checkbox property
            prop_key = find_prop_key_case_insensitive(props, CHECKBOX_PROP)
            checked = False
            if prop_key:
                v = props.get(prop_key, {})
                if v.get("type") == "checkbox" and v.get("checkbox"):
                    checked = True
            if not checked:
                preview = _NOTION_CACHE["preview_map"].get(pid, pid)
                date_iso = None
                date_key = find_prop_key_case_insensitive(props, DATE_PROP_NAME)
                if date_key:
                    d = props.get(date_key, {}).get("date", {})
                    date_iso = d.get("start")
                matches.append((pid, preview, date_iso))
    return matches

def find_matching_pages_counts(db_id: str, keyword: str) -> Tuple[int, int]:
    _refresh_notion_cache_if_needed()
    kw_tokens = normalize_and_tokenize(keyword)
    checked = 0
    unchecked = 0
    for pid, tokens in _NOTION_CACHE["tokens_map"].items():
        if all(t in tokens for t in kw_tokens):
            props = _NOTION_CACHE["props_map"].get(pid, {})
            prop_key = find_prop_key_case_insensitive(props, CHECKBOX_PROP)
            if prop_key and props.get(prop_key, {}).get("checkbox"):
                checked += 1
            else:
                unchecked += 1
    return unchecked, checked

def find_matching_all_pages_in_db(db_id: str, keyword: str, limit: int = 100) -> List[Tuple[str, str, Optional[str]]]:
    # broader search across db pages (includes archived)
    pages = notion_query_all_raw(db_id)
    kw_tokens = normalize_and_tokenize(keyword)
    matches = []
    for p in pages:
        pid = p.get("id")
        props = p.get("properties", {})
        title = extract_title_from_props(props) or pid
        tokens = normalize_and_tokenize(" ".join([title, extract_prop_text(props, SEARCH_PROPERTY) if SEARCH_PROPERTY else ""]))
        if all(t in tokens for t in kw_tokens):
            date_iso = None
            for k in props:
                if k.lower() == DATE_PROP_NAME.lower() and props[k].get("type") == "date":
                    date_iso = props[k].get("date", {}).get("start")
                    break
            matches.append((pid, title, date_iso))
            if len(matches) >= limit:
                break
    return matches

# ---------------- UI builders ----------------
def build_preview_lines(matches: List[Tuple[str, str, Optional[str]]]) -> List[str]:
    lines = []
    for i, (pid, preview, date_iso) in enumerate(matches, start=1):
        date_sh = date_iso[:10] if date_iso else "-"
        lines.append(f"{i}. [{date_sh}] {preview} - ({pid})")
    return lines

def build_dao_preview_text(preview: str, display_total, per_day, days, start_date, calc_total) -> str:
    lines = []
    lines.append(f"🔔 đáo lại cho: {preview}")
    lines.append(f"Tổng đáo: {int(display_total) if display_total is not None else 'N/A'}")
    lines.append(f"Góp mỗi ngày: {int(per_day) if per_day is not None else 'N/A'}")
    lines.append(f"Số ngày: {days}")
    lines.append(f"Bắt đầu từ: {start_date.isoformat()}")
    if calc_total is not None:
        lines.append(f"Giá trị công thức (trước): {int(calc_total)}")
    return "\n".join(lines)

# ---------------- Command handlers ----------------
def handle_command_mark(chat_id: str, keyword: str, orig_cmd: str):
    try:
        matches_full = find_matching_unchecked_pages(NOTION_DATABASE_ID, keyword, limit=MAX_PREVIEW)
        header = f"🔎 : '{keyword}'\n" \
                 f"✅ Đã tích: {find_matching_pages_counts(NOTION_DATABASE_ID, keyword)[1]}\n" \
                 f"🟡 Chưa tích: {find_matching_pages_counts(NOTION_DATABASE_ID, keyword)[0]}\n\n"
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

def handle_command_mark_quick(chat_id: str, keyword: str, n: int, orig_cmd: str):
    try:
        matches = find_matching_unchecked_pages(NOTION_DATABASE_ID, keyword, limit=n)
        if not matches:
            send_telegram(chat_id, f"Không tìm thấy mục nào cho '{keyword}' để đánh dấu.")
            return
        succeeded = []
        failed = []
        for pid, preview, d in matches:
            try:
                prop_key = find_prop_key_case_insensitive(notion_get_page(pid).get("properties", {}), CHECKBOX_PROP)
                ok, msg = notion_patch_page_properties(pid, {prop_key or CHECKBOX_PROP: {"checkbox": True}})
                if ok:
                    succeeded.append((pid, preview, d))
                else:
                    failed.append((pid, preview, d, msg))
                time.sleep(PATCH_DELAY)
            except Exception as e:
                failed.append((pid, preview, d, str(e)))
        # send result
        res_lines = [f"✅ Đã đánh dấu {len(succeeded)} mục:"]
        for i, (p, pr, dt) in enumerate(succeeded, start=1):
            res_lines.append(f"{i}. [{dt[:10] if dt else '-'}] {pr}")
        if failed:
            res_lines.append("\n⚠️ Một vài mục không được đánh dấu:")
            for i, item in enumerate(failed, start=1):
                res_lines.append(f"{i}. {item[1]} ({item[3]})")
        send_long_text(chat_id, "\n".join(res_lines))
        log_action({
            "ts": now_iso(), "type": "mark_manual_quick", "user_chat": chat_id,
            "command": orig_cmd, "keyword": keyword,
            "selected": [{"page_id": p, "preview": pr, "date": dt} for p, pr, dt in succeeded],
            "failed": failed
        })
    except Exception as e:
        print("handle_command_mark_quick exception:", e)
        traceback.print_exc()
        send_telegram(chat_id, f"❌ Lỗi xử lý mark_quick: {str(e)}")

def handle_command_archive(chat_id: str, keyword: str, orig_cmd: str):
    try:
        matches = find_matching_unchecked_pages(NOTION_DATABASE_ID, keyword, limit=MAX_PREVIEW)
        unchecked_count, checked_count = find_matching_pages_counts(NOTION_DATABASE_ID, keyword)
        if not matches:
            send_telegram(chat_id, f"Không tìm thấy mục để archive cho '{keyword}'.")
            return
        # build preview
        preview_lines = build_preview_lines(matches)
        header = f"🔎 Khách hàng: '{keyword}'\n" \
                 f"✅ Đã tích: {checked_count}\n" \
                 f"🟡 Chưa tích: {unchecked_count}\n\n" \
                 f"⚠️ CHÚ Ý: Bạn sắp archive {len(matches)} mục. Gửi chỉ số trong 120s để chọn, hoặc 'all' để archive tất cả, hoặc /cancel.\n"
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
    if pc["type"] == "dao_confirm":
        if text.strip().lower() not in ("/ok", "ok"):
            send_telegram(chat_id, "Gửi '/ok' để xác nhận.")
            return

        # Lấy dữ liệu từ pending
        source_page_id = pc["source_page_id"]
        name = pc["source_preview"]
        days = int(extract_number_from_prop(
            notion_get_page(source_page_id).get("properties", {}),
            "# ngày trước"
        ) or 0)

        if days <= 0:
            send_telegram(chat_id, "Số ngày không hợp lệ.")
            del pending_confirm[str(chat_id)]
            return

        # Tạo pages
        start = datetime.fromisoformat(pc.get("start_date")).date() if pc.get("start_date") else (datetime.now().date() + timedelta(days=1))
        dates = [start + timedelta(days=i) for i in range(pc.get("days", 0))]
        created = []
        skipped = []
        for d in dates:
            try:
                props = {
                    "Name": {"title": [{"type": "text", "text": {"content": f"{name} - {d.isoformat()}"}}]},
                    DATE_PROP_NAME: {"date": {"start": d.isoformat()}}
                }
                ok, res = notion_create_page_in_db(NOTION_DATABASE_ID, props)
                if ok:
                    created.append(res)
                else:
                    skipped.append(res)
                time.sleep(PATCH_DELAY)
            except Exception as e:
                skipped.append(str(e))
        # update original page to mark as dao processed
        try:
            prop_key = find_prop_key_case_insensitive(notion_get_page(source_page_id).get("properties", {}), DAO_CHECKFIELD_CANDIDATES[0])
            if prop_key:
                notion_patch_page_properties(source_page_id, {prop_key: {"checkbox": True}})
        except Exception as e:
            print("Error setting dao checkbox:", e)

        # send result
        lines = [f"Đã tạo {len(created)} page cho {name}:"]
        for i, c in enumerate(created, 1):
            try:
                date_val = c["properties"][DATE_PROP_NAME]["date"]["start"]
                lines.append(f"{i}. [{date_val}] {c['id']}")
            except:
                lines.append(f"{i}. [Lỗi ngày] {c.get('id')}")
        if skipped:
            lines.append(f"\nBỏ qua: {len(skipped)} (đã tồn tại hoặc lỗi)")
        send_long_text(chat_id, "\n".join(lines))
        del pending_confirm[str(chat_id)]
        return

    if typ.startswith("mark"):
        # expecting numbers like "1" or "1-3"
        sel = text.strip()
        found = pc.get("matches", [])
        selected = []
        if sel.lower() == "all":
            selected = list(range(1, len(found) + 1))
        else:
            parts = sel.split(",")
            for p in parts:
                p = p.strip()
                if "-" in p:
                    a, b = p.split("-", 1)
                    try:
                        a_i = int(a)
                        b_i = int(b)
                        selected.extend(list(range(a_i, b_i + 1)))
                    except:
                        pass
                else:
                    try:
                        selected.append(int(p))
                    except:
                        pass
        selected = sorted(set([i for i in selected if 1 <= i <= len(found)]))
        if not selected:
            send_telegram(chat_id, "Không có lựa chọn hợp lệ.")
            return
        succeeded = []
        failed = []
        op_id = f"mark_op_{int(time.time())}_{random.randint(1,9999)}"
        for idx in selected:
            pid, preview, d = found[idx - 1]
            try:
                prop_key = find_prop_key_case_insensitive(notion_get_page(pid).get("properties", {}), CHECKBOX_PROP)
                ok, msg = notion_patch_page_properties(pid, {prop_key or CHECKBOX_PROP: {"checkbox": True}})
                if ok:
                    succeeded.append((pid, preview, d))
                else:
                    failed.append((pid, preview, d, msg))
                time.sleep(PATCH_DELAY)
            except Exception as e:
                failed.append((pid, preview, d, str(e)))
        log_action({
            "ts": now_iso(), "type": "mark_manual", "op_id": op_id, "user_chat": chat_id,
            "command": pc.get("orig_command"), "keyword": pc.get("keyword"),
            "selected": [{"page_id": p, "preview": pr, "date": dt} for p, pr, dt in succeeded],
            "failed": failed
        })
        # --- Paste this after you have `succeeded` và trước khi gửi message kết quả ---
        try:
            # cập nhật lại counts sau khi đã mark (lấy từ DB calendar)
            unchecked_count, checked_count = find_matching_pages_counts(NOTION_DATABASE_ID, pc.get("keyword"))
            res_lines = [f"✅ Đã đánh dấu {len(succeeded)} mục:"]
            for i, (p, pr, dt) in enumerate(succeeded, start=1):
                res_lines.append(f"{i}. [{dt[:10] if dt else '-'}] {pr}")
            if failed:
                res_lines.append("\n⚠️ Một vài mục không thành công:")
                for i, item in enumerate(failed, start=1):
                    res_lines.append(f"{i}. {item[1]} ({item[3]})")
            res_lines.append("\nCập nhật thống kê:")
            res_lines.append(f"✅ Đã tích: {checked_count}")
            res_lines.append(f"🟡 Chưa tích: {unchecked_count}")
            send_long_text(chat_id, "\n".join(res_lines))
        except Exception as e:
            print("Post-mark result error:", e)
            send_long_text(chat_id, "Hoàn thành đánh dấu. (Lỗi khi báo cáo thống kê)")
        del pending_confirm[str(chat_id)]
        return

    elif typ.startswith("archive"):
        sel = text.strip()
        found = pc.get("matches", [])
        selected = []
        if sel.lower() == "all":
            selected = list(range(1, len(found) + 1))
        else:
            parts = sel.split(",")
            for p in parts:
                p = p.strip()
                if "-" in p:
                    a, b = p.split("-", 1)
                    try:
                        a_i = int(a)
                        b_i = int(b)
                        selected.extend(list(range(a_i, b_i + 1)))
                    except:
                        pass
                else:
                    try:
                        selected.append(int(p))
                    except:
                        pass
        selected = sorted(set([i for i in selected if 1 <= i <= len(found)]))
        if not selected:
            send_telegram(chat_id, "Không có lựa chọn hợp lệ.")
            return
        succeeded = []
        failed = []
        op_id = f"archive_op_{int(time.time())}_{random.randint(1,9999)}"
        for idx in selected:
            pid, preview, d = found[idx - 1]
            try:
                ok, msg = notion_archive_page(pid)
                if ok:
                    succeeded.append((pid, preview, d))
                else:
                    failed.append((pid, preview, d, msg))
                time.sleep(PATCH_DELAY)
            except Exception as e:
                failed.append((pid, preview, d, str(e)))
        log_action({
            "ts": now_iso(), "type": "archive_manual", "op_id": op_id, "user_chat": chat_id,
            "command": pc.get("orig_command"), "keyword": pc.get("keyword"),
            "selected": [{"page_id": p, "preview": pr, "date": dt} for p, pr, dt in succeeded],
            "failed": failed
        })
        res_lines = [f"✅ Đã archive {len(succeeded)} mục:"]
        for i, (p, pr, dt) in enumerate(succeeded, start=1):
            res_lines.append(f"{i}. [{dt[:10] if dt else '-'}] {pr}")
        if failed:
            res_lines.append("\n⚠️ Một vài mục không archive:")
            for i, item in enumerate(failed, start=1):
                res_lines.append(f"{i}. {item[1]} ({item[3]})")
        send_long_text(chat_id, "\n".join(res_lines))
        return

def undo_last(chat_id: str):
    # undo last mark or archive op logged in LOG_PATH
    try:
        if not LOG_PATH.exists():
            send_telegram(chat_id, "Chưa có hoạt động để undo.")
            return
        lines = LOG_PATH.read_text(encoding="utf-8").splitlines()
        # find last mark/archive op
        found = None
        for l in reversed(lines):
            try:
                entry = json.loads(l)
                if entry.get("type", "").startswith("mark") or entry.get("type", "").startswith("archive"):
                    found = entry
                    break
            except:
                continue
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
            log_action({"ts": now_iso(), "type": "undo", "op_id": found.get("op_id"), "reverted": reverted, "failed": failed})
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
            log_action({"ts": now_iso(), "type": "undo_archive", "op_id": found.get("op_id"), "reverted": reverted, "failed": failed})
            return
        else:
            send_telegram(chat_id, "Không thể undo cho loại op này.")
    except Exception as e:
        print("undo_last exception:", e)
        traceback.print_exc()
        send_telegram(chat_id, f"❌ Lỗi undo: {str(e)}")

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
                "type": "dao_select",
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
        # ĐỌC DỮ LIỆU TỪ CÁC CỘT CHÍNH XÁC
        display_total = extract_number_from_prop(props, "Đáo/thối")      # Cột tổng
        per_day       = extract_number_from_prop(props, "G ngày")        # Cột mỗi ngày
        days          = extract_number_from_prop(props, "# ngày trước")  # Cột số ngày
        calc_total    = extract_number_from_prop(props, "trước")         # CỘT FORMULA
        # Kiểm tra dữ liệu
        if display_total is None:
            send_telegram(chat_id, f"Không tìm thấy cột 'Đáo/thối' cho {preview}")
            return
        if per_day is None:
            send_telegram(chat_id, f"Không tìm thấy cột 'G ngày' cho {preview}")
            return
        if days is None or days <= 0:
            preview_text = f"đáo lại cho: {preview} - Tổng đáo: {int(display_total)}\nKhông Lấy trước"
            send_telegram(chat_id, preview_text)
            return
        if calc_total is None:
            preview_text = f"🔔 đáo lại cho: {preview} - Tổng đáo: {int(display_total) if display_total else 'N/A'}\nKhông Lấy trước"
            send_telegram(chat_id, preview_text)
            return
        days = int(math.ceil(calc_total / per_day))
        if days <= 0:
            send_telegram(chat_id, f"⚠️ Kết quả days không hợp lệ: {days}.")
            return
        if days > DAO_MAX_DAYS:
            send_telegram(chat_id, f"⚠️ Số ngày ({days}) vượt mức tối đa ({DAO_MAX_DAYS}). Hãy giảm hoặc thay đổi per_day.")
            return
        start_date = datetime.now().date() + timedelta(days=1)
        preview_text = build_dao_preview_text(preview, display_total, per_day, days, start_date, calc_total)
        pending_confirm[str(chat_id)] = {
            "type": "dao_confirm",
            "keyword": keyword,
            "source_page_id": pid,
            "display_total": display_total,
            "per_day": per_day,
            "calc_total": calc_total,
            "days": days,
            "start_date": start_date.isoformat(),
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
        send_telegram(chat_id, "Không có thao tác đáo đang chờ.")
        return
    if time.time() > pc.get("expires", 0):
        del pending_confirm[str(chat_id)]
        send_telegram(chat_id, "⏳ Hết thời gian chọn. Yêu cầu đã bị hủy.")
        return
    if pc.get("type") != "dao_confirm":
        send_telegram(chat_id, "Không có thao tác đáo đang chờ.")
        return
    if text.strip().lower() not in ("/ok", "ok"):
        send_telegram(chat_id, "Gửi '/ok' để xác nhận.")
        return

    # thực hiện tạo pages
    source_page_id = pc["source_page_id"]
    name = extract_title_from_props(notion_get_page(source_page_id).get("properties", {})) or pc.get("keyword")
    display_total = pc.get("display_total")
    per_day = pc.get("per_day")
    days = int(pc.get("days", 0))
    start_date = datetime.fromisoformat(pc.get("start_date")).date() if pc.get("start_date") else (datetime.now().date() + timedelta(days=1))

    created = []
    skipped = []
    for i in range(days):
        d = start_date + timedelta(days=i)
        try:
            props = {
                "Name": {"title": [{"type": "text", "text": {"content": f"{name} - {d.isoformat()}"}}]},
                DATE_PROP_NAME: {"date": {"start": d.isoformat()}}
            }
            ok, res = notion_create_page_in_db(NOTION_DATABASE_ID, props)
            if ok:
                created.append(res)
            else:
                skipped.append(res)
            time.sleep(PATCH_DELAY)
        except Exception as e:
            skipped.append(str(e))
    # update original page to tick dao field if possible
    try:
        prop_key = find_prop_key_case_insensitive(notion_get_page(source_page_id).get("properties", {}), DAO_CHECKFIELD_CANDIDATES[0])
        if prop_key:
            notion_patch_page_properties(source_page_id, {prop_key: {"checkbox": True}})
    except Exception as e:
        print("Error setting dao checkbox:", e)

    # send result
    lines = [f"Đã tạo {len(created)} page cho {name}:"]
    for i, c in enumerate(created, 1):
        try:
            date_val = c["properties"][DATE_PROP_NAME]["date"]["start"]
            lines.append(f"{i}. [{date_val}] {c.get('id')}")
        except:
            lines.append(f"{i}. [Lỗi ngày] {c.get('id')}")
    if skipped:
        lines.append(f"\nBỏ qua: {len(skipped)} (đã tồn tại hoặc lỗi)")
    send_long_text(chat_id, "\n".join(lines))
    del pending_confirm[str(chat_id)]
    return

# ---------------- Webhook / Dispatcher ----------------
def handle_incoming_message(chat_id: str, text: str):
    # main dispatcher for telegram incoming messages (text only)
    try:
        if not text:
            return
        orig = text.strip()
        t = orig.lower().strip()
        # commands
        if t == "/start":
            send_telegram(chat_id, "Xin chào! Gõ một từ khoá để bắt đầu (ví dụ: 'gam', 'gam 2', 'gam đáo', 'gam xóa').")
            return
        if t.startswith("undo"):
            # support "undo" or "undo gam"
            threading.Thread(target=undo_last, args=(chat_id,), daemon=True).start()
            return
        if t.endswith(" xóa") or t.endswith(" xoa"):
            # archive flow
            kw = orig[:-4].strip()
            threading.Thread(target=handle_command_archive, args=(chat_id, kw, orig), daemon=True).start()
            return
        # dao variants
        if t.endswith(" đáo") or t.endswith(" dao") or t.endswith(" đáo") or t.endswith(" đáo"):
            kw = orig.rsplit(None, 1)[0]
            threading.Thread(target=handle_command_dao, args=(chat_id, kw, orig), daemon=True).start()
            return
        # mark quick "keyword N"
        m = re.match(r"^(.+?)\s+(\d+)$", orig)
        if m:
            kw = m.group(1).strip()
            n = int(m.group(2))
            threading.Thread(target=handle_command_mark_quick, args=(chat_id, kw, n, orig), daemon=True).start()
            return
        # default mark preview
        threading.Thread(target=handle_command_mark, args=(chat_id, orig, orig), daemon=True).start()
    except Exception as e:
        print("handle_incoming_message exception:", e)
        traceback.print_exc()
        send_telegram(chat_id, f"❌ Lỗi xử lý tin nhắn: {str(e)}")

@app.route('/webhook', methods=['POST'])
def webhook():
    # expecting telegram webhook json
    try:
        j = request.get_json(force=True)
        if not j:
            return Response('No JSON', status=400)
        # extract message
        message = j.get("message") or j.get("edited_message") or {}
        if not message:
            return Response('No message', status=200)
        chat = message.get("chat", {})
        chat_id = chat.get("id")
        if TELEGRAM_CHAT_ID and str(chat_id) != str(TELEGRAM_CHAT_ID):
            # ignore other chats if TELEGRAM_CHAT_ID restricted
            return Response('Ignored', status=200)
        text = message.get("text")
        # if there is a pending dao confirm selection
        if pending_confirm.get(str(chat_id)):
            try:
                # route to selection handler
                pc = pending_confirm.get(str(chat_id), {})
                if pc.get("type") == "dao_confirm":
                    process_pending_selection_for_dao(chat_id, text)
                else:
                    process_pending_selection(chat_id, text)
            except Exception as e:
                print("Selection handling exception:", e)
                traceback.print_exc()
        else:
            # dispatch new command
            try:
                threading.Thread(target=handle_incoming_message, args=(chat_id, text), daemon=True).start()
            except Exception as e:
                print("Webhook handling exception:", e)
                traceback.print_exc()
    except Exception as e:
        print("webhook exception:", e)
        traceback.print_exc()
    return Response('OK', status=200)

@app.route('/')
def home():
    return "OK", 200

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
