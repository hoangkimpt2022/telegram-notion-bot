#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
app.py - Webhook version for Render deployment
Optimized: token-matching, caching, quick-ack + background processing.
"""
import os
import time
import json
import requests
import random
import traceback
import threading
import re
import unicodedata
from flask import Flask, request, Response
from typing import List, Tuple, Optional, Dict, Any
from pathlib import Path
from datetime import datetime, timezone

app = Flask(__name__)

# ---------------- CONFIG (from env for Render) ----------------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")  # optional restrict
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID")

SEARCH_PROPERTY = ""  # "" means search title (Name)
CHECKBOX_PROP = "Đã Góp"
DATE_PROP_NAME = "Ngày Góp"

# Operational settings (override via env if needed)
WAIT_CONFIRM = int(os.getenv("WAIT_CONFIRM", 120))
NOTION_PAGE_SIZE = int(os.getenv("NOTION_PAGE_SIZE", 100))
MAX_PREVIEW = int(os.getenv("MAX_PREVIEW", 30))
PATCH_DELAY = float(os.getenv("PATCH_DELAY", 0.45))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", 3))
RETRY_SLEEP = float(os.getenv("RETRY_SLEEP", 1.0))
LOG_PATH = Path(os.getenv("LOG_PATH", "notion_assistant_actions.log"))
NOTION_CACHE_TTL = int(os.getenv("NOTION_CACHE_TTL", 20))  # seconds

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

# ---------------- Simple Notion Cache ----------------
_NOTION_CACHE = {
    "ts": 0.0,
    "pages": [],      # raw page dicts
    "tokens_map": {}, # page_id -> list of normalized tokens
    "preview_map": {},# page_id -> preview text (title)
    "props_map": {},  # page_id -> properties dict
}

_cache_lock = threading.Lock()

def invalidate_notion_cache(page_id: Optional[str] = None):
    with _cache_lock:
        if page_id:
            _NOTION_CACHE["tokens_map"].pop(page_id, None)
            _NOTION_CACHE["preview_map"].pop(page_id, None)
            _NOTION_CACHE["props_map"].pop(page_id, None)
            _NOTION_CACHE["pages"] = [p for p in _NOTION_CACHE["pages"] if p.get("id") != page_id]
        else:
            _NOTION_CACHE.update({"ts": 0.0, "pages": [], "tokens_map": {}, "preview_map": {}, "props_map": {}})

# ---------------- Helpers / Utils ----------------
def normalize_notion_id(maybe_id: Optional[str]) -> Optional[str]:
    if not maybe_id:
        return None
    s = (maybe_id or "").strip()
    s = s.replace("-", "")
    if len(s) == 32:
        return f"{s[0:8]}-{s[8:12]}-{s[12:16]}-{s[16:20]}-{s[20:32]}"
    return maybe_id

NOTION_DATABASE_ID = normalize_notion_id(NOTION_DATABASE_ID)

def log_action(entry: dict):
    try:
        LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with LOG_PATH.open("a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception as e:
        print("Log write error:", e)

def now_iso():
    return datetime.now(timezone.utc).astimezone().isoformat()

def send_telegram(chat_id: str, text: str) -> bool:
    if not BASE_TELE_URL:
        print("No TELEGRAM_TOKEN set; cannot send message.")
        return False
    try:
        r = requests.post(BASE_TELE_URL + "/sendMessage",
                          data={"chat_id": chat_id, "text": text}, timeout=15)
        r.raise_for_status()
        return True
    except Exception as e:
        print("Telegram send error:", e)
        return False

def send_long_text(chat_id: str, text: str):
    limit = 3800
    parts = []
    cur = ""
    for line in text.splitlines(True):
        if len(cur) + len(line) > limit:
            parts.append(cur)
            cur = ""
        cur += line
    if cur:
        parts.append(cur)
    for p in parts:
        send_telegram(chat_id, p)
        time.sleep(0.2)

# ---------------- Text normalization/tokenization ----------------
def strip_accents(s: str) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    return s

def normalize_and_tokenize(s: str) -> List[str]:
    if not s:
        return []
    t = strip_accents(s).lower()
    return re.findall(r"\w+", t)

# ---------------- Notion helpers (no changes in API logic) ----------------
def notion_query_all_raw(db_id: str) -> List[dict]:
    """Fetch all pages from Notion DB (no caching here)."""
    if not db_id:
        return []
    url = f"https://api.notion.com/v1/databases/{db_id}/query"
    results = []
    payload = {"page_size": NOTION_PAGE_SIZE}
    next_cursor = None
    while True:
        if next_cursor:
            payload["start_cursor"] = next_cursor
        resp = requests.post(url, headers=NOTION_HEADERS, json=payload, timeout=30)
        if resp.status_code != 200:
            raise RuntimeError(f"Notion query all error {resp.status_code}: {resp.text}")
        data = resp.json()
        results.extend(data.get("results", []))
        next_cursor = data.get("next_cursor")
        if not next_cursor:
            break
    return results

def _refresh_notion_cache_if_needed():
    with _cache_lock:
        now = time.time()
        if now - _NOTION_CACHE["ts"] <= NOTION_CACHE_TTL and _NOTION_CACHE["pages"]:
            return
        # fetch fresh
        pages = notion_query_all_raw(NOTION_DATABASE_ID)
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
    if r.status_code != 200:
        raise RuntimeError(f"Notion get page error {r.status_code}: {r.text}")
    # update cache entry
    page = r.json()
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
    for attempt in range(1, MAX_RETRIES+1):
        try:
            r = requests.patch(url, headers=NOTION_HEADERS, json=body, timeout=15)
            if r.status_code in (200,201):
                invalidate_notion_cache(page_id)
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
    for attempt in range(1, MAX_RETRIES+1):
        try:
            r = requests.patch(url, headers=NOTION_HEADERS, json=body, timeout=15)
            if r.status_code in (200,201):
                invalidate_notion_cache(page_id)
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

def notion_archive_page_revert(page_id: str) -> Tuple[bool,str]:
    url = f"https://api.notion.com/v1/pages/{page_id}"
    body = {"archived": False}
    err = "unknown"
    for attempt in range(1, MAX_RETRIES+1):
        try:
            r = requests.patch(url, headers=NOTION_HEADERS, json=body, timeout=15)
            if r.status_code in (200,201):
                invalidate_notion_cache(page_id)
                return True, "OK"
            else:
                err = f"{r.status_code}: {r.text}"
                time.sleep(RETRY_SLEEP * attempt)
        except Exception as e:
            err = str(e)
            time.sleep(RETRY_SLEEP * attempt)
    return False, err

# ---------------- Extractors (unchanged) ----------------
def _join_plain_text_array(arr):
    if not arr:
        return ""
    return "".join([x.get("plain_text","") for x in arr if isinstance(x, dict)]).strip()

def extract_prop_text(props: dict, name: str) -> str:
    key = None
    for k in props.keys():
        if k.lower() == (name or "").lower():
            key = k; break
    if key is None:
        return ""
    prop = props.get(key, {})
    ptype = prop.get("type","")
    try:
        if ptype == "title":
            return _join_plain_text_array(prop.get("title", []))
        if ptype == "rich_text":
            return _join_plain_text_array(prop.get("rich_text", []))
        if ptype == "formula":
            form = prop.get("formula", {})
            return (form.get("string") or form.get("plain_text") or "").strip()
        if ptype == "url":
            return (prop.get("url") or "").strip()
        if ptype == "select":
            sel = prop.get("select")
            return (sel.get("name","") if isinstance(sel, dict) else "").strip()
        if ptype == "multi_select":
            arr = prop.get("multi_select", [])
            if isinstance(arr, list):
                return ", ".join([a.get("name","") for a in arr]).strip()
        for c in ("plain_text","text","name","value"):
            v = prop.get(c)
            if isinstance(v, str) and v.strip():
                return v.strip()
            if isinstance(v, list):
                return " ".join([x.get("plain_text","") for x in v if x.get("plain_text")]).strip()
    except Exception:
        pass
    return str(prop).strip()

def extract_title_from_props(props: dict) -> str:
    for k,v in props.items():
        if isinstance(v, dict) and v.get("type") == "title":
            return _join_plain_text_array(v.get("title", []))
    return extract_prop_text(props, "Name") or extract_prop_text(props, "Title") or ""

def find_date_for_page(p: dict) -> Optional[str]:
    props = p.get("properties", {})
    if DATE_PROP_NAME:
        date_text = extract_date_from_prop(props, DATE_PROP_NAME)
        if date_text:
            return date_text
    for k,v in props.items():
        if isinstance(v, dict) and v.get("type") == "date":
            dt = v.get("date", {})
            if isinstance(dt, dict):
                start = dt.get("start")
                if start:
                    return start
    return p.get("created_time")

def extract_date_from_prop(props: dict, prop_name: str) -> Optional[str]:
    key = None
    for k in props.keys():
        if k.lower() == prop_name.lower():
            key = k; break
    if key is None:
        return None
    v = props.get(key, {})
    if isinstance(v, dict) and v.get("type") == "date":
        dt = v.get("date", {})
        if isinstance(dt, dict):
            return dt.get("start")
    return None

def is_checked(props: dict, checkbox_name: str) -> Optional[bool]:
    key = None
    for k in props.keys():
        if k.lower() == checkbox_name.lower():
            key = k; break
    if key is None:
        return None
    v = props.get(key, {})
    if isinstance(v, dict) and v.get("type") == "checkbox":
        return bool(v.get("checkbox"))
    return None

# ---------------- Search & Sort (using cache and token match) ----------------
def parse_date_or_max(s: str):
    if not s:
        return datetime.max.replace(tzinfo=timezone.utc)
    try:
        ds = s
        if ds.endswith("Z"):
            ds = ds.replace("Z", "+00:00")
        return datetime.fromisoformat(ds)
    except Exception:
        try:
            return datetime.fromtimestamp(float(s))
        except:
            return datetime.max.replace(tzinfo=timezone.utc)

def _get_cached_pages_and_maps():
    _refresh_notion_cache_if_needed()
    with _cache_lock:
        return _NOTION_CACHE["pages"], _NOTION_CACHE["tokens_map"], _NOTION_CACHE["preview_map"], _NOTION_CACHE["props_map"]

def find_matching_unchecked_pages(db_id: str, keyword: str, limit: Optional[int]=None) -> List[Tuple[str,str,str]]:
    keyword = (keyword or "").strip()
    if not keyword or not db_id:
        return []
    keyword_norm = strip_accents(keyword).lower()
    pages, tokens_map, preview_map, props_map = _get_cached_pages_and_maps()
    matches = []
    for p in pages:
        pid = p.get("id")
        tokens = tokens_map.get(pid, [])
        if keyword_norm and keyword_norm in tokens:
            props = props_map.get(pid, {})
            cb = is_checked(props, CHECKBOX_PROP)
            if cb is True:
                continue
            date_iso = find_date_for_page(p) or ""
            preview = preview_map.get(pid, pid)
            matches.append((pid, preview, date_iso))
    matches_sorted = sorted(matches, key=lambda it: (parse_date_or_max(it[2]), it[1].lower()))
    return matches_sorted[:limit] if limit else matches_sorted

def find_matching_all_pages(db_id: str, keyword: str, limit: Optional[int]=None) -> List[Tuple[str,str,str]]:
    keyword = (keyword or "").strip()
    if not keyword or not db_id:
        return []
    keyword_norm = strip_accents(keyword).lower()
    pages, tokens_map, preview_map, props_map = _get_cached_pages_and_maps()
    matches = []
    for p in pages:
        pid = p.get("id")
        tokens = tokens_map.get(pid, [])
        if keyword_norm and keyword_norm in tokens:
            date_iso = find_date_for_page(p) or ""
            preview = preview_map.get(pid, pid)
            matches.append((pid, preview, date_iso))
    matches_sorted = sorted(matches, key=lambda it: (parse_date_or_max(it[2]), it[1].lower()))
    return matches_sorted[:limit] if limit else matches_sorted

def find_matching_pages_counts(db_id: str, keyword: str) -> Tuple[int,int]:
    keyword = (keyword or "").strip()
    if not keyword or not db_id:
        return 0,0
    keyword_norm = strip_accents(keyword).lower()
    pages, tokens_map, preview_map, props_map = _get_cached_pages_and_maps()
    unchecked = 0
    checked = 0
    for p in pages:
        pid = p.get("id")
        tokens = tokens_map.get(pid, [])
        if keyword_norm and keyword_norm in tokens:
            props = props_map.get(pid, {})
            cb = is_checked(props, CHECKBOX_PROP)
            if cb is True:
                checked += 1
            else:
                unchecked += 1
    return unchecked, checked

# ---------------- Commands & Handlers ----------------
def parse_user_command(text: str) -> Tuple[str, Optional[int], str]:
    text = (text or "").strip()
    if not text:
        return "", None, "mark"
    parts = text.lower().split()
    if parts[-1] == "undo":
        return " ".join(parts[:-1]).strip(), None, "undo"
    action = "mark"
    last = parts[-1]
    n = None
    try:
        n = int(last)
        parts = parts[:-1]
    except:
        n = None
    if parts and parts[-1] in ("xóa","xoa","delete","del"):
        action = "archive"
        parts = parts[:-1]
    keyword = " ".join(parts).strip()
    return keyword, n, action

def build_preview_lines(matches: List[Tuple[str,str,str]]) -> List[str]:
    lines = []
    for i,(pid,pre,d) in enumerate(matches, start=1):
        date_part = d[:10] if d else "-"
        lines.append(f"{i}. [{date_part}] {pre}")
    return lines

def parse_selection_text(sel_text: str, total: int) -> List[int]:
    s = sel_text.strip().lower()
    if not s:
        return []
    if s in ("all", "tất cả", "tat ca"):
        return list(range(1, total+1))
    if s in ("none","0"):
        return []
    s = s.replace(".", " ")
    parts = [p.strip() for p in s.replace(",", " ").split() if p.strip()]
    res = set()
    for p in parts:
        if "-" in p:
            try:
                a,b = p.split("-",1)
                a=int(a); b=int(b)
                for i in range(min(a,b), max(a,b)+1):
                    if 1 <= i <= total:
                        res.add(i)
            except:
                continue
        else:
            try:
                n = int(p)
                if 1 <= n <= total:
                    for i in range(1, n+1):
                        res.add(i)
            except:
                continue
    return sorted(res)

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
            for idx,(pid,pre,d) in enumerate(to_apply, start=1):
                try:
                    page = notion_get_page(pid)
                    props = page.get("properties", {})
                    cb = is_checked(props, CHECKBOX_PROP)
                    if cb is True:
                        failed.append((pid, pre, d, "Already checked"))
                        continue
                except Exception as e:
                    failed.append((pid, pre, d, f"fetch error {e}"))
                    continue
                prop_key = find_prop_key_case_insensitive(page.get("properties", {}), CHECKBOX_PROP)
                prop_obj = {prop_key if prop_key else CHECKBOX_PROP: {"checkbox": True}}
                ok,msg = notion_patch_page_properties(pid, prop_obj)
                if ok:
                    time.sleep(0.6)
                    try:
                        new_page = notion_get_page(pid)
                        new_props = new_page.get("properties", {})
                        if is_checked(new_props, CHECKBOX_PROP):
                            succeeded.append((pid,pre,d))
                        else:
                            failed.append((pid,pre,d,"verify failed"))
                    except Exception as e:
                        failed.append((pid,pre,d,f"verify error {e}"))
                else:
                    failed.append((pid,pre,d,msg))
                time.sleep(PATCH_DELAY)
            log_entry = {"ts": now_iso(), "type":"mark_auto", "op_id":op_id, "user_chat":chat_id,
                         "command": orig_cmd, "keyword": keyword, "requested_count": count,
                         "succeeded": [{"page_id":p,"preview":pr,"date":dt} for p,pr,dt in succeeded],
                         "failed": failed}
            log_action(log_entry)
            res_lines = []
            if succeeded:
                res_lines.append(f"✅ Đã đánh dấu {len(succeeded)} mục cho '{keyword}':")
                for i,(p,pr,dt) in enumerate(succeeded, start=1):
                    res_lines.append(f"{i}. [{dt[:10] if dt else '-'}] {pr}")
            if failed:
                res_lines.append("\n⚠️ Một vài mục không cập nhật:")
                for i,item in enumerate(failed, start=1):
                    res_lines.append(f"{i}. {item}")
            send_long_text(chat_id, "\n".join(res_lines))
            return
        # no count -> preview + counts
        matches_full = find_matching_unchecked_pages(NOTION_DATABASE_ID, keyword, limit=MAX_PREVIEW)
        header = f"🔎 Khách hàng: '{keyword}'\n" \
                 f"✅ Đã tích: {checked_count}\n" \
                 f"🟡 Chưa tích: {unchecked_count}\n\n"
        if not matches_full:
            send_telegram(chat_id, header + "Không còn mục chưa tích để hiển thị.")
            return
        header += f"📤 Gửi số (ví dụ 1-7) trong {WAIT_CONFIRM}s để chọn, hoặc /cancel.\n"
        preview_lines = build_preview_lines(matches_full)
        send_long_text(chat_id, header + "\n".join(preview_lines))
        pending_confirm[str(chat_id)] = {
            "type":"mark",
            "keyword": keyword,
            "matches": matches_full,
            "expires": time.time() + WAIT_CONFIRM,
            "orig_command": orig_cmd
        }
    except Exception as e:
        print("handle_command_mark exception:", e)
        traceback.print_exc()
        send_telegram(chat_id, f"❌ Lỗi xử lý mark: {e}")

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
            for idx,(pid,pre,d) in enumerate(to_apply, start=1):
                ok,msg = notion_archive_page(pid)
                if ok:
                    succeeded.append((pid,pre,d))
                else:
                    failed.append((pid,pre,d,msg))
                time.sleep(PATCH_DELAY)
            log_entry = {"ts": now_iso(), "type":"archive_auto", "op_id":op_id, "user_chat":chat_id,
                         "command": orig_cmd, "keyword": keyword, "requested_count": count,
                         "succeeded": [{"page_id":p,"preview":pr,"date":dt} for p,pr,dt in succeeded],
                         "failed": failed}
            log_action(log_entry)
            res_lines = []
            if succeeded:
                res_lines.append(f"🗑️ Đã archive {len(succeeded)} mục cho '{keyword}':")
                for i,(p,pr,dt) in enumerate(succeeded, start=1):
                    res_lines.append(f"{i}. [{dt[:10] if dt else '-'}] {pr}")
            if failed:
                res_lines.append("\n⚠️ Một vài mục không archive:")
                for i,item in enumerate(failed, start=1):
                    res_lines.append(f"{i}. {item}")
            send_long_text(chat_id, "\n".join(res_lines))
            return
        preview_lines = build_preview_lines(matches)
        header = f"🔎 Khách hàng: '{keyword}'\n" \
                 f"✅ Đã tích: {checked_count}\n" \
                 f"🟡 Chưa tích: {unchecked_count}\n\n" \
                 f"⚠️ CHÚ Ý: Bạn sắp archive {len(matches)} mục chứa '{keyword}'. Gửi số (ví dụ 1-7) trong {WAIT_CONFIRM}s để chọn, hoặc 'all' để archive tất cả, hoặc /cancel.\n"
        send_long_text(chat_id, header + "\n".join(preview_lines))
        pending_confirm[str(chat_id)] = {
            "type":"archive",
            "keyword": keyword,
            "matches": matches,
            "expires": time.time() + WAIT_CONFIRM,
            "orig_command": orig_cmd
        }
    except Exception as e:
        print("handle_command_archive exception:", e)
        traceback.print_exc()
        send_telegram(chat_id, f"❌ Lỗi xử lý archive: {e}")

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
    matches = pc.get("matches", [])
    total = len(matches)
    sel_indices = parse_selection_text(text, total)
    if not sel_indices:
        send_telegram(chat_id, "Không nhận được lựa chọn hợp lệ. Yêu cầu đã bị hủy.")
        del pending_confirm[str(chat_id)]
        return
    selected = [matches[i-1] for i in sel_indices]
    if typ == "mark":
        succeeded, failed = [], []
        op_id = f"op-{int(time.time())}-{random.randint(1000,9999)}"
        for (pid,pre,d) in selected:
            try:
                page = notion_get_page(pid); props = page.get("properties", {})
                cb = is_checked(props, CHECKBOX_PROP)
                if cb is True:
                    failed.append((pid,pre,d,"Already checked"))
                    continue
            except Exception as e:
                failed.append((pid,pre,d,f"fetch error {e}"))
                continue
            prop_key = find_prop_key_case_insensitive(page.get("properties", {}), CHECKBOX_PROP)
            prop_obj = {prop_key if prop_key else CHECKBOX_PROP: {"checkbox": True}}
            ok,msg = notion_patch_page_properties(pid, prop_obj)
            if ok:
                time.sleep(0.6)
                try:
                    new_page = notion_get_page(pid)
                    if is_checked(new_page.get("properties", {}), CHECKBOX_PROP):
                        succeeded.append((pid,pre,d))
                    else:
                        failed.append((pid,pre,d,"verify failed"))
                except Exception as e:
                    failed.append((pid,pre,d,f"verify error {e}"))
            else:
                failed.append((pid,pre,d,msg))
            time.sleep(PATCH_DELAY)
        log_entry = {"ts": now_iso(), "type":"mark_manual", "op_id":op_id, "user_chat":chat_id,
                     "command": pc.get("orig_command"), "keyword": pc.get("keyword"),
                     "selected": [{"page_id":p,"preview":pr,"date":dt} for p,pr,dt in succeeded],
                     "failed": failed}
        log_action(log_entry)
        out=[]
        if succeeded:
            out.append(f"✅ Đã đánh dấu {len(succeeded)} mục:")
            for i,(p,pr,dt) in enumerate(succeeded, start=1):
                out.append(f"{i}. [{dt[:10] if dt else '-'}] {pr}")
        if failed:
            out.append("\n⚠️ Một vài mục không cập nhật:")
            for i,item in enumerate(failed, start=1):
                out.append(f"{i}. {item}")
        send_long_text(chat_id, "\n".join(out))
        del pending_confirm[str(chat_id)]
        return
    elif typ == "archive":
        succeeded, failed = [], []
        op_id = f"op-archive-{int(time.time())}-{random.randint(1000,9999)}"
        for (pid,pre,d) in selected:
            ok,msg = notion_archive_page(pid)
            if ok:
                succeeded.append((pid,pre,d))
            else:
                failed.append((pid,pre,d,msg))
            time.sleep(PATCH_DELAY)
        log_entry = {"ts": now_iso(), "type":"archive_manual", "op_id":op_id, "user_chat":chat_id,
                     "command": pc.get("orig_command"), "keyword": pc.get("keyword"),
                     "selected": [{"page_id":p,"preview":pr,"date":dt} for p,pr,dt in succeeded],
                     "failed": failed}
        log_action(log_entry)
        out=[]
        if succeeded:
            out.append(f"🗑️ Đã archive {len(succeeded)} mục:")
            for i,(p,pr,dt) in enumerate(succeeded, start=1):
                out.append(f"{i}. [{dt[:10] if dt else '-'}] {pr}")
        if failed:
            out.append("\n⚠️ Một vài mục không archive:")
            for i,item in enumerate(failed, start=1):
                out.append(f"{i}. {item}")
        send_long_text(chat_id, "\n".join(out))
        del pending_confirm[str(chat_id)]
        return
    else:
        send_telegram(chat_id, "Pending type không nhận diện.")
        del pending_confirm[str(chat_id)]
        return

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
        except:
            continue
        if str(entry.get("user_chat")) != str(chat_id):
            continue
        if keyword and entry.get("keyword", "").lower() != keyword.lower():
            continue
        if op_id:
            if entry.get("op_id") == op_id:
                found = entry; break
        else:
            if entry.get("type","").startswith("mark") or entry.get("type","").startswith("archive"):
                found = entry; break
    if not found:
        send_telegram(chat_id, "Không tìm thấy op để undo.")
        return
    typ = found.get("type","")
    reverted = []; failed = []
    if typ.startswith("mark"):
        items = found.get("succeeded", []) or found.get("selected", [])
        for it in items:
            pid = it.get("page_id")
            try:
                ok,msg = notion_patch_page_properties(pid, {find_prop_key_case_insensitive(notion_get_page(pid).get("properties",{}), CHECKBOX_PROP) or CHECKBOX_PROP: {"checkbox": False}})
                if ok:
                    reverted.append(pid)
                else:
                    failed.append((pid,msg))
                time.sleep(PATCH_DELAY)
            except Exception as e:
                failed.append((pid,str(e)))
        send_telegram(chat_id, f"♻️ Undo done. Reverted {len(reverted)} items. Failed: {len(failed)}")
        log_action({"ts": now_iso(), "type":"undo", "user_chat": chat_id, "original_op": found.get("op_id"), "reverted": reverted, "failed": failed})
        return
    elif typ.startswith("archive"):
        items = found.get("succeeded", []) or found.get("selected", [])
        for it in items:
            pid = it.get("page_id")
            try:
                ok,msg = notion_archive_page_revert(pid)
                if ok:
                    reverted.append(pid)
                else:
                    failed.append((pid,msg))
                time.sleep(PATCH_DELAY)
            except Exception as e:
                failed.append((pid,str(e)))
        send_telegram(chat_id, f"♻️ Undo archive done. Reverted {len(reverted)} items. Failed: {len(failed)}")
        log_action({"ts": now_iso(), "type":"undo_archive", "user_chat": chat_id, "original_op": found.get("op_id"), "reverted": reverted, "failed": failed})
        return
    else:
        send_telegram(chat_id, "Không thể undo cho loại op này.")
        return

def find_prop_key_case_insensitive(props: dict, name: str) -> Optional[str]:
    for k in props.keys():
        if k.lower() == name.lower():
            return k
    return None

# ---------------- Message Handler (quick ack + background) ----------------
def handle_incoming_message(chat_id: str, text: str):
    try:
        if TELEGRAM_CHAT_ID and str(chat_id) != str(TELEGRAM_CHAT_ID):
            send_telegram(chat_id, "⚠️ Bot chưa được phép nhận lệnh từ chat này.")
            return

        raw = (text or "").strip()
        if not raw:
            send_telegram(chat_id, "Vui lòng gửi lệnh hoặc từ khoá.")
            return

        low = raw.lower().strip()

        # If pending exists, treat selection or cancel
        if str(chat_id) in pending_confirm:
            if low in ("/cancel", "cancel", "hủy", "huy"):
                del pending_confirm[str(chat_id)]
                send_telegram(chat_id, "Đã hủy thao tác đang chờ.")
                return
            if any(ch.isdigit() for ch in low) or low in ("all", "tất cả", "tat ca", "none"):
                send_telegram(chat_id, "Đang xử lý lựa chọn...")
                # process in same thread (it does network call) or background; use thread to avoid blocking handler
                threading.Thread(target=process_pending_selection, args=(chat_id, raw), daemon=True).start()
                return
            # else cancel pending and continue
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
            send_telegram(chat_id, "Đang xử lý archive...")  # quick ack
            threading.Thread(target=handle_command_archive, args=(chat_id, keyword, count, raw), daemon=True).start()
            return

        # default mark
        send_telegram(chat_id, "Đang xử lý...")  # quick ack
        threading.Thread(target=handle_command_mark, args=(chat_id, keyword, count, raw), daemon=True).start()
    except Exception as e:
        print("handle_incoming_message exception:", e)
        traceback.print_exc()
        try:
            send_telegram(chat_id, f"❌ Lỗi xử lý: {e}")
        except:
            pass

# ---------------- Webhook Handler ----------------
@app.route('/webhook', methods=['POST'])
def webhook():
    update = request.get_json(silent=True)
    if update and 'message' in update:
        try:
            chat_id = str(update['message']['chat']['id'])
            text = update['message'].get('text', '')
            handle_incoming_message(chat_id, text)
        except Exception as e:
            print("Webhook handling exception:", e)
            traceback.print_exc()
            return Response('OK', status=200)
    return Response('OK', status=200)

# simple health route
@app.route('/')
def home():
    return "OK", 200

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
