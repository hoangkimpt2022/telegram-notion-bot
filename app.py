#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
app.py - Telegram <-> Notion assistant (mark / archive / undo / dao flow)
Features implemented to match user spec:
 - "keyword" -> preview unchecked items (NOTION_DATABASE_ID)
 - "keyword N" -> mark N items as checked
 - "undo" -> revert last mark/archive
 - "keyword xóa" -> preview archive selection, can archive selected or 'all'
 - "keyword đáo" -> use TARGET_NOTION_DATABASE_ID, check activation (✅/🔴), preview DAO,
                 if /ok -> create pages in NOTION_DATABASE_ID starting tomorrow
Usage:
 - Set TELEGRAM_TOKEN, NOTION_TOKEN, NOTION_DATABASE_ID, TARGET_NOTION_DATABASE_ID
 - Run app, configure Telegram webhook to /webhook
"""
import os
import json
import time
import re
import math
import requests
import traceback
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple

from flask import Flask, request, Response

app = Flask(__name__)

# ---------------- CONFIG ----------------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")  # optional: restrict to one chat id
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID")  # calendar DB (where individual dates live)
TARGET_NOTION_DATABASE_ID = os.getenv("TARGET_NOTION_DATABASE_ID")  # dao DB (master entries)
CHECKBOX_PROP = os.getenv("CHECKBOX_PROP", "Đã Góp")  # checkbox property in calendar DB
DATE_PROP_NAME = os.getenv("DATE_PROP_NAME", "Ngày Góp")
DAO_CHECKFIELD_NAMES = os.getenv("DAO_CHECK_FIELDS", "Đáo/thối,Đáo/Thối,Đáo").split(",")
LOG_FILE = Path(os.getenv("LOG_FILE", "actions.log"))

# Operational
WAIT_CONFIRM = int(os.getenv("WAIT_CONFIRM", 120))
MAX_PREVIEW = int(os.getenv("MAX_PREVIEW", 200))
PATCH_DELAY = float(os.getenv("PATCH_DELAY", 0.45))
NOTION_VERSION = "2022-06-28"
MAX_RETRIES = int(os.getenv("MAX_RETRIES", 3))
RETRY_SLEEP = float(os.getenv("RETRY_SLEEP", 1.0))

# Verify env
if not TELEGRAM_TOKEN:
    print("WARNING: TELEGRAM_TOKEN not set. Bot cannot send Telegram messages.")
BASE_TELE_URL = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}" if TELEGRAM_TOKEN else None

if not NOTION_TOKEN:
    raise RuntimeError("NOTION_TOKEN not set.")
if not NOTION_DATABASE_ID:
    raise RuntimeError("NOTION_DATABASE_ID not set.")
if not TARGET_NOTION_DATABASE_ID:
    raise RuntimeError("TARGET_NOTION_DATABASE_ID not set.")

NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": NOTION_VERSION,
    "Content-Type": "application/json",
}

# in-memory pending confirmations: chat_id -> dict
pending: Dict[str, Dict[str, Any]] = {}

# ---------------- Helpers ----------------
def now_iso():
    return datetime.now(timezone.utc).astimezone().isoformat()

def send_telegram(chat_id: int, text: str) -> bool:
    if not BASE_TELE_URL:
        print("Telegram disabled, would send to", chat_id, "text:", text)
        return False
    try:
        r = requests.post(f"{BASE_TELE_URL}/sendMessage", json={"chat_id": chat_id, "text": text}, timeout=15)
        r.raise_for_status()
        return True
    except Exception as e:
        print("send_telegram error:", e)
        return False

def send_long_text(chat_id: int, text: str):
    # Telegram limit ~4096, keep safe
    limit = 3800
    lines = text.splitlines(keepends=True)
    cur = ""
    for ln in lines:
        if len(cur) + len(ln) > limit:
            send_telegram(chat_id, cur)
            time.sleep(0.1)
            cur = ""
        cur += ln
    if cur:
        send_telegram(chat_id, cur)

def log_action(entry: Dict[str, Any]):
    try:
        LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        with LOG_FILE.open("a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception as e:
        print("log_action error:", e)

# ---------------- Notion utility ----------------
def notion_post(url: str, body: Dict[str, Any]) -> Dict[str, Any]:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.post(url, headers=NOTION_HEADERS, json=body, timeout=20)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            print("notion_post error attempt", attempt, e)
            time.sleep(RETRY_SLEEP * attempt)
    raise RuntimeError("Notion POST failed after retries")

def notion_patch(url: str, body: Dict[str, Any]) -> Dict[str, Any]:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.patch(url, headers=NOTION_HEADERS, json=body, timeout=20)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            print("notion_patch error attempt", attempt, e)
            time.sleep(RETRY_SLEEP * attempt)
    raise RuntimeError("Notion PATCH failed after retries")

def notion_get(url: str) -> Dict[str, Any]:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, headers=NOTION_HEADERS, timeout=20)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            print("notion_get error attempt", attempt, e)
            time.sleep(RETRY_SLEEP * attempt)
    raise RuntimeError("Notion GET failed after retries")

# Query database (simple, not using filter)
def query_database_all(db_id: str, page_size=100) -> List[Dict[str, Any]]:
    url = f"https://api.notion.com/v1/databases/{db_id}/query"
    results = []
    payload = {"page_size": page_size}
    cursor = None
    while True:
        if cursor:
            payload["start_cursor"] = cursor
        data = notion_post(url, payload)
        results.extend(data.get("results", []))
        cursor = data.get("next_cursor")
        if not cursor:
            break
    return results

def get_page(page_id: str) -> Dict[str, Any]:
    url = f"https://api.notion.com/v1/pages/{page_id}"
    return notion_get(url)

def create_page_in_db(db_id: str, properties: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
    url = "https://api.notion.com/v1/pages"
    body = {"parent": {"database_id": db_id}, "properties": properties}
    try:
        res = notion_post(url, body)
        return True, res
    except Exception as e:
        return False, {"error": str(e)}

def patch_page_properties(page_id: str, properties: Dict[str, Any]) -> Tuple[bool, str]:
    url = f"https://api.notion.com/v1/pages/{page_id}"
    body = {"properties": properties}
    try:
        notion_patch(url, body)
        return True, "OK"
    except Exception as e:
        return False, str(e)

def archive_page(page_id: str) -> Tuple[bool, str]:
    url = f"https://api.notion.com/v1/pages/{page_id}"
    body = {"archived": True}
    try:
        notion_patch(url, body)
        return True, "OK"
    except Exception as e:
        return False, str(e)

def unarchive_page(page_id: str) -> Tuple[bool, str]:
    url = f"https://api.notion.com/v1/pages/{page_id}"
    body = {"archived": False}
    try:
        notion_patch(url, body)
        return True, "OK"
    except Exception as e:
        return False, str(e)

# ---------------- Notion property extractors ----------------
def extract_plain_text_from_rich_text(arr) -> str:
    if not arr:
        return ""
    return "".join([chunk.get("plain_text", "") for chunk in arr if isinstance(chunk, dict)])

def extract_prop_text(props: Dict[str, Any], key_name: str) -> str:
    if not props:
        return ""
    # case-insensitive find
    key = None
    for k in props:
        if k.lower() == key_name.lower():
            key = k
            break
    if key is None:
        # try contains
        for k in props:
            if key_name.lower() in k.lower():
                key = k
                break
    if not key:
        return ""
    prop = props.get(key, {})
    t = prop.get("type")
    if t == "title":
        return extract_plain_text_from_rich_text(prop.get("title", []))
    if t == "rich_text":
        return extract_plain_text_from_rich_text(prop.get("rich_text", []))
    if t == "number":
        return str(prop.get("number"))
    if t == "select":
        v = prop.get("select") or {}
        return v.get("name", "")
    if t == "multi_select":
        arr = prop.get("multi_select") or []
        return ", ".join([a.get("name", "") for a in arr])
    if t == "date":
        d = prop.get("date") or {}
        return d.get("start", "")
    if t == "formula":
        # try number or string
        if "number" in prop and prop.get("number") is not None:
            return str(prop.get("number"))
        if "string" in prop and prop.get("string") is not None:
            return prop.get("string")
    if t == "checkbox":
        return str(prop.get("checkbox"))
    return ""

def find_prop_key(props: Dict[str, Any], want: str) -> Optional[str]:
    if not props:
        return None
    for k in props:
        if k.lower() == want.lower():
            return k
    for k in props:
        if want.lower() in k.lower():
            return k
    return None

def parse_money_from_text(s: str) -> Optional[float]:
    if not s:
        return None
    # remove non-digit except dot and minus
    m = re.findall(r"-?\d+\.?\d*", s.replace(",", ""))
    if not m:
        return None
    try:
        return float(m[0])
    except:
        return None

# ---------------- Core flows ----------------

def find_calendar_matches(keyword: str, include_archived=False) -> List[Tuple[str, str, Optional[str]]]:
    """
    Return list of (page_id, title_preview, date_iso) from NOTION_DATABASE_ID
    Filter by title containing keyword (case-insensitive)
    """
    results = query_database_all(NOTION_DATABASE_ID, page_size=100)
    matches = []
    kw = keyword.strip().lower()
    for p in results:
        props = p.get("properties", {})
        title = extract_prop_text(props, "Name") or extract_prop_text(props, "Title") or ""
        if kw in title.lower():
            # exclude archived? Notion query_all returns pages whether archived or not; but page has "archived" at top-level sometimes
            date = None
            date_key = find_prop_key(props, DATE_PROP_NAME)
            if date_key and props.get(date_key, {}).get("date"):
                date = props.get(date_key, {}).get("date", {}).get("start")
            matches.append((p.get("id"), title, date))
            if len(matches) >= MAX_PREVIEW:
                break
    # sort by date ascending if available
    def date_sort_key(item):
        try:
            if item[2]:
                return item[2]
            return ""
        except:
            return ""
    matches.sort(key=date_sort_key)
    return matches

def count_checked_unchecked(keyword: str) -> Tuple[int, int]:
    results = query_database_all(NOTION_DATABASE_ID, page_size=200)
    checked = 0
    unchecked = 0
    kw = keyword.strip().lower()
    for p in results:
        props = p.get("properties", {})
        title = extract_prop_text(props, "Name") or ""
        if kw in title.lower():
            # check checkbox prop
            key = find_prop_key(props, CHECKBOX_PROP)
            checked_flag = False
            if key and props.get(key, {}).get("type") == "checkbox":
                checked_flag = bool(props.get(key, {}).get("checkbox"))
            if checked_flag:
                checked += 1
            else:
                unchecked += 1
    return checked, unchecked

def build_preview_text_for_matches(keyword: str, matches: List[Tuple[str, str, Optional[str]]]) -> str:
    checked, unchecked = count_checked_unchecked(keyword)
    header = f"🔎 : '{keyword}'\n\n✅ Đã tích: {checked}\n\n🟡 Chưa tích: {unchecked}\n\n"
    header += f"📤 Gửi số ( ví dụ 1 hoặc 1-3 ) trong {WAIT_CONFIRM}s để chọn, hoặc /cancel.\n\n"
    lines = []
    for i, (pid, title, date_iso) in enumerate(matches, start=1):
        ds = date_iso[:10] if date_iso else "-"
        lines.append(f"{i}. [{ds}] {title}")
    return header + "\n".join(lines)

def mark_pages_by_indices(chat_id: int, keyword: str, matches: List[Tuple[str,str,Optional[str]]], indices: List[int]):
    succeeded = []
    failed = []
    for idx in indices:
        if idx < 1 or idx > len(matches):
            failed.append((idx, "index out of range"))
            continue
        pid, title, date_iso = matches[idx-1]
        try:
            # find checkbox prop key
            page = get_page(pid)
            props = page.get("properties", {})
            key = find_prop_key(props, CHECKBOX_PROP) or CHECKBOX_PROP
            ok, msg = patch_page_properties(pid, {key: {"checkbox": True}})
            if ok:
                succeeded.append((pid, title, date_iso))
            else:
                failed.append((pid, msg))
            time.sleep(PATCH_DELAY)
        except Exception as e:
            failed.append((pid, str(e)))
    # logging
    log_action({"ts": now_iso(), "type": "mark", "chat": chat_id, "keyword": keyword,
                "selected": [{"page_id": p, "title": t, "date": d} for p,t,d in succeeded], "failed": failed})
    # send summary
    if succeeded:
        lines = [f"✅ Đã đánh dấu {len(succeeded)} mục:\n"]
        for i,(p,t,d) in enumerate(succeeded, start=1):
            lines.append(f"{i}. [{d[:10] if d else '-'}] {t}")
        checked, unchecked = count_checked_unchecked(keyword)
        lines.append(f"\n✅ Đã tích: {checked}\n\n🟡 Chưa tích: {unchecked}")
        send_long_text(chat_id, "\n".join(lines))
    else:
        send_telegram(chat_id, "Không có mục nào được đánh dấu.")

def quick_mark_first_n(chat_id: int, keyword: str, n: int):
    matches = find_calendar_matches(keyword)
    if not matches:
        send_telegram(chat_id, f"Không tìm thấy mục cho '{keyword}' để đánh dấu.")
        return
    indices = list(range(1, min(n, len(matches)) + 1))
    mark_pages_by_indices(chat_id, keyword, matches, indices)

def archive_pages_by_indices(chat_id: int, keyword: str, matches: List[Tuple[str,str,Optional[str]]], indices: List[int]):
    succeeded = []
    failed = []
    for idx in indices:
        if idx < 1 or idx > len(matches):
            failed.append((idx, "index out of range"))
            continue
        pid, title, date_iso = matches[idx-1]
        try:
            ok, msg = archive_page(pid)
            if ok:
                succeeded.append((pid, title, date_iso))
            else:
                failed.append((pid, msg))
            time.sleep(PATCH_DELAY)
        except Exception as e:
            failed.append((pid, str(e)))
    log_action({"ts": now_iso(), "type": "archive", "chat": chat_id, "keyword": keyword,
                "selected": [{"page_id": p, "title": t, "date": d} for p,t,d in succeeded], "failed": failed})
    # send summary
    lines = [f"✅ Đã archive {len(succeeded)} mục:"]
    for i,(p,t,d) in enumerate(succeeded, start=1):
        lines.append(f"{i}. [{d[:10] if d else '-'}] {t}")
    if failed:
        lines.append(f"\n⚠️ Một vài mục không archive:")
        for i,item in enumerate(failed, start=1):
            lines.append(f"{i}. {item[0]} ({item[1]})")
    send_long_text(chat_id, "\n".join(lines))

def undo_last_op(chat_id: int):
    # find last mark or archive op in log file
    if not LOG_FILE.exists():
        send_telegram(chat_id, "Chưa có hoạt động để undo.")
        return
    lines = LOG_FILE.read_text(encoding="utf-8").splitlines()
    found = None
    for ln in reversed(lines):
        try:
            obj = json.loads(ln)
            if obj.get("type") in ("mark","archive"):
                found = obj
                break
        except:
            continue
    if not found:
        send_telegram(chat_id, "Không tìm thấy op để undo.")
        return
    send_telegram(chat_id, "Đang tìm và undo...")
    typ = found.get("type")
    reverted = []
    failed = []
    if typ == "mark":
        items = found.get("selected", [])  # list of dicts {page_id,...}
        for it in items:
            pid = it.get("page_id")
            try:
                ok, msg = patch_page_properties(pid, {CHECKBOX_PROP: {"checkbox": False}})
                if ok:
                    reverted.append(pid)
                else:
                    failed.append((pid, msg))
                time.sleep(PATCH_DELAY)
            except Exception as e:
                failed.append((pid, str(e)))
        send_telegram(chat_id, f"♻️ Undo done. Reverted {len(reverted)} items. Failed: {len(failed)}")
        return
    elif typ == "archive":
        items = found.get("selected", [])
        for it in items:
            pid = it.get("page_id")
            try:
                ok, msg = unarchive_page(pid)
                if ok:
                    reverted.append(pid)
                else:
                    failed.append((pid, msg))
                time.sleep(PATCH_DELAY)
            except Exception as e:
                failed.append((pid, str(e)))
        send_telegram(chat_id, f"♻️ Undo done. Reverted {len(reverted)} items. Failed: {len(failed)}")
        return
    else:
        send_telegram(chat_id, "Không thể undo cho loại op này.")

# ---------------- DAO (đáo) flow ----------------
def find_target_matches(keyword: str) -> List[Tuple[str, str, Dict[str, Any]]]:
    """Search TARGET_NOTION_DATABASE_ID for entries whose Name contains keyword"""
    results = query_database_all(TARGET_NOTION_DATABASE_ID, page_size=200)
    matches = []
    kw = keyword.strip().lower()
    for p in results:
        props = p.get("properties", {})
        title = extract_prop_text(props, "Name") or extract_prop_text(props, "Title") or ""
        if kw in title.lower():
            matches.append((p.get("id"), title, props))
            if len(matches) >= MAX_PREVIEW:
                break
    return matches

def dao_preview_text_from_props(title: str, props: Dict[str, Any]) -> Tuple[bool, str]:
    """
    Build preview text for DAO. Return (can_dao, message)
    Rules:
     - read 'Đáo/thối' property text: if startswith '✅' -> active, if startswith '🔴' -> not active
       otherwise if numeric positive -> treat as active (we'll assume it's active)
     - read 'G ngày' numeric = per_day
     - read '# ngày trước' numeric = days_before
     - read 'trước' numeric = pre_amount
    """
    # Read Đáo/thối
    dt_text = extract_prop_text(props, "Đáo/thối")
    active = False
    total_val = None
    if dt_text:
        txt = dt_text.strip()
        if txt.startswith("✅"):
            active = True
            total_val = parse_money_from_text(txt)
        elif txt.startswith("🔴"):
            active = False
            total_val = parse_money_from_text(txt)
        else:
            # maybe it's just number string like "4910"
            total_val = parse_money_from_text(txt)
            active = (total_val is not None and total_val > 0)
    # G ngày
    per_day = parse_money_from_text(extract_prop_text(props, "G ngày") or "")
    # # ngày trước
    days_before = parse_money_from_text(extract_prop_text(props, "# ngày trước") or "")
    if days_before is not None:
        try:
            days_before = int(days_before)
        except:
            days_before = None
    # trước (formula)
    pre_amount = parse_money_from_text(extract_prop_text(props, "trước") or "")
    # Build message according to your spec
    if not active:
        return False, f"🔴 Không thể đáo cho: {title} (Đã tắt / không active)"
    # active True
    if not pre_amount or pre_amount == 0:
        # show "Không Lấy trước"
        msg = f"🔔 đáo lại cho: {title} - Tổng đáo: ✅ {int(total_val) if total_val else 'N/A'}\n\nKhông Lấy trước"
        return True, msg
    else:
        # compute days_to_create = days_before (if provided) else ceil(pre_amount / per_day)
        if days_before and days_before > 0:
            take_days = days_before
        else:
            # fallback compute
            if per_day and per_day > 0:
                take_days = int(math.ceil(pre_amount / per_day))
            else:
                take_days = 0
        # compute sum taken = take_days * per_day
        taken_sum = int(per_day) * int(take_days) if per_day else 0
        # build list of dates starting tomorrow
        start = datetime.now().date() + timedelta(days=1)
        dates = [start + timedelta(days=i) for i in range(take_days)]
        date_list_text = "\n".join([d.isoformat() for d in dates])
        msg = (f"🔔 đáo lại cho: {title} - Tổng đáo: ✅ {int(total_val) if total_val else 'N/A'}\n\n"
               f"Lấy trước: {take_days} ngày {int(per_day) if per_day else 0} là {taken_sum}\n\n"
               f"Danh sách ngày dự kiến tạo (bắt đầu từ ngày mai):\n{date_list_text}\n\n"
               f"Gửi /ok để tạo {take_days} page {NOTION_DATABASE_ID}, hoặc cancel để hủy.")
        # return also computed metadata if caller needs
        return True, msg

def dao_create_pages_from_props(chat_id: int, source_page_id: str, props: Dict[str, Any]):
    """
    Create pages in NOTION_DATABASE_ID based on 'trước' and 'G ngày' and '# ngày trước'.
    """
    # re-run compute like above
    title = extract_prop_text(props, "Name") or "UNKNOWN"
    total_text = extract_prop_text(props, "Đáo/thối")
    total_val = parse_money_from_text(total_text) or 0
    per_day = parse_money_from_text(extract_prop_text(props, "G ngày")) or 0
    days_before = parse_money_from_text(extract_prop_text(props, "# ngày trước")) or 0
    pre_amount = parse_money_from_text(extract_prop_text(props, "trước")) or 0
    if pre_amount == 0:
        send_telegram(chat_id, f"🔔 đáo lại cho: {title} - Tổng đáo: ✅ {int(total_val) if total_val else 'N/A'}\n\nKhông Lấy trước")
        return
    # compute take_days
    take_days = int(days_before) if days_before and days_before > 0 else (int(math.ceil(pre_amount / per_day)) if per_day else 0)
    if take_days <= 0:
        send_telegram(chat_id, "⚠️ Không tính được số ngày để tạo.")
        return
    start = datetime.now().date() + timedelta(days=1)
    created = []
    failed = []
    for i in range(take_days):
        d = start + timedelta(days=i)
        props_payload = {
            "Name": {"title": [{"type": "text", "text": {"content": f"{title} - {d.isoformat()}"}}]},
            DATE_PROP_NAME: {"date": {"start": d.isoformat()}}
        }
        ok, res = create_page_in_db(NOTION_DATABASE_ID, props_payload)
        if ok:
            created.append(res)
        else:
            failed.append(res)
        time.sleep(PATCH_DELAY)
    # mark source as processed if possible: try to find a checkbox property among DAO_CHECKFIELD_NAMES
    try:
        page = get_page(source_page_id)
        page_props = page.get("properties", {})
        key = None
        for candidate in DAO_CHECKFIELD_NAMES:
            k = find_prop_key(page_props, candidate)
            if k:
                key = k
                break
        if key:
            patch_page_properties(source_page_id, {key: {"checkbox": True}})
    except Exception as e:
        print("Error ticking dao checkbox:", e)
    # send summary
    lines = [f"✅ Đã tạo {len(created)} page cho {title}:"]
    for i, c in enumerate(created, start=1):
        try:
            date_val = c["properties"][DATE_PROP_NAME]["date"]["start"]
            lines.append(f"{i}. [{date_val}] {c.get('id')}")
        except:
            lines.append(f"{i}. {c.get('id')}")
    if failed:
        lines.append(f"\n⚠️ Failed: {len(failed)}")
    send_long_text(chat_id, "\n".join(lines))

# ---------------- Dispatcher & webhook ----------------
def handle_text_message(chat_id: int, text: str):
    """
    This dispatcher runs in a separate thread.
    Recognizes commands:
      gam
      gam 2
      gam xóa
      gam đáo
      undo
    """
    if not text:
        return
    txt = text.strip()
    low = txt.lower().strip()
    # undo command
    if low == "undo":
        undo_last_op(chat_id)
        return
    # archive command endswith ' xóa' or ' xoa'
    if low.endswith(" xóa") or low.endswith(" xoa"):
        kw = txt[:-4].strip()
        matches = find_calendar_matches(kw)
        checked, unchecked = count_checked_unchecked(kw)
        header = f"🔎 Khách hàng: '{kw}'\n\n✅ Đã tích: {checked}\n\n🟡 Chưa tích: {unchecked}\n\n"
        header += f"⚠️ CHÚ Ý: Bạn sắp archive {len(matches)} mục chứa '{kw}'.\n\nGửi số (ví dụ 1-7) trong {WAIT_CONFIRM}s để chọn, hoặc 'all' để archive tất cả, hoặc /cancel.\n\n"
        lines = []
        for i,(pid,title,date_iso) in enumerate(matches, start=1):
            lines.append(f"{i}. [{date_iso[:10] if date_iso else '-'}] {title}")
        send_long_text(chat_id, header + "\n".join(lines))
        pending[str(chat_id)] = {"type":"archive_select","keyword":kw,"matches":matches,"expires":time.time()+WAIT_CONFIRM}
        return
    # dao command endswith ' đáo' or ' dao'
    if low.endswith(" đáo") or low.endswith(" dao"):
        kw = txt.rsplit(None,1)[0]
        matches = find_target_matches(kw)
        if not matches:
            send_telegram(chat_id, f"⚠️ Không tìm thấy '{kw}' trong DB đáo.")
            return
        if len(matches) > 1:
            # list options to pick
            header = f"Tìm thấy {len(matches)} kết quả cho '{kw}'. Chọn index để tiếp tục hoặc gửi SĐT để match chính xác."
            lines = []
            for i,(pid,title,props) in enumerate(matches, start=1):
                # show some columns: Đáo/thối, G ngày, # ngày trước, trước
                dt = extract_prop_text(props, "Đáo/thối")
                gday = extract_prop_text(props, "G ngày")
                nb = extract_prop_text(props, "# ngày trước")
                prev = extract_prop_text(props, "trước")
                lines.append(f"{i}. {title} - Đáo/thối: {dt} - G ngày: {gday} - # ngày trước: {nb} - trước: {prev}")
            send_long_text(chat_id, header + "\n" + "\n".join(lines))
            pending[str(chat_id)] = {"type":"dao_select","keyword":kw,"matches":matches,"expires":time.time()+WAIT_CONFIRM}
            return
        # single match
        pid, title, props = matches[0]
        can, msg = dao_preview_text_from_props(title, props)
        send_long_text(chat_id, msg)
        if can:
            # store pending for confirm
            pending[str(chat_id)] = {"type":"dao_confirm", "source_page_id": pid, "props": props, "expires": time.time()+WAIT_CONFIRM}
        return
    # quick mark like 'gam 2'
    m = re.match(r"^(.+?)\s+(\d+)$", txt)
    if m:
        kw = m.group(1).strip()
        n = int(m.group(2))
        quick_mark_first_n(chat_id, kw, n)
        return
    # default: preview mark
    kw = txt.strip()
    matches = find_calendar_matches(kw)
    if not matches:
        send_telegram(chat_id, f"Không tìm thấy '{kw}' trong cơ sở dữ liệu.")
        return
    msg = build_preview_text_for_matches(kw, matches)
    send_long_text(chat_id, msg)
    pending[str(chat_id)] = {"type":"mark_select","keyword":kw,"matches":matches,"expires":time.time()+WAIT_CONFIRM}
    return

@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        j = request.get_json(force=True)
        if not j:
            return Response("no json", status=400)
        message = j.get("message") or j.get("edited_message") or {}
        if not message:
            return Response("no message", status=200)
        chat = message.get("chat", {})
        chat_id = chat.get("id")
        if TELEGRAM_CHAT_ID and str(chat_id) != str(TELEGRAM_CHAT_ID):
            return Response("ignored", status=200)
        text = message.get("text")
        # if pending selection exists for this chat, route to selection handler
        pc = pending.get(str(chat_id))
        if pc:
            # check expiry
            if time.time() > pc.get("expires", 0):
                del pending[str(chat_id)]
                send_telegram(chat_id, "⏳ Hết thời gian chọn. Yêu cầu đã bị hủy.")
                return Response("ok", status=200)
            # selection handling
            t = text.strip()
            if pc["type"] == "mark_select":
                if t.lower() == "/cancel" or t.lower() == "cancel":
                    del pending[str(chat_id)]
                    send_telegram(chat_id, "Đã hủy yêu cầu.")
                else:
                    # parse indices
                    indices = []
                    if t.lower() == "all":
                        indices = list(range(1, len(pc["matches"])+1))
                    else:
                        parts = [p.strip() for p in t.split(",")]
                        for part in parts:
                            if "-" in part:
                                a,b = part.split("-",1)
                                try:
                                    a=int(a); b=int(b)
                                    indices.extend(list(range(a,b+1)))
                                except:
                                    pass
                            else:
                                try:
                                    indices.append(int(part))
                                except:
                                    pass
                    indices = sorted(set([i for i in indices if 1<=i<=len(pc["matches"])]))
                    if not indices:
                        send_telegram(chat_id, "Không có lựa chọn hợp lệ.")
                    else:
                        mark_pages_by_indices(chat_id, pc["keyword"], pc["matches"], indices)
                        del pending[str(chat_id)]
            elif pc["type"] == "archive_select":
                if t.lower() == "/cancel" or t.lower() == "cancel":
                    del pending[str(chat_id)]
                    send_telegram(chat_id, "Đã hủy yêu cầu.")
                else:
                    indices = []
                    if t.lower() == "all":
                        indices = list(range(1, len(pc["matches"])+1))
                    else:
                        parts = [p.strip() for p in t.split(",")]
                        for part in parts:
                            if "-" in part:
                                a,b = part.split("-",1)
                                try:
                                    a=int(a); b=int(b)
                                    indices.extend(list(range(a,b+1)))
                                except:
                                    pass
                            else:
                                try:
                                    indices.append(int(part))
                                except:
                                    pass
                    indices = sorted(set([i for i in indices if 1<=i<=len(pc["matches"])]))
                    if not indices:
                        send_telegram(chat_id, "Không có lựa chọn hợp lệ.")
                    else:
                        archive_pages_by_indices(chat_id, pc["keyword"], pc["matches"], indices)
                        del pending[str(chat_id)]
            elif pc["type"] == "dao_select":
                # user picks index for target db
                tstr = text.strip()
                try:
                    idx = int(tstr)
                    matches = pc.get("matches", [])
                    if 1 <= idx <= len(matches):
                        pid, title, props = matches[idx-1]
                        can, msg = dao_preview_text_from_props(title, props)
                        send_long_text(chat_id, msg)
                        if can:
                            pending[str(chat_id)] = {"type":"dao_confirm", "source_page_id": pid, "props": props, "expires": time.time()+WAIT_CONFIRM}
                        else:
                            # keep no confirm
                            if str(chat_id) in pending:
                                del pending[str(chat_id)]
                    else:
                        send_telegram(chat_id, "Index không hợp lệ.")
                except Exception:
                    send_telegram(chat_id, "Vui lòng gửi số index để chọn hoặc /cancel.")
            elif pc["type"] == "dao_confirm":
                tstr = text.strip().lower()
                if tstr in ("/ok","ok"):
                    src_pid = pc.get("source_page_id")
                    props = pc.get("props") or get_page(src_pid).get("properties", {})
                    # perform create
                    dao_create_pages_from_props(chat_id, src_pid, props)
                    if str(chat_id) in pending:
                        del pending[str(chat_id)]
                elif tstr in ("/cancel","cancel"):
                    del pending[str(chat_id)]
                    send_telegram(chat_id, "Đã hủy đáo.")
                else:
                    send_telegram(chat_id, "Gửi /ok để tạo pages hoặc /cancel để hủy.")
            else:
                send_telegram(chat_id, "Không có thao tác đang chờ hoặc loại không xác định.")
            return Response("ok", status=200)
        # if no pending, dispatch a new handling thread
        threading.Thread(target=handle_text_message, args=(chat.get("id"), text), daemon=True).start()
        return Response("ok", status=200)
    except Exception as e:
        print("webhook exception:", e)
        traceback.print_exc()
        return Response("error", status=500)

@app.route("/", methods=["GET"])
def home():
    return "Notion-Telegram Bot OK", 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    print("Starting app on port", port)
    app.run(host="0.0.0.0", port=port)
