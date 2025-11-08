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
LA_NOTION_DATABASE_ID = os.getenv("LA_NOTION_DATABASE_ID")
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
def check_prop_exists(database_id: str, prop_name: str) -> bool:
    """
    Kiểm tra xem 1 property có tồn tại trong Notion database không.
    """
    try:
        url = f"https://api.notion.com/v1/databases/{database_id}"
        r = requests.get(url, headers=NOTION_HEADERS, timeout=10)
        if r.status_code == 200:
            data = r.json()
            return prop_name in data.get("properties", {})
        return False
    except Exception:
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
    """
    Trả về nội dung text/number của một property trong Notion (bao gồm hỗ trợ rollup, formula).
    """
    if not props:
        return ""

    # tìm key case-insensitive
    key = None
    for k in props:
        if k.lower() == key_name.lower():
            key = k
            break
    if not key:
        for k in props:
            if key_name.lower() in k.lower():
                key = k
                break
    if not key:
        return ""

    prop = props.get(key, {})
    ptype = prop.get("type")

    # --- HANDLE FORMULA ---
    if ptype == "formula":
        formula = prop.get("formula", {})
        if formula.get("type") == "number" and formula.get("number") is not None:
            return str(formula.get("number"))
        if formula.get("type") == "string" and formula.get("string"):
            return formula.get("string")
        if formula.get("type") == "boolean":
            return str(formula.get("boolean"))
        if formula.get("type") == "date" and formula.get("date"):
            return formula["date"].get("start", "")
        return ""

    # --- HANDLE ROLLUP ---
    if ptype == "rollup":
        roll = prop.get("rollup", {})
        roll_type = roll.get("type")

        # direct number rollup
        if roll_type == "number" and roll.get("number") is not None:
            return str(roll.get("number"))

        # rollup array (ví dụ nhiều giá trị)
        if roll_type == "array":
            arr = roll.get("array", [])
            if arr and isinstance(arr[0], dict):
                sub = arr[0]
                # rollup array element có thể là number hoặc rich_text
                if "number" in sub and sub["number"] is not None:
                    return str(sub["number"])
                if "plain_text" in sub:
                    return sub["plain_text"]
                if "title" in sub:
                    return "".join(t.get("plain_text", "") for t in sub["title"])
            return ""
        return ""

    # --- HANDLE CÁC KIỂU KHÁC ---
    if ptype == "title":
        return extract_plain_text_from_rich_text(prop.get("title", []))
    if ptype == "rich_text":
        return extract_plain_text_from_rich_text(prop.get("rich_text", []))
    if ptype == "number":
        return str(prop.get("number"))
    if ptype == "select":
        v = prop.get("select") or {}
        return v.get("name", "")
    if ptype == "multi_select":
        arr = prop.get("multi_select") or []
        return ", ".join([a.get("name", "") for a in arr])
    if ptype == "date":
        d = prop.get("date") or {}
        return d.get("start", "")
    if ptype == "checkbox":
        return str(prop.get("checkbox"))
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
    Trả về danh sách chỉ các page CHƯA TÍCH (Đã Góp == False)
    """
    results = query_database_all(NOTION_DATABASE_ID, page_size=200)
    matches = []
    kw = keyword.strip().lower()
    for p in results:
        props = p.get("properties", {})
        title = extract_prop_text(props, "Name") or extract_prop_text(props, "Title") or ""
        if kw not in title.lower():
            continue
        # Kiểm tra checkbox
        key = find_prop_key(props, CHECKBOX_PROP)
        is_checked = False
        if key and props[key].get("type") == "checkbox":
            is_checked = props[key].get("checkbox", False)
        if not is_checked:
            date_key = find_prop_key(props, DATE_PROP_NAME)
            date_iso = None
            if date_key and props.get(date_key, {}).get("date"):
                date_iso = props.get(date_key)["date"].get("start")
            matches.append((p.get("id"), title, date_iso))
    # sắp xếp theo ngày
    matches.sort(key=lambda x: x[2] or "")
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
    header = f"🔎 : '{keyword}'\n✅ Đã góp: {checked}\n🟡 Chưa góp: {unchecked}\n"
    header += f"📤  {WAIT_CONFIRM} /cancel.\n"
    lines = []
    for i, (pid, title, date_iso) in enumerate(matches, start=1):
        ds = date_iso[:10] if date_iso else "-"
        lines.append(f"{i}. [{ds}] {title}")
    return header + "\n".join(lines)

def mark_pages_by_indices(chat_id: int, keyword: str, matches: List[Tuple[str,str,Optional[str]]], indices: List[int]):
    succeeded = []
    failed = []

    # Nếu user chỉ gõ 1 số (vd: 3) -> chọn 3 mục đầu tiên
    if len(indices) == 1 and indices[0] > 1:
        max_n = indices[0]
        indices = list(range(1, min(max_n, len(matches)) + 1))

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
        lines = [f"🔎 : {keyword}✅ Đã đánh dấu {len(succeeded)} mục:\n"]
        for i,(p,t,d) in enumerate(succeeded, start=1):
            lines.append(f"{i}. [{d[:10] if d else '-'}] {t}")
        # sau khi đánh dấu xong, cập nhật lại số đếm
        checked, unchecked = count_checked_unchecked(keyword)
        lines.append(f"\n✅ Đã tích: {checked}")
        lines.append(f"\n🟡 Chưa tích: {unchecked}")
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
        msg = (f"🔔 đáo lại cho: {title} - Tổng đáo: CK ✅ {int(total_val) if total_val else 'N/A'}\n"
               f"Lấy trước: {take_days} ngày {int(per_day) if per_day else 0} là {taken_sum}\n\n"
               f"(bắt đầu từ ngày mai):\n{date_list_text}\n\n"
               f" /ok  hoặc cancel.")
        # return also computed metadata if caller needs
        return True, msg
def send_progress(chat_id: int, step: int, total: int, label: str):
    """
    Gửi tiến trình (mỗi 10 bước, hoặc khi xong)
    """
    try:
        if total == 0:
            return
        if step == 1 or step % 10 == 0 or step == total:
            send_telegram(chat_id, f"⏱️ {label}: {step}/{total} ...")
    except Exception as e:
        print("send_progress error:", e)


def dao_create_pages_from_props(chat_id: int, source_page_id: str, props: Dict[str, Any]):
    """
    Xử lý đáo:
     - Xóa toàn bộ page cũ có tên trùng trong NOTION_DATABASE_ID (checked + unchecked)
     - Tạo `take_days` page mới (ngày mai bắt đầu), mỗi page có Đã Góp=True
     - Tạo page Lãi trong LA_NOTION_DATABASE_ID nếu có
    """
    try:
        title = extract_prop_text(props, "Name") or "UNKNOWN"
        total_text = extract_prop_text(props, "Đáo/thối")
        total_val = parse_money_from_text(total_text) or 0
        per_day = parse_money_from_text(extract_prop_text(props, "G ngày")) or 0
        days_before = parse_money_from_text(extract_prop_text(props, "ngày trước")) or 0
        pre_amount = parse_money_from_text(extract_prop_text(props, "trước")) or 0

        if pre_amount == 0:
            send_telegram(chat_id, f"🔔 đáo lại cho: {title} - Tổng đáo: ✅ {int(total_val) if total_val else 'N/A'}\n\nKhông Lấy trước")
            return

        take_days = int(days_before) if days_before and int(days_before) > 0 else (
            int(math.ceil(pre_amount / per_day)) if per_day else 0
        )
        if take_days <= 0:
            send_telegram(chat_id, f"⚠️ Không xác định được số ngày hợp lệ cho {title}")
            return

        # === 1️⃣ XÓA PAGE CŨ ===
        # === Gọi lại chức năng xóa có sẵn ===
        send_telegram(chat_id, f"🧹 Đang archive toàn bộ page của {title} trước khi tạo mới...")
        handle_command_archive(chat_id, title, None, f"{title} xóa")

        # === 2️⃣ TẠO PAGE MỚI ===
        start = datetime.now().date() + timedelta(days=1)
        created = []
        send_telegram(chat_id, f"🛠️ Đang tạo {take_days} ngày mới cho {title} (bắt đầu từ ngày mai)...")

        for count in range(1, take_days + 1):
            d = start + timedelta(days=(count - 1))
            props_payload = {
                "Name": {"title": [{"type": "text", "text": {"content": f"{title} - {d.isoformat()}"}}]},
                DATE_PROP_NAME: {"date": {"start": d.isoformat()}},
                "Tiền": {"number": per_day} if per_day else {},
                CHECKBOX_PROP: {"checkbox": True},
                "Lịch G": {"relation": [{"id": source_page_id}]}
            }
            clean_props = {k: v for k, v in props_payload.items() if v}
            try:
                ok, res = create_page_in_db(NOTION_DATABASE_ID, clean_props)
                if ok:
                    created.append(res)
                    send_progress(chat_id, count, take_days, f"📅 Đang tạo ngày mới cho {title}")
                else:
                    send_telegram(chat_id, f"⚠️ Tạo lỗi: {res}")
            except Exception as e:
                send_telegram(chat_id, f"⚠️ Tạo lỗi: {str(e)}")
            time.sleep(PATCH_DELAY)

        send_telegram(chat_id, f"✅ Đã tạo {len(created)} ngày mới cho {title} (đã check 'Đã Góp').")

        # === 3️⃣ TẠO PAGE LÃI ===
        lai_text = extract_prop_text(props, "Lai") or extract_prop_text(props, "Lãi") or ""
        lai_amt = parse_money_from_text(lai_text) or 0
        if LA_NOTION_DATABASE_ID and lai_amt > 0:
            send_telegram(chat_id, f"💸 Tiếp tục tạo Lãi cho {title}...")
            relation_target_id = created[0]["id"] if created else source_page_id
            create_lai_page(chat_id, title, lai_amt, relation_target_id)
        else:
            send_telegram(chat_id, f"ℹ️ Không có giá trị Lãi hoặc LA_NOTION_DATABASE_ID chưa cài đặt.")

        send_telegram(chat_id, f"✅ Hoàn tất tiến trình đáo cho {title}.")
        return

    except Exception as e:
        traceback.print_exc()
        send_telegram(chat_id, f"❌ Lỗi tiến trình đáo cho {title}: {str(e)}")
        return


def create_lai_page(chat_id: int, title: str, lai_amount: float, relation_id: str):
    """
    Tạo 1 page trong LA_NOTION_DATABASE_ID với:
     - Name = title
     - Lãi = lai_amount
     - Ngày Lãi = hôm nay
     - Lịch G = relation tới NOTION_DATABASE_ID
    """
    try:
        today = datetime.now().date().isoformat()

        # kiểm tra xem LA_NOTION_DATABASE_ID có tồn tại
        if not LA_NOTION_DATABASE_ID:
            send_telegram(chat_id, "⚠️ LA_NOTION_DATABASE_ID chưa được thiết lập.")
            return

        # build payload linh hoạt: chỉ thêm field nếu database có cột đó
        props_payload = {}

        # Name
        props_payload["Name"] = {
            "title": [{"type": "text", "text": {"content": title}}]
        }

        # Lãi (chấp nhận 'Lai' hoặc 'Lãi')
        field_name_lai = None
        for f in ("Lãi", "Lai", "Lãi "):
            if check_prop_exists(LA_NOTION_DATABASE_ID, f):
                field_name_lai = f
                break
        if field_name_lai:
            props_payload[field_name_lai] = {"number": lai_amount}
        else:
            send_telegram(chat_id, "⚠️ Không tìm thấy cột 'Lãi' hoặc 'Lai' trong DB Lãi.")

        # Ngày Lãi
        field_name_date = None
        for f in ("Ngày Lãi", "Ngay Lãi", "Ngày lãi", "Ngay lai"):
            if check_prop_exists(LA_NOTION_DATABASE_ID, f):
                field_name_date = f
                break
        if field_name_date:
            props_payload[field_name_date] = {"date": {"start": today}}

        # Lịch G (relation)
        props_payload["Lịch G"] = {"relation": [{"id": relation_id}]}

        # gửi request tạo page
        url = "https://api.notion.com/v1/pages"
        body = {"parent": {"database_id": LA_NOTION_DATABASE_ID}, "properties": props_payload}
        r = requests.post(url, headers=NOTION_HEADERS, json=body, timeout=15)

        if r.status_code in (200, 201):
            send_telegram(chat_id, f"💰 Đã tạo Lãi cho {title} ({lai_amount})")
        else:
            send_telegram(chat_id, f"⚠️ Tạo Lãi lỗi: {r.status_code} - {r.text}")

    except Exception as e:
        send_telegram(chat_id, f"❌ Lỗi tạo Lãi cho {title}: {str(e)}")



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
        matches = find_matching_all_pages_in_db(NOTION_DATABASE_ID, keyword, limit=40)
        checked, unchecked = count_checked_unchecked(kw)
        header = f"🔎 Khách hàng: '{kw}'\n✅ Đã tích: {checked}\n🟡 Chưa tích: {unchecked}\n"
        header += f"⚠️ CHÚ Ý: Bạn sắp archive {len(matches)} mục chứa '{kw}'.\n\nGửi số (ví dụ 1-7) trong {WAIT_CONFIRM}s để chọn, hoặc /all để archive tất cả, hoặc /cancel.\n\n"
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
            send_telegram(chat_id, f"🛠️ Đang tạo {take_days} ngày mới cho {title} bắt đầu từ ngày mai...")
            for i,(pid,title,props) in enumerate(matches, start=1):
                # show some columns: Đáo/thối, G ngày, # ngày trước, trước
                dt = extract_prop_text(props, "Đáo/thối")
                gday = extract_prop_text(props, "G ngày")
                nb = extract_prop_text(props, "# ngày trước")
                prev = extract_prop_text(props, "trước")
                lines.append(f"{i}. {title} - Đáo/thối: {dt} - G ngày: {gday} - # ngày trước: {nb} - trước: {prev}")
                send_telegram(chat_id, f"✅ Đã tạo xong {len(created)} ngày mới cho {title}.")
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
