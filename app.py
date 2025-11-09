# app.py
# Production-ready Telegram <-> Notion automation
# Features:
# - Flask webhook /telegram_webhook (also supports /webhook)
# - Commands: "<key>", "<key> <n>", "<key> xóa", "<key> đáo", "undo", /cancel
# - mark: mark n oldest unchecked items (if input "3" -> mark 1..3 oldest)
# - archive: archive matched pages (checked+unchecked)
# - dao (đáo): archive & create pages in NOTION_DATABASE_ID and create Lãi page in LA_NOTION_DATABASE_ID
# - pending confirmations, progress messages, undo stack (in-memory)
# - robust extraction for Notion properties (title, rich_text, number, date, checkbox, rollup, formula)
# - safe retries for Notion create/patch

import os
import time
import re
import math
import json
import traceback
import threading
import requests
import unicodedata
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple
from flask import Flask, request, jsonify

# ------------- CONFIG -------------
NOTION_TOKEN = os.getenv("NOTION_TOKEN", "")
NOTION_VERSION = os.getenv("NOTION_VERSION", "2022-06-28")
NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}" if NOTION_TOKEN else "",
    "Notion-Version": NOTION_VERSION,
    "Content-Type": "application/json",
}
NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID", "")
TARGET_NOTION_DATABASE_ID = os.getenv("TARGET_NOTION_DATABASE_ID", "")
LA_NOTION_DATABASE_ID = os.getenv("LA_NOTION_DATABASE_ID", "")

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")  # optional: restrict bot to one chat id

WAIT_CONFIRM = int(os.getenv("WAIT_CONFIRM", "120"))  # seconds
PATCH_DELAY = float(os.getenv("PATCH_DELAY", "0.3"))  # seconds delay between Notion calls
MAX_QUERY_PAGE_SIZE = int(os.getenv("MAX_QUERY_PAGE_SIZE", "100"))

# ------------- IN-MEM STATE -------------
pending_confirm: Dict[str, Dict[str, Any]] = {}  # chat_id_str -> {type, ...}
undo_stack: Dict[str, List[Dict[str, Any]]] = {}  # chat_id_str -> list of actions for undo (in-memory)

# ------------- UTIL: Telegram send -------------
def send_telegram(chat_id: str, text: str):
    """Send message to Telegram or print if token not set."""
    try:
        if TELEGRAM_TOKEN:
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            data = {"chat_id": chat_id, "text": text, "disable_web_page_preview": True}
            requests.post(url, data=data, timeout=8)
        else:
            print(f"[TG:{chat_id}] {text}")
    except Exception as e:
        print("send_telegram error:", e)

def send_long_text(chat_id: str, text: str):
    """Chunk long text for Telegram."""
    max_len = 3000
    for i in range(0, len(text), max_len):
        send_telegram(chat_id, text[i:i+max_len])

def send_progress(chat_id: str, step: int, total: int, label: str):
    """Simple throttled progress messages."""
    try:
        if total == 0:
            return
        if step == 1 or step % 10 == 0 or step == total:
            send_telegram(chat_id, f"⏱️ {label}: {step}/{total} ...")
    except Exception as e:
        print("send_progress error:", e)

# ------------- UTIL: Notion API wrappers -------------
def _notion_post(url: str, json_body: dict, attempts: int = 3, timeout: int = 15):
    """POST with simple retry."""
    for i in range(attempts):
        try:
            r = requests.post(url, headers=NOTION_HEADERS, json=json_body, timeout=timeout)
            if r.status_code in (200, 201):
                return True, r.json()
            # transient server errors -> retry
            if r.status_code >= 500:
                time.sleep(1 + i)
                continue
            return False, {"status": r.status_code, "text": r.text}
        except Exception as e:
            last_exc = e
            time.sleep(1 + i)
    return False, str(last_exc)

def _notion_patch(url: str, json_body: dict, attempts: int = 3, timeout: int = 12):
    """PATCH with simple retry."""
    for i in range(attempts):
        try:
            r = requests.patch(url, headers=NOTION_HEADERS, json=json_body, timeout=timeout)
            if r.status_code in (200, 204):
                try:
                    return True, r.json() if r.text else {}
                except:
                    return True, {}
            if r.status_code >= 500:
                time.sleep(1 + i)
                continue
            return False, {"status": r.status_code, "text": r.text}
        except Exception as e:
            last_exc = e
            time.sleep(1 + i)
    return False, str(last_exc)

def query_database_all(database_id: str, page_size: int = MAX_QUERY_PAGE_SIZE) -> List[Dict[str, Any]]:
    """Query all pages in a database using pagination (Notion /query)."""
    if not NOTION_TOKEN or not database_id:
        print("query_database_all missing config")
        return []
    results: List[Dict[str, Any]] = []
    try:
        url = f"https://api.notion.com/v1/databases/{database_id}/query"
        payload = {"page_size": page_size}
        r = requests.post(url, headers=NOTION_HEADERS, json=payload, timeout=20)
        if r.status_code != 200:
            print("query_database_all failed:", r.status_code, r.text)
            return []
        data = r.json()
        results.extend(data.get("results", []))
        while data.get("has_more"):
            payload["start_cursor"] = data.get("next_cursor")
            r = requests.post(url, headers=NOTION_HEADERS, json=payload, timeout=20)
            if r.status_code != 200:
                print("pagination failed:", r.status_code, r.text)
                break
            data = r.json()
            results.extend(data.get("results", []))
        return results
    except Exception as e:
        print("query_database_all exception:", e)
        return []

def create_page_in_db(database_id: str, properties: Dict[str, Any]) -> Tuple[bool, Any]:
    if not NOTION_TOKEN or not database_id:
        return False, "Notion config missing"
    url = "https://api.notion.com/v1/pages"
    body = {"parent": {"database_id": database_id}, "properties": properties}
    return _notion_post(url, body)

def archive_page(page_id: str) -> Tuple[bool, str]:
    if not NOTION_TOKEN or not page_id:
        return False, "Notion config missing"
    url = f"https://api.notion.com/v1/pages/{page_id}"
    body = {"archived": True}
    return _notion_patch(url, body)

def update_page_properties(page_id: str, properties: Dict[str, Any]) -> Tuple[bool, Any]:
    if not NOTION_TOKEN or not page_id:
        return False, "Notion config missing"
    url = f"https://api.notion.com/v1/pages/{page_id}"
    body = {"properties": properties}
    return _notion_patch(url, body)

# ------------- UTIL: property extraction & parsing -------------
def normalize_text(s: Optional[str]) -> str:
    if not s:
        return ""
    s = str(s).strip().lower()
    nf = unicodedata.normalize("NFD", s)
    return "".join(c for c in nf if unicodedata.category(c) != "Mn")

def extract_plain_text_from_rich_text(arr: List[Dict[str, Any]]) -> str:
    if not arr:
        return ""
    return "".join([x.get("plain_text", "") for x in arr if isinstance(x, dict)])

def find_prop_key(props: Dict[str, Any], name_like: str) -> Optional[str]:
    if not props:
        return None
    for k in props.keys():
        if normalize_text(k) == normalize_text(name_like):
            return k
    # fallback: contains
    for k in props.keys():
        if normalize_text(name_like) in normalize_text(k):
            return k
    return None

def extract_prop_text(props: Dict[str, Any], key_like: str) -> str:
    """
    Robust extractor for Notion property values.
    Supports: title, rich_text, number, date, checkbox, select, multi_select, relation, formula, rollup.
    Returns string (empty if not present).
    """
    if not props:
        return ""
    k = find_prop_key(props, key_like)
    if not k:
        return ""
    prop = props.get(k, {}) or {}
    ptype = prop.get("type")

    # FORMULA
    if ptype == "formula":
        formula = prop.get("formula", {})
        ftype = formula.get("type")
        if ftype == "number" and formula.get("number") is not None:
            return str(formula.get("number"))
        if ftype == "string" and formula.get("string"):
            return str(formula.get("string"))
        if ftype == "boolean" and formula.get("boolean") is not None:
            return "1" if formula.get("boolean") else "0"
        if ftype == "date" and formula.get("date"):
            return formula["date"].get("start", "")
        return ""

    # ROLLUP
    if ptype == "rollup":
        roll = prop.get("rollup", {})
        rtype = roll.get("type")
        if rtype == "number" and roll.get("number") is not None:
            return str(roll.get("number"))
        if rtype == "array":
            arr = roll.get("array", [])
            if arr:
                first = arr[0]
                # attempt to extract number or text
                if isinstance(first, dict):
                    if "number" in first and first.get("number") is not None:
                        return str(first.get("number"))
                    # for title-like
                    if "title" in first:
                        return extract_plain_text_from_rich_text(first.get("title", []))
                    if "plain_text" in first:
                        return first.get("plain_text", "")
                return str(first)
        return ""

    # TITLE
    if ptype == "title":
        return extract_plain_text_from_rich_text(prop.get("title", []))
    if ptype == "rich_text":
        return extract_plain_text_from_rich_text(prop.get("rich_text", []))
    if ptype == "number":
        return str(prop.get("number"))
    if ptype == "date":
        d = prop.get("date", {}) or {}
        return d.get("start", "") or ""
    if ptype == "checkbox":
        return "1" if prop.get("checkbox") else "0"
    if ptype == "select":
        sel = prop.get("select") or {}
        return sel.get("name", "")
    if ptype == "multi_select":
        arr = prop.get("multi_select") or []
        return ", ".join(a.get("name", "") for a in arr)
    if ptype == "relation":
        rel = prop.get("relation") or []
        if rel:
            # return first relation id
            return rel[0].get("id", "")
    return ""

def parse_money_from_text(s: Optional[str]) -> float:
    """Extract first number from string; return 0.0 if none."""
    if s is None:
        return 0.0
    try:
        s2 = str(s).replace(",", "")
        m = re.search(r"-?\d+\.?\d*", s2)
        if not m:
            return 0.0
        return float(m.group(0))
    except Exception:
        return 0.0

# ------------- FINDERS & LIST BUILDERS -------------
def find_target_matches(keyword: str, db_id: str = TARGET_NOTION_DATABASE_ID) -> List[Tuple[str, str, Dict[str, Any]]]:
    """Find entries in TARGET DB where title contains keyword (case-insensitive)."""
    if not db_id:
        return []
    kw = normalize_text(keyword)
    pages = query_database_all(db_id, page_size=MAX_QUERY_PAGE_SIZE)
    matches = []
    for p in pages:
        props = p.get("properties", {})
        title = extract_prop_text(props, "Name") or extract_prop_text(props, "Title") or ""
        if kw in normalize_text(title):
            matches.append((p.get("id"), title, props))
    return matches

def find_calendar_matches(keyword: str) -> List[Tuple[str, str, Optional[str], Dict[str, Any]]]:
    """Return unchecked pages in NOTION_DATABASE_ID matching keyword; sorted by date asc."""
    if not NOTION_DATABASE_ID:
        return []
    kw = normalize_text(keyword)
    pages = query_database_all(NOTION_DATABASE_ID, page_size=MAX_QUERY_PAGE_SIZE)
    matches: List[Tuple[str, str, Optional[str], Dict[str, Any]]] = []
    for p in pages:
        props = p.get("properties", {})
        title = extract_prop_text(props, "Name") or extract_prop_text(props, "Title") or ""
        title_clean = normalize_text(title)
        kw_clean = normalize_text(kw)

        # exact match trước
        if title_clean == kw_clean or title_clean.strip() == kw_clean:
            score = 2
        # bắt đầu bằng keyword (ví dụ: "hương vip")
        elif title_clean.startswith(kw_clean + " "):
            score = 1
        # khớp mờ thì cho điểm thấp hơn
        elif kw_clean in title_clean:
            score = 0.5
        else:
            continue
        matches.append((p.get("id"), title, date_iso, props, score))

        # is checked?
        cb_key = find_prop_key(props, "Đã Góp") or find_prop_key(props, "ĐãGóp") or find_prop_key(props, "Sent") or find_prop_key(props, "Status")
        checked = False
        if cb_key and props.get(cb_key, {}).get("type") == "checkbox":
            checked = bool(props.get(cb_key, {}).get("checkbox"))
        if checked:
            continue
        # ✅ Lấy chính xác cột "Ngày Góp"
        date_key = find_prop_key(props, "Ngày Góp")
        date_iso = None
        if date_key:
            date_field = props.get(date_key, {})
            if date_field.get("type") == "date":
                date_iso = date_field["date"].get("start")
        matches.append((p.get("id"), title, date_iso, props))
    matches.sort(key=lambda x: (-x[4], x[2] or ""))
    return matches

def find_matching_all_pages_in_db(database_id: str, keyword: str, limit: int = 2000) -> List[Tuple[str, str, Optional[str]]]:
    """Helper: return all pages in a DB where title contains keyword (both checked/unchecked)."""
    if not database_id:
        return []
    kw = normalize_text(keyword)
    pages = query_database_all(database_id, page_size=MAX_QUERY_PAGE_SIZE)
    out = []
    for p in pages:
        props = p.get("properties", {})
        title = extract_prop_text(props, "Name") or extract_prop_text(props, "Title") or ""
        if kw in normalize_text(title):
            date_key = find_prop_key(props, "Ngày") or find_prop_key(props, "Date")
            date_iso = None
            if date_key and props.get(date_key, {}).get("date"):
                date_iso = props[date_key]["date"].get("start")
            out.append((p.get("id"), title, date_iso))
            if len(out) >= limit:
                break
    return out

# ------------- DAO preview & calculations -------------
def dao_preview_text_from_props(title: str, props: Dict[str, Any]) -> Tuple[bool, str]:
    """
    Prepare preview string for đáo action.
    Returns (can_do, message)
    """
    try:
        total_text = extract_prop_text(props, "Đáo/thối") or extract_prop_text(props, "Đáo") or ""
        total_val = parse_money_from_text(total_text)
        per_day = parse_money_from_text(extract_prop_text(props, "G ngày") or extract_prop_text(props, "Gngày") or "")
        days_before = int(float(extract_prop_text(props, "ngày trước") or "0"))
        pre_amount = parse_money_from_text(extract_prop_text(props, "trước") or "")
        if pre_amount == 0:
            msg = f"🔔 đáo lại cho: {title} - Tổng đáo: ✅ {int(total_val) if total_val else 'N/A'}\n\nKhông Lấy trước"
            return True, msg
        # compute take_days
        if days_before and days_before > 0:
            take_days = days_before
        else:
            take_days = int(math.ceil(pre_amount / per_day)) if per_day else 0
        if take_days <= 0:
            return False, f"⚠️ Không xác định số ngày hợp lệ cho {title}. (per_day={per_day}, pre_amount={pre_amount}, days_before={days_before})"
        lines = [
            f"🔔 đáo lại cho: {title} - Tổng đáo: ✅ {int(total_val) if total_val else 'N/A'}",
            "",
            f"Lấy trước: {take_days} ngày" if take_days else "Không Lấy trước",
            f"G ngày: {int(per_day) if per_day else 0}",
            f"Ngày trước: {days_before}",
            f"Trước: {int(pre_amount) if pre_amount else 0}",
            "",
            "Danh sách ngày dự kiến tạo (bắt đầu từ ngày mai):",
        ]
        start = datetime.now().date() + timedelta(days=1)
        for i in range(take_days):
            lines.append((start + timedelta(days=i)).isoformat())
        lines.append("")
        lines.append(f"Gửi /ok để tạo {take_days} page trong {WAIT_CONFIRM}s, hoặc /cancel.")
        return True, "\n".join(lines)
    except Exception as e:
        return False, f"Preview error: {e}"

# ------------- ACTIONS: mark / undo -------------
def count_checked_unchecked(keyword: str) -> Tuple[int, int]:
    results = query_database_all(NOTION_DATABASE_ID, page_size=MAX_QUERY_PAGE_SIZE)
    checked = 0
    unchecked = 0
    kw = normalize_text(keyword)
    for p in results:
        props = p.get("properties", {})
        title = extract_prop_text(props, "Name") or ""
        if kw in normalize_text(title):
            key = find_prop_key(props, "Đã Góp") or find_prop_key(props, "Sent") or find_prop_key(props, "Status")
            checked_flag = False
            if key and props.get(key, {}).get("type") == "checkbox":
                checked_flag = bool(props.get(key, {}).get("checkbox"))
            if checked_flag:
                checked += 1
            else:
                unchecked += 1
    return checked, unchecked

def mark_pages_by_indices(chat_id: str, keyword: str, matches: List[Tuple[str, str, Optional[str], Dict[str, Any]]], indices: List[int]) -> Dict[str, Any]:
    """
    Mark pages by indices. Business rule:
    - If indices == [n] and n > 1 => expand to select 1..n (oldest first).
    """
    succeeded = []
    failed = []
    if len(indices) == 1 and indices[0] > 1:
        n = indices[0]
        indices = list(range(1, min(n, len(matches)) + 1))
    for idx in indices:
        if idx < 1 or idx > len(matches):
            failed.append((idx, "index out of range"))
            continue
        pid, title, date_iso, props = matches[idx - 1]
        try:
            cb_key = find_prop_key(props, "Đã Góp") or find_prop_key(props, "Sent") or find_prop_key(props, "Status")
            update_props = {}
            if cb_key:
                update_props[cb_key] = {"checkbox": True}
            else:
                update_props["Đã Góp"] = {"checkbox": True}
            ok, res = update_page_properties(pid, update_props)
            if ok:
                succeeded.append((pid, title, date_iso))
                undo_stack.setdefault(str(chat_id), []).append({"action": "mark", "page_id": pid})
            else:
                failed.append((pid, res))
        except Exception as e:
            failed.append((pid, str(e)))
    return {"ok": len(failed) == 0, "succeeded": succeeded, "failed": failed}

def undo_last(chat_id: str, count: int = 1):
    ck = str(chat_id)
    stack = undo_stack.get(ck, [])
    if not stack:
        send_telegram(chat_id, "Không có hành động để undo.")
        return
    reverted = 0
    failed = 0
    for _ in range(min(count, len(stack))):
        rec = stack.pop()
        if rec.get("action") == "mark":
            pid = rec.get("page_id")
            try:
                ok, res = update_page_properties(pid, {"Đã Góp": {"checkbox": False}})
                if ok:
                    reverted += 1
                else:
                    failed += 1
            except Exception:
                failed += 1
    undo_stack[ck] = stack
    send_telegram(chat_id, f"♻️ Undo done. Reverted {reverted} items. Failed: {failed}")
    send_telegram(chat_id, f"🔎 Khách hàng: undone actions for chat {chat_id}")

# ------------- ACTIONS: archive -------------
def handle_command_archive(chat_id: str, keyword: str, auto_confirm_all: bool = True) -> Dict[str, Any]:
    """
    Archive all pages in NOTION_DATABASE_ID matching keyword.
    If auto_confirm_all True -> do it immediately (used by dao).
    If called interactively, use the handler in handle_incoming_message to present options.
    """
    try:
        matches = find_matching_all_pages_in_db(NOTION_DATABASE_ID, keyword, limit=5000)
        total = len(matches)
        send_telegram(chat_id, f"🧹 Đang xóa {total} ngày của {keyword} (check + uncheck)...")
        if total == 0:
            send_telegram(chat_id, f"✅ Không tìm thấy mục cần xóa cho '{keyword}'.")
            return {"ok": True, "deleted": [], "failed": []}
        deleted = []
        failed = []
        for i, (pid, title, date_iso) in enumerate(matches, start=1):
            send_progress(chat_id, i, total, f"🗑️ Đang xóa {keyword}")
            ok, msg = archive_page(pid)
            if ok:
                deleted.append(pid)
            else:
                failed.append((pid, msg))
            time.sleep(PATCH_DELAY)
        send_telegram(chat_id, f"✅ Đã xóa xong {len(deleted)}/{total} mục của {keyword}.")
        if failed:
            send_telegram(chat_id, f"⚠️ Có {len(failed)} mục xóa lỗi, xem logs.")
        return {"ok": True, "deleted": deleted, "failed": failed}
    except Exception as e:
        traceback.print_exc()
        send_telegram(chat_id, f"❌ Lỗi archive: {e}")
        return {"ok": False, "error": str(e)}

# ------------- ACTIONS: create lai page -------------
def create_lai_page(chat_id: int, title: str, lai_amount: float, relation_id: str):
    """
    Tạo 1 page Lãi trong LA_NOTION_DATABASE_ID với:
     - Name = title
     - Lãi = lấy số tiền từ cột "Lai lịch g" bên TARGET_NOTION_DATABASE_ID
     - Ngày Góp = ngày hôm nay
     - Lịch G = relation trỏ về page gốc
    """
    try:
        today = datetime.now().date().isoformat()

        props_payload = {
            "Name": {"title": [{"type": "text", "text": {"content": title}}]},
            "Lãi": {"number": lai_amount},
            "Ngày Góp": {"date": {"start": today}},
            "Lịch G": {"relation": [{"id": relation_id}]}
        }

        url = "https://api.notion.com/v1/pages"
        body = {"parent": {"database_id": LA_NOTION_DATABASE_ID}, "properties": props_payload}
        r = requests.post(url, headers=NOTION_HEADERS, json=body, timeout=15)

        if r.status_code in (200, 201):
            send_telegram(chat_id, f"💰 Đã tạo Lãi cho {title}: {lai_amount:,.0f}")
        else:
            send_telegram(chat_id, f"⚠️ Tạo Lãi lỗi: {r.status_code} - {r.text}")

    except Exception as e:
        send_telegram(chat_id, f"❌ Lỗi tạo Lãi cho {title}: {str(e)}")


# ------------- DAO flow (xóa + tạo pages + create lai) -------------
def dao_create_pages_from_props(chat_id: int, source_page_id: str, props: Dict[str, Any]):
    """
    Xử lý đáo:
     - archive toàn bộ page của 'key' trong NOTION_DATABASE_ID (checked + unchecked)
     - tạo `take_days` page mới bắt đầu từ ngày mai, mỗi page có Đã Góp = True
     - tạo 1 page Lãi trong LA_NOTION_DATABASE_ID (nếu có giá trị Lãi)
     - báo tiến trình chi tiết qua Telegram
    """
    try:
        title = extract_prop_text(props, "Name") or "UNKNOWN"
        total_text = extract_prop_text(props, "Đáo/thối")
        total_val = parse_money_from_text(total_text) or 0

        # đọc các trường cần thiết từ DB đáo
        per_day = parse_money_from_text(extract_prop_text(props, "G ngày")) or 0
        days_before = parse_money_from_text(extract_prop_text(props, "ngày trước")) or 0
        pre_amount = parse_money_from_text(extract_prop_text(props, "trước")) or 0

        # kiểm tra điều kiện
        if pre_amount == 0:
            send_telegram(chat_id, f"🔔 đáo lại cho: {title} - Tổng đáo: ✅ {int(total_val)}\n\nKhông Lấy trước")
            return

        take_days = int(days_before) if days_before else int(math.ceil(pre_amount / per_day)) if per_day else 0
        if take_days <= 0:
            send_telegram(chat_id, f"⚠️ Không xác định được số ngày hợp lệ cho {title} (per_day={per_day}, pre_amount={pre_amount})")
            return

        # --- 1️⃣ XÓA PAGE CŨ ---
        all_pages = query_database_all(NOTION_DATABASE_ID, page_size=500)
        kw = title.strip().lower()
        matched = []
        for p in all_pages:
            props_p = p.get("properties", {})
            name_p = extract_prop_text(props_p, "Name") or ""
            if kw in name_p.lower():
                matched.append(p.get("id"))
        total_to_delete = len(matched)
        send_telegram(chat_id, f"🧹 Đang xóa {total_to_delete} ngày của {title} (check + uncheck)...")

        deleted = 0
        for i, pid in enumerate(matched, start=1):
            try:
                archive_page(pid)
                deleted += 1
            except Exception as e:
                send_telegram(chat_id, f"⚠️ Lỗi xóa {pid}: {str(e)}")
            time.sleep(PATCH_DELAY)
        send_telegram(chat_id, f"✅ Đã xóa xong {deleted}/{total_to_delete} mục của {title}.")

        # --- 2️⃣ TẠO PAGE MỚI ---
        start = datetime.now().date() + timedelta(days=1)
        created = []
        send_telegram(chat_id, f"🛠️ Đang tạo {take_days} ngày mới cho {title} (bắt đầu từ ngày mai)...")

        for i in range(1, take_days + 1):
            d = start + timedelta(days=i - 1)
            props_payload = {
                "Name": {"title": [{"type": "text", "text": {"content": title}}]},
                "Ngày Góp": {"date": {"start": d.isoformat()}},
                "Tiền": {"number": per_day},
                "Đã Góp": {"checkbox": True},
                "Lịch G": {"relation": [{"id": source_page_id}]},
            }

            try:
                url = "https://api.notion.com/v1/pages"
                body = {"parent": {"database_id": NOTION_DATABASE_ID}, "properties": props_payload}
                r = requests.post(url, headers=NOTION_HEADERS, json=body, timeout=20)
                if r.status_code in (200, 201):
                    created.append(r.json())
                    send_progress(chat_id, i, take_days, f"📅 Tạo ngày {d} cho {title}")
                else:
                    send_telegram(chat_id, f"⚠️ Tạo lỗi {r.status_code}: {r.text}")
            except Exception as e:
                send_telegram(chat_id, f"⚠️ Lỗi tạo ngày {i}: {str(e)}")
            time.sleep(PATCH_DELAY)

        send_telegram(chat_id, f"✅ Đã tạo {len(created)} ngày mới cho {title} (đã check 'Đã Góp').")

                # --- 3️⃣ TẠO LÃI (nếu có) ---
        lai_text = extract_prop_text(props, "Lai lịch g") or extract_prop_text(props, "Lãi") or extract_prop_text(props, "Lai") or ""
        lai_amt = parse_money_from_text(lai_text) or 0
        if LA_NOTION_DATABASE_ID and lai_amt > 0:
            send_telegram(chat_id, f"💸 Tiếp tục tạo Lãi cho {title}...")
            relation_target_id = created[0].get("id", source_page_id) if created else source_page_id
            create_lai_page(chat_id, title, lai_amt, relation_target_id)
        else:
            send_telegram(chat_id, f"ℹ️ Không có giá trị Lãi hoặc chưa cấu hình LA_NOTION_DATABASE_ID. Bỏ qua tạo Lãi.")
    
    except Exception as e:
        send_telegram(chat_id, f"❌ Lỗi tiến trình đáo cho {title}: {str(e)}")
        traceback.print_exc()
        return

# ------------- PENDING / SELECTION PROCESSING -------------
def parse_user_selection_text(sel_text: str, found_len: int) -> List[int]:
    """Parse selection input like '1', '1,2', '1-3', 'all', or '3' (meaning 1..3)."""
    s = sel_text.strip().lower()
    if s in ("all", "tất cả", "tat ca"):
        return list(range(1, found_len + 1))
    parts = s.split(",")
    selected = []
    for p in parts:
        p = p.strip()
        if "-" in p:
            try:
                a, b = p.split("-", 1)
                a_i = int(a); b_i = int(b)
                for i in range(min(a_i, b_i), max(a_i, b_i) + 1):
                    selected.append(i)
            except:
                pass
        else:
            try:
                n = int(p)
                if n > 1 and found_len >= n:
                    selected.extend(list(range(1, n + 1)))
                else:
                    selected.append(n)
            except:
                pass
    selected = sorted(list(dict.fromkeys([i for i in selected if isinstance(i, int)])))
    return selected

def process_pending_selection_for_dao(chat_id: str, raw: str):
    key = str(chat_id)
    data = pending_confirm.get(key)
    if not data:
        send_telegram(chat_id, "Không có thao tác đang chờ.")
        return
    try:
        if data.get("type") == "dao_choose":
            matches = data.get("matches", [])
            indices = parse_user_selection_text(raw, len(matches))
            if not indices:
                send_telegram(chat_id, "Không nhận được lựa chọn hợp lệ.")
                return
            chosen = []
            for idx in indices:
                if 1 <= idx <= len(matches):
                    pid, title, props = matches[idx - 1]
                    chosen.append((pid, title, props))
            for pid, title, props in chosen:
                send_telegram(chat_id, f"✅ Đang thực hiện đáo cho {title} ...")
                dao_create_pages_from_props(chat_id, pid, props)
            del pending_confirm[key]
            return
        if data.get("type") == "dao_confirm":
            if raw.strip().lower() in ("/cancel", "cancel", "hủy", "huy"):
                del pending_confirm[key]
                send_telegram(chat_id, "Đã hủy thao tác đáo.")
                return
            if raw.strip().lower() in ("ok", "/ok", "yes", "đồng ý", "dong y"):
                source_page_id = data.get("source_page_id")
                props = data.get("props")
                dao_create_pages_from_props(chat_id, source_page_id, props)
                del pending_confirm[key]
                return
            send_telegram(chat_id, "Gửi /ok để thực hiện hoặc /cancel để hủy.")
            return
    except Exception as e:
        traceback.print_exc()
        send_telegram(chat_id, f"❌ Lỗi xử lý lựa chọn: {e}")
        if key in pending_confirm:
            del pending_confirm[key]

def process_pending_selection(chat_id: str, raw: str):
    key = str(chat_id)
    data = pending_confirm.get(key)
    if not data:
        send_telegram(chat_id, "Không có thao tác đang chờ.")
        return
    try:
        if raw.strip().lower() in ("/cancel", "cancel", "hủy", "huy"):
            del pending_confirm[key]
            send_telegram(chat_id, "Đã hủy thao tác đang chờ.")
            return
        matches = data.get("matches", [])
        indices = parse_user_selection_text(raw, len(matches))
        if not indices:
            send_telegram(chat_id, "Không nhận được lựa chọn hợp lệ.")
            return
        action = data.get("type")
        if action == "mark":
            keyword = data.get("keyword")
            res = mark_pages_by_indices(chat_id, keyword, matches, indices)
            if res.get("succeeded"):
                txt = "✅ Đã đánh dấu:\n"
                for pid, title, date_iso in res["succeeded"]:
                    ds = date_iso[:10] if date_iso else "-"
                    txt += f"{ds} — {title}\n"
                send_long_text(chat_id, txt)
            if res.get("failed"):
                send_telegram(chat_id, f"⚠️ Lỗi khi đánh dấu: {res['failed']}")
            checked, unchecked = count_checked_unchecked(keyword)
            send_telegram(chat_id, f"✅ Đã tích: {checked}\n\n🟡 Chưa tích: {unchecked}")
            del pending_confirm[key]
            return
        if action == "archive_select":
            for idx in indices:
                if 1 <= idx <= len(matches):
                    pid, title, date_iso = matches[idx - 1]
                    handle_command_archive(chat_id, title)
            del pending_confirm[key]
            return
    except Exception as e:
        traceback.print_exc()
        send_telegram(chat_id, f"❌ Lỗi xử lý lựa chọn: {e}")
        if key in pending_confirm:
            del pending_confirm[key]

# ------------- Command parsing & main handler -------------
def parse_user_command(raw: str) -> Tuple[str, int, str]:
    txt = raw.strip()
    low = txt.lower()
    parts = txt.split()
    if not parts:
        return "", 0, "unknown"
    if low in ("undo",):
        return "", 0, "undo"
    if low.endswith(" đáo") or low.endswith(" dao"):
        kw = txt.rsplit(None, 1)[0]
        return kw, 0, "dao"
    if low.endswith(" xóa") or low.endswith(" xoa"):
        kw = txt.rsplit(None, 1)[0]
        return kw, 0, "archive"
    keyword = parts[0]
    action = "mark"
    count = 0
    if len(parts) >= 2:
        sec = parts[1]
        if sec.isdigit():
            count = int(sec)
    return keyword, count, action

def handle_incoming_message(chat_id: int, text: str):
    """
    Main entry point for Telegram messages.
    """
    try:
        # optional restrict by chat id
        if TELEGRAM_CHAT_ID and str(chat_id) != str(TELEGRAM_CHAT_ID):
            send_telegram(chat_id, "Bot chưa được phép nhận lệnh từ chat này.")
            return
        raw = text.strip()
        if not raw:
            send_telegram(chat_id, "Vui lòng gửi lệnh hoặc từ khoá.")
            return
        low = raw.lower()

        # if pending confirm exists -> route selection handling
        if str(chat_id) in pending_confirm:
            if low in ("/cancel", "cancel", "hủy", "huy"):
                del pending_confirm[str(chat_id)]
                send_telegram(chat_id, "Đã hủy thao tác đang chờ.")
                return
            pc = pending_confirm[str(chat_id)]
            if pc.get("type") in ("dao_choose", "dao_confirm"):
                threading.Thread(target=process_pending_selection_for_dao, args=(chat_id, raw), daemon=True).start()
                return
            threading.Thread(target=process_pending_selection, args=(chat_id, raw), daemon=True).start()
            return

        if low in ("/cancel", "cancel", "hủy", "huy"):
            send_telegram(chat_id, "Không có thao tác đang chờ. /cancel ignored.")
            return

        keyword, count, action = parse_user_command(raw)

        if action == "undo":
            send_telegram(chat_id, "Đang tìm và undo...")
            threading.Thread(target=undo_last, args=(chat_id, 1), daemon=True).start()
            return

        if action == "archive":
            kw = keyword
            # interactive archive: list all matched pages and ask selection or 'all'
            matches = find_matching_all_pages_in_db(NOTION_DATABASE_ID, kw, limit=5000)
            checked, unchecked = count_checked_unchecked(kw)
            header = f"🔎 : '{kw}'\n\n✅ Đã tích: {checked}\n\n🟡 Chưa tích: {unchecked}\n\n"
            header += f"⚠️ CHÚ Ý: Bạn sắp archive {len(matches)} mục chứa '{kw}'.\n\nGửi số (ví dụ 1-7) trong {WAIT_CONFIRM}s để chọn, hoặc 'all' để archive tất cả, hoặc /cancel.\n\n"
            lines = []
            for i, (pid, title, date_iso) in enumerate(matches, start=1):
                ds = date_iso[:10] if date_iso else "-"
                lines.append(f"{i}. [{ds}] {title}")
            send_long_text(chat_id, header + "\n".join(lines))
            pending_confirm[str(chat_id)] = {"type": "archive_select", "keyword": kw, "matches": matches, "expires": time.time() + WAIT_CONFIRM}
            return

        if action == "dao":
            kw = keyword
            matches = find_target_matches(kw)
            if not matches:
                send_telegram(chat_id, f"⚠️ Không tìm thấy '{kw}' trong DB đáo.")
                return
            if len(matches) > 1:
                header = f"Tìm thấy {len(matches)} kết quả cho '{kw}'. Chọn index để tiếp tục hoặc gửi SĐT để match chính xác."
                lines = []
                for i, (pid, title, props) in enumerate(matches, start=1):
                    dt = extract_prop_text(props, "Đáo/thối") or "-"
                    gday = extract_prop_text(props, "G ngày") or "-"
                    nb = extract_prop_text(props, "ngày trước") or extract_prop_text(props, "# ngày trước") or "-"
                    prev = extract_prop_text(props, "trước") or "-"
                    lines.append(f"{i}. {title} — Đáo/thối: {dt} — G ngày: {gday} — # ngày trước: {nb} — trước: {prev}")
                send_long_text(chat_id, header + "\n\n" + "\n".join(lines))
                pending_confirm[str(chat_id)] = {"type": "dao_choose", "matches": matches, "expires": time.time() + WAIT_CONFIRM}
                send_telegram(chat_id, f"📤 Gửi số (ví dụ 1 hoặc 1-3) trong {WAIT_CONFIRM}s để chọn, hoặc /cancel.")
                return
            # single match -> preview
            pid, title, props = matches[0]
            can, preview = dao_preview_text_from_props(title, props)
            send_long_text(chat_id, preview)
            if can:
                pending_confirm[str(chat_id)] = {"type": "dao_confirm", "source_page_id": pid, "props": props, "expires": time.time() + WAIT_CONFIRM}
                send_telegram(chat_id, f"✅ Có thể đáo cho '{title}'. Gõ /ok để thực hiện trong {WAIT_CONFIRM}s hoặc /cancel để hủy.")
            else:
                send_telegram(chat_id, f"⚠️ Không thể thực hiện đáo cho '{title}'. Vui lòng kiểm tra dữ liệu.")
            return

        # default: mark flow
        kw = keyword
        matches = find_calendar_matches(kw)
        checked, unchecked = count_checked_unchecked(kw)
        header = f"🔎 : '{kw}'\n\n✅ Đã tích: {checked}\n\n🟡 Chưa tích: {unchecked}\n\n"
        header += f"📤 Gửi số ( ví dụ 1 hoặc 1-3 ) trong {WAIT_CONFIRM}s để chọn, hoặc /cancel.\n\n"
        if not matches:
            send_telegram(chat_id, f"Không tìm thấy mục nào chưa tích cho '{kw}'.")
            return
        lines = []
        for i, (pid, title, date_iso, props) in enumerate(matches, start=1):
            ds = date_iso[:10] if date_iso else "-"
            lines.append(f"{i}. [{ds}] {title}")
        send_long_text(chat_id, header + "\n".join(lines))
        pending_confirm[str(chat_id)] = {"type": "mark", "keyword": kw, "matches": matches, "expires": time.time() + WAIT_CONFIRM}
    except Exception as e:
        traceback.print_exc()
        # ⚡ AUTO-MARK MODE: nếu user gõ "gam 2" thì tự động tích 2 ngày đầu tiên
        if count > 0 and matches:
            send_telegram(chat_id, f"⚡ Đang tự động đánh dấu {count} mục cho '{kw}'...")
            # chọn n mục đầu tiên
            indices = list(range(1, min(count, len(matches)) + 1))
            res = mark_pages_by_indices(chat_id, kw, matches, indices)
            if res.get("succeeded"):
                txt = "✅ Đã đánh dấu:\n"
                for pid, title, date_iso in res["succeeded"]:
                    ds = date_iso[:10] if date_iso else "-"
                    txt += f"{ds} — {title}\n"
                send_long_text(chat_id, txt)
            if res.get("failed"):
                send_telegram(chat_id, f"⚠️ Có {len(res['failed'])} mục đánh dấu lỗi.")
            checked, unchecked = count_checked_unchecked(kw)
            send_telegram(chat_id, f"✅ Đã tích: {checked}\n🟡 Chưa tích: {unchecked}")
            return
        send_telegram(chat_id, f"Lỗi xử lý: {e}")

# ------------- BACKGROUND: sweep expired pending -------------
def sweep_pending_expirations():
    while True:
        try:
            now = time.time()
            keys = list(pending_confirm.keys())
            for k in keys:
                item = pending_confirm.get(k)
                if item and item.get("expires") and item.get("expires") < now:
                    try:
                        send_telegram(k, "⏳ Thao tác chờ đã hết hạn.")
                    except:
                        pass
                    del pending_confirm[k]
        except Exception:
            pass
        time.sleep(5)

threading.Thread(target=sweep_pending_expirations, daemon=True).start()

# ------------- FLASK APP / WEBHOOK -------------
# ------------- FLASK APP / WEBHOOK -------------
app = Flask(__name__)

# ✅ Route kiểm tra app đang chạy
@app.route("/", methods=["GET"])
def index():
    return "app_final_v4 running ✅"

# ✅ Route chính cho Telegram webhook (và dự phòng)
@app.route("/telegram_webhook", methods=["POST"])
@app.route("/webhook", methods=["POST"])
def telegram_webhook():
    try:
        data = request.get_json(force=True)
    except Exception:
        return jsonify({"ok": False, "error": "invalid json"}), 400

    if not data:
        return jsonify({"ok": False, "error": "no data"}), 400

    message = data.get("message") or data.get("edited_message") or {}
    if not message:
        return jsonify({"ok": True})

    chat = message.get("chat", {})
    chat_id = chat.get("id")
    text = message.get("text") or message.get("caption") or ""

    if chat_id and text:
        threading.Thread(
            target=handle_incoming_message,
            args=(chat_id, text),
            daemon=True
        ).start()

    # ✅ Quan trọng: trả lại JSON để Telegram biết bot đã nhận
    return jsonify({"ok": True})



# ------------- RUN (local test) -------------
if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    print("Launching app.py on port", port)
    print("NOTION_DATABASE_ID:", NOTION_DATABASE_ID[:8] + "..." if NOTION_DATABASE_ID else "(none)")
    print("TARGET_NOTION_DATABASE_ID:", TARGET_NOTION_DATABASE_ID[:8] + "..." if TARGET_NOTION_DATABASE_ID else "(none)")
    print("LA_NOTION_DATABASE_ID:", LA_NOTION_DATABASE_ID[:8] + "..." if LA_NOTION_DATABASE_ID else "(none)")
    print("TELEGRAM_TOKEN set?:", bool(TELEGRAM_TOKEN))
    app.run(host="0.0.0.0", port=port)
