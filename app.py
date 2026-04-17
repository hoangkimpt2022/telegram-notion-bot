# app.py — Consolidated (app.py + switch_app.py)
# Production-ready Telegram <-> Notion automation
# All-in-one: mark, archive, dao, switch ON/OFF, undo
#
# BUGS FIXED:
# 1. stop_waiting_animation — dùng _animation_stop dict riêng, thread animation check cờ này
# 2. dao_create_pages_from_props KHÔNG LẤY TRƯỚC — sửa biến `matched` → `children`, `lai_page_id` safe
# 3. Undo log trong process_pending_selection_for_dao CASE 1 — children là list string, không phải dict
# 4. dao_create_pages_from_props gọi đúng signature (bỏ tham số `title` thừa)
# 5. Thêm debug log cho find_target_matches + query_database_all
# 6. Bỏ duplicate import re
# 7. Animation dừng đúng khi confirm/cancel
# 8. Tách hàm _match_keyword_to_title chung → tránh duplicate logic match
# 9. Tách hàm find_children_by_relation → tìm ngày theo relation thay vì tên

import os
os.environ["PYTHONUNBUFFERED"] = "1"  # Fix: print() hiện ngay trên Render logs
import sys
sys.stdout.reconfigure(line_buffering=True)
import re
import math
import json
import time
import traceback
import threading
import requests
import unicodedata
from datetime import datetime, timedelta, timezone
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
TONG_THU_DONG_G_PAGE_ID = os.getenv("TONG_THU_DONG_G_PAGE_ID", "")  # Page ID của "G" trong relation Tổng Thụ Động

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

WAIT_CONFIRM = int(os.getenv("WAIT_CONFIRM", "120"))
PATCH_DELAY = float(os.getenv("PATCH_DELAY", "0.3"))
MAX_QUERY_PAGE_SIZE = int(os.getenv("MAX_QUERY_PAGE_SIZE", "100"))

VN_TZ = timezone(timedelta(hours=7))

# ------------- IN-MEM STATE -------------
pending_confirm: Dict[str, Dict[str, Any]] = {}
undo_stack: Dict[str, List[Dict[str, Any]]] = {}
_animation_stop: Dict[str, bool] = {}  # FIX #1: cờ dừng animation riêng


# =====================================================================
#  TELEGRAM HELPERS
# =====================================================================
def send_telegram(chat_id, text, parse_mode=None):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": chat_id, "text": text}
    if parse_mode:
        payload["parse_mode"] = parse_mode
    try:
        r = requests.post(url, json=payload, timeout=10)
        data = r.json()
        if not data.get("ok"):
            print("send_telegram failed:", data)
            return {}
        return data
    except Exception as e:
        print("send_telegram exception:", e)
        return {}


def edit_telegram_message(chat_id, message_id, new_text, parse_mode=None):
    if not message_id:
        return {}
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/editMessageText"
    payload = {"chat_id": chat_id, "message_id": message_id, "text": new_text}
    if parse_mode:
        payload["parse_mode"] = parse_mode
    try:
        r = requests.post(url, json=payload, timeout=10)
        data = r.json()
        if not data.get("ok"):
            print("edit_telegram_message failed:", data)
            return {}
        return data
    except Exception as e:
        print("edit_telegram_message exception:", e)
        return {}


def start_waiting_animation(chat_id: int, message_id: int, duration: int = 120,
                            interval: float = 2.0, label: str = "đang chờ"):
    """FIX #1: animation loop kiểm tra _animation_stop để dừng đúng lúc."""
    key = str(chat_id)
    _animation_stop[key] = False

    def animate():
        start_time = time.time()
        emojis = ["🔄", "💫", "✨", "🌙", "🕒", "⏳"]
        idx = 0
        while time.time() - start_time < duration:
            if _animation_stop.get(key, False):
                return
            try:
                text = f"{emojis[idx % len(emojis)]} Đang chờ {label}... ({int(time.time() - start_time)}s/{duration}s)"
                edit_telegram_message(chat_id, message_id, text)
                time.sleep(interval)
                idx += 1
            except Exception as e:
                print("⚠️ animation error:", e)
                break
        if not _animation_stop.get(key, False):
            try:
                edit_telegram_message(chat_id, message_id, "⏳ Thao tác chờ đã hết hạn.")
            except Exception as e:
                print("⚠️ lỗi khi gửi thông báo hết hạn:", e)

    threading.Thread(target=animate, daemon=True).start()


def stop_waiting_animation(chat_id):
    """FIX #1: đặt cờ dừng → animation thread thoát ngay."""
    key = str(chat_id)
    _animation_stop[key] = True
    if key in pending_confirm:
        pending_confirm[key]["expires"] = 0


def send_long_text(chat_id: str, text: str):
    max_len = 3000
    for i in range(0, len(text), max_len):
        send_telegram(chat_id, text[i:i + max_len])


def send_progress(chat_id: str, step: int, total: int, label: str):
    try:
        if total == 0:
            return
        if step == 1 or step % 10 == 0 or step == total:
            send_telegram(chat_id, f"⏱️ {label}: {step}/{total} ...")
    except Exception as e:
        print("send_progress error:", e)


# =====================================================================
#  NOTION API WRAPPERS
# =====================================================================
def _notion_post(url: str, json_body: dict, attempts: int = 3, timeout: int = 15):
    for i in range(attempts):
        try:
            r = requests.post(url, headers=NOTION_HEADERS, json=json_body, timeout=timeout)
            if r.status_code in (200, 201):
                return True, r.json()
            if r.status_code >= 500:
                time.sleep(1 + i)
                continue
            return False, {"status": r.status_code, "text": r.text}
        except Exception as e:
            last_exc = e
            time.sleep(1 + i)
    return False, str(last_exc)


def _notion_patch(url: str, json_body: dict, attempts: int = 3, timeout: int = 12):
    for i in range(attempts):
        try:
            r = requests.patch(url, headers=NOTION_HEADERS, json=json_body, timeout=timeout)
            if r.status_code in (200, 204):
                try:
                    return True, r.json() if r.text else {}
                except Exception:
                    return True, {}
            if r.status_code >= 500:
                time.sleep(1 + i)
                continue
            return False, {"status": r.status_code, "text": r.text}
        except Exception as e:
            last_exc = e
            time.sleep(1 + i)
    return False, str(last_exc)


def query_database_all(database_id: str, page_size: int = MAX_QUERY_PAGE_SIZE, _retries: int = 1) -> List[Dict[str, Any]]:
    """Query all pages with retry + increased timeout."""
    if not NOTION_TOKEN:
        print("[query_database_all] SKIP — NOTION_TOKEN is EMPTY")
        return []
    if not database_id:
        print("[query_database_all] SKIP — database_id is EMPTY")
        return []

    db_short = database_id[:16]

    for attempt in range(1, _retries + 1):
        results: List[Dict[str, Any]] = []
        try:
            url = f"https://api.notion.com/v1/databases/{database_id}/query"
            payload: dict = {"page_size": page_size}
            r = requests.post(url, headers=NOTION_HEADERS, json=payload, timeout=30)
            if r.status_code != 200:
                print(f"[query_database_all] FAILED db={db_short}... status={r.status_code} attempt={attempt}")
                if r.status_code >= 500 and attempt < _retries:
                    time.sleep(2 * attempt)
                    continue
                return []
            data = r.json()
            results.extend(data.get("results", []))
            while data.get("has_more"):
                payload["start_cursor"] = data.get("next_cursor")
                r = requests.post(url, headers=NOTION_HEADERS, json=payload, timeout=30)
                if r.status_code != 200:
                    print(f"[query_database_all] pagination FAILED status={r.status_code}")
                    break
                data = r.json()
                results.extend(data.get("results", []))
            print(f"[query_database_all] OK db={db_short}... total_pages={len(results)}")
            return results
        except Exception as e:
            print(f"[query_database_all] EXCEPTION attempt={attempt}/{_retries} db={db_short}... {e}")
            if attempt < _retries:
                time.sleep(2 * attempt)
                continue
            return []
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
    return _notion_patch(url, {"archived": True})


def unarchive_page(page_id: str) -> Tuple[bool, str]:
    if not NOTION_TOKEN or not page_id:
        return False, "Notion config missing"
    url = f"https://api.notion.com/v1/pages/{page_id}"
    return _notion_patch(url, {"archived": False})


def update_page_properties(page_id: str, properties: Dict[str, Any]) -> Tuple[bool, Any]:
    if not NOTION_TOKEN or not page_id:
        return False, "Notion config missing"
    url = f"https://api.notion.com/v1/pages/{page_id}"
    return _notion_patch(url, {"properties": properties})


def update_checkbox(page_id: str, checked: bool) -> Tuple[bool, Any]:
    if not NOTION_TOKEN or not page_id:
        return False, "Notion config missing"
    return update_page_properties(page_id, {"Đã Góp": {"checkbox": checked}})


# =====================================================================
#  PROPERTY EXTRACTION & PARSING
# =====================================================================
def normalize_text(s: Optional[str]) -> str:
    if not s:
        return ""
    s = str(s).strip().lower()
    nf = unicodedata.normalize("NFD", s)
    return "".join(c for c in nf if unicodedata.category(c) != "Mn")


def tokenize_title(title: str) -> List[str]:
    if not title:
        return []
    t = normalize_text(title)
    tokens = re.split(r'[^a-z0-9]+', t)
    return [x for x in tokens if x]


def normalize_gcode(token: str) -> str:
    if not token:
        return token
    m = re.match(r'^(g)0*([0-9]+)$', token)
    if m:
        return f"g{int(m.group(2))}"
    return token


def extract_plain_text_from_rich_text(arr: List[Dict[str, Any]]) -> str:
    if not arr:
        return ""
    return "".join([x.get("plain_text", "") for x in arr if isinstance(x, dict)])


def find_prop_key(props: Dict[str, Any], name_like: str) -> Optional[str]:
    if not props:
        return None
    nl = normalize_text(name_like)
    for k in props.keys():
        if normalize_text(k) == nl:
            return k
    for k in props.keys():
        if nl in normalize_text(k):
            return k
    return None


def extract_prop_text(props: Dict[str, Any], key_like: str) -> str:
    if not props:
        return ""
    k = find_prop_key(props, key_like)
    if not k:
        return ""
    prop = props.get(k, {}) or {}
    ptype = prop.get("type")

    if ptype == "formula":
        formula = prop.get("formula", {})
        ftype = formula.get("type")
        if ftype == "number" and formula.get("number") is not None:
            return str(formula["number"])
        if ftype == "string" and formula.get("string"):
            return str(formula["string"])
        if ftype == "boolean" and formula.get("boolean") is not None:
            return "1" if formula["boolean"] else "0"
        if ftype == "date" and formula.get("date"):
            return formula["date"].get("start", "")
        return ""

    if ptype == "rollup":
        roll = prop.get("rollup", {})
        rtype = roll.get("type")
        if rtype == "number" and roll.get("number") is not None:
            return str(roll["number"])
        if rtype == "array":
            arr = roll.get("array", [])
            if arr:
                first = arr[0]
                if isinstance(first, dict):
                    if "number" in first and first.get("number") is not None:
                        return str(first["number"])
                    if "title" in first:
                        return extract_plain_text_from_rich_text(first.get("title", []))
                    if "plain_text" in first:
                        return first.get("plain_text", "")
                return str(first)
        return ""

    if ptype == "title":
        return extract_plain_text_from_rich_text(prop.get("title", []))
    if ptype == "rich_text":
        return extract_plain_text_from_rich_text(prop.get("rich_text", []))
    if ptype == "number":
        val = prop.get("number")
        return str(val) if val is not None else ""
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
            return rel[0].get("id", "")
    return ""


def parse_money_from_text(s: Optional[str]) -> float:
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


def _num(props, key_like):
    return parse_money_from_text(extract_prop_text(props, key_like)) or 0


# =====================================================================
#  MATCHING HELPERS  (FIX #8: logic match tập trung 1 chỗ)
# =====================================================================
def _match_keyword_to_title(kw: str, title: str) -> bool:
    """
    Logic match chung: so sánh keyword (đã normalize) với title.
    """
    title_clean = normalize_text(title)
    tokens = tokenize_title(title)

    is_gcode = bool(re.match(r'^g[0-9]+$', kw))
    kw_g = normalize_gcode(kw) if is_gcode else None

    if title_clean == kw:
        return True
    if is_gcode:
        for tk in tokens:
            if normalize_gcode(tk) == kw_g:
                return True
    if not is_gcode:
        for tk in tokens:
            if kw in tk:
                return True
    if title_clean.startswith(kw + "-"):
        return True
    return False


def find_target_matches(keyword: str, db_id: str = None, _pages: list = None):
    """
    Tìm khách trong TARGET DB.
    page_size=10 vì TARGET DB có nhiều formula/rollup → page_size lớn sẽ timeout.
    _pages: truyền sẵn data đã query (tránh query lại).
    """
    if db_id is None:
        db_id = TARGET_NOTION_DATABASE_ID

    if not db_id:
        print("[find_target_matches] TARGET_NOTION_DATABASE_ID is EMPTY — return []")
        return []

    kw = normalize_text(keyword).strip()
    if not kw:
        print("[find_target_matches] keyword empty after normalize")
        return []

    if _pages is not None:
        pages = _pages
    else:
        pages = query_database_all(db_id, page_size=10)
    print(f"[find_target_matches] keyword='{kw}' pages_from_db={len(pages)}")

    out = []
    for p in pages:
        props = p.get("properties", {})
        title = extract_prop_text(props, "Name") or extract_prop_text(props, "Title") or ""
        if not title:
            continue
        if _match_keyword_to_title(kw, title):
            out.append((p.get("id"), title, props))

    print(f"[find_target_matches] matched={len(out)} for kw='{kw}'")
    return out


def find_calendar_data(keyword: str):
    """
    Query CALENDAR DB 1 LẦN DUY NHẤT, trả về:
      (unchecked_matches, checked_count, unchecked_count)
    - unchecked_matches: list[(pid, title, date_iso, props)] — chưa tích, sorted by date
    - checked_count: số page đã tích
    - unchecked_count: số page chưa tích
    """
    if not NOTION_DATABASE_ID:
        return [], 0, 0

    kw = normalize_text(keyword)
    pages = query_database_all(NOTION_DATABASE_ID, page_size=MAX_QUERY_PAGE_SIZE)
    unchecked_matches = []
    checked_count = 0
    unchecked_count = 0

    for p in pages:
        props = p.get("properties", {})
        title = extract_prop_text(props, "Name") or extract_prop_text(props, "Title") or ""
        if not title:
            continue

        if not _match_keyword_to_title(kw, title):
            continue

        cb_key = (
            find_prop_key(props, "Đã Góp")
            or find_prop_key(props, "Sent")
            or find_prop_key(props, "Status")
        )
        is_checked = bool(cb_key and props.get(cb_key, {}).get("checkbox"))

        if is_checked:
            checked_count += 1
        else:
            unchecked_count += 1
            date_iso = None
            date_key = find_prop_key(props, "Ngày Góp")
            if date_key:
                df = props.get(date_key, {}).get("date")
                if df:
                    date_iso = df.get("start")
            unchecked_matches.append((p.get("id"), title, date_iso, props))

    unchecked_matches.sort(key=lambda x: (x[2] is None, x[2] or ""))
    return unchecked_matches, checked_count, unchecked_count


# Backward compat wrappers (cho code cũ gọi)
def find_calendar_matches(keyword: str):
    matches, _, _ = find_calendar_data(keyword)
    return matches

def count_checked_unchecked(keyword: str) -> Tuple[int, int]:
    _, checked, unchecked = find_calendar_data(keyword)
    return checked, unchecked


def find_matching_all_pages_in_db(database_id: str, keyword: str, limit: int = 2000):
    if not database_id:
        return []

    kw = normalize_text(keyword)
    pages = query_database_all(database_id, page_size=MAX_QUERY_PAGE_SIZE)
    out = []

    for p in pages:
        props = p.get("properties", {})
        title = extract_prop_text(props, "Name") or extract_prop_text(props, "Title") or ""
        if not title:
            continue

        if not _match_keyword_to_title(kw, title):
            continue

        date_iso = None
        date_key = (
            find_prop_key(props, "Ngày")
            or find_prop_key(props, "Date")
            or find_prop_key(props, "Ngày Góp")
        )
        if date_key and props.get(date_key, {}).get("date"):
            date_iso = props[date_key]["date"].get("start")

        out.append((p.get("id"), title, date_iso))
        if len(out) >= limit:
            break

    return out


def find_children_by_relation(target_page_id: str) -> List[str]:
    """
    FIX #9: Tìm tất cả page trong CALENDAR DB có relation Lịch G trỏ về target_page_id.
    Trả về list page_id (string).
    """
    if not NOTION_DATABASE_ID:
        return []
    calendar_pages = query_database_all(NOTION_DATABASE_ID, page_size=500)
    children = []
    for p in calendar_pages:
        props_p = p.get("properties", {})
        rel_key = find_prop_key(props_p, "Lịch G")
        if not rel_key:
            continue
        rel_arr = props_p.get(rel_key, {}).get("relation", [])
        if any(r.get("id") == target_page_id for r in rel_arr):
            children.append(p.get("id"))
    return children


# =====================================================================
#  DAO PREVIEW
# =====================================================================
def dao_preview_text_from_props(title: str, props: dict):
    try:
        # ========== KHÚC TRÊN: LẤY DỮ LIỆU (giữ nguyên + bổ sung) ==========
        dao_text = extract_prop_text(props, "Đáo/thối") or extract_prop_text(props, "Đáo") or ""
        total_val = parse_money_from_text(dao_text) or 0
        per_day = _num(props, "G ngày")

        # 🆕 BỔ SUNG: lấy thêm thông tin hiển thị chi tiết
        total_money = _num(props, "tiền")           # Tổng tiền gốc (ví dụ 10,000)
        total_days = int(_num(props, "tổng ngày g")) # Tổng số ngày phải góp (ví dụ 50)
        checked, unchecked = count_checked_unchecked(title)  # Đã góp / Chưa góp

        raw_days = extract_prop_text(props, "ngày trước")
        try:
            days_before = int(float(raw_days)) if raw_days not in (None, "", "None") else 0
        except Exception:
            days_before = 0

        # ========== KHÚC GIỮA: CHẶN 🔴 (giữ nguyên) ==========
        if "🔴" in dao_text:
            return False, f"🔔 Chưa thể đáo cho 🔴: {title} ."

        # ========== KHÚC DƯỚI: XỬ LÝ ✅ (UPGRADE) ==========
        if "✅" in dao_text:
            # 🆕 Dòng header chi tiết — hiển thị chung cho cả 2 nhánh
            header_line = (
                f"{title} : với số tiền {int(total_money):,} "
                f"ngày {int(per_day):,} góp {total_days} ngày "
                f"Còn {unchecked} ngày chưa góp"
            )

            # --- NHÁNH KHÔNG LẤY TRƯỚC ---
            if not days_before or days_before <= 0:
                tomorrow = (datetime.now(VN_TZ)).date() + timedelta(days=1)
                restart = tomorrow.strftime("%d-%m-%Y")
                msg = (
                    f"🔔 {header_line}\n\n"
                    f"💴 Không Lấy trước\n"
                    f"🏛️ Tổng CK: ✅ {int(total_val):,}\n"
                    f"📆 Bắt đầu góp lại ngày mai ({restart})"
                )
                props["ONLY_LAI"] = True
                return True, msg

            # --- NHÁNH CÓ LẤY TRƯỚC ---
            take_days = int(days_before)
            total_pre = int(per_day * take_days) if per_day else 0
            start = (datetime.now(VN_TZ)).date() + timedelta(days=1)
            date_list = [(start + timedelta(days=i)).isoformat() for i in range(take_days)]
            restart_date = (start + timedelta(days=take_days)).strftime("%d-%m-%Y")

            lines = [
                f"🔔 {header_line}",
                "",
                f"💴 Lấy trước: {take_days} ngày {int(per_day):,} là {total_pre:,} ( từ ngày mai):",
            ]
            for idx, d in enumerate(date_list, start=1):
                lines.append(f"   {idx}. {d}")
            lines.append("")
            lines.append(f"🔔 Đáo lại khách sẽ nhận : ✅ {int(total_val):,}")
            lines.append(f"📆 Đến ngày {restart_date} bắt đầu góp lại")
            return True, "\n".join(lines)

        # ========== KHÚC CUỐI: FALLBACK (giữ nguyên) ==========
        msg = f"🔔 đáo lại cho: {title} - Tổng CK: {int(total_val)}\n\nKhông Lấy trước\n\nGửi /ok để chỉ tạo Lãi."
        props["ONLY_LAI"] = True
        return True, msg

    except Exception as e:
        print("dao_preview_text_from_props error:", e)
        return False, f"Preview error: {e}"

# =====================================================================
#  ACTIONS: MARK / UNDO
# =====================================================================
def mark_pages_by_indices(chat_id: str, keyword: str,
                          matches: List[Tuple[str, str, Optional[str], Dict[str, Any]]],
                          indices: List[int]) -> Dict[str, Any]:
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
            else:
                failed.append((pid, res))
        except Exception as e:
            failed.append((pid, str(e)))
    if succeeded:
        undo_stack.setdefault(str(chat_id), []).append({"action": "mark", "pages": [p[0] for p in succeeded]})
    return {"ok": len(failed) == 0, "succeeded": succeeded, "failed": failed}


def undo_last(chat_id: str, count: int = 1):
    chat_key = str(chat_id)

    if not undo_stack.get(chat_key):
        send_telegram(chat_id, "❌ Không có hành động nào để hoàn tác.")
        return

    log = undo_stack[chat_key].pop()
    if not log:
        send_telegram(chat_id, "❌ Không có dữ liệu undo.")
        return

    action = log.get("action")

    # --- UNDO MARK / ARCHIVE ---
    if action in ("mark", "archive"):
        pages = log.get("pages", [])
        total = len(pages)
        if total == 0:
            send_telegram(chat_id, "⚠️ Không có page trong log undo.")
            return

        msg = send_telegram(chat_id, f"♻️ Đang hoàn tác {total} mục ({action})...")
        message_id = msg.get("result", {}).get("message_id")
        undone = 0
        failed = 0

        for idx, pid in enumerate(pages, start=1):
            try:
                if action == "mark":
                    update_checkbox(pid, False)
                elif action == "archive":
                    unarchive_page(pid)
                bar = int((idx / total) * 10)
                progress = "█" * bar + "░" * (10 - bar)
                icon = ["♻️", "🔄", "💫", "✨"][idx % 4]
                edit_telegram_message(chat_id, message_id,
                                      f"{icon} Hoàn tác {idx}/{total} [{progress}]")
                undone += 1
                time.sleep(0.3)
            except Exception as e:
                print("Undo lỗi:", e)
                failed += 1

        final = f"✅ Hoàn tác {undone}/{total} mục"
        if failed:
            final += f" (⚠️ {failed} lỗi)"
        edit_telegram_message(chat_id, message_id, final)
        return

    # --- UNDO ĐÁO ---
    if action == "dao":
        created_pages = log.get("created_pages", [])
        archived_pages = log.get("archived_pages", [])
        lai_page = log.get("lai_page")

        send_telegram(chat_id, "♻️ Đang hoàn tác đáo...")

        for pid in created_pages:
            try:
                archive_page(pid)
            except Exception as e:
                print("Undo dao — delete created_page lỗi:", e)

        if lai_page:
            try:
                archive_page(lai_page)
            except Exception as e:
                print("Undo dao — delete lai_page lỗi:", e)

        for pid in archived_pages:
            try:
                unarchive_page(pid)
            except Exception as e:
                print("Undo dao — restore old_day lỗi:", e)

        send_telegram(chat_id, "✅ Hoàn tác đáo thành công.")
        return

    # --- UNDO SWITCH ON ---
    if action == "switch_on":
        _undo_switch_on(chat_id, log)
        return

    # --- UNDO SWITCH OFF ---
    if action == "switch_off":
        _undo_switch_off(chat_id, log)
        return

    send_telegram(chat_id, f"⚠️ Không hỗ trợ undo cho action '{action}'.")


# =====================================================================
#  ACTIONS: ARCHIVE
# =====================================================================
def handle_command_archive(chat_id: str, keyword: str, auto_confirm_all: bool = True) -> Dict[str, Any]:
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
            ok, msg_r = archive_page(pid)
            if ok:
                deleted.append(pid)
            else:
                failed.append((pid, msg_r))
            time.sleep(PATCH_DELAY)
        send_telegram(chat_id, f"✅ Đã xóa xong {len(deleted)}/{total} mục của {keyword}.")
        if failed:
            send_telegram(chat_id, f"⚠️ Có {len(failed)} mục xóa lỗi, xem logs.")
        if deleted:
            undo_stack.setdefault(str(chat_id), []).append({"action": "archive", "pages": deleted})
        return {"ok": True, "deleted": deleted, "failed": failed}
    except Exception as e:
        traceback.print_exc()
        send_telegram(chat_id, f"❌ Lỗi archive: {e}")
        return {"ok": False, "error": str(e)}


# =====================================================================
#  ACTIONS: CREATE LAI PAGE
# =====================================================================
def create_lai_page(chat_id: int, title: str, lai_amount: float, relation_id: str):
    try:
        today = datetime.now(VN_TZ).date().isoformat()
        props_payload = {
            "Name": {"title": [{"type": "text", "text": {"content": title}}]},
            "Lai": {"number": lai_amount},
            "ngày lai": {"date": {"start": today}},
            "Lịch G": {"relation": [{"id": relation_id}]}
        }
        url = "https://api.notion.com/v1/pages"
        body = {"parent": {"database_id": LA_NOTION_DATABASE_ID}, "properties": props_payload}
        r = requests.post(url, headers=NOTION_HEADERS, json=body, timeout=15)
        if r.status_code in (200, 201):
            send_telegram(chat_id, f"💰 Đã tạo Lãi cho {title}: {lai_amount:,.0f}")
            return r.json().get("id")
        else:
            send_telegram(chat_id, f"⚠️ Tạo Lãi lỗi: {r.status_code} - {r.text[:200]}")
            return None
    except Exception as e:
        send_telegram(chat_id, f"❌ Lỗi tạo Lãi cho {title}: {str(e)}")
        return None


# =====================================================================
#  DAO FLOW  (FIX #2, #4: sửa biến sai, signature đúng)
# =====================================================================
def dao_create_pages_from_props(chat_id: int, source_page_id: str, props: Dict[str, Any]):
    try:
        title = extract_prop_text(props, "Name") or "UNKNOWN"
        total_val = parse_money_from_text(extract_prop_text(props, "Đáo/thối")) or 0
        per_day = _num(props, "G ngày")
        days_before = _num(props, "ngày trước")
        pre_amount = _num(props, "trước")

        start_msg = send_telegram(chat_id, f"⏳ Đang xử lý đáo cho '{title}' ...")
        message_id = start_msg.get("result", {}).get("message_id")

        def update(text):
            if message_id:
                try:
                    edit_telegram_message(chat_id, message_id, text)
                    return
                except Exception:
                    pass
            send_telegram(chat_id, text)

        # ==============================
        # NHÁNH KHÔNG LẤY TRƯỚC
        # ==============================
        if pre_amount == 0:
            update(
                f"🔔 Đáo lại cho: {title}\n"
                f"🏛️ Tổng CK: {int(total_val)}\n"
                f"💴 Không Lấy Trước."
            )
            time.sleep(0.4)

            # FIX #2: dùng find_children_by_relation thay vì tìm bằng tên
            children = find_children_by_relation(source_page_id)
            total = len(children)

            if total == 0:
                update(f"🧹 Không có ngày cũ để xóa cho '{title}'.")
                time.sleep(0.3)
            else:
                update(f"🧹 Đang xóa {total} ngày của '{title}' ...")
                time.sleep(0.3)
                for idx, day_id in enumerate(children, start=1):
                    try:
                        archive_page(day_id)
                    except Exception as e:
                        print(f"⚠️ Lỗi archive: {day_id} — {e}")
                    bar = int((idx / total) * 10)
                    progress = "█" * bar + "░" * (10 - bar)
                    update(f"🧹 Xóa {idx}/{total} [{progress}]")
                    time.sleep(0.28)
                update(f"✅ Đã xóa toàn bộ {total} ngày cũ của '{title}' 🎉")
                time.sleep(0.4)

            # Tạo Lãi
            lai_text = (
                extract_prop_text(props, "Lai lịch g")
                or extract_prop_text(props, "Lãi")
                or extract_prop_text(props, "Lai")
                or ""
            )
            lai_amt = parse_money_from_text(lai_text) or 0
            lai_page_id = None  # FIX #2: khởi tạo trước

            if LA_NOTION_DATABASE_ID and lai_amt > 0:
                lai_page_id = create_lai_page(chat_id, title, lai_amt, source_page_id)
                update(f"💰 Đã tạo Lãi cho {title}.")
            else:
                update("ℹ️ Không có giá trị Lãi hoặc chưa cấu hình LA_NOTION_DATABASE_ID.")

            # Cập nhật Ngày Đáo = hôm nay
            today_vn = datetime.now(VN_TZ).date().isoformat()
            try:
                ngaydao_key = find_prop_key(props, "Ngày Đáo") or find_prop_key(props, "ngày đáo")
                if ngaydao_key:
                    update_page_properties(source_page_id, {ngaydao_key: {"date": {"start": today_vn}}})
                    update(f"📅 Ngày Đáo → {today_vn}")
            except Exception as e:
                update(f"⚠️ Lỗi cập nhật Ngày Đáo (bỏ qua): {e}")

            update("🎉 Hoàn thành đáo — KHÔNG LẤY TRƯỚC.")

            undo_stack.setdefault(str(chat_id), []).append({
                "action": "dao",
                "archived_pages": children,
                "created_pages": [],
                "lai_page": lai_page_id
            })
            return

        # ==============================
        # NHÁNH LẤY TRƯỚC
        # ==============================
        take_days = (
            int(days_before) if days_before else
            int(math.ceil(pre_amount / per_day)) if per_day else 0
        )

        if take_days <= 0:
            update(
                f"⚠️ Không tính được số ngày hợp lệ cho {title}\n"
                f"(per_day={per_day}, pre_amount={pre_amount})"
            )
            return

        # Xóa ngày cũ — dùng relation
        matched = find_children_by_relation(source_page_id)
        total = len(matched)

        if total == 0:
            update(f"🧹 Không có ngày cũ để xóa cho '{title}'.")
            time.sleep(0.3)
        else:
            update(f"🧹 Đang xóa {total} ngày của '{title}' ...")
            time.sleep(0.3)
            for idx, day_id in enumerate(matched, start=1):
                try:
                    archive_page(day_id)
                except Exception as e:
                    print(f"⚠️ Lỗi archive {day_id}: {e}")
                bar = int((idx / total) * 10)
                progress = "█" * bar + "░" * (10 - bar)
                update(f"🧹 Xóa {idx}/{total} [{progress}]")
                time.sleep(0.28)
            update(f"✅ Đã xóa {total} ngày cũ của '{title}'.")
            time.sleep(0.4)

        # Tạo ngày mới
        now_vn = datetime.now(VN_TZ)
        start_date = now_vn.date() + timedelta(days=1)

        update(f"🛠️ Đang tạo {take_days} ngày mới ...")
        time.sleep(0.4)

        created = []
        for i in range(1, take_days + 1):
            d = start_date + timedelta(days=i - 1)
            props_payload = {
                "Name": {"title": [{"type": "text", "text": {"content": title}}]},
                "Ngày Góp": {"date": {"start": d.isoformat()}},
                "Tiền": {"number": per_day},
                "Đã Góp": {"checkbox": True},
                "Lịch G": {"relation": [{"id": source_page_id}]},
            }
            try:
                r = requests.post(
                    "https://api.notion.com/v1/pages",
                    headers=NOTION_HEADERS,
                    json={"parent": {"database_id": NOTION_DATABASE_ID}, "properties": props_payload},
                    timeout=15
                )
                if r.status_code in (200, 201):
                    created.append(r.json())
                else:
                    update(f"⚠️ Lỗi tạo ngày: {r.status_code}")
            except Exception as e:
                update(f"⚠️ Lỗi tạo ngày {i}: {e}")

            bar = int((i / take_days) * 10)
            progress = "█" * bar + "░" * (10 - bar)
            update(f"📅 Tạo ngày {i}/{take_days} [{progress}] — {d.isoformat()}")
            time.sleep(0.25)

        update(f"✅ Đã tạo {len(created)} ngày mới cho '{title}' 🎉")
        time.sleep(0.4)

        # Tạo Lãi
        lai_text = (
            extract_prop_text(props, "Lai lịch g")
            or extract_prop_text(props, "Lãi")
            or extract_prop_text(props, "Lai")
            or ""
        )
        lai_amt = parse_money_from_text(lai_text) or 0
        lai_page_id = None

        if LA_NOTION_DATABASE_ID and lai_amt > 0:
            lai_page_id = create_lai_page(chat_id, title, lai_amt, source_page_id)
            send_telegram(chat_id, f"💰 Đã tạo Lãi cho {title}.")
        else:
            send_telegram(chat_id, "ℹ️ Không có giá trị Lãi hoặc chưa cấu hình LA_NOTION_DATABASE_ID.")

        send_telegram(chat_id, "🎉 Hoàn tất đáo vào đặt lại Repeat every day liền!")

        # Cập nhật Ngày Đáo = hôm nay
        today_vn = datetime.now(VN_TZ).date().isoformat()
        try:
            ngaydao_key = find_prop_key(props, "Ngày Đáo") or find_prop_key(props, "ngày đáo")
            if ngaydao_key:
                update_page_properties(source_page_id, {ngaydao_key: {"date": {"start": today_vn}}})
                send_telegram(chat_id, f"📅 Ngày Đáo → {today_vn}")
        except Exception as e:
            send_telegram(chat_id, f"⚠️ Lỗi cập nhật Ngày Đáo (bỏ qua): {e}")

        undo_stack.setdefault(str(chat_id), []).append({
            "action": "dao",
            "archived_pages": matched,
            "created_pages": [p.get("id") for p in created],
            "lai_page": lai_page_id
        })

    except Exception as e:
        send_telegram(chat_id, f"❌ Lỗi tiến trình đáo: {e}")
        traceback.print_exc()


# =====================================================================
#  PENDING / SELECTION PROCESSING
# =====================================================================
def parse_user_selection_text(sel_text: str, found_len: int) -> List[int]:
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
                a_i = int(a)
                b_i = int(b)
                for i in range(min(a_i, b_i), max(a_i, b_i) + 1):
                    selected.append(i)
            except Exception:
                pass
        else:
            try:
                n = int(p)
                if n > 1 and found_len >= n:
                    selected.extend(list(range(1, n + 1)))
                else:
                    selected.append(n)
            except Exception:
                pass
    selected = sorted(list(dict.fromkeys([i for i in selected if isinstance(i, int)])))
    return selected


def process_pending_selection_for_dao(chat_id: str, raw: str):
    key = str(chat_id)
    data = pending_confirm.get(key)

    if not data:
        send_telegram(chat_id, "⚠️ Không có thao tác đáo nào đang chờ.")
        return

    # =========================================================
    # 1) CHỌN DANH SÁCH (dao_choose)
    # =========================================================
    if data.get("type") == "dao_choose":
        matches = data.get("matches", []) or []
        indices = parse_user_selection_text(raw, len(matches))

        if not indices:
            send_telegram(chat_id, "⚠️ Lựa chọn không hợp lệ. Ví dụ 1 hoặc 1-2")
            return

        selected = []
        previews = []

        for idx in indices:
            if 1 <= idx <= len(matches):
                pid, title, props = matches[idx - 1]
                props = props if isinstance(props, dict) else {}
                selected.append((pid, title, props))
                try:
                    can, pv = dao_preview_text_from_props(title, props)
                except Exception as e:
                    pv = f"🔔 Đáo lại cho: {title}\n⚠️ Preview lỗi: {e}"
                previews.append(pv)

        agg_title = ", ".join([t for (_, t, _) in selected])
        agg_preview = "\n\n".join(previews)

        send_telegram(chat_id, f"🔔 Đáo lại cho: {agg_title}\n\n{agg_preview}")

        ok_msg = send_telegram(
            chat_id,
            f"⚠️ Gõ /ok trong {WAIT_CONFIRM}s để xác nhận hoặc /cancel."
        )
        try:
            timer_id = ok_msg["result"]["message_id"]
        except Exception:
            timer_id = None

        pending_confirm[key] = {
            "type": "dao_confirm",
            "targets": selected,
            "preview_text": agg_preview,
            "title": agg_title,
            "expires": time.time() + WAIT_CONFIRM,
            "timer_message_id": timer_id,
        }
        start_waiting_animation(chat_id, timer_id, WAIT_CONFIRM, interval=2.0, label="xác nhận đáo")
        return

    # =========================================================
    # 2) /OK HOẶC /CANCEL (dao_confirm)
    # =========================================================
    if data.get("type") == "dao_confirm":
        token = (raw or "").strip().lower()

        if not token:
            send_telegram(chat_id, "⚠️ Gửi /ok để xác nhận hoặc /cancel để hủy.")
            return

        if token in ("/cancel", "cancel", "hủy", "huỷ", "huy"):
            stop_waiting_animation(chat_id)
            pending_confirm.pop(key, None)
            send_telegram(chat_id, "❌ Đã hủy thao tác đáo.")
            return

        if token not in ("ok", "/ok", "yes", "đồng ý", "dong y"):
            send_telegram(chat_id, "⚠️ Gửi /ok để xác nhận hoặc /cancel để hủy.")
            return

        # OK
        stop_waiting_animation(chat_id)

        targets = data.get("targets") or []
        title_all = data.get("title") or ""

        if not targets:
            pending_confirm.pop(key, None)
            send_telegram(chat_id, "⚠️ Không có dữ liệu để đáo.")
            return

        send_telegram(chat_id, f"✅ Đã xác nhận OK — đang xử lý đáo cho: {title_all}")

        results = []

        for pid, ttitle, props in targets:
            try:
                props = props if isinstance(props, dict) else {}
                truoc_val = _num(props, "trước")
                is_no_take = (truoc_val == 0)

                lai_text = (
                    extract_prop_text(props, "Lai lịch g")
                    or extract_prop_text(props, "Lãi")
                    or extract_prop_text(props, "Lai")
                    or ""
                )
                lai_amt = parse_money_from_text(lai_text) or 0

                # ========================
                # CASE 1 — KHÔNG LẤY TRƯỚC
                # ========================
                if is_no_take:
                    # FIX #3, #9: dùng find_children_by_relation
                    children = find_children_by_relation(pid)
                    total = len(children)

                    msg_r = send_telegram(chat_id, f"🧹 Đang xóa ngày cũ của '{ttitle}' ...")
                    mid = msg_r.get("result", {}).get("message_id")

                    def _update_no_take(text, _mid=mid):
                        if _mid:
                            try:
                                edit_telegram_message(chat_id, _mid, text)
                                return
                            except Exception:
                                pass
                        send_telegram(chat_id, text)

                    if total == 0:
                        _update_no_take("🧹 Không có ngày nào để xóa.")
                        time.sleep(0.3)
                    else:
                        _update_no_take(f"🧹 Bắt đầu xóa {total} ngày ...")
                        time.sleep(0.25)
                        for idx, day_id in enumerate(children, start=1):
                            archive_page(day_id)
                            bar = int((idx / total) * 10)
                            progress = "█" * bar + "░" * (10 - bar)
                            _update_no_take(f"🧹 Xóa {idx}/{total} [{progress}]")
                            time.sleep(0.25)
                        _update_no_take(f"✅ Đã xóa toàn bộ {total} ngày 🎉")
                        time.sleep(0.3)

                    lai_page_id = None
                    if LA_NOTION_DATABASE_ID and lai_amt > 0:
                        lai_page_id = create_lai_page(chat_id, ttitle, lai_amt, pid)
                        results.append((pid, ttitle, True, "Lãi only"))
                    else:
                        results.append((pid, ttitle, False, "Không có lãi"))

                    # Cập nhật Ngày Đáo = hôm nay
                    today_vn = datetime.now(VN_TZ).date().isoformat()
                    try:
                        ngaydao_key = find_prop_key(props, "Ngày Đáo") or find_prop_key(props, "ngày đáo")
                        if ngaydao_key:
                            update_page_properties(pid, {ngaydao_key: {"date": {"start": today_vn}}})
                    except Exception as e:
                        print(f"⚠️ Lỗi cập nhật Ngày Đáo cho {ttitle}: {e}")

                    # FIX #3: children là list string → dùng trực tiếp
                    undo_stack.setdefault(str(chat_id), []).append({
                        "action": "dao",
                        "archived_pages": children,
                        "created_pages": [],
                        "lai_page": lai_page_id
                    })
                    continue

                # ========================
                # CASE 2 — CÓ LẤY TRƯỚC  (FIX #4: đúng signature)
                # ========================
                dao_create_pages_from_props(chat_id, pid, props)
                results.append((pid, ttitle, True, "DAO Complete"))

            except Exception as e:
                results.append((pid, ttitle, False, f"Unhandled: {e}"))

        # REPORT
        ok_list = [r for r in results if r[2]]
        fail_list = [r for r in results if not r[2]]

        text = f"🎉 Hoàn tất đáo cho: {title_all}\n"
        text += f"✅ Thành công: {len(ok_list)}\n"
        if fail_list:
            text += f"⚠️ Lỗi: {len(fail_list)}\n"
            for pid_, nm, ok_, er in fail_list:
                text += f"- {nm}: {er}\n"

        send_telegram(chat_id, text)
        pending_confirm.pop(key, None)
        return


def process_pending_selection(chat_id: str, raw: str):
    key = str(chat_id)
    data = pending_confirm.get(key)

    if not data:
        send_telegram(chat_id, "❌ Không có thao tác nào đang chờ.")
        return

    try:
        raw_input = raw.strip().lower()

        if raw_input in ("/cancel", "cancel", "hủy", "huỷ", "huy"):
            stop_waiting_animation(chat_id)
            pending_confirm.pop(key, None)
            send_telegram(chat_id, "🛑 Đã hủy thao tác đang chờ.")
            return

        matches = data.get("matches", [])
        if not matches:
            send_telegram(chat_id, "⚠️ Không tìm thấy danh sách mục đang xử lý.")
            pending_confirm.pop(key, None)
            return

        indices = parse_user_selection_text(raw_input, len(matches))
        if not indices:
            send_telegram(chat_id, "⚠️ Không nhận được lựa chọn hợp lệ.")
            return

        action = data.get("type")

        # ======= ARCHIVE MODE =======
        if action == "archive_select":
            stop_waiting_animation(chat_id)
            selected = [matches[i - 1] for i in indices if 1 <= i <= len(matches)]
            total_sel = len(selected)
            if total_sel == 0:
                send_telegram(chat_id, "⚠️ Không có mục nào được chọn để xóa.")
                pending_confirm.pop(key, None)
                return

            msg_r = send_telegram(chat_id, f"🧹 Bắt đầu xóa {total_sel} mục của '{data['keyword']}' ...")
            message_id = msg_r.get("result", {}).get("message_id")

            deleted = []
            for idx, (pid, title, date_iso, props) in enumerate(selected, start=1):
                try:
                    ok, res = archive_page(pid)
                    if not ok:
                        send_telegram(chat_id, f"⚠️ Lỗi khi xóa {title}: {res}")
                        continue
                    deleted.append(pid)
                    bar = int((idx / total_sel) * 10)
                    progress = "█" * bar + "░" * (10 - bar)
                    percent = int((idx / total_sel) * 100)
                    edit_telegram_message(chat_id, message_id, f"🧹 Xóa {idx}/{total_sel} [{progress}] {percent}%")
                    time.sleep(0.4)
                except Exception as e:
                    send_telegram(chat_id, f"⚠️ Lỗi khi xóa {idx}/{total_sel}: {e}")

            edit_telegram_message(
                chat_id, message_id,
                f"✅ Hoàn tất xóa {total_sel}/{total_sel} mục của '{data['keyword']}' 🎉"
            )
            if deleted:
                undo_stack.setdefault(str(chat_id), []).append({"action": "archive", "pages": deleted})
            pending_confirm.pop(key, None)
            return

        # ======= MARK MODE =======
        if action == "mark":
            stop_waiting_animation(chat_id)
            keyword = data.get("keyword")
            total_sel = len(indices)
            msg_r = send_telegram(chat_id, f"🟢 Bắt đầu đánh dấu {total_sel} mục cho '{keyword}' ...")
            message_id = msg_r.get("result", {}).get("message_id")

            succeeded, failed = [], []

            for idx in indices:
                if 1 <= idx <= len(matches):
                    pid, title, date_iso, props = matches[idx - 1]
                    try:
                        cb_key = (
                            find_prop_key(props, "Đã Góp")
                            or find_prop_key(props, "Sent")
                            or find_prop_key(props, "Status")
                        )
                        update_props = {cb_key or "Đã Góp": {"checkbox": True}}
                        ok, res = update_page_properties(pid, update_props)
                        if ok:
                            succeeded.append((pid, title))
                            bar = int((len(succeeded) / total_sel) * 10)
                            progress = "█" * bar + "░" * (10 - bar)
                            percent = int((len(succeeded) / total_sel) * 100)
                            edit_telegram_message(chat_id, message_id,
                                                  f"🟢 Đánh dấu {len(succeeded)}/{total_sel} [{progress}] {percent}%")
                        else:
                            failed.append((pid, res))
                    except Exception as e:
                        failed.append((pid, str(e)))
                    time.sleep(0.3)

            result_text = f"✅ Hoàn tất đánh dấu {len(succeeded)}/{total_sel} mục 🎉"
            if failed:
                result_text += f"\n⚠️ Lỗi: {len(failed)} mục không thể cập nhật."

            if message_id:
                edit_telegram_message(chat_id, message_id, result_text)
            else:
                send_telegram(chat_id, result_text)

            if succeeded:
                undo_stack.setdefault(str(chat_id), []).append({"action": "mark", "pages": [p[0] for p in succeeded]})

            # Tính count từ data cũ — không query lại
            n_ok = len(succeeded)
            old_checked = data.get("checked", 0)
            old_unchecked = data.get("unchecked", 0)
            send_telegram(chat_id, f"💴 {keyword}\n\n📊 Đã góp: {old_checked + n_ok}\n🟡 Chưa góp: {old_unchecked - n_ok}")
            pending_confirm.pop(key, None)
            return

        send_telegram(chat_id, "⚠️ Không xác định được loại thao tác. Vui lòng thử lại.")
        pending_confirm.pop(key, None)

    except Exception as e:
        traceback.print_exc()
        send_telegram(chat_id, f"❌ Lỗi xử lý lựa chọn: {e}")
        pending_confirm.pop(key, None)


# =====================================================================
#  SWITCH ON / OFF — PREVIEW → CONFIRM → EXECUTE
# =====================================================================

def preview_switch_on(chat_id: int, keyword: str):
    """Bước 1: Tìm target, hiển thị preview, chờ /ok"""
    try:
        send_telegram(chat_id, f"🟢 Đang tìm '{keyword}' trong TARGET DB ...")
        matches = find_target_matches(keyword)

        if not matches:
            send_telegram(chat_id, f"❌ Không tìm thấy '{keyword}' trong TARGET DB.")
            return

        if len(matches) > 1:
            send_telegram(chat_id, f"⚠️ Tìm thấy {len(matches)} kết quả cho '{keyword}'. Vui lòng nhập chính xác hơn.")
            return

        target_id, title, props = matches[0]

        total_money = _num(props, "tiền")
        per_day = _num(props, "G ngày")
        total_days = _num(props, "tổng ngày g")
        take_days = int(_num(props, "ngày trước"))
        truoc_val = _num(props, "trước")
        ck_val = _num(props, "CK")

        if take_days <= 0:
            send_telegram(chat_id, f"⚠️ 'ngày trước' = 0 → Không tạo ngày nào.")
            return

        start_date = datetime.now(VN_TZ).date()
        days = [start_date + timedelta(days=i) for i in range(take_days)]
        next_start = (start_date + timedelta(days=take_days)).strftime("%d-%m-%Y")

        lines = [
            f"🟢 Bật ON cho: {title}",
            f"💰 Tiền: {int(total_money):,} | G ngày: {int(per_day):,} | Góp {int(total_days)} ngày",
            f"💴 Lấy trước: {take_days} ngày × {int(per_day):,} = {int(truoc_val):,}",
            f"📅 Tạo {take_days} ngày từ hôm nay:",
        ]
        for i, d in enumerate(days, start=1):
            lines.append(f"  {i}. {d.isoformat()}")
        lines.append(f"🏛️ Tổng CK: ✅ {int(ck_val):,}")
        lines.append(f"📆 Đến ngày {next_start} bắt đầu góp lại")

        send_telegram(chat_id, "\n".join(lines))
        send_telegram(chat_id, f"⚠️ Gõ /ok trong {WAIT_CONFIRM}s hoặc /cancel.")

        timer_msg = send_telegram(chat_id, f"⏳ Đang chờ xác nhận trong {WAIT_CONFIRM}s...")
        timer_message_id = timer_msg.get("result", {}).get("message_id")

        pending_confirm[str(chat_id)] = {
            "type": "switch_on_confirm",
            "target_id": target_id,
            "title": title,
            "props": props,
            "expires": time.time() + WAIT_CONFIRM,
            "timer_message_id": timer_message_id,
        }
        start_waiting_animation(chat_id, timer_message_id, WAIT_CONFIRM, interval=2.0, label="xác nhận ON")

    except Exception as e:
        traceback.print_exc()
        send_telegram(chat_id, f"❌ Lỗi preview ON: {e}")


def preview_switch_off(chat_id: int, keyword: str):
    """Bước 1: Tìm target, đếm ngày, hiển thị preview, chờ /ok"""
    try:
        send_telegram(chat_id, f"🔴 Đang tìm '{keyword}' trong TARGET DB ...")
        matches = find_target_matches(keyword)

        if not matches:
            send_telegram(chat_id, f"❌ Không tìm thấy '{keyword}' trong TARGET DB.")
            return

        if len(matches) > 1:
            send_telegram(chat_id, f"⚠️ Tìm thấy {len(matches)} kết quả. Vui lòng nhập chính xác hơn.")
            return

        target_id, title, props = matches[0]

        # Đếm số ngày sẽ xóa
        children = find_children_by_relation(target_id)
        total_days = len(children)

        # Đọc Lãi
        lai_text = (
            extract_prop_text(props, "Lai lịch g")
            or extract_prop_text(props, "Lãi")
            or extract_prop_text(props, "Lai")
            or ""
        )
        lai_amt = parse_money_from_text(lai_text) or 0

        lines = [
            f"🔴 Tắt OFF cho: {title}",
            f"🧹 Sẽ xóa {total_days} ngày trong CALENDAR DB",
        ]
        if lai_amt > 0:
            lines.append(f"💰 Tạo Lãi: {int(lai_amt):,}")
        else:
            lines.append("ℹ️ Không có Lãi")
        lines.append("📝 Cập nhật trạng thái → Done")

        send_telegram(chat_id, "\n".join(lines))
        send_telegram(chat_id, f"⚠️ Gõ /ok trong {WAIT_CONFIRM}s hoặc /cancel.")

        timer_msg = send_telegram(chat_id, f"⏳ Đang chờ xác nhận trong {WAIT_CONFIRM}s...")
        timer_message_id = timer_msg.get("result", {}).get("message_id")

        pending_confirm[str(chat_id)] = {
            "type": "switch_off_confirm",
            "target_id": target_id,
            "title": title,
            "props": props,
            "children": children,
            "expires": time.time() + WAIT_CONFIRM,
            "timer_message_id": timer_message_id,
        }
        start_waiting_animation(chat_id, timer_message_id, WAIT_CONFIRM, interval=2.0, label="xác nhận OFF")

    except Exception as e:
        traceback.print_exc()
        send_telegram(chat_id, f"❌ Lỗi preview OFF: {e}")


def process_pending_switch(chat_id: int, raw: str):
    """Xử lý /ok hoặc /cancel cho switch ON/OFF"""
    key = str(chat_id)
    data = pending_confirm.get(key)

    if not data:
        send_telegram(chat_id, "⚠️ Không có thao tác ON/OFF nào đang chờ.")
        return

    token = (raw or "").strip().lower()

    # --- CANCEL ---
    if token in ("/cancel", "cancel", "hủy", "huỷ", "huy"):
        stop_waiting_animation(chat_id)
        pending_confirm.pop(key, None)
        send_telegram(chat_id, "❌ Đã hủy thao tác ON/OFF.")
        return

    # --- KHÔNG PHẢI OK ---
    if token not in ("ok", "/ok", "yes", "đồng ý", "dong y"):
        send_telegram(chat_id, "⚠️ Gửi /ok để xác nhận hoặc /cancel để hủy.")
        return

    # --- OK ---
    stop_waiting_animation(chat_id)
    ptype = data.get("type")

    if ptype == "switch_on_confirm":
        pending_confirm.pop(key, None)
        execute_switch_on(
            chat_id,
            data.get("target_id"),
            data.get("title"),
            data.get("props", {}),
        )
    elif ptype == "switch_off_confirm":
        pending_confirm.pop(key, None)
        execute_switch_off(
            chat_id,
            data.get("target_id"),
            data.get("title"),
            data.get("props", {}),
            data.get("children", []),
        )
    else:
        send_telegram(chat_id, f"⚠️ Không xác định được loại thao tác: {ptype}")
        pending_confirm.pop(key, None)


def execute_switch_on(chat_id: int, target_id: str, title: str, props: dict):
    """Bước 2 (sau /ok): Thực thi ON"""
    try:
        msg = send_telegram(chat_id, f"✅ Đã xác nhận — đang xử lý ON cho '{title}' ...")
        message_id = msg.get("result", {}).get("message_id")

        def update(text):
            if message_id:
                try:
                    edit_telegram_message(chat_id, message_id, text)
                    return
                except Exception:
                    pass
            send_telegram(chat_id, text)

        per_day = _num(props, "G ngày")
        total_money = _num(props, "tiền")
        total_days = _num(props, "tổng ngày g")
        take_days = int(_num(props, "ngày trước"))
        truoc_val = _num(props, "trước")
        ck_val = _num(props, "CK")

        # Cập nhật TARGET DB → In progress + Ngày Đáo = hôm nay
        update("📝 Đang cập nhật TARGET → In progress ...")
        today_vn = datetime.now(VN_TZ).date().isoformat()
        try:
            status_key = find_prop_key(props, "trạng thái")
            ngaydao_key = find_prop_key(props, "Ngày Đáo") or find_prop_key(props, "ngày đáo")
            up_props = {}
            if status_key:
                up_props[status_key] = {"status": {"name": "In progress"}}
            if ngaydao_key:
                up_props[ngaydao_key] = {"date": {"start": today_vn}}
            if up_props:
                ok, res = update_page_properties(target_id, up_props)
                if not ok:
                    update(f"⚠️ Cập nhật TARGET thất bại (bỏ qua): {res}")
                else:
                    update("✅ TARGET → In progress, Ngày Đáo cập nhật.")
            else:
                update("⚠️ Không tìm thấy property hợp lệ → bỏ qua.")
        except Exception as e:
            update(f"⚠️ Lỗi cập nhật TARGET (bỏ qua): {e}")
        time.sleep(0.3)

        # Cập nhật relation Tổng Thụ Động → G
        old_ttd_relation = []
        if TONG_THU_DONG_G_PAGE_ID:
            try:
                ttd_key = find_prop_key(props, "Tổng Thụ Động") or find_prop_key(props, "tổng thụ động")
                if ttd_key:
                    # Lưu giá trị cũ để undo
                    old_rel = props.get(ttd_key, {}).get("relation", [])
                    old_ttd_relation = [r.get("id") for r in old_rel if r.get("id")]
                    # Set relation → G
                    ok, res = update_page_properties(target_id, {
                        ttd_key: {"relation": [{"id": TONG_THU_DONG_G_PAGE_ID}]}
                    })
                    if ok:
                        update("✅ Tổng Thụ Động → G")
                    else:
                        update(f"⚠️ Lỗi set Tổng Thụ Động: {res}")
            except Exception as e:
                update(f"⚠️ Lỗi Tổng Thụ Động (bỏ qua): {e}")

        # Tạo các ngày
        update(f"🛠️ Đang tạo {take_days} ngày trong CALENDAR DB ...")
        time.sleep(0.3)

        start_date = datetime.now(VN_TZ).date()
        days = [start_date + timedelta(days=i) for i in range(take_days)]
        created_pages = []

        for idx, d in enumerate(days, start=1):
            props_payload = {
                "Name": {"title": [{"type": "text", "text": {"content": title}}]},
                "Ngày Góp": {"date": {"start": d.isoformat()}},
                "Tiền": {"number": per_day},
                "Đã Góp": {"checkbox": True},
                "Lịch G": {"relation": [{"id": target_id}]}
            }
            ok, res = create_page_in_db(NOTION_DATABASE_ID, props_payload)
            if ok:
                created_pages.append(res.get("id"))
            else:
                update(f"⚠️ Lỗi tạo ngày {idx}: {res}")

            bar = int((idx / take_days) * 10)
            progress = "▬" * bar + "▭" * (10 - bar)
            update(f"📅 Tạo ngày {idx}/{take_days} [{progress}] – {d.isoformat()}")
            time.sleep(0.25)

        update(f"✅ Đã tạo {len(created_pages)} ngày mới cho '{title}' 🎉")
        time.sleep(0.4)

        # Thông báo kết quả
        next_start = (start_date + timedelta(days=take_days)).strftime("%d-%m-%Y")
        lines = [
            f"🔔 Đã bật ON cho: {title}",
            f"với số tiền {int(total_money):,} ngày {int(per_day):,} góp {int(total_days)} ngày",
            f"💴 Lấy trước: {take_days} ngày {int(per_day):,} là {int(truoc_val):,}",
            "   ( từ hôm nay):",
        ]
        for i, d in enumerate(days, start=1):
            lines.append(f"{i}. {d.isoformat()}")
        lines.append("")
        lines.append(f"🏛️ Tổng CK: ✅ {int(ck_val):,}")
        lines.append(f"📆 Đến ngày {next_start} bắt đầu góp lại")
        lines.append("")
        lines.append("🎉 Hoàn tất ON.")
        update("\n".join(lines))

        # Ghi undo log
        undo_stack.setdefault(str(chat_id), []).append({
            "action": "switch_on",
            "target_id": target_id,
            "title": title,
            "created_pages": created_pages,
            "old_trangthai": extract_prop_text(props, "trạng thái") or "",
            "old_ngaydao": extract_prop_text(props, "Ngày Đáo") or "",
            "old_ttd_relation": old_ttd_relation,
        })

    except Exception as e:
        traceback.print_exc()
        send_telegram(chat_id, f"❌ Lỗi ON: {e}")


def execute_switch_off(chat_id: int, target_id: str, title: str, props: dict, children: list):
    """Bước 2 (sau /ok): Thực thi OFF"""
    try:
        msg = send_telegram(chat_id, f"✅ Đã xác nhận — đang xử lý OFF cho '{title}' ...")
        message_id = msg.get("result", {}).get("message_id")

        def update(text):
            if message_id:
                try:
                    edit_telegram_message(chat_id, message_id, text)
                    return
                except Exception:
                    pass
            send_telegram(chat_id, text)

        # Xóa các ngày
        total = len(children)

        if total == 0:
            update(f"🧹 Không có ngày nào để xóa cho '{title}'.")
            time.sleep(0.3)
        else:
            update(f"🧹 Bắt đầu xóa {total} ngày ...")
            time.sleep(0.3)
            for idx, day_id in enumerate(children, start=1):
                archive_page(day_id)
                bar = int((idx / total) * 10)
                progress = "▬" * bar + "▭" * (10 - bar)
                update(f"🧹 Xóa {idx}/{total} [{progress}]")
                time.sleep(0.25)
            update(f"✅ Đã xóa toàn bộ {total} ngày 🎉")
            time.sleep(0.4)

        # Tạo Lãi
        lai_text = (
            extract_prop_text(props, "Lai lịch g")
            or extract_prop_text(props, "Lãi")
            or extract_prop_text(props, "Lai")
            or ""
        )
        lai_amt = parse_money_from_text(lai_text) or 0
        lai_page_id = None

        if lai_amt > 0:
            update(f"💰 Đang tạo Lãi {int(lai_amt):,} ...")
            lai_page_id = create_lai_page(chat_id, title, lai_amt, target_id)
            update(f"✅ Đã tạo Lãi cho {title}.")
        else:
            update("ℹ️ Không có giá trị Lãi.")
        time.sleep(0.3)

        # Cập nhật TARGET DB → Done
        update("📝 Đang cập nhật TARGET → Done ...")
        today_vn = datetime.now(VN_TZ).date().isoformat()
        try:
            status_key = find_prop_key(props, "trạng thái")
            ngayxong_key = find_prop_key(props, "ngày xong")
            up_props = {}
            if status_key:
                up_props[status_key] = {"status": {"name": "Done"}}
            if ngayxong_key:
                up_props[ngayxong_key] = {"date": {"start": today_vn}}
            if up_props:
                ok, res = update_page_properties(target_id, up_props)
                if not ok:
                    update(f"⚠️ Cập nhật TARGET thất bại (bỏ qua): {res}")
                else:
                    update("✅ TARGET → Done.")
            else:
                update("⚠️ Không tìm thấy property hợp lệ → bỏ qua.")
        except Exception as e:
            update(f"⚠️ Lỗi cập nhật TARGET (bỏ qua): {e}")
        time.sleep(0.3)

        # Xóa relation Tổng Thụ Động
        old_ttd_relation = []
        try:
            ttd_key = find_prop_key(props, "Tổng Thụ Động") or find_prop_key(props, "tổng thụ động")
            if ttd_key:
                old_rel = props.get(ttd_key, {}).get("relation", [])
                old_ttd_relation = [r.get("id") for r in old_rel if r.get("id")]
                ok, res = update_page_properties(target_id, {
                    ttd_key: {"relation": []}
                })
                if ok:
                    update("✅ Tổng Thụ Động → bỏ G")
                else:
                    update(f"⚠️ Lỗi xóa Tổng Thụ Động: {res}")
        except Exception as e:
            update(f"⚠️ Lỗi Tổng Thụ Động (bỏ qua): {e}")

        update(f"🎉 Hoàn tất OFF cho: {title}")

        # Ghi undo log
        undo_stack.setdefault(str(chat_id), []).append({
            "action": "switch_off",
            "target_id": target_id,
            "title": title,
            "archived_pages": children,
            "lai_page": lai_page_id,
            "old_trangthai": extract_prop_text(props, "trạng thái") or "",
            "old_ngayxong": extract_prop_text(props, "ngày xong") or "",
            "old_ttd_relation": old_ttd_relation,
        })

    except Exception as e:
        traceback.print_exc()
        send_telegram(chat_id, f"❌ Lỗi OFF: {e}")


# =====================================================================
#  UNDO SWITCH
# =====================================================================
def _undo_switch_on(chat_id: int, log: dict):
    msg = send_telegram(chat_id, "♻️ Đang hoàn tác ON...")
    message_id = msg.get("result", {}).get("message_id")

    created = log.get("created_pages", [])
    total = len(created)

    for idx, pid in enumerate(created, start=1):
        try:
            archive_page(pid)
            bar = int((idx / total) * 10) if total > 0 else 0
            progress = "▬" * bar + "▭" * (10 - bar)
            if message_id:
                edit_telegram_message(chat_id, message_id,
                                      f"♻️ Xóa ngày {idx}/{total} [{progress}]")
            time.sleep(0.25)
        except Exception as e:
            print(f"⚠️ Lỗi xóa page: {pid} – {e}")

    target_id = log.get("target_id")
    old_tt = log.get("old_trangthai")
    old_nd = log.get("old_ngaydao")

    restore_props = {}
    if old_tt:
        restore_props["trạng thái"] = {"status": {"name": old_tt}}
    if old_nd:
        restore_props["Ngày Đáo"] = {"date": {"start": old_nd}}
    if restore_props and target_id:
        update_page_properties(target_id, restore_props)

    # Restore Tổng Thụ Động
    old_ttd = log.get("old_ttd_relation", [])
    if target_id:
        try:
            ttd_rel = [{"id": rid} for rid in old_ttd] if old_ttd else []
            update_page_properties(target_id, {"Tổng Thụ Động": {"relation": ttd_rel}})
        except Exception as e:
            print(f"⚠️ Undo TTD lỗi: {e}")

    final_msg = f"✅ Đã hoàn tác ON cho: {log.get('title')}"
    if message_id:
        edit_telegram_message(chat_id, message_id, final_msg)
    else:
        send_telegram(chat_id, final_msg)


def _undo_switch_off(chat_id: int, log: dict):
    msg = send_telegram(chat_id, "♻️ Đang hoàn tác OFF...")
    message_id = msg.get("result", {}).get("message_id")

    archived = log.get("archived_pages", [])
    total = len(archived)

    for idx, pid in enumerate(archived, start=1):
        try:
            unarchive_page(pid)
            bar = int((idx / total) * 10) if total > 0 else 0
            progress = "▬" * bar + "▭" * (10 - bar)
            if message_id:
                edit_telegram_message(chat_id, message_id,
                                      f"♻️ Khôi phục {idx}/{total} [{progress}]")
            time.sleep(0.25)
        except Exception as e:
            print(f"⚠️ Lỗi khôi phục page: {pid} – {e}")

    lai_page = log.get("lai_page")
    if lai_page:
        try:
            archive_page(lai_page)
        except Exception as e:
            print(f"⚠️ Lỗi xóa Lãi: {lai_page} – {e}")

    target_id = log.get("target_id")
    old_tt = log.get("old_trangthai")
    old_nx = log.get("old_ngayxong")

    restore_props = {}
    if old_tt:
        restore_props["trạng thái"] = {"status": {"name": old_tt}}
    if old_nx:
        restore_props["ngày xong"] = {"date": {"start": old_nx}}
    if restore_props and target_id:
        update_page_properties(target_id, restore_props)

    # Restore Tổng Thụ Động
    old_ttd = log.get("old_ttd_relation", [])
    if target_id:
        try:
            ttd_rel = [{"id": rid} for rid in old_ttd] if old_ttd else []
            update_page_properties(target_id, {"Tổng Thụ Động": {"relation": ttd_rel}})
        except Exception as e:
            print(f"⚠️ Undo TTD lỗi: {e}")

    final_msg = f"✅ Đã hoàn tác OFF cho: {log.get('title')}"
    if message_id:
        edit_telegram_message(chat_id, message_id, final_msg)
    else:
        send_telegram(chat_id, final_msg)


# =====================================================================
#  COMMAND PARSING & MAIN HANDLER
# =====================================================================
def parse_user_command(raw: str) -> Tuple[str, int, Optional[str]]:
    raw = raw.strip()
    if not raw:
        return "", 0, None

    parts = raw.split()
    kw = parts[0]
    count = 0
    action = None

    if len(parts) > 1 and parts[1].isdigit():
        count = int(parts[1])
        action = "mark"
    elif raw.lower() in ("undo", "/undo"):
        action = "undo"
    elif any(x in raw.lower() for x in ["xóa", "archive", "del", "delete"]):
        action = "archive"
    elif any(x in raw.lower() for x in ["đáo", "dao", "daó", "đáo hạn"]):
        action = "dao"

    return kw, count, action


def handle_incoming_message(chat_id: int, text: str):
    try:
        matches = []
        kw = ""

        if TELEGRAM_CHAT_ID and str(chat_id) != str(TELEGRAM_CHAT_ID):
            send_telegram(chat_id, "Bot chưa được phép nhận lệnh từ chat này.")
            return

        raw = text.strip()
        if not raw:
            send_telegram(chat_id, "Vui lòng gửi lệnh hoặc từ khoá.")
            return

        low = raw.lower()

        # ===== DEBUG COMMAND =====
        if low.startswith("debug "):
            debug_kw = raw[6:].strip()
            lines = [
                f"🔧 DEBUG cho '{debug_kw}'",
                f"━━━━━━━━━━━━━━━━━━",
                f"TARGET_DB_ID: {'✅ ' + TARGET_NOTION_DATABASE_ID[:16] + '...' if TARGET_NOTION_DATABASE_ID else '❌ TRỐNG'}",
                f"CALENDAR_DB_ID: {'✅ ' + NOTION_DATABASE_ID[:16] + '...' if NOTION_DATABASE_ID else '❌ TRỐNG'}",
                f"LA_DB_ID: {'✅ ' + LA_NOTION_DATABASE_ID[:16] + '...' if LA_NOTION_DATABASE_ID else '❌ TRỐNG'}",
                f"SAME? {'⚠️ CẢ HAI GIỐNG NHAU!' if TARGET_NOTION_DATABASE_ID == NOTION_DATABASE_ID else '✅ Khác nhau'}",
                f"NOTION_TOKEN: {'✅ set' if NOTION_TOKEN else '❌ TRỐNG'}",
                f"TTD_G_PAGE: {'✅ ' + TONG_THU_DONG_G_PAGE_ID[:16] + '...' if TONG_THU_DONG_G_PAGE_ID else '❌ TRỐNG (set TONG_THU_DONG_G_PAGE_ID)'}",
            ]
            # Test query TARGET DB
            pages = []
            try:
                pages = query_database_all(TARGET_NOTION_DATABASE_ID, page_size=10)
                lines.append(f"\n📦 Query TARGET DB: {len(pages)} pages (top 5)")
                for i, p in enumerate(pages[:5]):
                    props = p.get("properties", {})
                    title = extract_prop_text(props, "Name") or extract_prop_text(props, "Title") or "(no title)"
                    lines.append(f"  {i+1}. {title}")
            except Exception as e:
                lines.append(f"\n❌ Query TARGET DB lỗi: {e}")

            # Test matching — reuse pages đã query, không query lại
            try:
                matches_debug = find_target_matches(debug_kw, _pages=pages)
                lines.append(f"\n🔍 find_target_matches('{debug_kw}'): {len(matches_debug)} kết quả")
                for pid, title, props in matches_debug[:5]:
                    lines.append(f"  → {title}")
            except Exception as e:
                lines.append(f"\n❌ find_target_matches lỗi: {e}")

            send_telegram(chat_id, "\n".join(lines))
            return

        # ===== DEBUG TTD COMMAND =====
        if low.startswith("debug ttd "):
            debug_kw = raw[10:].strip()
            send_telegram(chat_id, f"🔧 Đang chẩn đoán Tổng Thụ Động cho '{debug_kw}' ...")

            lines = [f"🔧 DEBUG TTD cho '{debug_kw}'", "━━━━━━━━━━━━━━━━━━"]

            # 1. Check env var
            g_id = TONG_THU_DONG_G_PAGE_ID
            lines.append(f"ENV TONG_THU_DONG_G_PAGE_ID: {'✅ ' + g_id if g_id else '❌ TRỐNG'}")

            # 2. Find target page
            try:
                pages = query_database_all(TARGET_NOTION_DATABASE_ID, page_size=10)
                matches = find_target_matches(debug_kw, _pages=pages)
                if not matches:
                    lines.append(f"❌ Không tìm thấy '{debug_kw}' trong TARGET DB")
                    send_telegram(chat_id, "\n".join(lines))
                    return

                target_id, title, props = matches[0]
                lines.append(f"✅ Target: {title} (id={target_id[:12]}...)")

                # 3. Scan ALL property names containing "thu dong" or "thụ động"
                lines.append(f"\n📋 Tất cả property keys:")
                for k in sorted(props.keys()):
                    ptype = props[k].get("type", "?")
                    lines.append(f"  [{ptype}] {k}")

                # 4. Find TTD property specifically
                ttd_key = find_prop_key(props, "Tổng Thụ Động") or find_prop_key(props, "tổng thụ động")
                lines.append(f"\n🔍 find_prop_key('Tổng Thụ Động'): {ttd_key or '❌ KHÔNG TÌM THẤY'}")

                if ttd_key:
                    ttd_prop = props.get(ttd_key, {})
                    ttd_type = ttd_prop.get("type", "?")
                    lines.append(f"  type: {ttd_type}")
                    lines.append(f"  raw value: {json.dumps(ttd_prop, ensure_ascii=False, default=str)[:300]}")

                    # 5. Thử GET page trực tiếp để xem full relation config
                    try:
                        r = requests.get(
                            f"https://api.notion.com/v1/pages/{target_id}/properties/{ttd_key}",
                            headers=NOTION_HEADERS, timeout=15
                        )
                        lines.append(f"\n📡 GET property API: status={r.status_code}")
                        lines.append(f"  response: {r.text[:300]}")
                    except Exception as e:
                        lines.append(f"  GET lỗi: {e}")

                    # 6. Thử PATCH set relation
                    if g_id:
                        try:
                            patch_body = {ttd_key: {"relation": [{"id": g_id}]}}
                            lines.append(f"\n🔨 Thử PATCH: {json.dumps(patch_body, ensure_ascii=False)[:200]}")
                            ok, res = update_page_properties(target_id, patch_body)
                            if ok:
                                lines.append("  ✅ PATCH thành công!")
                            else:
                                lines.append(f"  ❌ PATCH thất bại: {json.dumps(res, ensure_ascii=False, default=str)[:300]}")
                        except Exception as e:
                            lines.append(f"  ❌ PATCH exception: {e}")

                        # 7. Thử với format có dấu gạch ngang
                        g_id_fmt = g_id.replace("-", "")
                        if len(g_id_fmt) == 32:
                            g_id_dashed = f"{g_id_fmt[:8]}-{g_id_fmt[8:12]}-{g_id_fmt[12:16]}-{g_id_fmt[16:20]}-{g_id_fmt[20:]}"
                            if g_id_dashed != g_id:
                                try:
                                    patch_body2 = {ttd_key: {"relation": [{"id": g_id_dashed}]}}
                                    lines.append(f"\n🔨 Thử PATCH (dashed): {g_id_dashed[:20]}...")
                                    ok2, res2 = update_page_properties(target_id, patch_body2)
                                    if ok2:
                                        lines.append("  ✅ PATCH (dashed) thành công!")
                                    else:
                                        lines.append(f"  ❌ PATCH (dashed) thất bại: {json.dumps(res2, ensure_ascii=False, default=str)[:300]}")
                                except Exception as e:
                                    lines.append(f"  ❌ PATCH (dashed) exception: {e}")

            except Exception as e:
                lines.append(f"❌ Lỗi: {e}")
                traceback.print_exc()

            # Chia nhỏ nếu quá dài
            msg_text = "\n".join(lines)
            if len(msg_text) > 3500:
                send_long_text(chat_id, msg_text)
            else:
                send_telegram(chat_id, msg_text)
            return

        # Route pending DAO
        _pending = pending_confirm.get(str(chat_id))
        if _pending and isinstance(raw, str) and _pending.get("type", "").startswith("dao_"):
            try:
                process_pending_selection_for_dao(chat_id, raw)
            except Exception:
                traceback.print_exc()
                send_telegram(chat_id, "❌ Lỗi khi xử lý thao tác đang chờ.")
            return

        # Route pending SWITCH ON/OFF confirm
        if _pending and isinstance(raw, str) and _pending.get("type", "").startswith("switch_"):
            try:
                process_pending_switch(chat_id, raw)
            except Exception:
                traceback.print_exc()
                send_telegram(chat_id, "❌ Lỗi khi xử lý thao tác ON/OFF đang chờ.")
            return

        # Pending confirm (mark / archive)
        if str(chat_id) in pending_confirm:
            if low in ("/cancel", "cancel", "hủy", "huy"):
                stop_waiting_animation(chat_id)
                pending_confirm.pop(str(chat_id), None)
                send_telegram(chat_id, "Đã hủy thao tác đang chờ.")
                return

            pc = pending_confirm[str(chat_id)]
            if pc.get("type") in ("dao_choose", "dao_confirm"):
                threading.Thread(
                    target=process_pending_selection_for_dao,
                    args=(chat_id, raw),
                    daemon=True
                ).start()
                return

            if pc.get("type") in ("switch_on_confirm", "switch_off_confirm"):
                threading.Thread(
                    target=process_pending_switch,
                    args=(chat_id, raw),
                    daemon=True
                ).start()
                return

            threading.Thread(
                target=process_pending_selection,
                args=(chat_id, raw),
                daemon=True
            ).start()
            return

        # Cancel khi không có pending
        if low in ("/cancel", "cancel", "hủy", "huy"):
            stop_waiting_animation(chat_id)
            send_telegram(chat_id, "Không có thao tác đang chờ. /cancel ignored.")
            return

        # Phân tích lệnh
        keyword, count, action = parse_user_command(raw)
        kw = keyword
        low_raw = raw.strip().lower()

        # ===== SWITCH ON / OFF → PREVIEW + CHỜ /OK =====
        if low_raw.endswith(" on"):
            threading.Thread(
                target=preview_switch_on,
                args=(chat_id, kw),
                daemon=True
            ).start()
            return

        if low_raw.endswith(" off"):
            threading.Thread(
                target=preview_switch_off,
                args=(chat_id, kw),
                daemon=True
            ).start()
            return

        # --- AUTO-MARK ---
        if action == "mark" and count > 0:
            send_telegram(chat_id, f"🎏 Đang auto tích🔄...  {kw} ")
            matches, checked, unchecked = find_calendar_data(kw)
            if not matches:
                send_telegram(chat_id, f"Không tìm thấy mục nào cho '{kw}'.")
                return

            matches.sort(key=lambda x: x[2] or "")
            selected_indices = list(range(1, min(count, len(matches)) + 1))
            res = mark_pages_by_indices(chat_id, kw, matches, selected_indices)

            if res.get("succeeded"):
                txt = "✅ ngày mới góp 📆:\n"
                for pid, title, date_iso in res["succeeded"]:
                    ds = date_iso[:10] if date_iso else "-"
                    txt += f"{ds} — {title}\n"
                send_long_text(chat_id, txt)

            if res.get("failed"):
                send_telegram(chat_id, f"⚠️ Có {len(res['failed'])} mục đánh dấu lỗi.")

            # Tính count từ data cũ — không query lại
            n_ok = len(res.get("succeeded", []))
            checked_new = checked + n_ok
            unchecked_new = unchecked - n_ok
            send_telegram(chat_id, f"💴 {kw}\n\n ✅ Đã góp: {checked_new}\n🟡 Chưa góp: {unchecked_new}")
            return

        # --- UNDO ---
        if action == "undo":
            send_telegram(chat_id, "♻️ Đang hoàn tác hành động gần nhất ...")
            threading.Thread(
                target=undo_last,
                args=(chat_id, 1),
                daemon=True
            ).start()
            return

        # --- ARCHIVE ---
        if action == "archive":
            send_telegram(chat_id, f"🗑️đang tìm để xóa ⏳...{kw} ")

            kw_norm = normalize_text(keyword)
            pages = query_database_all(NOTION_DATABASE_ID, page_size=MAX_QUERY_PAGE_SIZE)
            matches = []

            for p in pages:
                props = p.get("properties", {})
                title = extract_prop_text(props, "Name") or extract_prop_text(props, "Title") or ""
                if not title:
                    continue
                if not _match_keyword_to_title(kw_norm, title):
                    continue

                date_key = find_prop_key(props, "Ngày Góp") or find_prop_key(props, "Date")
                date_iso = None
                if date_key:
                    df = props.get(date_key, {}).get("date")
                    if df:
                        date_iso = df.get("start")
                matches.append((p.get("id"), title, date_iso, props))

            matches.sort(key=lambda x: (x[2] is None, x[2] or ""), reverse=True)

            if not matches:
                send_telegram(chat_id, f"❌ Không tìm thấy '{kw}'.")
                return

            header = f"🗑️ Chọn mục cần xóa cho '{kw}':\n\n"
            lines = []
            for i, (pid, title, date_iso, props) in enumerate(matches, start=1):
                ds = date_iso[:10] if date_iso else "-"
                lines.append(f"{i}. [{ds}] {title}")

            send_telegram(chat_id, header + "\n".join(lines))

            timer_msg = send_telegram(
                chat_id,
                f"⏳ Đang chờ bạn chọn trong {WAIT_CONFIRM}s ...\nNhập số hoặc /cancel"
            )
            timer_message_id = timer_msg.get("result", {}).get("message_id")

            pending_confirm[str(chat_id)] = {
                "type": "archive_select",
                "keyword": kw,
                "matches": matches,
                "expires": time.time() + WAIT_CONFIRM,
                "timer_message_id": timer_message_id
            }
            start_waiting_animation(chat_id, timer_message_id, WAIT_CONFIRM, interval=2.0, label="chọn mục xóa")
            return

        # --- ĐÁO ---
        if action == "dao":
            send_telegram(chat_id, f"💼 Đang xử lý đáo cho {kw} ... ⏳")

            try:
                matches = find_target_matches(kw)
            except Exception as e:
                send_telegram(chat_id, f"⚠️ Lỗi khi tìm khách: {e}")
                return

            if not matches:
                send_telegram(chat_id, f"❌ Không tìm thấy '{kw}'.")
                return

            if len(matches) > 1:
                header = f"💼 Chọn mục đáo cho '{kw}':\n\n"
                lines = []
                for i, (pid, title, props) in enumerate(matches, start=1):
                    lines.append(f"{i}. {title}")

                send_telegram(chat_id, header + "\n".join(lines))

                timer_msg = send_telegram(
                    chat_id,
                    f"⏳ Đang chờ bạn chọn trong {WAIT_CONFIRM}s ...\nGõ số (ví dụ: 1 hoặc 1-3) hoặc /cancel"
                )
                timer_message_id = timer_msg.get("result", {}).get("message_id")

                pending_confirm[str(chat_id)] = {
                    "type": "dao_choose",
                    "matches": matches,
                    "expires": time.time() + WAIT_CONFIRM,
                    "timer_message_id": timer_message_id
                }
                start_waiting_animation(chat_id, timer_message_id, WAIT_CONFIRM, interval=2.0, label="chọn đáo")
                return

            # 1 kết quả
            pid, title, props = matches[0]
            props = props if isinstance(props, dict) else {}

            try:
                can, preview = dao_preview_text_from_props(title, props)
            except Exception as e:
                can, preview = False, f"🔔 Đáo lại cho: {title}\n⚠️ Lỗi lấy preview: {e}"

            if not preview:
                preview = f"🔔 Đáo lại cho: {title}\n⚠️ Không lấy được dữ liệu preview."

            send_telegram(chat_id, preview)
            send_telegram(chat_id, f"⚠️ Gõ /ok trong {WAIT_CONFIRM}s hoặc /cancel.")

            timer_msg = send_telegram(
                chat_id,
                f"⏳ Đang chờ bạn xác nhận trong {WAIT_CONFIRM}s..."
            )
            timer_message_id = timer_msg.get("result", {}).get("message_id")

            pending_confirm[str(chat_id)] = {
                "type": "dao_confirm",
                "targets": [(pid, title, props)],
                "preview_text": preview,
                "title": title,
                "expires": time.time() + WAIT_CONFIRM,
                "timer_message_id": timer_message_id
            }
            start_waiting_animation(chat_id, timer_message_id, WAIT_CONFIRM, interval=2.0, label="xác nhận đáo")
            return

        # --- INTERACTIVE MARK MODE ---
        send_telegram(chat_id, f"🔍 Đang tìm ... 🔄 {kw} ")
        matches, checked, unchecked = find_calendar_data(kw)

        if not matches or unchecked == 0:
            msg_text = (
                f"💴 {kw}\n\n"
                f"✅ Đã góp: {checked}\n"
                f"🟡 Chưa góp: {unchecked}\n\n"
                f"💫 Không có ngày chưa góp ."
            )
            send_telegram(chat_id, msg_text)
            return

        header = f"💴 {kw}\n\n✅ Đã góp: {checked}\n🟡 Chưa góp: {unchecked}\n\n📤 ngày chưa góp /cancel.\n"
        lines = []
        for i, (pid, title, date_iso, props) in enumerate(matches, start=1):
            ds = date_iso[:10] if date_iso else "-"
            lines.append(f"{i}. [{ds}] {title} ☐")

        send_telegram(chat_id, header + "\n".join(lines))

        timer_msg = send_telegram(chat_id, f"⏳ Đang chờ chọn {WAIT_CONFIRM}s ...")
        timer_message_id = timer_msg.get("result", {}).get("message_id")

        pending_confirm[str(chat_id)] = {
            "type": "mark",
            "keyword": kw,
            "matches": matches,
            "checked": checked,
            "unchecked": unchecked,
            "expires": time.time() + WAIT_CONFIRM,
            "timer_message_id": timer_message_id
        }
        start_waiting_animation(chat_id, timer_message_id, WAIT_CONFIRM, label="chọn đánh dấu")

    except Exception as e:
        traceback.print_exc()
        send_telegram(chat_id, f"❌ Lỗi xử lý: {e}")


# =====================================================================
#  BACKGROUND: sweep expired pending
# =====================================================================
def sweep_pending_expirations():
    while True:
        try:
            now = time.time()
            keys = list(pending_confirm.keys())
            for k in keys:
                item = pending_confirm.get(k)
                if item and item.get("expires") and item["expires"] < now:
                    try:
                        send_telegram(k, "⏳ Thao tác chờ đã hết hạn.")
                    except Exception:
                        pass
                    pending_confirm.pop(k, None)
        except Exception:
            pass
        time.sleep(5)


threading.Thread(target=sweep_pending_expirations, daemon=True).start()


# =====================================================================
#  FLASK APP / WEBHOOK
# =====================================================================
app = Flask(__name__)


@app.route("/", methods=["GET"])
def index():
    return "app_consolidated running ✅"


@app.route("/telegram_webhook", methods=["POST"])
@app.route("/webhook", methods=["POST"])
def telegram_webhook():
    try:
        data = request.get_json(force=True, silent=True) or {}
    except Exception as e:
        print("❌ JSON decode error:", e)
        data = {}

    if not data:
        return jsonify({"ok": False, "error": "no data"}), 400

    message = data.get("message") or data.get("edited_message") or {}
    if not message:
        return jsonify({"ok": True})

    chat = message.get("chat", {})
    chat_id = chat.get("id")
    text_msg = message.get("text") or message.get("caption") or ""

    if chat_id and text_msg:
        threading.Thread(
            target=handle_incoming_message,
            args=(chat_id, text_msg),
            daemon=True
        ).start()

    return jsonify({"ok": True})


def auto_ping_render():
    RENDER_URL = os.getenv("RENDER_URL", "https://telegram-notion-bot-tpm2.onrender.com")
    while True:
        now_vn = datetime.now(VN_TZ)
        hour = now_vn.hour
        if 9 <= hour < 24:
            try:
                r = requests.get(RENDER_URL, timeout=10)
                print(f"[{now_vn:%H:%M:%S}] 🔄 Ping Render: {r.status_code}")
            except Exception as e:
                print(f"[{now_vn:%H:%M:%S}] ⚠️ Ping lỗi: {e}")
        else:
            print(f"[{now_vn:%H:%M:%S}] 🌙 Ngoài giờ làm việc — không ping.")
        time.sleep(300)


# =====================================================================
#  RUN
# =====================================================================
if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    print("Launching app.py (consolidated) on port", port)
    print("NOTION_DATABASE_ID:", NOTION_DATABASE_ID[:8] + "..." if NOTION_DATABASE_ID else "(none)")
    print("TARGET_NOTION_DATABASE_ID:", TARGET_NOTION_DATABASE_ID[:8] + "..." if TARGET_NOTION_DATABASE_ID else "(none)")
    print("LA_NOTION_DATABASE_ID:", LA_NOTION_DATABASE_ID[:8] + "..." if LA_NOTION_DATABASE_ID else "(none)")
    print("TELEGRAM_TOKEN set?:", bool(TELEGRAM_TOKEN))
    threading.Thread(target=auto_ping_render, daemon=True).start()
    app.run(host="0.0.0.0", port=port)
