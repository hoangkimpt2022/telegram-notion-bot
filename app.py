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
import re
import os
import time
import re
import math
import json
import traceback
import threading
import requests
import unicodedata
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional, Tuple
from flask import Flask, request, jsonify
# ===== Switch ON/OFF plugin =====

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
def send_telegram(chat_id, text, parse_mode=None):
    """
    Trả về dict response của Telegram nếu thành công (data),
    hoặc {} nếu lỗi. Caller phải lấy message_id = data.get('result',{}).get('message_id')
    """
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
    """Trả về dict / {} nếu lỗi"""
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

def start_waiting_animation(chat_id: int, message_id: int, duration: int = 120, interval: float = 2.0, label: str = "đang chờ"):
    """
    Hiển thị emoji động trong suốt thời gian chờ confirm (ví dụ 120s).
    """
    def animate():
        start_time = time.time()
        emojis = ["🔄", "💫", "✨", "🌙", "🕒", "⏳"]
        idx = 0
        while time.time() - start_time < duration:
            try:
                text = f"{emojis[idx % len(emojis)]} Đang chờ {label}... ({int(time.time() - start_time)}s/{duration}s)"
                edit_telegram_message(chat_id, message_id, text)
                time.sleep(interval)
                idx += 1
            except Exception as e:
                print("⚠️ animation error:", e)
                break

        # khi hết 120s thì cập nhật thông báo hết hạn
        try:
            edit_telegram_message(chat_id, message_id, "⏳ Thao tác chờ đã hết hạn.")
        except Exception as e:
            print("⚠️ lỗi khi gửi thông báo hết hạn:", e)

    threading.Thread(target=animate, daemon=True).start()
    
# STOP ANIMATION
def stop_waiting_animation(chat_id):
    """
    Đặt expires về 0 → animation loop dừng ngay lập tức.
    Nếu animation không chạy → không lỗi.
    """
    key = str(chat_id)
    if key in pending_confirm:
        pending_confirm[key]["expires"] = 0

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
def get_page(page_id: str):
    """Retrieve full Notion page (pages.retrieve)."""
    if not NOTION_TOKEN or not page_id:
        print("get_page missing config")
        return {}

    try:
        url = f"https://api.notion.com/v1/pages/{page_id}"
        r = requests.get(url, headers=NOTION_HEADERS, timeout=15)
        if r.status_code != 200:
            print("get_page failed:", r.status_code, r.text)
            return {}
        return r.json()
    except Exception as e:
        print("get_page exception:", e)
        return {}
def now_vn_date():
    return (datetime.utcnow() + timedelta(hours=7)).date().isoformat()

pending_switch_on = {}  # chat_id -> payload

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

def unarchive_page(page_id: str) -> Tuple[bool, str]:
    if not NOTION_TOKEN or not page_id:
        return False, "Notion config missing"
    url = f"https://api.notion.com/v1/pages/{page_id}"
    body = {"archived": False}
    return _notion_patch(url, body)

def update_page_properties(page_id: str, properties: Dict[str, Any]) -> Tuple[bool, Any]:
    if not NOTION_TOKEN or not page_id:
        return False, "Notion config missing"
    url = f"https://api.notion.com/v1/pages/{page_id}"
    body = {"properties": properties}
    return _notion_patch(url, body)

def update_checkbox(page_id: str, checked: bool) -> Tuple[bool, Any]:
    if not NOTION_TOKEN or not page_id:
        return False, "Notion config missing"
    # Giả sử checkbox key là "Đã Góp" – nếu khác, cần query lại props để find key
    # Để đơn giản, giả sử luôn dùng "Đã Góp"
    properties = {"Đã Góp": {"checkbox": checked}}
    return update_page_properties(page_id, properties)

# ------------- UTIL: property extraction & parsing -------------
def normalize_text(s: Optional[str]) -> str:
    if not s:
        return ""
    s = str(s).strip().lower()
    nf = unicodedata.normalize("NFD", s)
    return "".join(c for c in nf if unicodedata.category(c) != "Mn")

def tokenize_title(title: str) -> List[str]:
    """Chuẩn hoá và tách title thành tokens alnum (loại bỏ dấu, lowercase)."""
    if not title:
        return []
    t = normalize_text(title)  # remove diacritics + lowercase
    tokens = re.split(r'[^a-z0-9]+', t)
    return [x for x in tokens if x]

def normalize_gcode(token: str) -> str:
    """
    Chuẩn hoá mã kiểu Gxxx: 'g024' -> 'g24', 'G004' -> 'g4'.
    Nếu không phải dạng G<number> thì trả lại token gốc.
    """
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
def find_target_matches(keyword: str, db_id: str = TARGET_NOTION_DATABASE_ID):
    """
    Tìm khách trong TARGET DB:
    - Nếu keyword dạng Gxxx (g024, g24…) → so theo token normalize_gcode.
    - Nếu keyword là text (tam) → match theo token.
    - Tên kiểu G024-tam14-xxxx → đều match.
    """
    if not db_id:
        return []

    kw = normalize_text(keyword).strip()
    pages = query_database_all(db_id, page_size=MAX_QUERY_PAGE_SIZE)
    out = []

    is_gcode = bool(re.match(r'^g[0-9]+$', kw))
    kw_g = normalize_gcode(kw) if is_gcode else None

    for p in pages:
        props = p.get("properties", {})
        title = extract_prop_text(props, "Name") or extract_prop_text(props, "Title") or ""
        if not title:
            continue

        title_clean = normalize_text(title)
        tokens = tokenize_title(title)

        matched = False

        # 1) exact match
        if title_clean == kw:
            matched = True

        # 2) gcode logic
        if not matched and is_gcode:
            for tk in tokens:
                if normalize_gcode(tk) == kw_g:
                    matched = True
                    break

        # 3) text token logic
        if not matched and not is_gcode:
            for tk in tokens:
                if kw in tk:
                    matched = True
                    break

        # 4) fallback: title startswith keyword-
        if not matched and title_clean.startswith(kw + "-"):
            matched = True

        if matched:
            out.append((p.get("id"), title, props))

    return out
# Replace your existing handle_switch_on with this implementation:
def handle_switch_on(chat_id: int, keyword: str):
    try:
        # find target
        matches = find_target_matches(keyword)
        if not matches:
            _safe_send(chat_id, f"❌ Không tìm thấy {keyword}")
            return

        page_id, title, _ = matches[0]
        page = get_page(page_id)
        props = page["properties"]


        # get full page props (must be full properties for find_prop_key to work)
        page = get_page(page_id)
        props = page.get("properties", {})

        # resolve property ids (property keys)
        status_key   = find_prop_key(props, "trạng thái")
        ngay_dao_key = find_prop_key(props, "Ngày Đáo")
        qdt_key      = find_prop_key(props, "Tổng Quan Đầu Tư")
        ttd_key      = find_prop_key(props, "Tổng Thụ Động")

        # start telegram animated message
        msg = _safe_send(chat_id, f"🔄 Đang bật ON cho {title} ...")
        mid = _extract_mid(msg)

        # update target page once (property ids)
        upd = {
            status_key: {"select": {"name": "In progress"}},
            ngay_dao_key: {"date": {"start": now_vn_date()}},
        }
        # try to get relation page ids from env
        qdt_pid = os.getenv("SWITCH_QDT_PAGE_ID")
        ttd_pid = os.getenv("SWITCH_TTD_PAGE_ID")
        if qdt_pid:
            upd[qdt_key] = {"relation": [{"id": qdt_pid}]}
        else:
            upd[qdt_key] = {"relation": []}
            _safe_edit(chat_id, mid, "⚠️ Warning: SWITCH_QDT_PAGE_ID not set — 'Tổng Quan Đầu Tư' not linked.")
        if ttd_pid:
            upd[ttd_key] = {"relation": [{"id": ttd_pid}]}
        else:
            upd[ttd_key] = {"relation": []}
            _safe_edit(chat_id, mid, "⚠️ Warning: SWITCH_TTD_PAGE_ID not set — 'Tổng Thụ Động' not linked.")

        try:
            update_page_properties(page_id, upd)
        except Exception as e:
            # still continue but warn
            _safe_edit(chat_id, mid, f"⚠️ Cảnh báo khi cập nhật trang: {e}")

        # read numeric fields (exact mapping)
        try:
            total_money = int(parse_money_from_text(extract_prop_text(props, "tiền") or "0"))
        except Exception:
            total_money = int(extract_prop_text(props, "tiền") or 0)
        try:
            g_ngay = int(parse_money_from_text(extract_prop_text(props, "G ngày") or "0"))
        except Exception:
            g_ngay = int(extract_prop_text(props, "G ngày") or 0)
        try:
            total_gop = int(extract_prop_text(props, "tổng ngày g") or 0)
        except Exception:
            total_gop = int(extract_prop_text(props, "tổng ngày g") or 0)
        try:
            take_days = int(extract_prop_text(props, "ngày trước") or 0)
        except Exception:
            take_days = int(extract_prop_text(props, "ngày trước") or 0)
        try:
            truoc_val = int(parse_money_from_text(extract_prop_text(props, "trước") or "0"))
        except Exception:
            truoc_val = int(extract_prop_text(props, "trước") or 0)
        try:
            ck_val = int(parse_money_from_text(extract_prop_text(props, "CK") or "0"))
        except Exception:
            ck_val = int(extract_prop_text(props, "CK") or 0)

        # Prepare day list
        start_date = (datetime.utcnow() + timedelta(hours=7)).date()
        days = [(start_date + timedelta(days=i)) for i in range(take_days)]

        # Animation: spinner + per-day progress bar while creating pages
        spinner = ["⠋","⠙","⠚","⠞","⠖","⠦","⠴","⠲"]
        bar_len = 10
        created_pages = []

        for i, d in enumerate(days, start=1):
            # create the page in calendar DB
            try:
                res = create_page_in_db(NOTION_DATABASE_ID, {
                    "Name": {"title": [{"text": {"content": title}}]},
                    "Ngày Góp": {"date": {"start": d.isoformat()}},
                    "Tiền": {"number": g_ngay},
                    "Đã Góp": {"checkbox": True},
                    "Lịch G": {"relation": [{"id": page_id}]},
                })
                # if your create returns dict with id, append; adapt if create_page_in_db returns different shape
                if isinstance(res, dict) and res.get("id"):
                    created_pages.append(res["id"])
                elif isinstance(res, tuple) and len(res) >= 2 and isinstance(res[1], dict) and res[1].get("id"):
                    created_pages.append(res[1]["id"])
            except Exception as e:
                # don't fail whole flow; log and continue
                print("WARN create page:", e)

            # build progress bar & spinner
            pct = int((i / max(1, take_days)) * 100)
            filled = int((i / max(1, take_days)) * bar_len)
            bar = "█" * filled + "░" * (bar_len - filled)
            spin = spinner[i % len(spinner)]
            _safe_edit(chat_id, mid, f"{spin} 📆 {i}/{take_days} — {d.isoformat()}\nProgress: [{bar}] {pct}%")
            time.sleep(0.14)

        # push undo record
        try:
            undo_stack.setdefault(str(chat_id), []).append({
                "action": "switch_on",
                "target_id": page_id,
                "snapshot": {
                    status_key: props.get(status_key),
                    ngay_dao_key: props.get(ngay_dao_key),
                    qdt_key: props.get(qdt_key),
                    ttd_key: props.get(ttd_key),
                },
                "created_pages": created_pages,
            })
        except Exception:
            print("WARN: cannot push undo_stack")

        # Final report (exact format you requested)
        lines = []
        lines.append(f"🔔 Đã bật ON cho: {title}")
        lines.append(f"với số tiền {total_money:,} ngày {g_ngay:,} góp {total_gop} ngày")
        lines.append(f"💴 Lấy trước: {take_days} ngày {g_ngay:,} là {truoc_val:,}")
        lines.append("   ( từ hôm nay):")
        for idx, d in enumerate(days, start=1):
            lines.append(f"{idx}. {d.isoformat()}")
        lines.append("")
        lines.append(f"🏛️ Tổng CK: ✅ {ck_val:,}")
        lines.append(f"📆 Đến ngày {(start_date + timedelta(days=take_days)).strftime('%d-%m-%Y')} bắt đầu góp lại")
        lines.append("")
        lines.append("🎉 Hoàn tất ON.")
        _safe_edit(chat_id, mid, "\n".join(lines))

    except Exception as e:
        traceback.print_exc()
        _safe_edit(chat_id, None, f"❌ Lỗi khi bật ON: {e}")

def handle_switch_off(chat_id: int, keyword: str):
    try:
        matches = find_target_matches(keyword)
        if not matches:
            send_telegram(chat_id, f"❌ Không tìm thấy {keyword}")
            return

        page_id, title, _ = matches[0]
        page = get_page(page_id)
        props = page["properties"]

        # get full page
        page = get_page(page_id)
        props = page.get("properties", {})

        # resolve property ids
        status_key = find_prop_key(props, "trạng thái")
        ngay_xong_key = find_prop_key(props, "ngày xong")
        qdt_key = find_prop_key(props, "Tổng Quan Đầu Tư")
        ttd_key = find_prop_key(props, "Tổng Thụ Động")

        # start message
        msg = send_telegram(chat_id, f"⏳ Đang OFF {title} ...")
        mid = msg.get("result", {}).get("message_id")

        # find related calendar pages
        all_pages = query_database_all(NOTION_DATABASE_ID)
        related_ids = []
        for p in all_pages:
            rels = p.get("properties", {}).get("Lịch G", {}).get("relation", [])
            if any(r.get("id") == page_id for r in rels):
                related_ids.append(p.get("id"))

        total = len(related_ids)
        bar_len = 10

        # animation delete
        for i, pid in enumerate(related_ids, start=1):
            try:
                archive_page(pid)
            except Exception as e:
                print("WARN archive_page:", e)

            filled = int((i / max(1, total)) * bar_len)
            bar = "█" * filled + "░" * (bar_len - filled)

            edit_telegram_message(
                chat_id,
                mid,
                f"🗑️ {i}/{total} [{bar}]"
            )
            time.sleep(0.12)

        # read CK (lãi)
        try:
            ck_val = int(parse_money_from_text(extract_prop_text(props, "CK") or "0"))
        except Exception:
            ck_val = int(extract_prop_text(props, "CK") or 0)

        # create lãi page if any
        if ck_val > 0:
            try:
                create_lai_page(chat_id, title, ck_val, page_id)
            except Exception as e:
                print("WARN create_lai_page:", e)

        # update target page: Done + clear relations
        update_page_properties(page_id, {
            status_key: {"select": {"name": "Done"}},
            ngay_xong_key: {"date": {"start": now_vn_date()}},
            qdt_key: {"relation": []},
            ttd_key: {"relation": []},
        })

        edit_telegram_message(
            chat_id,
            mid,
            f"✅ Đã OFF {title}\n"
            f"💰 Lãi tạo: {ck_val:,}\n\n"
            f"| Cột | Giá trị |\n"
            f"| --- | --- |\n"
            f"| trạng thái | Done |\n"
            f"| ngày xong | Hôm nay |\n"
            f"| Tổng Quan Đầu Tư | clear |\n"
            f"| Tổng Thụ Động | clear |"
        )

        # undo log
        undo_stack.setdefault(str(chat_id), []).append({
            "action": "switch_off",
            "page_id": page_id,
            "deleted_pages": related_ids,
        })

    except Exception as e:
        traceback.print_exc()
        send_telegram(chat_id, f"❌ Lỗi OFF: {e}")

def find_calendar_matches(keyword: str):
    """
    MATCH linh hoạt trong NOTION_DATABASE_ID:
    - Tìm theo mã Gxxx (normalize G024 → g24)
    - Tìm theo tên (tam → match tam, tam14, tam-xxx…)
    - Tự động loại bỏ page đã tích Đã Góp
    """
    if not NOTION_DATABASE_ID:
        return []

    kw = normalize_text(keyword)
    is_gcode = bool(re.match(r'^g[0-9]+$', kw))
    kw_g = normalize_gcode(kw) if is_gcode else None

    pages = query_database_all(NOTION_DATABASE_ID, page_size=MAX_QUERY_PAGE_SIZE)
    matches = []

    for p in pages:
        props = p.get("properties", {})
        title = extract_prop_text(props, "Name") or extract_prop_text(props, "Title") or ""
        if not title:
            continue

        title_clean = normalize_text(title)
        tokens = tokenize_title(title)

        matched = False

        if title_clean == kw:
            matched = True

        if not matched and is_gcode:
            for tk in tokens:
                if normalize_gcode(tk) == kw_g:
                    matched = True
                    break

        if not matched and not is_gcode:
            for tk in tokens:
                if kw in tk:
                    matched = True
                    break

        if not matched and title_clean.startswith(kw + "-"):
            matched = True

        if not matched:
            continue

        # Bỏ page đã tích
        cb_key = (
            find_prop_key(props, "Đã Góp")
            or find_prop_key(props, "Sent")
            or find_prop_key(props, "Status")
        )
        if cb_key and props.get(cb_key, {}).get("checkbox"):
            continue

        date_iso = None
        date_key = find_prop_key(props, "Ngày Góp")
        if date_key:
            df = props.get(date_key, {}).get("date")
            if df:
                date_iso = df.get("start")

        matches.append((p.get("id"), title, date_iso, props))

    matches.sort(key=lambda x: (x[2] is None, x[2] or ""))
    return matches

def find_matching_all_pages_in_db(database_id: str, keyword: str, limit: int = 2000):
    if not database_id:
        return []

    kw = normalize_text(keyword)
    is_gcode = bool(re.match(r'^g[0-9]+$', kw))
    kw_g = normalize_gcode(kw) if is_gcode else None

    pages = query_database_all(database_id, page_size=MAX_QUERY_PAGE_SIZE)
    out = []

    for p in pages:
        props = p.get("properties", {})
        title = extract_prop_text(props, "Name") or extract_prop_text(props, "Title") or ""
        if not title:
            continue

        title_clean = normalize_text(title)
        tokens = tokenize_title(title)

        matched = False

        # exact match
        if title_clean == kw:
            matched = True

        # gcode
        if not matched and is_gcode:
            for tk in tokens:
                if normalize_gcode(tk) == kw_g:
                    matched = True
                    break

        # text search
        if not matched and not is_gcode:
            for tk in tokens:
                if kw in tk:
                    matched = True
                    break

        if not matched and title_clean.startswith(kw + "-"):
            matched = True

        if not matched:
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


# ------------- DAO preview & calculations -------------
def dao_preview_text_from_props(title: str, props: dict):
    """
    Trả về (can: bool, preview: str)
    - can=True nếu có thể thực hiện (✅)
    - preview: text để gửi (luôn có giá trị string)
    """
    try:
        dao_text = extract_prop_text(props, "Đáo/thối") or extract_prop_text(props, "Đáo") or ""
        total_val = parse_money_from_text(dao_text) or 0
        per_day = parse_money_from_text(
            extract_prop_text(props, "G ngày") or extract_prop_text(props, "Gngày") or ""
        ) or 0

        raw_days = extract_prop_text(props, "ngày trước")
        try:
            days_before = int(float(raw_days)) if raw_days not in (None, "", "None") else 0
        except:
            days_before = 0

        # 🔴 cannot
        if "🔴" in dao_text:
            return False, f"🔔 Chưa thể đáo cho 🔴: {title} ."

        # ✅ can
        if "✅" in dao_text:
            if not days_before or days_before <= 0:
                tomorrow = (datetime.utcnow() + timedelta(hours=7)).date() + timedelta(days=1)
                restart = tomorrow.strftime("%d-%m-%Y")
                msg = (
                    f"🔔 đáo lại cho: {title} - Tổng CK: ✅ {int(total_val)}\n\n"
                    f"💴 Không Lấy trước\n"
                    f"📆 ngày mai Bắt đầu góp lại \n"
                    f"{restart}"
                )
                props["ONLY_LAI"] = True
                return True, msg

            take_days = int(days_before)
            total_pre = int(per_day * take_days) if per_day else 0
            start = (datetime.utcnow() + timedelta(hours=7)).date() + timedelta(days=1)
            date_list = [(start + timedelta(days=i)).isoformat() for i in range(take_days)]
            restart_date = (start + timedelta(days=take_days)).strftime("%d-%m-%Y")

            lines = [
                f"🔔 Đáo lại cho: {title} ",
                f"💴 Lấy trước: {take_days} ngày {int(per_day)} là {total_pre}",
                f"   ( từ ngày mai):",
            ]
            for idx, d in enumerate(date_list, start=1):
                lines.append(f"{idx}. {d}")
            lines.append(f"\n🏛️ Tổng CK: ✅ {int(total_val)}")         
            lines.append(f"📆 Đến ngày {restart_date} bắt đầu góp lại")              
            return True, "\n".join(lines)

        # fallback
        msg = f"🔔 đáo lại cho: {title} - Tổng CK: {int(total_val)}\n\nKhông Lấy trước\n\nGửi /ok để chỉ tạo Lãi."
        props["ONLY_LAI"] = True
        return True, msg

    except Exception as e:
        print("dao_preview_text_from_props error:", e)
        return False, f"Preview error: {e}"

# ------------- ACTIONS: mark / undo -------------
def count_checked_unchecked(keyword: str) -> Tuple[int, int]:
    results = query_database_all(NOTION_DATABASE_ID, page_size=MAX_QUERY_PAGE_SIZE)
    checked = 0
    unchecked = 0

    # chuẩn hoá keyword
    kw_clean = normalize_text(keyword)

    for p in results:
        props = p.get("properties", {})
        title = extract_prop_text(props, "Name") or ""
        title_clean = normalize_text(title)

        # 🔒 chỉ match chính xác tên (không chứa chuỗi con)
        parts = title_clean.split('-')
        if kw_clean in [p.strip() for p in parts] or title_clean == kw_clean:
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
            else:
                failed.append((pid, res))
        except Exception as e:
            failed.append((pid, str(e)))
    if succeeded:
        undo_stack.setdefault(str(chat_id), []).append({"action": "mark", "pages": [p[0] for p in succeeded]})
    return {"ok": len(failed) == 0, "succeeded": succeeded, "failed": failed}

def undo_last(chat_id: str, count: int = 1):
    """
    Hoàn tác hành động gần nhất.
    Hỗ trợ:
        - mark          → bỏ check
        - archive       → unarchive
        - dao (lấy trước)
        - dao (không lấy trước)
    """
    chat_key = str(chat_id)

    if not undo_stack.get(chat_key):
        send_telegram(chat_id, "❌ Không có hành động nào để hoàn tác.")
        return

    log = undo_stack[chat_key].pop()
    if not log:
        send_telegram(chat_id, "❌ Không có dữ liệu undo.")
        return

    action = log.get("action")

    # ---------------------------------------------------------
    # 1) UNDO — MARK / ARCHIVE (logic cũ)
    # ---------------------------------------------------------
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

    # ---------------------------------------------------------
    # 2) UNDO — ĐÁO (LẤY TRƯỚC / KHÔNG LẤY TRƯỚC)
    # ---------------------------------------------------------
    if action == "dao":
        created_pages = log.get("created_pages", [])
        archived_pages = log.get("archived_pages", [])
        lai_page = log.get("lai_page")

        send_telegram(chat_id, "♻️ Đang hoàn tác đáo...")

        # --- A) Xóa các ngày mới tạo (nếu có)
        for pid in created_pages:
            try:
                archive_page(pid)
            except Exception as e:
                print("Undo dao — delete created_page lỗi:", e)

        # --- B) Xóa page LÃI nếu có
        if lai_page:
            try:
                archive_page(lai_page)
            except Exception as e:
                print("Undo dao — delete lai_page lỗi:", e)

        # --- C) Khôi phục lại những ngày cũ đã archive
        for pid in archived_pages:
            try:
                unarchive_page(pid)
            except Exception as e:
                print("Undo dao — restore old_day lỗi:", e)

        send_telegram(chat_id, "✅ Hoàn tác đáo thành công.")
        return

    # ---------------------------------------------------------
    # 3) FALLBACK — không xác định được loại undo
    # ---------------------------------------------------------
    send_telegram(chat_id, f"⚠️ Không hỗ trợ undo cho action '{action}'.")

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
        if deleted:
            undo_stack.setdefault(str(chat_id), []).append({"action": "archive", "pages": deleted})
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
     - Lai = lấy số tiền từ cột "Lai lich g" bên TARGET_NOTION_DATABASE_ID
     - ngày lai = ngày hôm nay
     - Lịch G = relation trỏ về page gốc
    """
    try:
        today = datetime.now().date().isoformat()

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
            send_telegram(chat_id, f"⚠️ Tạo Lãi lỗi: {r.status_code} - {r.text}")
            return None

    except Exception as e:
        send_telegram(chat_id, f"❌ Lỗi tạo Lãi cho {title}: {str(e)}")
        return None


# ------------- DAO flow (xóa + tạo pages + create lai) -------------
def dao_create_pages_from_props(chat_id: int, source_page_id: str, props: Dict[str, Any]):
    """
    Tiến trình đáo:
    - Nếu KHÔNG LẤY TRƯỚC: chỉ xóa ngày + tạo Lãi (không tạo ngày mới)
    - Nếu CÓ LẤY TRƯỚC: giữ nguyên logic đáo đầy đủ
    """

    try:
        # -----------------------------------------
        # LẤY DỮ LIỆU
        # -----------------------------------------
        title = extract_prop_text(props, "Name") or "UNKNOWN"
        total_val = parse_money_from_text(extract_prop_text(props, "Đáo/thối")) or 0
        per_day = parse_money_from_text(extract_prop_text(props, "G ngày")) or 0
        days_before = parse_money_from_text(extract_prop_text(props, "ngày trước")) or 0
        pre_amount = parse_money_from_text(extract_prop_text(props, "trước")) or 0

        # -----------------------------------------
        # TẠO HÀM UPDATE CHUNG
        # -----------------------------------------
        start_msg = send_telegram(chat_id, f"⏳ Đang xử lý đáo cho '{title}' ...")
        message_id = start_msg.get("result", {}).get("message_id")

        def update(text):
            if message_id:
                try:
                    edit_telegram_message(chat_id, message_id, text)
                    return
                except:
                    pass
            send_telegram(chat_id, text)

        # -----------------------------------------
        # 0️⃣ — NHÁNH KHÔNG LẤY TRƯỚC
        # -----------------------------------------
        if pre_amount == 0:
            update(
                f"🔔 Đáo lại cho: {title}\n"
                f"🏛️ Tổng CK: {int(total_val)}\n"
                f"💴 Không Lấy Trước."
            )
            time.sleep(0.4)

            # --- TÌM CÁC PAGE NGÀY CỦA KHÁCH ---
            all_pages = query_database_all(NOTION_DATABASE_ID, page_size=500)
            kw = title.strip().lower()
            children = []

            for p in all_pages:
                props_p = p.get("properties", {})
                name_p = extract_prop_text(props_p, "Name") or ""
                if kw in name_p.lower():
                    children.append(p.get("id"))

            total = len(children)

            # --- XÓA NGÀY CŨ ---
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

            # --- TẠO LÃI ---
            lai_text = (
                extract_prop_text(props, "Lai lịch g")
                or extract_prop_text(props, "Lãi")
                or extract_prop_text(props, "Lai")
                or ""
            )
            lai_amt = parse_money_from_text(lai_text) or 0

            if LA_NOTION_DATABASE_ID and lai_amt > 0:
                create_lai_page(chat_id, title, lai_amt, source_page_id)
                update(f"💰 Đã tạo Lãi cho {title}.")
            else:
                update("ℹ️ Không có giá trị Lãi hoặc chưa cấu hình LA_NOTION_DATABASE_ID.")

            update("🎉 Hoàn thành đáo — KHÔNG LẤY TRƯỚC.")
            # --- GHI LOG UNDO CHO CHẾ ĐỘ KHÔNG LẤY TRƯỚC ---
            undo_stack.setdefault(str(chat_id), []).append({
                "action": "dao",
                "archived_pages": matched,      # các ngày bạn vừa xóa
                "created_pages": [],            # không tạo ngày mới
                "lai_page": lai_page_id if 'lai_page_id' in locals() else None
            })

            return

        # -----------------------------------------
        # 1️⃣ — NHÁNH LẤY TRƯỚC (GIỮ NGUYÊN LOGIC)
        # -----------------------------------------

        # Tính số ngày cần tạo
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

        # -----------------------------------------
        # XÓA NGÀY CŨ
        # -----------------------------------------
        all_pages = query_database_all(NOTION_DATABASE_ID, page_size=500)
        kw = title.strip().lower()
        matched = []

        for p in all_pages:
            props_p = p.get("properties", {})
            name_p = extract_prop_text(props_p, "Name") or ""
            if kw in name_p.lower():
                matched.append(p.get("id"))

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

        # -----------------------------------------
        # TẠO NGÀY MỚI
        # -----------------------------------------
        VN_TZ = timezone(timedelta(hours=7))
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

        # -----------------------------------------
        # TẠO LÃI
        # -----------------------------------------
        lai_text = (
            extract_prop_text(props, "Lai lịch g")
            or extract_prop_text(props, "Lãi")
            or extract_prop_text(props, "Lai")
            or ""
        )
        lai_amt = parse_money_from_text(lai_text) or 0

        # Lưu id trang lãi vào biến để undo được
        if LA_NOTION_DATABASE_ID and lai_amt > 0:
            lai_page_id = create_lai_page(chat_id, title, lai_amt, source_page_id)
            send_telegram(chat_id, f"💰 Đã tạo Lãi cho {title}.")
        else:
            lai_page_id = None
            send_telegram(chat_id, "ℹ️ Không có giá trị Lãi hoặc chưa cấu hình LA_NOTION_DATABASE_ID.")

        send_telegram(chat_id, "🎉 Hoàn tất đáo vào đặt lại Repeat every day liền!")

        # --- GHI LOG UNDO CHO CHẾ ĐỘ LẤY TRƯỚC ---
        undo_stack.setdefault(str(chat_id), []).append({
            "action": "dao",
            "archived_pages": matched,                       # các ngày cũ đã xoá
            "created_pages": [p.get("id") for p in created], # các ngày mới tạo
            "lai_page": lai_page_id                          # ID trang Lãi đã tạo
        })

    except Exception as e:
        send_telegram(chat_id, f"❌ Lỗi tiến trình đáo cho {title}: {e}")
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
    """
    Xử lý xác nhận đáo:
    - dao_choose  → người dùng chọn khách (1, 1-2…)
    - dao_confirm → người dùng gõ /ok hoặc /cancel
    """
    key = str(chat_id)
    data = pending_confirm.get(key)

    if not data:
        send_telegram(chat_id, "⚠️ Không có thao tác đáo nào đang chờ.")
        return

    # =========================================================
    # 1) PHẦN CHỌN DANH SÁCH (dao_choose)
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

                # lấy preview an toàn
                try:
                    can, pv = dao_preview_text_from_props(title, props)
                except Exception as e:
                    pv = f"🔔 Đáo lại cho: {title}\n⚠️ Preview lỗi: {e}"
                previews.append(pv)

        agg_title = ", ".join([t for (_, t, _) in selected])
        agg_preview = "\n\n".join(previews)

        send_telegram(
            chat_id,
            f"🔔 Đáo lại cho: {agg_title}\n\n{agg_preview}"
        )

        ok_msg = send_telegram(
            chat_id,
            f"⚠️ Gõ /ok trong {WAIT_CONFIRM}s để xác nhận hoặc /cancel."
        )
        try:
            timer_id = ok_msg["result"]["message_id"]
        except:
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
    # 2) PHẦN XỬ LÝ /OK HOẶC /CANCEL (dao_confirm)
    # =========================================================
    if data.get("type") == "dao_confirm":

        key = str(chat_id)

        # đảm bảo token luôn tồn tại
        token = (raw or "").strip().lower()

        if not token:
            send_telegram(chat_id, "⚠️ Gửi /ok để xác nhận hoặc /cancel để hủy.")
            return

        # ---------- CANCEL ----------
        if token in ("/cancel", "cancel", "hủy", "huỷ", "huy"):

            # dừng countdown đúng cách
            try:
                data["expires"] = 0
            except:
                pass
            try:
                stop_waiting_animation(chat_id)
            except:
                pass

            pending_confirm.pop(key, None)
            send_telegram(chat_id, "❌ Đã hủy thao tác đáo.")
            return

        # ---------- KHÔNG PHẢI OK ----------
        if token not in ("ok", "/ok", "yes", "đồng ý", "dong y"):
            send_telegram(chat_id, "⚠️ Gửi /ok để xác nhận hoặc /cancel để hủy.")
            return

        # ---------- OK ----------
        # dừng countdown trước
        try:
            data["expires"] = 0
        except:
            pass
        try:
            stop_waiting_animation(chat_id)
        except:
            pass

        targets = data.get("targets") or []
        preview_text = data.get("preview_text") or ""
        title_all = data.get("title") or ""

        if not targets:
            pending_confirm.pop(key, None)
            send_telegram(chat_id, "⚠️ Không có dữ liệu để đáo.")
            return

        send_telegram(chat_id, f"✅ Đã xác nhận OK — đang xử lý đáo cho: {title_all}")

        results = []

        # =========================================================
        # XỬ LÝ TỪNG KHÁCH TRONG DANH SÁCH
        # =========================================================
        for pid, ttitle, props in targets:
            try:
                props = props if isinstance(props, dict) else {}

                # đọc giá trị cột "trước" để xác định KHÔNG LẤY TRƯỚC
                truoc_raw = extract_prop_text(props, "trước") or "0"
                try:
                    truoc_val = float(truoc_raw)
                except:
                    truoc_val = 0

                is_no_take = (truoc_val == 0)

                # chuẩn bị thông tin lãi
                lai_text = (
                    extract_prop_text(props, "Lai lịch g")
                    or extract_prop_text(props, "Lãi")
                    or extract_prop_text(props, "Lai")
                    or ""
                )
                lai_amt = parse_money_from_text(lai_text) or 0

                # =====================================================
                # CASE 1 — KHÔNG LẤY TRƯỚC → CHỈ XÓA NGÀY + TẠO LÃI
                # =====================================================
                if is_no_take:

                    # 🔍 Truy vấn trực tiếp Calendar DB để tìm ngày theo relation Lịch G
                    calendar_pages = query_database_all(NOTION_DATABASE_ID, page_size=500)
                    children = []

                    for p in calendar_pages:
                        props_p = p.get("properties", {})
                        rel_key = find_prop_key(props_p, "Lịch G")
                        if not rel_key:
                            continue

                        rel_arr = props_p.get(rel_key, {}).get("relation", [])
                        if any(r.get("id") == pid for r in rel_arr):
                            children.append(p.get("id"))

                    total = len(children)
                    msg = send_telegram(chat_id, f"🧹 Đang xóa ngày cũ của '{ttitle}' ...")
                    mid = msg.get("result", {}).get("message_id")

                    def update(text):
                        if mid:
                            try:
                                edit_telegram_message(chat_id, mid, text); return
                            except: pass
                        send_telegram(chat_id, text)

                    if total == 0:
                        update("🧹 Không có ngày nào để xóa.")
                        time.sleep(0.3)
                    else:
                        update(f"🧹 Bắt đầu xóa {total} ngày ...")
                        time.sleep(0.25)

                        for idx, day_id in enumerate(children, start=1):
                            archive_page(day_id)

                            bar = int((idx / total) * 10)
                            progress = "█" * bar + "░" * (10 - bar)
                            update(f"🧹 Xóa {idx}/{total} [{progress}]")
                            time.sleep(0.25)

                        update(f"✅ Đã xóa toàn bộ {total} ngày 🎉")
                        time.sleep(0.3)

                    # tạo Lãi
                    if LA_NOTION_DATABASE_ID and lai_amt > 0:
                        lai_page_id = create_lai_page(chat_id, ttitle, lai_amt, pid)
                        results.append((pid, ttitle, True, "Lãi only"))
                    else:
                        lai_page_id = None
                        results.append((pid, ttitle, False, "Không có lãi"))

                    # Ghi log undo cho NHÁNH KHÔNG LẤY TRƯỚC
                    undo_stack.setdefault(str(chat_id), []).append({
                        "action": "dao",
                        "archived_pages": [
                            row["id"]
                            for row in children
                            if isinstance(row, dict) and "id" in row
                        ],
                        "created_pages": [],          # không tạo ngày mới
                        "lai_page": lai_page_id
                    })
                    continue

                # =====================================================
                # CASE 2 — CÓ LẤY TRƯỚC → FULL DAO
                # =====================================================
                try:
                    dao_create_pages_from_props(chat_id, pid, props, ttitle)
                    results.append((pid, ttitle, True, "DAO Complete"))
                except TypeError:
                    dao_create_pages_from_props(chat_id, pid, props)
                    results.append((pid, ttitle, True, "DAO Fallback"))
                except Exception as e:
                    results.append((pid, ttitle, False, f"DAO error: {e}"))

            except Exception as e:
                results.append((pid, ttitle, False, f"Unhandled: {e}"))

        # =====================================================
        # REPORT
        # =====================================================
        ok = [r for r in results if r[2]]
        fail = [r for r in results if not r[2]]

        text = f"🎉 Hoàn tất đáo cho: {title_all}\n"
        text += f"✅ Thành công: {len(ok)}\n"
        if fail:
            text += f"⚠️ Lỗi: {len(fail)}\n"
            for pid_, nm, ok_, er in fail:
                text += f"- {nm}: {er}\n"

        send_telegram(chat_id, text)
        pending_confirm.pop(key, None)
        return

def process_pending_selection(chat_id: str, raw: str):
    """
    Xử lý các lựa chọn đang chờ xác nhận (MARK / ARCHIVE).
    Có hiển thị progress bar và emoji sinh động để báo tiến trình.
    """
    key = str(chat_id)
    data = pending_confirm.get(key)

    if not data:
        send_telegram(chat_id, "❌ Không có thao tác nào đang chờ.")
        return

    try:
        raw_input = raw.strip().lower()

        # 🛑 HỦY thao tác nếu người dùng gõ /cancel
        if raw_input in ("/cancel", "cancel", "hủy", "huỷ", "huy"):
            del pending_confirm[key]
            send_telegram(chat_id, "🛑 Đã hủy thao tác đang chờ.")
            return

        matches = data.get("matches", [])
        if not matches:
            send_telegram(chat_id, "⚠️ Không tìm thấy danh sách mục đang xử lý.")
            del pending_confirm[key]
            return

        indices = parse_user_selection_text(raw_input, len(matches))
        if not indices:
            send_telegram(chat_id, "⚠️ Không nhận được lựa chọn hợp lệ.")
            return

        action = data.get("type")

        # ======================================================
        # 🧹 ARCHIVE MODE — XÓA PAGE CÓ THANH BAR
        # ======================================================
        if action == "archive_select":
            selected = [matches[i - 1] for i in indices if 1 <= i <= len(matches)]
            total_sel = len(selected)
            if total_sel == 0:
                send_telegram(chat_id, "⚠️ Không có mục nào được chọn để xóa.")
                del pending_confirm[key]
                return

            msg = send_telegram(chat_id, f"🧹 Bắt đầu xóa {total_sel} mục của '{data['keyword']}' ...")
            message_id = msg.get("result", {}).get("message_id")

            deleted = []
            for idx, (pid, title, date_iso, props) in enumerate(selected, start=1):
                try:
                    ok, res = archive_page(pid)
                    if not ok:
                        send_telegram(chat_id, f"⚠️ Lỗi khi xóa {title}: {res}")
                        continue
                    deleted.append(pid)
                    # 🔄 Thanh tiến trình (10 khối)
                    bar = int((idx / total_sel) * 10)
                    progress = "█" * bar + "░" * (10 - bar)
                    percent = int((idx / total_sel) * 100)
                    new_text = f"🧹 Xóa {idx}/{total_sel} [{progress}] {percent}%"
                    edit_telegram_message(chat_id, message_id, new_text)

                    time.sleep(0.4)
                except Exception as e:
                    send_telegram(chat_id, f"⚠️ Lỗi khi xóa {idx}/{total_sel}: {e}")

            # ✅ Kết thúc
            edit_telegram_message(
                chat_id,
                message_id,
                f"✅ Hoàn tất xóa {total_sel}/{total_sel} mục của '{data['keyword']}' 🎉"
            )
            if deleted:
                undo_stack.setdefault(str(chat_id), []).append({"action": "archive", "pages": deleted})
            del pending_confirm[key]
            return

        # ======================================================
        # ✅ MARK MODE — ĐÁNH DẤU (CHECK) CÁC MỤC CHỌN
        # ======================================================
        if action == "mark":
            key = str(chat_id)
            data = pending_confirm.get(key)
            if not data:
                send_telegram(chat_id, "⚠️ Không có thao tác đang chờ.")
                return

            keyword = data.get("keyword")
            total_sel = len(indices)
            msg = send_telegram(chat_id, f"🟢 Bắt đầu đánh dấu {total_sel} mục cho '{keyword}' ...")
            message_id = msg.get("result", {}).get("message_id")

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

                            # 🔄 Thanh tiến trình
                            bar = int((len(succeeded) / total_sel) * 10)
                            progress = "█" * bar + "░" * (10 - bar)
                            percent = int((len(succeeded) / total_sel) * 100)
                            new_text = f"🟢 Đánh dấu {len(succeeded)}/{total_sel} [{progress}] {percent}%"
                            edit_telegram_message(chat_id, message_id, new_text)
                        else:
                            failed.append((pid, res))
                    except Exception as e:
                        failed.append((pid, str(e)))
                    time.sleep(0.3)

            # ✅ Kết quả cuối cùng
            result_text = f"✅ Hoàn tất đánh dấu {len(succeeded)}/{total_sel} mục 🎉"
            if failed:
                result_text += f"\n⚠️ Lỗi: {len(failed)} mục không thể cập nhật."

            # update result to the message (edit if possible)
            try:
                if message_id:
                    edit_telegram_message(chat_id, message_id, result_text)
                else:
                    send_telegram(chat_id, result_text)
            except Exception:
                send_telegram(chat_id, result_text)

            # 📊 Thống kê sau khi mark
            checked, unchecked = count_checked_unchecked(keyword)
            send_telegram(chat_id, f"💴 {keyword}\n\n📊 Đã góp: {checked}\n🟡 Chưa góp: {unchecked}")

            # ---- DỌN SẠCH pending (chỉ 1 lần, an toàn) ----
            pending_confirm.pop(key, None)
            return

        # ======================================================
        # ❓ Nếu không xác định được loại action
        # ======================================================
        send_telegram(chat_id, "⚠️ Không xác định được loại thao tác. Vui lòng thử lại.")
        del pending_confirm[key]
        return

    except Exception as e:
        traceback.print_exc()
        send_telegram(chat_id, f"❌ Lỗi xử lý lựa chọn: {e}")
        if key in pending_confirm:
            del pending_confirm[key]

# ------------- Command parsing & main handler -------------
def parse_user_command(raw: str) -> Tuple[str, int, Optional[str]]:
    """
    Phân tích lệnh Telegram: tách keyword, count, action.
    Ví dụ:
      'gam' -> ('gam', 0, None)
      'gam 2' -> ('gam', 2, 'mark')
      'gam xóa' -> ('gam', 0, 'archive')
      'gam đáo' -> ('gam', 0, 'dao')
      'undo' -> ('', 0, 'undo')
    """
    raw = raw.strip()
    if not raw:
        return "", 0, None

    parts = raw.split()
    kw = parts[0]
    count = 0
    action = None

    # --- AUTO MARK (vd: gam 2) ---
    if len(parts) > 1 and parts[1].isdigit():
        count = int(parts[1])
        action = "mark"

    # --- UNDO ---
    elif raw.lower() in ("undo", "/undo"):
        action = "undo"

    # --- ARCHIVE ---
    elif any(x in raw.lower() for x in ["xóa", "archive", "del", "delete"]):
        action = "archive"

    # --- ĐÁO ---
    elif any(x in raw.lower() for x in ["đáo", "dao", "daó", "đáo hạn"]):
        action = "dao"

    return kw, count, action

def handle_incoming_message(chat_id: int, text: str):
    """
    Main entry point for Telegram messages.
    """
    try:
        matches = []  # ✅ tránh UnboundLocalError
        kw = ""
        count = 0

        # 🔒 Giới hạn chat ID (nếu cấu hình)
        if TELEGRAM_CHAT_ID and str(chat_id) != str(TELEGRAM_CHAT_ID):
            send_telegram(chat_id, "Bot chưa được phép nhận lệnh từ chat này.")
            return

        raw = text.strip()
        if not raw:
            send_telegram(chat_id, "Vui lòng gửi lệnh hoặc từ khoá.")
            return

        low = raw.lower()
        # Nếu có thao tác đang chờ liên quan đến "dao", route tin này vào handler chuyên biệt
        _pending = pending_confirm.get(str(chat_id))
        if _pending and isinstance(raw, str) and _pending.get("type", "").startswith("dao_"):
            try:
                process_pending_selection_for_dao(chat_id, raw)
            except Exception as e:
                # tránh crash toàn bộ handler nếu handler con lỗi
                import traceback
                traceback.print_exc()
                send_telegram(chat_id, "❌ Lỗi khi xử lý thao tác đang chờ.")
            return

        # ⏳ Kiểm tra nếu đang có thao tác chờ xác nhận
        if str(chat_id) in pending_confirm:
            if low in ("/cancel", "cancel", "hủy", "huy"):
                del pending_confirm[str(chat_id)]
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

            threading.Thread(
                target=process_pending_selection, 
                args=(chat_id, raw),
                daemon=True
            ).start()
            return

        # 🧹 Hủy thao tác nếu không có gì đang chờ
        if low in ("/cancel", "cancel", "hủy", "huy"):
            try:
                stop_waiting_animation(chat_id)
            except:
                pass
            send_telegram(chat_id, "Không có thao tác đang chờ. /cancel ignored.")
            return

        # --- PHÂN TÍCH LỆNH ---
        keyword, count, action = parse_user_command(raw)
        kw = keyword  # giữ lại cho auto-mark
        if low_raw.endswith(" on"):
            threading.Thread(
                target=handle_switch_on,
                args=(chat_id, kw),
                daemon=True
            ).start()
            return

        if low_raw.endswith(" off"):
            threading.Thread(
                target=handle_switch_off,
                args=(chat_id, kw),
                daemon=True
            ).start()
            return

        # --- AUTO-MARK MODE ---
        if action == "mark" and count > 0:
            send_telegram(chat_id, f"🎏 Đang auto tích🔄...  {kw} ")
            matches = find_calendar_matches(kw)
            if not matches:
                send_telegram(chat_id, f"Không tìm thấy mục nào cho '{kw}'.")
                return

            # sắp xếp theo ngày tăng (cũ nhất trước)
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

            checked, unchecked = count_checked_unchecked(kw)
            send_telegram(chat_id, f"💴 {title}\n\n ✅ Đã góp: {checked}\n🟡 Chưa góp: {unchecked}")
            return

        # --- UNDO ---
        if action == "undo":
            # ưu tiên undo ON / OFF nếu có
            if undo_stack.get(str(chat_id)):
                threading.Thread(
                    target=undo_switch,
                    args=(chat_id,),
                    daemon=True
                ).start()
                return

            # fallback undo cũ
            send_telegram(chat_id, "♻️ Đang hoàn tác hành động gần nhất ...")
            threading.Thread(
                target=undo_last,
                args=(chat_id, 1),
                daemon=True
            ).start()
            return

        # 📦 ARCHIVE MODE — XÓA NGÀY CỤ THỂ (KHÔNG CHỒNG ANIMATION)
        if action == "archive":
            send_telegram(chat_id, f"🗑️đang tìm để xóa ⏳...{kw} ")

            kw_norm = normalize_text(keyword)
            pages = query_database_all(NOTION_DATABASE_ID, page_size=MAX_QUERY_PAGE_SIZE)
            matches = []

            # --- Lọc bằng logic token/gcode (mềm hơn, không loại trừ checked) ---
            is_gcode = bool(re.match(r'^g[0-9]+$', kw_norm))
            kw_g = normalize_gcode(kw_norm) if is_gcode else None

            for p in pages:
                props = p.get("properties", {})
                title = extract_prop_text(props, "Name") or extract_prop_text(props, "Title") or ""
                if not title:
                    continue
                title_clean = normalize_text(title)
                tokens = tokenize_title(title)

                matched = False
                # exact
                if title_clean == kw_norm:
                    matched = True
                # gcode match on tokens
                if not matched and is_gcode:
                    for tk in tokens:
                        if normalize_gcode(tk) == kw_g:
                            matched = True
                            break
                # token contains
                if not matched and not is_gcode:
                    for tk in tokens:
                        if kw_norm in tk:
                            matched = True
                            break
                # fallback startswith
                if not matched and title_clean.startswith(kw_norm + "-"):
                    matched = True

                if not matched:
                    continue

                date_key = find_prop_key(props, "Ngày Góp") or find_prop_key(props, "Date")
                date_iso = None
                if date_key:
                    df = props.get(date_key, {}).get("date")
                    if df:
                        date_iso = df.get("start")

                matches.append((p.get("id"), title, date_iso, props))

            # sort giống các chỗ khác
            matches.sort(key=lambda x: (x[2] is None, x[2] or ""), reverse=True)

            if not matches:
                send_telegram(chat_id, f"❌ Không tìm thấy '{kw}'.")
                return

            # ===== HIỂN THỊ DANH SÁCH =====
            header = f"🗑️ Chọn mục cần xóa cho '{kw}':\n\n"
            lines = []
            for i, (pid, title, date_iso, props) in enumerate(matches, start=1):
                ds = date_iso[:10] if date_iso else "-"
                lines.append(f"{i}. [{ds}] {title}")

            # Gửi tin danh sách (KHÔNG animation ở đây)
            list_msg = send_telegram(chat_id, header + "\n".join(lines))

            # ===== TẠO TIN COUNTDOWN RIÊNG =====
            timer_msg = send_telegram(
                chat_id,
                f"⏳ Đang chờ bạn chọn trong {WAIT_CONFIRM}s ...\nNhập số hoặc /cancel"
            )

            try:
                timer_message_id = timer_msg.get("result", {}).get("message_id")
            except:
                timer_message_id = None

            # ===== LƯU pending =====
            pending_confirm[str(chat_id)] = {
                "type": "archive_select",
                "keyword": kw,
                "matches": matches,
                "expires": time.time() + WAIT_CONFIRM,
                "timer_message_id": timer_message_id
            }

            # ===== ANIMATION (trên tin Timer) =====
            start_waiting_animation(
                chat_id,
                timer_message_id,
                WAIT_CONFIRM,
                interval=2.0,
                label="chọn mục xóa"
            )
            return

        # --- ĐÁO ---
        if action == "dao":
            send_telegram(chat_id, f"💼 Đang xử lý đáo cho {kw} ... ⏳")

            # ---- TÌM KHÁCH ----
            try:
                matches = find_target_matches(kw)
            except Exception as e:
                send_telegram(chat_id, f"⚠️ Lỗi khi tìm khách: {e}")
                return

            if not matches:
                send_telegram(chat_id, f"❌ Không tìm thấy '{kw}'.")
                return

            # ======================================================
            # 1) NHIỀU KẾT QUẢ → CHO CHỌN
            # ======================================================
            if len(matches) > 1:
                header = f"💼 Chọn mục đáo cho '{kw}':\n\n"
                lines = []
                for i, (pid, title, props) in enumerate(matches, start=1):
                    lines.append(f"{i}. {title}")

                # Gửi danh sách khách (KHÔNG animation)
                send_telegram(chat_id, header + "\n".join(lines))

                # ---- Gửi tin countdown RIÊNG (dùng để animation) ----
                timer_msg = send_telegram(
                    chat_id,
                    f"⏳ Đang chờ bạn chọn trong {WAIT_CONFIRM}s ...\nGõ số (ví dụ: 1 hoặc 1-3) hoặc /cancel"
                )
                try:
                    timer_message_id = timer_msg.get("result", {}).get("message_id")
                except:
                    timer_message_id = None

                # ---- LƯU PENDING: ĐANG Ở GIAI ĐOẠN CHỌN SỐ ----
                pending_confirm[str(chat_id)] = {
                    "type": "dao_choose",
                    "matches": matches,
                    "expires": time.time() + WAIT_CONFIRM,
                    "timer_message_id": timer_message_id
                }

                # ---- Animation countdown chạy trên tin riêng ----
                start_waiting_animation(
                    chat_id,
                    timer_message_id,
                    WAIT_CONFIRM,
                    interval=2.0,
                    label="chọn đáo"
                )
                return

            # ======================================================
            # 2) CHỈ 1 KẾT QUẢ → HIỂN THỊ PREVIEW + CHỜ /OK
            # ======================================================
            pid, title, props = matches[0]
            props = props if isinstance(props, dict) else {}

            # ---- Lấy preview an toàn ----
            try:
                can, preview = dao_preview_text_from_props(title, props)
            except Exception as e:
                can, preview = False, f"🔔 Đáo lại cho: {title}\n⚠️ Lỗi lấy preview: {e}"

            if not preview:
                preview = f"🔔 Đáo lại cho: {title}\n⚠️ Không lấy được dữ liệu preview."

            # ---- Gửi PREVIEW (tĩnh) ----
            send_telegram(chat_id, preview)

            # ---- Gửi tin yêu cầu xác nhận (/ok) (tĩnh, không animate) ----
            ok_msg = send_telegram(
                chat_id,
                f"⚠️ Gõ /ok trong {WAIT_CONFIRM}s hoặc /cancel."
            )
            try:
                ok_message_id = ok_msg.get("result", {}).get("message_id")
            except:
                ok_message_id = None

            # ---- Gửi 1 TIN RIÊNG để chạy animation (countdown) ----
            timer_msg = send_telegram(
                chat_id,
                f"⏳ Đang chờ bạn xác nhận trong {WAIT_CONFIRM}s..."
            )
            try:
                timer_message_id = timer_msg.get("result", {}).get("message_id")
            except:
                timer_message_id = None

            # ---- Lưu pending ----
            pending_confirm[str(chat_id)] = {
                "type": "dao_confirm",
                "targets": [(pid, title, props)],
                "preview_text": preview,
                "title": title,
                "expires": time.time() + WAIT_CONFIRM,
                "timer_message_id": timer_message_id
            }

            # ---- Animation chạy trên TIN RIÊNG, không đè lên OK ----
            start_waiting_animation(
                chat_id,
                timer_message_id,
                WAIT_CONFIRM,
                interval=2.0,
                label="xác nhận đáo"
            )
            return

            # nếu rơi tới đây nghĩa là không thể xử lý đáo
            send_telegram(chat_id, f"🔴 Chưa thể đáo cho '{kw}'. Vui lòng kiểm tra lại.")
            return


        # --- INTERACTIVE MARK MODE ---
        matches = find_calendar_matches(kw)
        send_telegram(chat_id, f"🔍 Đang tìm ... 🔄 {kw} ")
        checked, unchecked = count_checked_unchecked(kw)

        # nếu không có mục chưa tích vẫn hiển thị thống kê
        if not matches or unchecked == 0:
            msg = (
                f"💴 {kw}\n\n"
                f"✅ Đã góp: {checked}\n"
                f"🟡 Chưa góp: {unchecked}\n\n"
                f"💫 Không có ngày chưa góp ."
            )
            send_telegram(chat_id, msg)
            return

        header = f"💴 {kw}\n\n✅ Đã góp: {checked}\n🟡 Chưa góp: {unchecked}\n\n📤 ngày chưa góp /cancel.\n"
        lines = []
        for i, (pid, title, date_iso, props) in enumerate(matches, start=1):
            ds = date_iso[:10] if date_iso else "-"
            lines.append(f"{i}. [{ds}] {title} ☐")

        msg = send_telegram(chat_id, header + "\n".join(lines))
        list_message_id = msg.get("result", {}).get("message_id")

        timer_msg = send_telegram(chat_id, f"⏳ Đang chờ chọn {WAIT_CONFIRM}s ...")
        timer_message_id = timer_msg.get("result", {}).get("message_id")

        pending_confirm[str(chat_id)] = {
            "type": "mark",
            "keyword": kw,
            "matches": matches,
            "expires": time.time() + WAIT_CONFIRM,
            "timer_message_id": timer_message_id
        }
        start_waiting_animation(chat_id, timer_message_id, WAIT_CONFIRM, label="chọn đánh dấu")

    except Exception as e:
        traceback.print_exc()
        send_telegram(chat_id, f"❌ Lỗi xử lý: {e}")

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
        data = request.get_json(force=True, silent=True) or {}
    except Exception as e:
        print("❌ JSON decode error:", e)
        data = {}

    # ✅ Kiểm tra có dữ liệu không
    if not data:
        return jsonify({"ok": False, "error": "no data"}), 400

    message = data.get("message") or data.get("edited_message") or {}
    if not message:
        return jsonify({"ok": True})

    chat = message.get("chat", {})
    chat_id = chat.get("id")
    text = message.get("text") or message.get("caption") or ""

    if chat_id and text:
        def _forward():
            try:
                # adjust URL/port if your command_worker listens elsewhere
                requests.post(
                    "http://127.0.0.1:5001/process_command",
                    json={"text": text, "chat_id": chat_id},
                    timeout=2
                )
            except Exception as e:
                # Log but do not raise — do not break webhook flow
                print("Forward to command worker failed:", e)
        threading.Thread(
            target=handle_incoming_message,
            args=(chat_id, text),
            daemon=True
        ).start()

    return jsonify({"ok": True})

def auto_ping_render():
    """
    Giữ Render hoạt động trong khung giờ 9:00 - 23:59 (UTC+7)
    """
    RENDER_URL = "https://telegram-notion-bot-tpm2.onrender.com"  # ⚠️ anh đổi thành URL thật của app Flask[](https://tên-app.onrender.com)
    VN_TZ = timezone(timedelta(hours=7))

    while True:
        now_vn = datetime.now(VN_TZ)
        hour = now_vn.hour

        # chỉ ping trong khung giờ 9h - 23h59 (giờ VN)
        if 9 <= hour < 24:
            try:
                r = requests.get(RENDER_URL, timeout=10)
                print(f"[{now_vn:%H:%M:%S}] 🔄 Ping Render: {r.status_code}")
            except Exception as e:
                print(f"[{now_vn:%H:%M:%S}] ⚠️ Ping lỗi: {e}")
        else:
            print(f"[{now_vn:%H:%M:%S}] 🌙 Ngoài giờ làm việc — không ping.")

        # đợi 5 phút rồi ping lại
        time.sleep(300)  # 30780s = 13 phút
        
# ------------- RUN (local test) -------------
if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    print("Launching app.py on port", port)
    print("NOTION_DATABASE_ID:", NOTION_DATABASE_ID[:8] + "..." if NOTION_DATABASE_ID else "(none)")
    print("TARGET_NOTION_DATABASE_ID:", TARGET_NOTION_DATABASE_ID[:8] + "..." if TARGET_NOTION_DATABASE_ID else "(none)")
    print("LA_NOTION_DATABASE_ID:", LA_NOTION_DATABASE_ID[:8] + "..." if LA_NOTION_DATABASE_ID else "(none)")
    print("TELEGRAM_TOKEN set?:", bool(TELEGRAM_TOKEN))
    threading.Thread(target=auto_ping_render, daemon=True).start()
    app.run(host="0.0.0.0", port=port)
