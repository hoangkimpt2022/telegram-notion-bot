# app_final_v4.py
# Production-ready Telegram <-> Notion automation (mark / archive / dao / create-lai / undo / progress)
# Usage: deploy on Render, set env vars then set Telegram webhook to /telegram_webhook
import os
import time
import math
import json
import traceback
import threading
import requests
import unicodedata
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple
from flask import Flask, request, jsonify

# -------------------- CONFIG / ENV --------------------
NOTION_TOKEN = os.getenv("NOTION_TOKEN", "")
NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": os.getenv("NOTION_VERSION", "2022-06-28"),
    "Content-Type": "application/json",
}
NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID", "")
TARGET_NOTION_DATABASE_ID = os.getenv("TARGET_NOTION_DATABASE_ID", "")
LA_NOTION_DATABASE_ID = os.getenv("LA_NOTION_DATABASE_ID", "")

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")  # optional restrict
WAIT_CONFIRM = int(os.getenv("WAIT_CONFIRM", "120"))
PATCH_DELAY = float(os.getenv("PATCH_DELAY", "0.25"))
MAX_PAGE_QUERY_SIZE = 200

# in-memory structures (simple, ephemeral)
pending_confirm: Dict[str, Dict[str, Any]] = {}
undo_stack: Dict[str, List[Dict[str, Any]]] = {}  # per chat_id

# -------------------- TELEGRAM HELPERS --------------------
def send_telegram(chat_id: str, text: str):
    chat_id_str = str(chat_id)
    try:
        if TELEGRAM_TOKEN:
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            data = {"chat_id": chat_id, "text": text}
            # set disable_web_page_preview to True to avoid previews
            data["disable_web_page_preview"] = True
            requests.post(url, data=data, timeout=8)
        else:
            print(f"[TG:{chat_id}] {text}")
    except Exception as e:
        print("send_telegram error:", e)

def send_long_text(chat_id: str, text: str):
    # chunk Telegram messages
    max_len = 3000
    for i in range(0, len(text), max_len):
        send_telegram(chat_id, text[i : i + max_len])

def send_progress(chat_id: str, step: int, total: int, label: str):
    try:
        if total == 0:
            return
        # send only when step==1, every 10, and final
        if step == 1 or step % 10 == 0 or step == total:
            send_telegram(chat_id, f"⏱️ {label}: {step}/{total} ...")
    except Exception as e:
        print("send_progress error:", e)

# -------------------- NOTION API HELPERS --------------------
def normalize_text(s: Optional[str]) -> str:
    if not s:
        return ""
    s = str(s).strip().lower()
    nf = unicodedata.normalize("NFD", s)
    return "".join(c for c in nf if unicodedata.category(c) != "Mn")

def query_database_all(database_id: str, page_size: int = MAX_PAGE_QUERY_SIZE) -> List[Dict[str, Any]]:
    """
    Query all pages in a Notion database using pagination.
    Returns list of page objects.
    """
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
                print("query_database_all pagination failed:", r.status_code, r.text)
                break
            data = r.json()
            results.extend(data.get("results", []))
        return results
    except Exception as e:
        print("query_database_all exception:", e)
        return []

def archive_page(page_id: str) -> Tuple[bool, str]:
    try:
        url = f"https://api.notion.com/v1/pages/{page_id}"
        r = requests.patch(url, headers=NOTION_HEADERS, json={"archived": True}, timeout=10)
        if r.status_code in (200, 204):
            return True, "Archived"
        return False, f"{r.status_code}: {r.text}"
    except Exception as e:
        return False, str(e)

def create_page_in_db(database_id: str, properties: Dict[str, Any]) -> Tuple[bool, Any]:
    try:
        url = "https://api.notion.com/v1/pages"
        body = {"parent": {"database_id": database_id}, "properties": properties}
        r = requests.post(url, headers=NOTION_HEADERS, json=body, timeout=20)
        if r.status_code in (200, 201):
            return True, r.json()
        return False, {"status": r.status_code, "text": r.text}
    except Exception as e:
        return False, str(e)

def update_page_properties(page_id: str, properties: Dict[str, Any]) -> Tuple[bool, Any]:
    try:
        url = f"https://api.notion.com/v1/pages/{page_id}"
        body = {"properties": properties}
        r = requests.patch(url, headers=NOTION_HEADERS, json=body, timeout=10)
        if r.status_code in (200, 204):
            return True, r.json() if r.text else {}
        return False, {"status": r.status_code, "text": r.text}
    except Exception as e:
        return False, str(e)

def check_prop_exists(database_id: str, prop_name: str) -> bool:
    try:
        url = f"https://api.notion.com/v1/databases/{database_id}"
        r = requests.get(url, headers=NOTION_HEADERS, timeout=10)
        if r.status_code == 200:
            data = r.json()
            return prop_name in data.get("properties", {})
        return False
    except Exception:
        return False

# -------------------- PROPERTY/EXTRACTION HELPERS --------------------
def find_prop_key(props: Dict[str, Any], name_like: str) -> Optional[str]:
    for k in props.keys():
        if normalize_text(k) == normalize_text(name_like):
            return k
    return None

def extract_prop_text(props: Dict[str, Any], key_like: str) -> str:
    """
    Extract human readable text / number from a property dict.
    """
    if not props:
        return ""
    k = find_prop_key(props, key_like)
    if not k:
        return ""
    v = props.get(k) or {}
    t = v.get("type")
    if t == "title":
        arr = v.get("title", [])
        return "".join([x.get("plain_text", "") for x in arr])
    if t == "rich_text":
        arr = v.get("rich_text", [])
        return "".join([x.get("plain_text", "") for x in arr])
    if t == "number":
        return str(v.get("number"))
    if t == "checkbox":
        return "1" if v.get("checkbox") else "0"
    if t == "date":
        d = v.get("date") or {}
        return d.get("start") or ""
    if t == "relation":
        rels = v.get("relation", [])
        if rels:
            # return first relation id
            return rels[0].get("id") or ""
    return ""

def parse_money_from_text(s: Optional[str]) -> float:
    if not s:
        return 0.0
    try:
        t = "".join([c for c in str(s) if c.isdigit() or c in ".-"])
        return float(t) if t else 0.0
    except:
        return 0.0

# -------------------- FINDERS & PREVIEWS --------------------
def find_target_matches(keyword: str, db_id: str = TARGET_NOTION_DATABASE_ID) -> List[Tuple[str, str, Dict[str, Any]]]:
    """
    Find entries in TARGET_NOTION_DATABASE_ID with Name containing keyword.
    Return list of (page_id, title, props)
    """
    kw = normalize_text(keyword)
    results = query_database_all(db_id, page_size=MAX_PAGE_QUERY_SIZE)
    matches = []
    for p in results:
        props = p.get("properties", {})
        title = extract_prop_text(props, "Name") or extract_prop_text(props, "Title") or ""
        if kw in normalize_text(title):
            matches.append((p.get("id"), title, props))
    return matches

def find_calendar_matches(keyword: str, include_archived: bool = False) -> List[Tuple[str, str, Optional[str], Dict[str, Any]]]:
    """
    Return list of pages in NOTION_DATABASE_ID that are NOT checked (Đã Góp == False)
    Each item: (page_id, title, date_iso, props)
    Sorted by date asc (oldest first)
    """
    kw = normalize_text(keyword)
    results = query_database_all(NOTION_DATABASE_ID, page_size=MAX_PAGE_QUERY_SIZE)
    matches: List[Tuple[str, str, Optional[str], Dict[str, Any]]] = []
    for p in results:
        props = p.get("properties", {})
        title = extract_prop_text(props, "Name") or extract_prop_text(props, "Title") or ""
        if kw not in normalize_text(title):
            continue
        # check checkbox keys
        cb_key = find_prop_key(props, "Đã Góp") or find_prop_key(props, "Sent") or find_prop_key(props, "Status")
        checked = False
        if cb_key and props.get(cb_key, {}).get("type") == "checkbox":
            checked = bool(props.get(cb_key, {}).get("checkbox"))
        if checked:
            continue
        # date
        date_key = find_prop_key(props, "Ngày") or find_prop_key(props, "Date")
        date_iso = None
        if date_key and props.get(date_key, {}).get("date"):
            date_iso = props[date_key]["date"].get("start")
        matches.append((p.get("id"), title, date_iso, props))
    # sort by date (oldest first)
    matches.sort(key=lambda x: x[2] or "")
    return matches

def dao_preview_text_from_props(title: str, props: Dict[str, Any]) -> Tuple[bool, str]:
    try:
        total_text = extract_prop_text(props, "Đáo/thối") or extract_prop_text(props, "Đáo")
        total_val = parse_money_from_text(total_text)
        per_day = parse_money_from_text(extract_prop_text(props, "G ngày"))
        days_before = int(float(extract_prop_text(props, "ngày trước") or "0"))
        pre_amount = parse_money_from_text(extract_prop_text(props, "trước"))
        if pre_amount == 0:
            return True, f"🔔 đáo lại cho: {title} - Tổng đáo: ✅ {int(total_val) if total_val else 'N/A'}\n\nKhông Lấy trước"
        if days_before and days_before > 0:
            take_days = days_before
        else:
            take_days = int(math.ceil(pre_amount / per_day)) if per_day else 0
        if take_days <= 0:
            return False, f"⚠️ Không xác định được số ngày để tạo cho {title} (per_day={per_day}, pre_amount={pre_amount}, days_before={days_before})"
        lines = [f"🔔 đáo lại cho: {title} - Tổng đáo: ✅ {int(total_val) if total_val else 'N/A'}", "", f"Lấy trước: {take_days} ngày" if take_days else "Không Lấy trước", f"G ngày: {per_day}", f"Ngày trước: {days_before}", f"Trước: {pre_amount}", "", "Danh sách ngày dự kiến tạo (bắt đầu từ ngày mai):"]
        start = datetime.now().date() + timedelta(days=1)
        for i in range(take_days):
            lines.append((start + timedelta(days=i)).isoformat())
        lines.append("")
        lines.append(f"Gửi /ok để tạo {take_days} page trong {WAIT_CONFIRM}s, hoặc /cancel.")
        return True, "\n".join(lines)
    except Exception as e:
        return False, f"Preview error: {e}"

# -------------------- MARK / ARCHIVE / UNDO --------------------
def count_checked_unchecked(keyword: str) -> Tuple[int, int]:
    results = query_database_all(NOTION_DATABASE_ID, page_size=MAX_PAGE_QUERY_SIZE)
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
    marks pages (set 'Đã Góp' checkbox to True)
    Business rule: if indices == [n] and n > 1 -> expand to 1..n (oldest first)
    """
    succeeded = []
    failed = []
    if len(indices) == 1 and indices[0] > 1:
        max_n = indices[0]
        indices = list(range(1, min(max_n, len(matches)) + 1))
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
                # attempt to set default name
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
    # show summary search
    send_telegram(chat_id, f"🔎 Khách hàng: last actions undone for chat {chat_id}")

# -------------------- ARCHIVE FLOW --------------------
def handle_command_archive(chat_id: str, keyword: str, auto_confirm_all: bool = True) -> Dict[str, Any]:
    """
    Archive all pages in NOTION_DATABASE_ID matching keyword (checked + unchecked).
    If auto_confirm_all True then automatically delete all matched pages (used by dao flow).
    Returns dict with deleted/failed lists.
    """
    try:
        kw = normalize_text(keyword)
        pages = query_database_all(NOTION_DATABASE_ID, page_size=500)
        matched: List[Tuple[str, str, Optional[str]]] = []
        for p in pages:
            props = p.get("properties", {})
            title = extract_prop_text(props, "Name") or extract_prop_text(props, "Title") or ""
            if kw in normalize_text(title):
                # ensure parent is correct DB if info present
                parent = p.get("parent") or {}
                parent_db = parent.get("database_id") or p.get("parent_database_id")
                if parent_db and str(parent_db) != str(NOTION_DATABASE_ID):
                    continue
                date_key = find_prop_key(props, "Ngày")
                date_iso = None
                if date_key and props.get(date_key, {}).get("date"):
                    date_iso = props[date_key]["date"].get("start")
                matched.append((p.get("id"), title, date_iso))
        total = len(matched)
        send_telegram(chat_id, f"🧹 Đang xóa {total} ngày của {keyword} (check + uncheck)...")
        if total == 0:
            send_telegram(chat_id, f"✅ Không tìm thấy mục cần xóa cho '{keyword}'.")
            return {"ok": True, "deleted": [], "failed": []}
        deleted = []
        failed = []
        for i, (pid, title, date_iso) in enumerate(matched, start=1):
            send_progress(chat_id, i, total, f"🗑️ Đang xóa {keyword}")
            ok, msg = archive_page(pid)
            if ok:
                deleted.append(pid)
            else:
                failed.append((pid, msg))
            time.sleep(PATCH_DELAY)
        send_telegram(chat_id, f"✅ Đã xóa xong {len(deleted)}/{total} mục của {keyword}.")
        if failed:
            send_telegram(chat_id, f"⚠️ Có {len(failed)} mục xóa lỗi. Xem logs.")
        return {"ok": True, "deleted": deleted, "failed": failed}
    except Exception as e:
        traceback.print_exc()
        send_telegram(chat_id, f"❌ Lỗi archive: {e}")
        return {"ok": False, "error": str(e)}

# -------------------- DAO (đáo) FLOW --------------------
def create_lai_page_from_target(chat_id: str, key_name: str, target_props: Dict[str, Any], target_page_id: str) -> Tuple[bool, Any]:
    """
    Create Lãi page in LA_NOTION_DATABASE_ID:
    - Name = key_name
    - Lai = copy from target_props['Lai lịch g']
    - Ngày Lãi = today
    - Lịch G = relation -> point to target_page_id (source in TARGET DB)
    """
    try:
        # find candidate keys
        lai_val = 0.0
        candidates = ["Lai lịch g", "Lai lich g", "Lai_lịch_g", "Lai lịch", "Lai"]
        for cand in candidates:
            prop_key = find_prop_key(target_props, cand)
            if prop_key:
                prop = target_props.get(prop_key)
                if prop:
                    if prop.get("type") == "number":
                        lai_val = float(prop.get("number") or 0)
                    elif prop.get("type") == "rich_text":
                        txt = "".join([x.get("plain_text", "") for x in prop.get("rich_text", [])])
                        lai_val = parse_money_from_text(txt)
                    break
        props_payload = {
            "Name": {"title": [{"type": "text", "text": {"content": key_name}}]},
            "Lai": {"number": lai_val},
            "Ngày Lãi": {"date": {"start": datetime.now().date().isoformat()}},
            "Lịch G": {"relation": [{"id": target_page_id}]}
        }
        ok, res = create_page_in_db(LA_NOTION_DATABASE_ID, props_payload)
        if ok:
            send_telegram(chat_id, f"💰 Đã tạo Lãi cho {key_name} ({lai_val})")
            return True, res
        else:
            send_telegram(chat_id, f"⚠️ Tạo Lãi lỗi: {res}")
            return False, res
    except Exception as e:
        send_telegram(chat_id, f"❌ Lỗi tạo Lãi: {e}")
        return False, str(e)

def dao_create_pages_from_props(chat_id: str, source_page_id: str, props: Dict[str, Any]) -> Dict[str, Any]:
    """
    Full dao process:
    - archive all pages in NOTION_DATABASE_ID for key
    - create take_days pages starting tomorrow (Đã Góp = True), Lịch G relation -> source_page_id
    - create Lãi page in LA_NOTION_DATABASE_ID (Name=key, Lai from target props, Lịch G -> source_page_id)
    """
    try:
        # extract key name
        name_prop_key = find_prop_key(props, "Name") or find_prop_key(props, "Tên")
        title = "UNKNOWN"
        if name_prop_key:
            v = props.get(name_prop_key, {})
            if v.get("type") == "title":
                title = "".join([x.get("plain_text", "") for x in v.get("title", [])]) or title
        # compute financial numbers
        total_text = extract_prop_text(props, "Đáo/thối") or extract_prop_text(props, "Đáo")
        total_val = parse_money_from_text(total_text)
        per_day = parse_money_from_text(extract_prop_text(props, "G ngày"))
        days_before = int(float(extract_prop_text(props, "ngày trước") or "0"))
        pre_amount = parse_money_from_text(extract_prop_text(props, "trước"))
        if pre_amount == 0:
            send_telegram(chat_id, f"🔔 đáo lại cho: {title} - Tổng đáo: ✅ {int(total_val) if total_val else 'N/A'}\n\nKhông Lấy trước")
            return {"ok": True, "note": "no_pre"}
        take_days = days_before if days_before and days_before > 0 else (int(math.ceil(pre_amount / per_day)) if per_day else 0)
        if take_days <= 0:
            send_telegram(chat_id, f"⚠️ Không xác định được số ngày hợp lệ cho {title}.")
            return {"ok": False, "error": "invalid_take_days"}
        # 1) archive existing pages
        send_telegram(chat_id, f"🧹 Đang archive toàn bộ page của {title} trước khi tạo mới...")
        archive_res = handle_command_archive(chat_id, title, auto_confirm_all=True)
        if not archive_res.get("ok"):
            send_telegram(chat_id, f"⚠️ Lỗi khi archive trước khi đáo: {archive_res.get('error')}")
        send_telegram(chat_id, f"🧾 Đã hoàn tất xoá toàn bộ page cũ của {title}, chuẩn bị tạo ngày mới...")
        # 2) create new pages
        start = datetime.now().date() + timedelta(days=1)
        created = []
        send_telegram(chat_id, f"🛠️ Đang tạo {take_days} ngày mới cho {title} (bắt đầu từ ngày mai)...")
        for i in range(1, take_days + 1):
            d = start + timedelta(days=(i - 1))
            props_payload = {
                "Name": {"title": [{"type": "text", "text": {"content": f"{title} - {d.isoformat()}"}}]},
                "Ngày": {"date": {"start": d.isoformat()}},
                "Tiền": {"number": per_day} if per_day else {},
                "Đã Góp": {"checkbox": True},
                "Lịch G": {"relation": [{"id": source_page_id}]}
            }
            ok, res = create_page_in_db(NOTION_DATABASE_ID, props_payload)
            if ok:
                created.append(res)
                send_progress(chat_id, i, take_days, f"📅 Đang tạo ngày mới cho {title}")
            else:
                send_telegram(chat_id, f"⚠️ Lỗi tạo page: {res}")
            time.sleep(PATCH_DELAY)
        send_telegram(chat_id, f"✅ Đã tạo {len(created)} ngày mới cho {title}.")
        # 3) create Lãi page referencing the source page id from TARGET DB
        send_telegram(chat_id, f"💸 Tiếp tục tạo Lãi cho {title}...")
        ok_l, res_l = create_lai_page_from_target(chat_id, title, props, source_page_id)
        if not ok_l:
            send_telegram(chat_id, f"⚠️ Tạo Lãi lỗi: {res_l}")
        send_telegram(chat_id, f"✅ Hoàn tất tiến trình đáo cho {title}.")
        return {"ok": True, "created": created}
    except Exception as e:
        traceback.print_exc()
        send_telegram(chat_id, f"❌ Lỗi tiến trình đáo cho {title}: {e}")
        return {"ok": False, "error": str(e)}

# -------------------- PENDING / SELECTION PROCESSING --------------------
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
    # dedupe & sort
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
            found_len = len(matches)
            indices = parse_user_selection_text(raw, found_len)
            if not indices:
                send_telegram(chat_id, "Không nhận được lựa chọn hợp lệ.")
                return
            chosen = []
            for idx in indices:
                if 1 <= idx <= found_len:
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
        found_len = len(matches)
        indices = parse_user_selection_text(raw, found_len)
        if not indices:
            send_telegram(chat_id, "Không nhận được lựa chọn hợp lệ.")
            return
        action = data.get("type")
        if action == "mark":
            keyword = data.get("keyword")
            res = mark_pages_by_indices(chat_id, keyword, matches, indices)
            # report succeeded
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
        if action == "archive_choose":
            # call archive on selected indices
            for idx in indices:
                if 1 <= idx <= found_len:
                    pid, title, date_iso = matches[idx - 1]
                    handle_command_archive(chat_id, title)
            del pending_confirm[key]
            return
    except Exception as e:
        traceback.print_exc()
        send_telegram(chat_id, f"❌ Lỗi xử lý lựa chọn: {e}")
        if key in pending_confirm:
            del pending_confirm[key]

# -------------------- COMMAND PARSING & ENTRY --------------------
def parse_user_command(raw: str) -> Tuple[str, int, str]:
    txt = raw.strip()
    low = txt.lower()
    parts = txt.split()
    if not parts:
        return "", 0, "unknown"
    if low in ("undo",):
        return "", 0, "undo"
    # endswith đáo
    if low.endswith(" đáo") or low.endswith(" dao"):
        kw = txt.rsplit(None, 1)[0]
        return kw, 0, "dao"
    # endswith xóa / xoa
    if low.endswith(" xóa") or low.endswith(" xoa"):
        kw = txt.rsplit(None, 1)[0]
        return kw, 0, "archive"
    # default: keyword possibly with count
    keyword = parts[0]
    action = "mark"
    count = 0
    if len(parts) >= 2:
        sec = parts[1]
        if sec.isdigit():
            count = int(sec)
    return keyword, count, action

def handle_incoming_message(chat_id: int, text: str):
    try:
        # restrict by chat id if configured
        if TELEGRAM_CHAT_ID and str(chat_id) != str(TELEGRAM_CHAT_ID):
            send_telegram(chat_id, "Bot chưa được phép nhận lệnh từ chat này.")
            return
        raw = text.strip()
        if not raw:
            send_telegram(chat_id, "Vui lòng gửi lệnh hoặc từ khoá.")
            return
        low = raw.lower()

        # process pending first
        if str(chat_id) in pending_confirm:
            # cancel handling
            if low in ("/cancel", "cancel", "hủy", "huy"):
                del pending_confirm[str(chat_id)]
                send_telegram(chat_id, "Đã hủy thao tác đang chờ.")
                return
            # route to dao or general pending
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
            send_telegram(chat_id, "Đang xử lý archive...")
            threading.Thread(target=handle_command_archive, args=(chat_id, keyword), daemon=True).start()
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
                    nb = extract_prop_text(props, "ngày trước") or "-"
                    prev = extract_prop_text(props, "trước") or "-"
                    lines.append(f"{i}. {title} — Đáo/thối: {dt} — G ngày: {gday} — # ngày trước: {nb} — trước: {prev}")
                send_long_text(chat_id, header + "\n\n" + "\n".join(lines))
                pending_confirm[str(chat_id)] = {"type": "dao_choose", "matches": matches, "expires": time.time() + WAIT_CONFIRM}
                send_telegram(chat_id, f"📤 Gửi số (ví dụ 1 hoặc 1-3) trong {WAIT_CONFIRM}s để chọn, hoặc /cancel.")
                return
            pid, title, props = matches[0]
            can, preview = dao_preview_text_from_props(title, props)
            send_long_text(chat_id, preview)
            if can:
                pending_confirm[str(chat_id)] = {"type": "dao_confirm", "source_page_id": pid, "props": props, "expires": time.time() + WAIT_CONFIRM}
                send_telegram(chat_id, f"✅ Có thể đáo cho '{title}'. Gõ /ok để thực hiện trong {WAIT_CONFIRM}s hoặc /cancel để hủy.")
            else:
                send_telegram(chat_id, f"⚠️ Không thể thực hiện đáo cho '{title}'. Vui lòng kiểm tra dữ liệu.")
            return

        # default: mark flow (e.g., "gam" or "gam 2")
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
        return

    except Exception as e:
        traceback.print_exc()
        send_telegram(chat_id, f"Lỗi xử lý: {e}")

# -------------------- BACKGROUND SWEEP: expire pending --------------------
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

# start sweep thread
threading.Thread(target=sweep_pending_expirations, daemon=True).start()

# -------------------- FLASK WEBHOOK --------------------
app = Flask(__name__)

@app.route("/", methods=["GET"])
def index():
    return "app_final_v4 running ✅"

@app.route("/telegram_webhook", methods=["POST"])
def telegram_webhook():
    try:
        data = request.get_json(force=True)
    except Exception as e:
        return jsonify({"ok": False, "error": "invalid json"})
    if not data:
        return jsonify({"ok": False})
    # Telegram updates may contain message or edited_message
    message = data.get("message") or data.get("edited_message") or {}
    if not message:
        return jsonify({"ok": True})
    chat = message.get("chat", {})
    chat_id = chat.get("id")
    text = message.get("text") or message.get("caption") or ""
    if chat_id and text:
        # process in background
        threading.Thread(target=handle_incoming_message, args=(chat_id, text), daemon=True).start()
    return jsonify({"ok": True})

# -------------------- SELF-TEST / RUN --------------------
if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    print("Launching app_final_v4 on port", port)
    # minimal checks
    print("NOTION_DATABASE_ID:", NOTION_DATABASE_ID[:8] + "..." if NOTION_DATABASE_ID else "(none)")
    print("TARGET_NOTION_DATABASE_ID:", TARGET_NOTION_DATABASE_ID[:8] + "..." if TARGET_NOTION_DATABASE_ID else "(none)")
    print("LA_NOTION_DATABASE_ID:", LA_NOTION_DATABASE_ID[:8] + "..." if LA_NOTION_DATABASE_ID else "(none)")
    print("TELEGRAM_TOKEN set?:", bool(TELEGRAM_TOKEN))
    app.run(host="0.0.0.0", port=port)
