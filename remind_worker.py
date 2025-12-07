#!/usr/bin/env python3
# remind_v1.py — warm-run on start, then daily schedule
# Env required:
#   NOTION_TOKEN
#   REMIND_NOTION_DATABASE_   (database id)
#   TELEGRAM_TOKEN
#   TELEGRAM_CHAT_ID (số)
# Optional:
#   TIMEZONE (default Asia/Ho_Chi_Minh)
#   REMIND_HOUR (default 14)
#   REMIND_MINUTE (default 0)
#   NO_TASK_MESSAGE (default Vietnamese friendly message)

import os
import requests
import datetime
import pytz
import traceback
import time
from dateutil import parser as dateparser
from apscheduler.schedulers.background import BackgroundScheduler

# --- Config from env ---
NOTION_TOKEN = os.getenv("NOTION_TOKEN", "").strip()
REMIND_DB = os.getenv("REMIND_NOTION_DATABASE", "").strip()
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

TIMEZONE = os.getenv("TIMEZONE", "Asia/Ho_Chi_Minh")
try:
    REMIND_HOUR = int(os.getenv("REMIND_HOUR", "14"))
    REMIND_MINUTE = int(os.getenv("REMIND_MINUTE", "0"))
except Exception:
    REMIND_HOUR, REMIND_MINUTE = 14, 0

NO_TASK_MESSAGE = os.getenv("NO_TASK_MESSAGE",
                            "Hôm nay không có việc cần làm — chúc sếp hôm nay vui vẻ!")
NOTION_VERSION = os.getenv("NOTION_VERSION", "2022-06-28")

HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}" if NOTION_TOKEN else "",
    "Notion-Version": NOTION_VERSION,
    "Content-Type": "application/json",
}

LAST_REMIND_PROP_CANDIDATES = [
    "Last reminded at",
    "Last reminded time",
    "Last reminded",
    "Last edited time",
    "Last reminded time (auto)"
]

# ---------- Helpers ----------
def log(*args, **kwargs):
    try:
        ts = datetime.datetime.now(pytz.timezone(TIMEZONE)).isoformat()
    except Exception:
        ts = datetime.datetime.utcnow().isoformat()
    print(ts, *args, **kwargs)

def notion_post(path, json_payload, timeout=20):
    url = f"https://api.notion.com/v1{path}"
    r = requests.post(url, headers=HEADERS, json=json_payload, timeout=timeout)
    r.raise_for_status()
    return r.json()

def notion_patch(path, json_payload, timeout=12):
    url = f"https://api.notion.com/v1{path}"
    r = requests.patch(url, headers=HEADERS, json=json_payload, timeout=timeout)
    r.raise_for_status()
    return r.json()

def extract_plain_text(arr):
    if not arr:
        return ""
    return "".join([it.get("plain_text", "") for it in arr if isinstance(it, dict)])

def get_title_from_page(page):
    props = page.get("properties", {})
    for key, val in props.items():
        if val.get("type") == "title":
            return extract_plain_text(val.get("title", []))
    v = props.get("name") or props.get("Name")
    if isinstance(v, dict) and v.get("type") in ("title", "rich_text"):
        return extract_plain_text(v.get(v.get("type", ""), []))
    return page.get("id", "")[:8]

def get_note_from_page(page):
    props = page.get("properties", {})
    for k, v in props.items():
        key_lower = k.lower()
        if key_lower in ("note", "ghi chú", "ghi_chu", "note_text", "description"):
            t = v.get("type")
            if t == "rich_text":
                return extract_plain_text(v.get("rich_text", []))
            if t == "title":
                return extract_plain_text(v.get("title", []))
    for k, v in props.items():
        if v.get("type") == "rich_text":
            s = extract_plain_text(v.get("rich_text", []))
            if s:
                return s
    return ""

def find_date_property(page):
    props = page.get("properties", {})
    for k, v in props.items():
        if v.get("type") == "date":
            return k, v
    for k, v in props.items():
        if "ngày" in k.lower() or "date" in k.lower() or "due" in k.lower() or "deadline" in k.lower():
            if v.get("type") in ("date", "rich_text", "title"):
                return k, v
    return None, None

def parse_notion_date(date_obj):
    if not date_obj:
        return None
    if isinstance(date_obj, dict):
        start = date_obj.get("start")
        if not start:
            return None
        try:
            return dateparser.isoparse(start)
        except Exception:
            try:
                return dateparser.parse(start)
            except Exception:
                return None
    return None

def notion_query_active_not_done(page_size=100):
    if not NOTION_TOKEN or not REMIND_DB:
        log("Missing NOTION_TOKEN or REMIND_NOTION_DATABASE_ env")
        return []
    payload = {
        "filter": {
            "and": [
                {"property": "active", "checkbox": {"equals": True}},
                {"property": "Done", "checkbox": {"equals": False}}
            ]
        },
        "page_size": page_size
    }
    try:
        res = notion_post(f"/databases/{REMIND_DB}/query", payload)
        return res.get("results", [])
    except Exception as e:
        log("Primary query failed (maybe property names differ). Error:", e)
        try:
            fb = {"page_size": page_size}
            res2 = notion_post(f"/databases/{REMIND_DB}/query", fb)
            return res2.get("results", [])
        except Exception as ex:
            log("Fallback query failed:", ex)
            return []

def is_due_today_or_overdue(page):
    tz = pytz.timezone(TIMEZONE)
    today = datetime.datetime.now(tz).date()
    prop_name, prop_value = find_date_property(page)
    if not prop_name:
        return False, None, None
    p = page.get("properties", {}).get(prop_name, {})
    date_field = None
    if isinstance(p, dict) and p.get("type") == "date":
        date_field = p.get("date")
    dt = parse_notion_date(date_field)
    if not dt:
        return False, None, None
    if dt.tzinfo is None:
        dt = tz.localize(dt)
    local_date = dt.astimezone(tz).date()
    if local_date <= today:
        status = "overdue" if local_date < today else "due_today"
        return True, status, local_date
    return False, None, local_date

def update_last_reminded_if_exists(page_id):
    now_iso = datetime.datetime.now(pytz.timezone(TIMEZONE)).isoformat()
    for prop in LAST_REMIND_PROP_CANDIDATES:
        body = {"properties": {prop: {"date": {"start": now_iso}}}}
        try:
            notion_patch(f"/pages/{page_id}", body)
            return True
        except Exception:
            continue
    return False

# ---------- Telegram ----------
def send_telegram(chat_id, text):
    if not TELEGRAM_TOKEN or not chat_id:
        log("Missing TELEGRAM_TOKEN or chat_id")
        return False, None
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
    try:
        r = requests.post(url, json=payload, timeout=10)
        data = r.json() if r.content else {}
        if r.status_code == 200 and data.get("ok"):
            return True, data
        return False, data
    except Exception as e:
        return False, str(e)

# ---------- Main job ----------
def job_remind_once():
    try:
        log("Running remind job (warm or scheduled)...")
        pages = notion_query_active_not_done(page_size=200)
        if pages is None:
            log("Query returned None")
            pages = []
        if not pages:
            # Send NO TASK message
            log("No pages found in query. Sending NO_TASK_MESSAGE.")
            ok, resp = send_telegram(TELEGRAM_CHAT_ID, NO_TASK_MESSAGE)
            if ok:
                log("Sent NO_TASK_MESSAGE successfully.")
            else:
                log("Failed to send NO_TASK_MESSAGE:", resp)
            return

        due_pages = []
        for p in pages:
            props = p.get("properties", {})
            active = None
            done = None
            if "active" in props and props["active"].get("type") == "checkbox":
                active = props["active"].get("checkbox", None)
            if "Done" in props and props["Done"].get("type") == "checkbox":
                done = props["Done"].get("checkbox", None)
            if active is not None and not active:
                continue
            if done is not None and done:
                continue

            ok, status, local_date = is_due_today_or_overdue(p)
            if ok:
                title = get_title_from_page(p)
                note = get_note_from_page(p)
                due_pages.append((p, title, status, local_date, note))

        if not due_pages:
            # No due task specifically today/overdue -> send friendly "no task" message
            log("No tasks due today or overdue. Sending NO_TASK_MESSAGE.")
            ok, resp = send_telegram(TELEGRAM_CHAT_ID, NO_TASK_MESSAGE)
            if ok:
                log("Sent NO_TASK_MESSAGE successfully.")
            else:
                log("Failed to send NO_TASK_MESSAGE:", resp)
            return

        tz = pytz.timezone(TIMEZONE)
        today_str = datetime.datetime.now(tz).strftime("%Y-%m-%d")
        header = f"🔔 <b>Có — {len(due_pages)} công việc hôm nay ({today_str})</b>\n\n"
        lines = [header]
        for idx, (p, title, status, local_date, note) in enumerate(due_pages, start=1):
            if status == "overdue":
                flag = "🔴 Quá hạn"
            else:
                flag = "⏳ Hạn hôm nay"
            lines.append(f"• <b>{title}</b> — {flag}")
            if note:
                short = note.strip()
                if len(short) > 300:
                    short = short[:300] + "..."
                lines.append(f"  ↳ {short}")
            lines.append(f"  ↳ Ngày hoàn thành: {local_date.isoformat()}")
            lines.append("")

        message = "\n".join(lines)
        ok, resp = send_telegram(TELEGRAM_CHAT_ID, message)
        if ok:
            log("Sent reminder message successfully. Count:", len(due_pages))
            for p, title, status, local_date, note in due_pages:
                try:
                    pid = p.get("id")
                    updated = update_last_reminded_if_exists(pid)
                    if updated:
                        log("Updated last reminded for", title)
                except Exception:
                    pass
        else:
            log("Failed to send reminder:", resp)

    except Exception as e:
        log("Exception in job_remind_once:", e)
        traceback.print_exc()

def start_scheduler():
    tz = pytz.timezone(TIMEZONE)
    sched = BackgroundScheduler(timezone=tz)
    sched.add_job(job_remind_once, 'cron', hour=REMIND_HOUR, minute=REMIND_MINUTE)
    sched.start()
    log(f"Scheduler started — daily at {REMIND_HOUR:02d}:{REMIND_MINUTE:02d} {TIMEZONE}")
    try:
        while True:
            time.sleep(30)
    except (KeyboardInterrupt, SystemExit):
        log("Scheduler stopping...")

if __name__ == "__main__":
    log("remind_v1 starting — timezone:", TIMEZONE)
    # Warm-run immediately
    try:
        job_remind_once()
    except Exception as e:
        log("Warm-run error:", e)
        traceback.print_exc()
    start_scheduler()
