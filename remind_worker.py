# remind_worker.py
"""
Reminder worker (variant): send a single Telegram message at REMIND_HOUR:REMIND_MINUTE
containing only tasks due today or overdue, based on Notion property "Ngày hoàn thành".
This version intentionally DOES NOT include Notion links.
Env vars required:
 - NOTION_TOKEN
 - REMIND_NOTION_DATABASE_   (Notion database id)
 - TELEGRAM_TOKEN
 - TELEGRAM_CHAT_ID
Optional:
 - REMIND_HOUR (default 14)
 - REMIND_MINUTE (default 0)
 - TIMEZONE (default Asia/Ho_Chi_Minh)
 - MIN_REPEAT_MINUTES (default 120)  # kept for safety, not used to filter tasks today
"""
import os
import requests
import datetime
import pytz
import time
from apscheduler.schedulers.blocking import BlockingScheduler

# --- Config from env ---
NOTION_TOKEN = os.getenv("NOTION_TOKEN", "").strip()
REMIND_DB = os.getenv("REMIND_NOTION_DATABASE_", "").strip()
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

NOTION_VERSION = os.getenv("NOTION_VERSION", "2022-06-28")
HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}" if NOTION_TOKEN else "",
    "Notion-Version": NOTION_VERSION,
    "Content-Type": "application/json",
}

TIMEZONE = os.getenv("TIMEZONE", "Asia/Ho_Chi_Minh")
REMIND_HOUR = int(os.getenv("REMIND_HOUR", "14"))
REMIND_MINUTE = int(os.getenv("REMIND_MINUTE", "0"))
MIN_REPEAT_MINUTES = int(os.getenv("MIN_REPEAT_MINUTES", "120"))

# Candidate last-reminded prop names to try updating (kept but optional)
LAST_REMIND_PROP_CANDIDATES = [
    "Last reminded at",
    "Last reminded time",
    "Last reminded",
    "Last edited time",
    "Last reminded time (auto)"
]

# ---------- Notion helpers ----------
def notion_query_due_today_or_overdue(page_size=100):
    """
    Query pages where:
      - active checkbox == True
      - Done checkbox == False (if Done is checkbox)
      - "Ngày hoàn thành" date on_or_before today
    """
    if not NOTION_TOKEN or not REMIND_DB:
        print("Missing NOTION_TOKEN or REMIND_NOTION_DATABASE_ env")
        return []

    today_iso = datetime.datetime.now(pytz.timezone(TIMEZONE)).date().isoformat()
    payload = {
        "filter": {
            "and": [
                {"property": "active", "checkbox": {"equals": True}},
                {"property": "Done", "checkbox": {"equals": False}},
                {"property": "Ngày hoàn thành", "date": {"on_or_before": today_iso}}
            ]
        },
        "page_size": page_size,
        "sorts": [
            {"property":"Ngày hoàn thành","direction":"ascending"}
        ]
    }
    try:
        r = requests.post(f"https://api.notion.com/v1/databases/{REMIND_DB}/query",
                          headers=HEADERS, json=payload, timeout=20)
        r.raise_for_status()
        return r.json().get("results", [])
    except Exception as e:
        # Fallback: if Done is not checkbox or property name mismatch, try simpler filter by date & active only
        print("Primary query failed (maybe Done not checkbox). Error:", e)
        try:
            fb = {
                "filter": {
                    "and": [
                        {"property": "active", "checkbox": {"equals": True}},
                        {"property": "Ngày hoàn thành", "date": {"on_or_before": today_iso}}
                    ]
                },
                "page_size": page_size
            }
            r2 = requests.post(f"https://api.notion.com/v1/databases/{REMIND_DB}/query",
                               headers=HEADERS, json=fb, timeout=20)
            r2.raise_for_status()
            return r2.json().get("results", [])
        except Exception as ex:
            print("Fallback query failed:", ex)
            return []

def extract_plain_text(arr):
    if not arr:
        return ""
    return "".join([it.get("plain_text", "") for it in arr if isinstance(it, dict)])

def get_title_from_page(page):
    props = page.get("properties", {})
    # title property detection
    for k, v in props.items():
        if v.get("type") == "title":
            return extract_plain_text(v.get("title", []))
    # fallback to 'name' if exists
    if "name" in props:
        v = props["name"]
        if v.get("type") in ("title", "rich_text"):
            return extract_plain_text(v.get(v.get("type"), []))
    return page.get("id", "")[:8]

def get_note_from_page(page):
    props = page.get("properties", {})
    # prioritize property named 'note' or variants
    for k, v in props.items():
        if k.lower() in ("note", "ghi chú", "ghi_chu"):
            t = v.get("type")
            if t == "rich_text":
                return extract_plain_text(v.get("rich_text", []))
            if t == "title":
                return extract_plain_text(v.get("title", []))
    # fallback: any rich_text content
    for k, v in props.items():
        if v.get("type") == "rich_text":
            s = extract_plain_text(v.get("rich_text", []))
            if s:
                return s
    return ""

def get_due_date_from_page(page):
    props = page.get("properties", {})
    for k, v in props.items():
        if k == "Ngày hoàn thành":
            if v.get("type") == "date":
                d = v.get("date", {}).get("start")
                return d  # ISO date or datetime string
    # fallback: try case-insensitive match
    for k, v in props.items():
        if k.lower() == "ngày hoàn thành":
            if v.get("type") == "date":
                return v.get("date", {}).get("start")
    return None

def parse_iso_date_to_date(iso):
    if not iso:
        return None
    try:
        # iso may be "YYYY-MM-DD" or "YYYY-MM-DDTHH:MM:SSZ"
        s = iso.split("T")[0]
        y, m, d = s.split("-")
        return datetime.date(int(y), int(m), int(d))
    except Exception:
        return None

def update_last_reminded_if_exists(page_id):
    now_iso = datetime.datetime.now(pytz.timezone(TIMEZONE)).isoformat()
    for prop in LAST_REMIND_PROP_CANDIDATES:
        body = {"properties": {prop: {"date": {"start": now_iso}}}}
        try:
            r = requests.patch(f"https://api.notion.com/v1/pages/{page_id}", headers=HEADERS, json=body, timeout=12)
            if r.status_code in (200, 204):
                return True
        except Exception:
            pass
    return False

# ---------- Telegram ----------
def send_telegram(chat_id, text):
    if not TELEGRAM_TOKEN or not chat_id:
        print("Missing TELEGRAM_TOKEN or chat_id")
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
def job_remind_14h():
    now_dt = datetime.datetime.now(pytz.timezone(TIMEZONE))
    today_date = now_dt.date()
    print(f"[{now_dt.isoformat()}] Running remind job (due today or overdue)")

    pages = notion_query_due_today_or_overdue()
    if not pages:
        print("No tasks due today or overdue.")
        return

    # build summary list
    items = []
    for p in pages:
        title = get_title_from_page(p)
        note = get_note_from_page(p)
        due_iso = get_due_date_from_page(p)
        due_date = parse_iso_date_to_date(due_iso)
        overdue_days = None
        status_label = ""
        if due_date:
            delta = (today_date - due_date).days
            if delta > 0:
                status_label = f"🔴 Quá hạn {delta} ngày"
                overdue_days = delta
            elif delta == 0:
                status_label = "⏳ Hạn hôm nay"
            else:
                status_label = f"📅 Hạn {abs(delta)} ngày nữa"  # shouldn't happen due to filter
        else:
            status_label = "⏳ Hạn: không xác định"

        # prepare item text: Title + status + note (if any)
        item_lines = [f"• {title} — {status_label}"]
        if note:
            # keep note short
            n = note.strip()
            if len(n) > 400:
                n = n[:400] + "..."
            item_lines.append(f"  ↳ {n}")
        items.append("\n".join(item_lines))

    # compose final message
    header = f"🔔 Nhắc việc — {len(items)} công việc hôm nay ({today_date.isoformat()})"
    body = "\n\n".join(items)
    final_msg = f"{header}\n\n{body}"

    # send single message
    ok, resp = send_telegram(TELEGRAM_CHAT_ID, final_msg)
    if ok:
        print("Sent summary reminder, count:", len(items))
        # update last reminded for each page if possible
        for p in pages:
            pid = p.get("id")
            try:
                update_last_reminded_if_exists(pid)
            except Exception:
                pass
            time.sleep(0.15)
    else:
        print("Failed to send summary reminder:", resp)

# ---------- Scheduler ----------
def start_scheduler():
    sched = BlockingScheduler(timezone=TIMEZONE)
    sched.add_job(job_remind_14h, 'cron', hour=REMIND_HOUR, minute=REMIND_MINUTE)
    print(f"Scheduled daily reminder at {REMIND_HOUR:02d}:{REMIND_MINUTE:02d} {TIMEZONE}")
    # warm run once for immediate feedback on start
    try:
        job_remind_14h()
    except Exception as e:
        print("Initial run failed:", e)
    try:
        sched.start()
    except (KeyboardInterrupt, SystemExit):
        print("Scheduler stopped.")

if __name__ == "__main__":
    start_scheduler()
