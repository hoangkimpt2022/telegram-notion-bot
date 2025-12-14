# ============================================================
# switch_app.py
# ON  = mở vòng góp (In progress + Thụ động + G + Ngày Đáo)
# OFF = đóng vòng góp (Done + bỏ Thụ động + bỏ G + ngày xong)
# ============================================================

import time
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional

VN_TZ = timezone(timedelta(hours=7))
deps: Dict[str, Any] = {}

# ================== INIT ==================
def init_switch_deps(**kwargs):
    deps.update(kwargs)

# ================== HELPERS ==================
def today_vn():
    return datetime.now(VN_TZ).date().isoformat()

def safe_edit(chat_id, mid, text):
    try:
        if mid:
            deps["edit_telegram_message"](chat_id, mid, text)
            return
    except Exception:
        pass
    deps["send_telegram"](chat_id, text)

def prop_key(props, name_like):
    return deps["find_prop_key"](props, name_like) or name_like

def snapshot_target(props):
    snap = {}
    for k in ["trạng thái", "Tổng Quan Đầu Tư", "Tổng Thụ Động", "Ngày Đáo", "ngày xong"]:
        key = prop_key(props, k)
        if key in props:
            snap[key] = props[key]
    return snap

# ================== SWITCH ON ==================
def handle_switch_on(chat_id: int, keyword: str):
    send = deps["send_telegram"]
    find_target = deps["find_target_matches"]
    update = deps["update_page_properties"]
    create_page = deps["create_page_in_db"]
    parse_money = deps["parse_money_from_text"]
    extract = deps["extract_prop_text"]
    undo_stack = deps["undo_stack"]

    matches = find_target(keyword)
    if not matches:
        send(chat_id, f"❌ Không tìm thấy {keyword}")
        return

    pid, title, props = matches[0]
    msg = send(chat_id, f"🔄 Đang bật ON cho {title} ...")
    mid = msg.get("result", {}).get("message_id")

    snap = snapshot_target(props)

    # === 1. UPDATE TARGET (BẮT BUỘC) ===
    update(pid, {
        prop_key(props, "trạng thái"): {"select": {"name": "In progress"}},
        prop_key(props, "Tổng Quan Đầu Tư"): {"select": {"name": "Thụ động"}},
        prop_key(props, "Tổng Thụ Động"): {"select": {"name": "G"}},
        prop_key(props, "Ngày Đáo"): {"date": {"start": today_vn()}},
    })

    safe_edit(chat_id, mid, "⚙️ Đã cập nhật TARGET → In progress / Thụ động / G")
    time.sleep(0.3)

    # === 2. CREATE DAYS ===
    raw_days = extract(props, "ngày trước") or "0"
    try:
        days = int(float(raw_days))
    except:
        days = 0

    per_day = parse_money(extract(props, "G ngày") or "")
    created = []
    start = datetime.now(VN_TZ).date()

    for i in range(days):
        d = start + timedelta(days=i)
        ok, res = create_page(deps["NOTION_DATABASE_ID"], {
            "Name": {"title": [{"text": {"content": title}}]},
            "Ngày Góp": {"date": {"start": d.isoformat()}},
            "Tiền": {"number": per_day},
            "Đã Góp": {"checkbox": True},
            "Lịch G": {"relation": [{"id": pid}]},
        })
        if ok and res.get("id"):
            created.append(res["id"])
        safe_edit(chat_id, mid, f"📆 {i+1}/{days} tạo ngày")
        time.sleep(0.2)

    undo_stack.setdefault(str(chat_id), []).append({
        "action": "switch_on",
        "target_id": pid,
        "snapshot": snap,
        "created_pages": created,
    })

    safe_edit(chat_id, mid, f"✅ Đã bật ON cho {title}")

# ================== SWITCH OFF ==================
def handle_switch_off(chat_id: int, keyword: str):
    send = deps["send_telegram"]
    find_target = deps["find_target_matches"]
    update = deps["update_page_properties"]
    extract = deps["extract_prop_text"]
    parse_money = deps["parse_money_from_text"]
    archive = deps["archive_page"]
    create_lai = deps["create_lai_page"]
    query_all = deps["query_database_all"]
    undo_stack = deps["undo_stack"]

    matches = find_target(keyword)
    if not matches:
        send(chat_id, f"❌ Không tìm thấy {keyword}")
        return

    pid, title, props = matches[0]
    msg = send(chat_id, f"⏳ Đang OFF {title} ...")
    mid = msg.get("result", {}).get("message_id")

    snap = snapshot_target(props)

    # === 1. ARCHIVE ALL DAYS ===
    archived = []
    for p in query_all(deps["NOTION_DATABASE_ID"]):
        pprops = p.get("properties", {})
        rel_key = prop_key(pprops, "Lịch G")
        if any(r.get("id") == pid for r in pprops.get(rel_key, {}).get("relation", [])):
            archive(p["id"])
            archived.append(p["id"])

    # === 2. CREATE LAI ===
    lai_amt = parse_money(extract(props, "Lai lịch g") or "")
    lai_id = None
    if lai_amt > 0:
        lai_id = create_lai(chat_id, title, lai_amt, pid)

    # === 3. UPDATE TARGET (ĐÓNG VÒNG) ===
    update(pid, {
        prop_key(props, "trạng thái"): {"select": {"name": "Done"}},
        prop_key(props, "Tổng Quan Đầu Tư"): {"select": None},
        prop_key(props, "Tổng Thụ Động"): {"select": None},
        prop_key(props, "ngày xong"): {"date": {"start": today_vn()}},
    })

    undo_stack.setdefault(str(chat_id), []).append({
        "action": "switch_off",
        "target_id": pid,
        "snapshot": snap,
        "archived_pages": archived,
        "lai_page_id": lai_id,
    })

    safe_edit(chat_id, mid, f"✅ Đã OFF {title} – vòng góp kết thúc")

# ================== UNDO ==================
def undo_switch(chat_id: int):
    send = deps["send_telegram"]
    unarchive = deps["unarchive_page"]
    archive = deps["archive_page"]
    undo_stack = deps["undo_stack"]

    stack = undo_stack.get(str(chat_id), [])
    if not stack:
        send(chat_id, "❌ Không có thao tác để undo")
        return

    log = stack.pop()
    deps["update_page_properties"](log["target_id"], log["snapshot"])

    if log["action"] == "switch_on":
        for pid in log["created_pages"]:
            archive(pid)
        send(chat_id, "♻️ Undo ON hoàn tất")

    if log["action"] == "switch_off":
        for pid in log["archived_pages"]:
            unarchive(pid)
        if log.get("lai_page_id"):
            archive(log["lai_page_id"])
        send(chat_id, "♻️ Undo OFF hoàn tất")

__all__ = ["init_switch_deps", "handle_switch_on", "handle_switch_off", "undo_switch"]
