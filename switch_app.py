# ============================================================
# switch_app.py
# Plugin mở rộng cho app.py (ON / OFF / UNDO switch)
# KHÔNG import app.py — dùng dependency injection (deps)
# ============================================================

import time
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional

VN_TZ = timezone(timedelta(hours=7))

# ============================================================
# Dependency container (inject từ app.py)
# ============================================================
deps: Dict[str, Any] = {}


def init_switch_deps(**kwargs):
    """
    app.py PHẢI gọi hàm này 1 lần sau khi load xong các hàm core.

    Required keys:
      send_telegram
      edit_telegram_message
      find_target_matches
      extract_prop_text
      parse_money_from_text
      create_page_in_db
      archive_page
      unarchive_page
      update_page_properties
      create_lai_page
      query_database_all
      undo_stack
      NOTION_DATABASE_ID
      find_prop_key
    """
    deps.update(kwargs)


# ============================================================
# Helper utilities
# ============================================================
def _now_vn_date() -> str:
    return datetime.now(VN_TZ).date().isoformat()


def _progress_bar(i: int, total: int, width: int = 10) -> str:
    if total <= 0:
        return "█" * width
    filled = max(1, int((i / total) * width))
    return "█" * filled + "░" * (width - filled)


def _safe_edit(chat_id: int, message_id: Optional[int], text: str):
    try:
        if message_id:
            deps["edit_telegram_message"](chat_id, message_id, text)
            return
    except Exception:
        pass
    deps["send_telegram"](chat_id, text)


def _snapshot_target(props: Dict[str, Any]) -> Dict[str, Any]:
    """
    Lưu snapshot các cột sẽ bị thay đổi để UNDO
    """
    snap = {}
    for name_like in [
        "trạng thái",
        "Tổng Quan Đầu Tư",
        "Tổng Thụ Động",
        "Ngày Đáo",
        "ngày xong",
    ]:
        key = deps["find_prop_key"](props, name_like)
        if key and key in props:
            snap[key] = props[key]
    return snap


def _restore_snapshot(page_id: str, snapshot: Dict[str, Any]):
    if snapshot:
        deps["update_page_properties"](page_id, snapshot)


# ============================================================
# SWITCH ON
# ============================================================
def handle_switch_on(chat_id: int, keyword: str):
    send_telegram = deps["send_telegram"]
    update_page_properties = deps["update_page_properties"]
    find_target_matches = deps["find_target_matches"]
    extract_prop_text = deps["extract_prop_text"]
    parse_money = deps["parse_money_from_text"]
    create_page = deps["create_page_in_db"]
    undo_stack = deps["undo_stack"]
    NOTION_DB = deps["NOTION_DATABASE_ID"]
    find_prop_key = deps["find_prop_key"]

    matches = find_target_matches(keyword)
    if not matches:
        send_telegram(chat_id, f"❌ Không tìm thấy {keyword}")
        return

    page_id, title, props = matches[0]
    msg = send_telegram(chat_id, f"🔄 Bật ON cho {title} ...")
    mid = msg.get("result", {}).get("message_id")

    snapshot = _snapshot_target(props)

    def pk(name_like: str) -> str:
        return find_prop_key(props, name_like) or name_like

    # --- Update target ---
    update_page_properties(page_id, {
        pk("trạng thái"): {"select": {"name": "In progress"}},
        pk("Tổng Quan Đầu Tư"): {"select": {"name": "Thụ động"}},
        pk("Tổng Thụ Động"): {"select": {"name": "G"}},
        pk("Ngày Đáo"): {"date": {"start": _now_vn_date()}},
    })
    _safe_edit(chat_id, mid, "⚙️ Đã cập nhật trạng thái, đang tạo ngày ...")
    time.sleep(0.25)

    # --- Read days ---
    raw_days = extract_prop_text(props, "ngày trước") or "0"
    try:
        days = int(float(raw_days))
    except Exception:
        days = 0

    per_day = parse_money(extract_prop_text(props, "G ngày") or "")

    created_pages: List[str] = []
    start = datetime.now(VN_TZ).date()

    if days <= 0:
        undo_stack.setdefault(str(chat_id), []).append({
            "action": "switch_on",
            "target_id": page_id,
            "snapshot": snapshot,
            "created_pages": [],
        })
        _safe_edit(chat_id, mid, f"✅ Đã bật ON cho {title} (không có ngày)")
        return

    for i in range(days):
        d = start + timedelta(days=i)
        payload = {
            "Name": {"title": [{"text": {"content": title}}]},
            "Ngày Góp": {"date": {"start": d.isoformat()}},
            "Tiền": {"number": per_day},
            "Đã Góp": {"checkbox": True},
            "Lịch G": {"relation": [{"id": page_id}]},
        }
        ok, res = create_page(NOTION_DB, payload)
        if ok and isinstance(res, dict) and res.get("id"):
            created_pages.append(res["id"])

        bar = _progress_bar(i + 1, days)
        _safe_edit(chat_id, mid, f"📆 {i+1}/{days} [{bar}]")
        time.sleep(0.25)

    undo_stack.setdefault(str(chat_id), []).append({
        "action": "switch_on",
        "target_id": page_id,
        "snapshot": snapshot,
        "created_pages": created_pages,
    })

    _safe_edit(chat_id, mid, f"✅ Đã bật ON cho {title}")


# ============================================================
# SWITCH OFF
# ============================================================
def handle_switch_off(chat_id: int, keyword: str):
    send_telegram = deps["send_telegram"]
    update_page_properties = deps["update_page_properties"]
    find_target_matches = deps["find_target_matches"]
    extract_prop_text = deps["extract_prop_text"]
    parse_money = deps["parse_money_from_text"]
    archive_page = deps["archive_page"]
    unarchive_page = deps["unarchive_page"]
    create_lai_page = deps["create_lai_page"]
    query_all = deps["query_database_all"]
    undo_stack = deps["undo_stack"]
    NOTION_DB = deps["NOTION_DATABASE_ID"]
    find_prop_key = deps["find_prop_key"]

    matches = find_target_matches(keyword)
    if not matches:
        send_telegram(chat_id, f"❌ Không tìm thấy {keyword}")
        return

    page_id, title, props = matches[0]
    msg = send_telegram(chat_id, f"⏳ Đang OFF {title} ...")
    mid = msg.get("result", {}).get("message_id")

    snapshot = _snapshot_target(props)

    # --- Find & archive day pages ---
    pages = query_all(NOTION_DB)
    archived: List[str] = []

    for p in pages:
        pprops = p.get("properties", {})
        rel_key = find_prop_key(pprops, "Lịch G")
        if not rel_key:
            continue
        rels = pprops.get(rel_key, {}).get("relation", [])
        if any(r.get("id") == page_id for r in rels):
            archive_page(p["id"])
            archived.append(p["id"])

    if archived:
        for i in range(len(archived)):
            bar = _progress_bar(i + 1, len(archived))
            _safe_edit(chat_id, mid, f"🗑️ {i+1}/{len(archived)} [{bar}]")
            time.sleep(0.25)

    # --- Create Lãi ---
    lai_amt = parse_money(extract_prop_text(props, "Lai lịch g") or "")
    lai_page_id = None
    if lai_amt > 0:
        lai_page_id = create_lai_page(chat_id, title, lai_amt, page_id)

    # --- Update target ---
    def pk(name_like: str) -> str:
        return find_prop_key(props, name_like) or name_like

    update_page_properties(page_id, {
        pk("trạng thái"): {"select": {"name": "Done"}},
        pk("Tổng Quan Đầu Tư"): {"select": None},
        pk("Tổng Thụ Động"): {"select": None},
        pk("ngày xong"): {"date": {"start": _now_vn_date()}},
    })

    undo_stack.setdefault(str(chat_id), []).append({
        "action": "switch_off",
        "target_id": page_id,
        "snapshot": snapshot,
        "archived_pages": archived,
        "lai_page_id": lai_page_id,
    })

    _safe_edit(chat_id, mid, f"✅ Đã OFF {title}")


# ============================================================
# UNDO SWITCH
# ============================================================
def undo_switch(chat_id: int):
    send_telegram = deps["send_telegram"]
    archive_page = deps["archive_page"]
    unarchive_page = deps["unarchive_page"]
    undo_stack = deps["undo_stack"]

    stack = undo_stack.get(str(chat_id), [])
    if not stack:
        send_telegram(chat_id, "❌ Không có thao tác để undo")
        return

    log = stack.pop()
    _restore_snapshot(log["target_id"], log.get("snapshot", {}))

    if log["action"] == "switch_on":
        for pid in log.get("created_pages", []):
            archive_page(pid)
        send_telegram(chat_id, "♻️ Hoàn tác ON hoàn tất")
        return

    if log["action"] == "switch_off":
        for pid in log.get("archived_pages", []):
            unarchive_page(pid)
        if log.get("lai_page_id"):
            archive_page(log["lai_page_id"])
        send_telegram(chat_id, "♻️ Hoàn tác OFF hoàn tất")
        return


__all__ = ["init_switch_deps", "handle_switch_on", "handle_switch_off", "undo_switch"]
