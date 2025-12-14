# switch_app.py
# Extension module for app.py (do NOT modify app.py)
# Place this file alongside app.py on your Render service.
#
# Exports three functions to be called from app.py or other modules:
#   handle_switch_on(chat_id: int, keyword: str)
#   handle_switch_off(chat_id: int, keyword: str)
#   undo_switch(chat_id: int)
#
# Requirements: app.py must expose the following names:
# send_telegram, edit_telegram_message, find_target_matches, extract_prop_text,
# parse_money_from_text, create_page_in_db, archive_page, unarchive_page,
# update_page_properties, create_lai_page, query_database_all, undo_stack,
# NOTION_DATABASE_ID, find_prop_key
#
# The code below is defensive about missing fields and logs errors to Telegram.

import time
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional

# Import utilities and shared state from app.py
from app import (
    send_telegram,
    edit_telegram_message,
    find_target_matches,
    extract_prop_text,
    parse_money_from_text,
    create_page_in_db,
    archive_page,
    unarchive_page,
    update_page_properties,
    create_lai_page,
    query_database_all,
    undo_stack,
    NOTION_DATABASE_ID,
    find_prop_key,
)


VN_TZ = timezone(timedelta(hours=7))


# -----------------------
# Helpers: snapshot/restore
# -----------------------
def snapshot_target_props(props: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build a snapshot mapping of the target page's properties that we will change.
    The snapshot stores the actual property objects from Notion (so they can be PATCHed back).
    """
    keys_like = [
        "trạng thái",
        "Tổng Quan Đầu Tư",
        "Tổng Thụ Động",
        "Ngày Đáo",
        "ngày xong",
    ]
    snap = {}
    if not props:
        return snap
    for kl in keys_like:
        actual = find_prop_key(props, kl)
        if actual and actual in props:
            snap[actual] = props[actual]
    return snap


def restore_target_snapshot(page_id: str, snapshot: Dict[str, Any]) -> None:
    """
    Restore a previously-saved snapshot. snapshot keys must be the property names (as present in DB).
    """
    if not snapshot:
        return
    try:
        update_page_properties(page_id, snapshot)
    except Exception as e:
        # best-effort: notify but continue
        try:
            send_telegram("", f"⚠️ restore_target_snapshot error: {e}")
        except:
            pass


def safe_edit(chat_id: int, message_id: Optional[int], text: str) -> None:
    """Helper that falls back to send_telegram if edit fails."""
    try:
        if message_id:
            edit_telegram_message(chat_id, message_id, text)
            return
    except Exception:
        pass
    send_telegram(chat_id, text)


# -----------------------
# SWITCH ON implementation
# -----------------------
def handle_switch_on(chat_id: int, keyword: str) -> None:
    """
    ON:
      - find target in TARGET_NOTION_DATABASE_ID (via find_target_matches)
      - snapshot target properties
      - set target properties:
          trạng thái = In progress
          Tổng Quan Đầu Tư = Thụ động
          Tổng Thụ Động = G
          Ngày Đáo = today (VN)
      - read "ngày trước" (N)
      - create N pages in NOTION_DATABASE_ID:
          Name, Ngày Góp (today + i), Tiền = G ngày, Đã Góp = True, Lịch G = relation -> target
      - push undo log to undo_stack[chat_id]
      - animate progress in Telegram (single editable message)
    """
    try:
        matches = find_target_matches(keyword)
    except Exception as e:
        send_telegram(chat_id, f"❌ Lỗi tìm target: {e}")
        return

    if not matches:
        send_telegram(chat_id, f"❌ Không tìm thấy target '{keyword}'")
        return

    # pick first match
    page_id, title, props = matches[0]

    # start animation message
    m = send_telegram(chat_id, f"🔄 Bật ON cho {title} ...")
    mid = m.get("result", {}).get("message_id")

    def update(txt: str):
        safe_edit(chat_id, mid, txt)

    # snapshot
    snapshot = snapshot_target_props(props)

    # prepare property names (use actual property keys if possible)
    # fallback to literal name if find_prop_key not found
    def prop_key(name_like: str) -> str:
        k = find_prop_key(props, name_like)
        return k if k else name_like

    try:
        # TODAY in VN timezone
        today = datetime.now(VN_TZ).date().isoformat()

        # update target props
        update_page_properties(
            page_id,
            {
                prop_key("trạng thái"): {"select": {"name": "In progress"}},
                prop_key("Tổng Quan Đầu Tư"): {"select": {"name": "Thụ động"}},
                prop_key("Tổng Thụ Động"): {"select": {"name": "G"}},
                prop_key("Ngày Đáo"): {"date": {"start": today}},
            },
        )
        update("⚙️ Đã cập nhật trạng thái, đang chuẩn bị tạo ngày ...")
        time.sleep(0.25)
    except Exception as e:
        update(f"❌ Lỗi khi cập nhật target: {e}")
        return

    # read days and per_day
    try:
        raw_days = extract_prop_text(props, "ngày trước") or ""
        try:
            days = int(float(raw_days))
            if days < 0:
                days = 0
        except Exception:
            days = 0
        per_day_val = parse_money_from_text(extract_prop_text(props, "G ngày") or "")
    except Exception:
        days = 0
        per_day_val = 0

    created_pages: List[str] = []
    start_date = datetime.now(VN_TZ).date()

    if days <= 0:
        update("ℹ️ 'ngày trước' = 0 -> không tạo ngày. Hoàn tất ON.")
        # still record undo (snapshot only, no created pages) so undo will restore original props
        undo_stack.setdefault(str(chat_id), []).append(
            {
                "action": "switch_on",
                "target_id": page_id,
                "snapshot": snapshot,
                "created_pages": created_pages,
                "timestamp": datetime.now(VN_TZ).isoformat(),
            }
        )
        return

    update(f"📅 Bắt đầu tạo {days} ngày (Đã Góp = True) ...")
    for i in range(days):
        d = start_date + timedelta(days=i)
        props_payload = {
            "Name": {"title": [{"type": "text", "text": {"content": title}}]},
            "Ngày Góp": {"date": {"start": d.isoformat()}},
            "Tiền": {"number": per_day_val},
            "Đã Góp": {"checkbox": True},
            "Lịch G": {"relation": [{"id": page_id}]},
        }
        try:
            ok, res = create_page_in_db(NOTION_DATABASE_ID, props_payload)
            if ok and isinstance(res, dict):
                new_id = res.get("id")
                if new_id:
                    created_pages.append(new_id)
        except Exception as e:
            # continue on error but report
            update(f"⚠️ Lỗi tạo ngày {i+1}: {e}")

        # progress bar
        try:
            pct = (i + 1) / days
            bar = int(pct * 10)
            if bar < 1 and days > 0:
                bar = 1
            bar_str = "█" * bar + "░" * (10 - bar)
        except Exception:
            bar_str = "██████████"

        update(f"📆 Tạo ngày {i+1}/{days} [{bar_str}] — {d.isoformat()}")
        time.sleep(0.25)

    # push undo log
    undo_stack.setdefault(str(chat_id), []).append(
        {
            "action": "switch_on",
            "target_id": page_id,
            "snapshot": snapshot,
            "created_pages": created_pages,
            "timestamp": datetime.now(VN_TZ).isoformat(),
        }
    )

    update(f"✅ Đã bật ON cho {title} — Đã tạo {len(created_pages)} ngày.")


# ------------------------
# SWITCH OFF implementation
# ------------------------
def handle_switch_off(chat_id: int, keyword: str) -> None:
    """
    OFF:
      - find target
      - snapshot target
      - find all day pages by relation "Lịch G" -> target
      - archive all day pages
      - create Lãi page in LA_NOTION_DATABASE_ID (via create_lai_page)
      - update target:
          trạng thái = Done
          remove Tổng Quan Đầu Tư (clear select)
          remove Tổng Thụ Động (clear select)
          ngày xong = today (VN)
      - push undo log with archived_pages and lai_page_id
      - animate progress in Telegram (single editable message)
    """
    try:
        matches = find_target_matches(keyword)
    except Exception as e:
        send_telegram(chat_id, f"❌ Lỗi tìm target: {e}")
        return

    if not matches:
        send_telegram(chat_id, f"❌ Không tìm thấy target '{keyword}'")
        return

    page_id, title, props = matches[0]
    m = send_telegram(chat_id, f"⏳ Đang OFF cho {title} ...")
    mid = m.get("result", {}).get("message_id")

    def update(txt: str):
        safe_edit(chat_id, mid, txt)

    snapshot = snapshot_target_props(props)

    # find day pages in calendar DB that link to this target via relation "Lịch G"
    try:
        all_pages = query_database_all(NOTION_DATABASE_ID)
    except Exception as e:
        update(f"❌ Lỗi query calendar DB: {e}")
        return

    to_archive: List[str] = []
    for p in all_pages:
        props_p = p.get("properties", {}) or {}
        rel_key = find_prop_key(props_p, "Lịch G")
        if not rel_key:
            continue
        rel_arr = props_p.get(rel_key, {}).get("relation", []) or []
        if any(r.get("id") == page_id for r in rel_arr):
            to_archive.append(p.get("id"))

    total = len(to_archive)
    if total == 0:
        update("🧹 Không có ngày để xóa.")
    else:
        update(f"🧹 Bắt đầu xóa {total} ngày ...")
        for idx, pid in enumerate(to_archive, start=1):
            try:
                archive_page(pid)
            except Exception as e:
                # continue but log
                update(f"⚠️ Lỗi xóa ngày {idx}: {e}")

            pct = idx / total if total else 1
            bar = int(pct * 10)
            if bar < 1 and total > 0:
                bar = 1
            bar_str = "█" * bar + "░" * (10 - bar)
            update(f"🗑️ Xóa {idx}/{total} [{bar_str}]")
            time.sleep(0.25)

    # create lai page (if configured and value > 0)
    lai_text = extract_prop_text(props, "Lai lịch g") or extract_prop_text(props, "Lãi") or extract_prop_text(props, "Lai") or ""
    lai_amt = parse_money_from_text(lai_text) or 0
    lai_page_id: Optional[str] = None
    if lai_amt > 0:
        try:
            lai_page_id = create_lai_page(chat_id, title, lai_amt, page_id)
            # create_lai_page sends its own telegram notification (consistent with app.py)
        except Exception as e:
            update(f"⚠️ Lỗi tạo Lãi: {e}")

    # update target: Done + clear selects + set ngày xong
    try:
        today = datetime.now(VN_TZ).date().isoformat()
        # pick actual property names if available
        def prop_key_local(name_like: str) -> str:
            return find_prop_key(props, name_like) or name_like

        update_page_properties(
            page_id,
            {
                prop_key_local("trạng thái"): {"select": {"name": "Done"}},
                prop_key_local("Tổng Quan Đầu Tư"): {"select": None},
                prop_key_local("Tổng Thụ Động"): {"select": None},
                prop_key_local("ngày xong"): {"date": {"start": today}},
            },
        )
    except Exception as e:
        update(f"⚠️ Lỗi cập nhật target sau OFF: {e}")

    # log undo
    undo_stack.setdefault(str(chat_id), []).append(
        {
            "action": "switch_off",
            "target_id": page_id,
            "snapshot": snapshot,
            "archived_pages": to_archive,
            "lai_page_id": lai_page_id,
            "timestamp": datetime.now(VN_TZ).isoformat(),
        }
    )

    update(f"✅ Đã OFF cho {title} — Đã xóa {total} ngày.")


# ------------------------
# UNDO for switch on/off
# ------------------------
def undo_switch(chat_id: int) -> None:
    """
    Undo the last switch_on / switch_off action for this chat_id.
    - For switch_on: archive created pages, restore target snapshot
    - For switch_off: unarchive archived pages, archive lai_page (if created), restore snapshot
    """
    stack = undo_stack.get(str(chat_id), [])
    if not stack:
        send_telegram(chat_id, "❌ Không có thao tác để undo")
        return

    log = stack.pop()
    if not isinstance(log, dict):
        send_telegram(chat_id, "⚠️ Undo log malformed")
        return

    action = log.get("action")
    target_id = log.get("target_id")
    snapshot = log.get("snapshot", {}) or {}

    # attempt to restore snapshot first (best-effort)
    try:
        if snapshot:
            update_page_properties(target_id, snapshot)
    except Exception as e:
        send_telegram(chat_id, f"⚠️ Lỗi restore target: {e}")

    if action == "switch_on":
        created = log.get("created_pages", []) or []
        total = len(created)
        if total:
            msg = send_telegram(chat_id, f"♻️ Đang undo ON — xóa {total} page vừa tạo ...")
            mid = msg.get("result", {}).get("message_id")
            for idx, pid in enumerate(created, start=1):
                try:
                    archive_page(pid)
                except Exception:
                    pass
                pct = idx / total
                bar = int(pct * 10)
                if bar < 1:
                    bar = 1
                bar_str = "█" * bar + "░" * (10 - bar)
                safe_edit(chat_id, mid, f"♻️ Xóa {idx}/{total} [{bar_str}]")
                time.sleep(0.25)
        send_telegram(chat_id, "✅ Hoàn tác ON hoàn tất.")

    elif action == "switch_off":
        archived = log.get("archived_pages", []) or []
        lai_page_id = log.get("lai_page_id")
        total = len(archived)
        if total:
            msg = send_telegram(chat_id, f"♻️ Đang undo OFF — phục hồi {total} ngày ...")
            mid = msg.get("result", {}).get("message_id")
            for idx, pid in enumerate(archived, start=1):
                try:
                    unarchive_page(pid)
                except Exception:
                    pass
                pct = idx / total
                bar = int(pct * 10)
                if bar < 1:
                    bar = 1
                bar_str = "█" * bar + "░" * (10 - bar)
                safe_edit(chat_id, mid, f"♻️ Phục hồi {idx}/{total} [{bar_str}]")
                time.sleep(0.25)
        # archive lai page created during OFF
        if lai_page_id:
            try:
                archive_page(lai_page_id)
            except Exception:
                pass
        send_telegram(chat_id, "✅ Hoàn tác OFF hoàn tất.")

    else:
        send_telegram(chat_id, "⚠️ Không có hành động switch trong log để undo")


# Exported names for convenience if imported with *
__all__ = ["handle_switch_on", "handle_switch_off", "undo_switch"]
