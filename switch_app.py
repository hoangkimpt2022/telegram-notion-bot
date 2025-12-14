# switch_app.py
# Plugin: SWITCH ON / OFF / UNDO (dependency-injected)
# Compatible with app.py in this repo (uses same helper names).
# Exports: init_switch_deps, handle_switch_on, handle_switch_off, undo_switch
# ---------------------------------------------------------------

import os
import time
import traceback
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional

VN_TZ = timezone(timedelta(hours=7))

# optional: page ids for relations (can be set via env)
SWITCH_QDT_PAGE_ID = os.getenv("SWITCH_QDT_PAGE_ID")  # page that represents "Thụ động"
SWITCH_TTD_PAGE_ID = os.getenv("SWITCH_TTD_PAGE_ID")  # page that represents "G"

# dependency container (injected from app.py)
deps: Dict[str, Any] = {}

# ---------------------------------------------------------------
def init_switch_deps(**kwargs):
    """
    Inject dependencies from app.py. app.py must call this once.
    Required keys (app.py passes these):
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
      find_prop_key
    """
    deps.update(kwargs)


# ----------------- Helpers -----------------
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
    Save a minimal snapshot of properties we will touch (so undo can restore).
    Stores the raw property objects returned by Notion (so update_page_properties can patch them back).
    """
    snap = {}
    for name in ["trạng thái", "Tổng Quan Đầu Tư", "Tổng Thụ Động", "Ngày Đáo", "ngày xong"]:
        try:
            key = deps["find_prop_key"](props, name)
            if key and key in props:
                snap[key] = props[key]
        except Exception:
            pass
    return snap


def _restore_snapshot(page_id: str, snapshot: Dict[str, Any]):
    if not snapshot:
        return
    try:
        deps["update_page_properties"](page_id, snapshot)
    except Exception as e:
        print("WARN restore snapshot:", e)


def _resolve_relation_page_id_by_env_or_search(name: str) -> Optional[str]:
    """
    Try env-provided page id first, otherwise attempt to find a page by title (best-effort).
    Note: app.py doesn't expose Notion search; we attempt to find in TARGET_NOTION_DATABASE_ID or skip.
    """
    # env override
    if name == "Thụ động" and SWITCH_QDT_PAGE_ID:
        return SWITCH_QDT_PAGE_ID
    if name == "G" and SWITCH_TTD_PAGE_ID:
        return SWITCH_TTD_PAGE_ID

    # attempt to find page in NOTION_DATABASE_ID (best effort)
    try:
        pages = deps["query_database_all"](deps["NOTION_DATABASE_ID"])
        for p in pages:
            props = p.get("properties", {}) or {}
            title = deps["extract_prop_text"](props, "Name") or ""
            if title and title.strip().lower() == name.strip().lower():
                return p.get("id")
    except Exception:
        pass

    return None


# ----------------- SWITCH ON -----------------
def handle_switch_on(chat_id: int, keyword: str):
    """
    - find target
    - snapshot props
    - update target properties:
        trạng thái -> In progress (select)
        Tổng Quan Đầu Tư -> relation -> Thụ động (if resolvable)
        Tổng Thụ Động -> relation -> G (if resolvable)
        Ngày Đáo -> today
    - create N day pages in NOTION_DATABASE_ID (Đã Góp = True)
    - save undo record into deps['undo_stack']
    - animated progress via edit_telegram_message
    """
    try:
        send_telegram = deps["send_telegram"]
        edit_msg = deps["edit_telegram_message"]
        find_matches = deps["find_target_matches"]
        extract_prop_text = deps["extract_prop_text"]
        parse_money = deps["parse_money_from_text"]
        create_page = deps["create_page_in_db"]
        update_page = deps["update_page_properties"]
        undo_stack = deps["undo_stack"]
        query_calendar_db = deps["query_database_all"]
        find_prop_key = deps["find_prop_key"]
        NOTION_DB = deps["NOTION_DATABASE_ID"]
    except Exception as e:
        traceback.print_exc()
        return

    matches = find_matches(keyword)
    if not matches:
        return send_telegram(chat_id, f"❌ Không tìm thấy '{keyword}'")

    page_id, title, props = matches[0]
    props = props if isinstance(props, dict) else {}

    msg = send_telegram(chat_id, f"🔄 Đang bật ON cho {title} ...")
    mid = msg.get("result", {}).get("message_id")

    snapshot = _snapshot_target(props)

    # helper to get property key name
    def pk(name_like: str) -> str:
        return find_prop_key(props, name_like) or name_like

    # resolve relation page ids (best-effort)
    qdt_pid = _resolve_relation_page_id_by_env_or_search("Thụ động")
    g_pid = _resolve_relation_page_id_by_env_or_search("G")

    # Build update payload for target page
    upd = {
        pk("trạng thái"): {"select": {"name": "In progress"}},
        pk("Ngày Đáo"): {"date": {"start": _now_vn_date()}},
    }
    # relation update if resolvable
    if qdt_pid:
        upd[pk("Tổng Quan Đầu Tư")] = {"relation": [{"id": qdt_pid}]}
    else:
        # attempt safe: if property type is select for some reason, set select name
        upd.setdefault(pk("Tổng Quan Đầu Tư"), {"relation": []})

    if g_pid:
        upd[pk("Tổng Thụ Động")] = {"relation": [{"id": g_pid}]}
    else:
        upd.setdefault(pk("Tổng Thụ Động"), {"relation": []})

    try:
        update_page(page_id, upd)
    except Exception as e:
        print("WARN update target on:", e)

    _safe_edit(chat_id, mid, "⚙️ Đã cập nhật trạng thái & Ngày Đáo. Đang tạo ngày (nếu có)...")

    # read day count & per-day value from props
    raw_days = extract_prop_text(props, "ngày trước") or extract_prop_text(props, "tổng ngày g") or "0"
    try:
        days_count = int(float(raw_days))
    except Exception:
        days_count = 0

    raw_gngay = extract_prop_text(props, "G ngày") or ""
    try:
        per_day = int(parse_money(raw_gngay))
    except Exception:
        # fallback to parse as float then int
        try:
            per_day = int(float(raw_gngay or 0))
        except:
            per_day = 0

    # compute created pages
    created_pages: List[str] = []
    start_date = datetime.now(VN_TZ).date()
    if days_count <= 0:
        # record undo (no created pages)
        undo_stack.setdefault(str(chat_id), []).append({
            "action": "switch_on",
            "target_id": page_id,
            "snapshot": snapshot,
            "created_pages": [],
        })
        _safe_edit(chat_id, mid, f"✅ Đã bật ON cho {title} (không có ngày để tạo).")
        return

    # create pages loop
    for i in range(days_count):
        d = start_date + timedelta(days=i)
        props_payload = {
            "Name": {"title": [{"text": {"content": title}}]},
            "Ngày Góp": {"date": {"start": d.isoformat()}},
            "Tiền": {"number": per_day},
            "Đã Góp": {"checkbox": True},
            "Lịch G": {"relation": [{"id": page_id}]},
        }
        try:
            ok, res = create_page(NOTION_DB, props_payload)
            if ok and isinstance(res, dict) and res.get("id"):
                created_pages.append(res.get("id"))
            else:
                # if create_page returns full json or (False,msg), attempt to handle
                if isinstance(res, dict) and res.get("id"):
                    created_pages.append(res.get("id"))
        except Exception as e:
            print("WARN create day page:", e)

        # update progress message
        try:
            bar = _progress_bar(i + 1, days_count)
            _safe_edit(chat_id, mid, f"📅 Tạo ngày {i+1}/{days_count} [{bar}] — {d.isoformat()}")
        except Exception:
            pass
        time.sleep(0.18)

    # Finalize and push undo log
    undo_stack.setdefault(str(chat_id), []).append({
        "action": "switch_on",
        "target_id": page_id,
        "snapshot": snapshot,
        "created_pages": created_pages,
    })

    # compute summary values for final message (CK = tiền - trước)
    try:
        money = int(parse_money(extract_prop_text(props, "tiền") or "0"))
    except Exception:
        money = 0
    try:
        truoc = int(float(extract_prop_text(props, "trước") or extract_prop_text(props, "ngày trước") or 0))
    except Exception:
        truoc = 0
    ck = money - (truoc * per_day)

    lines = [
        f"🔔 Đã bật ON cho: {title}",
        f"với số tiền {money:,} ngày {per_day:,} góp {days_count}",
        f"💴 Lấy trước: {truoc} ngày {per_day:,} là {truoc * per_day:,}",
        "   ( từ hôm nay):"
    ]
    for idx in range(days_count):
        d = (start_date + timedelta(days=idx)).isoformat()
        lines.append(f"{idx+1}. {d}")
    lines += [
        "",
        f"🏛️ Tổng CK: ✅ {ck:,}",
        f"📆 Đến ngày {(start_date + timedelta(days=days_count)).strftime('%d-%m-%Y')} bắt đầu góp lại",
        "",
        "🎉 Hoàn tất ON."
    ]
    _safe_edit(chat_id, mid, "\n".join(lines))


# ----------------- SWITCH OFF -----------------
def handle_switch_off(chat_id: int, keyword: str):
    """
    - find target
    - snapshot props
    - find & archive day pages that have relation Lịch G -> target
    - create Lãi page if applicable
    - update target: trạng thái -> Done, ngày xong = today, clear relation fields
    - save undo log (archived_pages list, lai_page_id)
    """
    try:
        send_telegram = deps["send_telegram"]
        find_matches = deps["find_target_matches"]
        query_all = deps["query_database_all"]
        archive_page = deps["archive_page"]
        unarchive_page = deps["unarchive_page"]
        create_lai_page = deps["create_lai_page"]
        update_page = deps["update_page_properties"]
        extract_prop_text = deps["extract_prop_text"]
        parse_money = deps["parse_money_from_text"]
        undo_stack = deps["undo_stack"]
        find_prop_key = deps["find_prop_key"]
        NOTION_DB = deps["NOTION_DATABASE_ID"]
    except Exception as e:
        traceback.print_exc()
        return

    matches = find_matches(keyword)
    if not matches:
        return send_telegram(chat_id, f"❌ Không tìm thấy '{keyword}'")

    page_id, title, props = matches[0]
    props = props if isinstance(props, dict) else {}

    msg = send_telegram(chat_id, f"⏳ Đang OFF {title} ...")
    mid = msg.get("result", {}).get("message_id")

    snapshot = _snapshot_target(props)

    # find & archive calendar pages related via "Lịch G"
    archived: List[str] = []
    try:
        all_pages = query_all(NOTION_DB)
        for p in all_pages:
            pprops = p.get("properties", {}) or {}
            rel_key = find_prop_key(pprops, "Lịch G")
            if not rel_key:
                continue
            rels = pprops.get(rel_key, {}).get("relation", []) or []
            if any(r.get("id") == page_id for r in rels):
                archive_page(p["id"])
                archived.append(p["id"])
    except Exception as e:
        print("WARN find/archive days:", e)

    # animate progress
    if archived:
        for i in range(len(archived)):
            try:
                bar = _progress_bar(i + 1, len(archived))
                _safe_edit(chat_id, mid, f"🗑️ {i+1}/{len(archived)} [{bar}]")
            except:
                pass
            time.sleep(0.22)

    # create Lãi if present (try fields: "Lai lịch g", "Lãi", "Lai")
    try:
        lai_text = extract_prop_text(props, "Lai lịch g") or extract_prop_text(props, "Lãi") or extract_prop_text(props, "Lai") or ""
        lai_amt = int(parse_money(lai_text) or 0)
    except Exception:
        lai_amt = 0

    lai_page_id = None
    if lai_amt and lai_amt > 0:
        try:
            lai_page_id = create_lai_page(chat_id, title, lai_amt, page_id)
        except Exception as e:
            print("WARN create_lai:", e)

    # update target: trạng thái Done, clear relations, set ngày xong
    try:
        def pk(name_like: str) -> str:
            return find_prop_key(props, name_like) or name_like

        upd = {
            pk("trạng thái"): {"select": {"name": "Done"}},
            pk("ngày xong"): {"date": {"start": _now_vn_date()}},
            # clear relations (Notion relation is array)
            pk("Tổng Quan Đầu Tư"): {"relation": []},
            pk("Tổng Thụ Động"): {"relation": []},
        }
        update_page(page_id, upd)
    except Exception as e:
        print("WARN update target off:", e)

    # save undo log
    try:
        undo_stack.setdefault(str(chat_id), []).append({
            "action": "switch_off",
            "target_id": page_id,
            "snapshot": snapshot,
            "archived_pages": archived,
            "lai_page_id": lai_page_id,
        })
    except Exception:
        pass

    _safe_edit(chat_id, mid, f"✅ Đã OFF {title}\n💰 Lãi tạo: {int(lai_amt) if lai_amt else 0}")


# ----------------- UNDO SWITCH -----------------
def undo_switch(chat_id: int):
    """
    Undo last switch_on / switch_off action saved in deps['undo_stack'] for this chat_id.
    Restores properties and created/archived pages.
    """
    try:
        send_telegram = deps["send_telegram"]
        archive_page = deps["archive_page"]
        unarchive_page = deps["unarchive_page"]
        update_page = deps["update_page_properties"]
        undo_stack = deps["undo_stack"]
    except Exception as e:
        traceback.print_exc()
        return send_telegram(chat_id, f"❌ Plugin chưa init đúng: {e}")

    stack = undo_stack.get(str(chat_id), [])
    if not stack:
        return send_telegram(chat_id, "❌ Không có thao tác để undo")

    log = stack.pop()
    try:
        # restore snapshot props
        _restore_snapshot(log["target_id"], log.get("snapshot", {}))
    except Exception as e:
        print("WARN undo restore snapshot:", e)

    # handle ON rollback: archive created pages
    if log.get("action") == "switch_on":
        for pid in log.get("created_pages", []):
            try:
                archive_page(pid)
            except Exception:
                pass
        return send_telegram(chat_id, "♻️ Hoàn tác ON hoàn tất")

    # handle OFF rollback: unarchive archived pages, archive lai_page
    if log.get("action") == "switch_off":
        for pid in log.get("archived_pages", []):
            try:
                unarchive_page(pid)
            except Exception:
                pass
        if log.get("lai_page_id"):
            try:
                archive_page(log["lai_page_id"])
            except Exception:
                pass
        return send_telegram(chat_id, "♻️ Hoàn tác OFF hoàn tất")

    return send_telegram(chat_id, "♻️ Hoàn tác xong")


# Exports
__all__ = ["init_switch_deps", "handle_switch_on", "handle_switch_off", "undo_switch"]
