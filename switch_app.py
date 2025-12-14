# switch_app.py
# Full standalone plugin for ON / OFF / UNDO operations (Notion + Telegram).
# Dependencies are injected via init_switch_deps(...) from app.py.
# Uses env/injected keys SWITCH_QDT_PAGE_ID and SWITCH_TTD_PAGE_ID for relations.
# Exported functions: init_switch_deps, handle_switch_on, handle_switch_off, undo_switch

import os
import time
import traceback
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional

VN_TZ = timezone(timedelta(hours=7))

# dependency container (to be injected from app.py)
deps: Dict[str, Any] = {}

# helper: read page id for relation from deps or environment
def _relation_page_id(name: str) -> Optional[str]:
    # priority: deps override -> environment
    if name == "Thụ động":
        return deps.get("SWITCH_QDT_PAGE_ID") or os.getenv("SWITCH_QDT_PAGE_ID")
    if name == "G":
        return deps.get("SWITCH_TTD_PAGE_ID") or os.getenv("SWITCH_TTD_PAGE_ID")
    return None

# ---------------- Dependency injection ----------------
def init_switch_deps(**kwargs):
    """
    Inject dependencies from app.py. Call once after app has defined helper functions.
    Required keys (app.py should pass at least these):
      - send_telegram
      - edit_telegram_message
      - find_target_matches
      - extract_prop_text
      - parse_money_from_text
      - create_page_in_db
      - archive_page
      - unarchive_page
      - update_page_properties
      - create_lai_page
      - query_database_all
      - undo_stack
      - NOTION_DATABASE_ID
      - TARGET_NOTION_DATABASE_ID
      - LA_NOTION_DATABASE_ID (optional)
      - find_prop_key
    Optional keys:
      - SWITCH_QDT_PAGE_ID
      - SWITCH_TTD_PAGE_ID
    """
    deps.update(kwargs)

# ---------------- Utilities ----------------
def _now_vn_date() -> str:
    return datetime.now(VN_TZ).date().isoformat()


def _safe_send(chat_id: Optional[int], text: str) -> Optional[Dict[str, Any]]:
    """
    Call app's send_telegram safely. May return dict (Telegram response) or None.
    """
    try:
        return deps["send_telegram"](chat_id, text)
    except Exception:
        # swallow exception and return None so caller can fallback
        try:
            print("safe_send fallback:", text)
        except Exception:
            pass
        return None


def _extract_mid(msg: Optional[Dict[str, Any]]) -> Optional[int]:
    if not isinstance(msg, dict):
        return None
    return msg.get("result", {}).get("message_id")


def _safe_edit(chat_id: Optional[int], message_id: Optional[int], text: str):
    """
    Try to edit message if message_id available; otherwise send a new message.
    """
    if message_id:
        try:
            deps["edit_telegram_message"](chat_id, message_id, text)
            return
        except Exception:
            # fallback to send
            pass
    _safe_send(chat_id, text)


def _find_prop_key(props: Dict[str, Any], name_like: str) -> str:
    """
    Return the exact property key in a page's properties for a user-facing name.
    If helper not available or fails, fall back to the name_like string.
    """
    try:
        key = deps["find_prop_key"](props, name_like)
        return key or name_like
    except Exception:
        return name_like


def _parse_int_from_prop(props: Dict[str, Any], name_like: str, default: int = 0) -> int:
    """
    Use extract_prop_text from app to get raw text, then parse to int via parse_money_from_text if available.
    """
    try:
        raw = deps["extract_prop_text"](props, name_like)
        if raw is None or raw == "":
            return default
        parse_money = deps.get("parse_money_from_text")
        if parse_money:
            try:
                val = parse_money(raw)
                # parse_money may return float/str/int
                if val is None:
                    return default
                return int(val)
            except Exception:
                pass
        # fallback simple numeric parsing
        s = str(raw).replace(",", "").strip()
        return int(float(s))
    except Exception:
        return default


# ---------------- Core: ON ----------------
def handle_switch_on(chat_id: Optional[int], keyword: str):
    """
    Robust, fixed handle_switch_on:
    - Resolves PROPERTY IDs first
    - Builds `upd` payload before any reference to it
    - Uses safe_send/safe_edit helpers to avoid thread crash
    - Creates day pages and pushes undo record to deps['undo_stack']
    """
    # Validate dependencies (fail fast if missing)
    try:
        send = deps["send_telegram"]
        find_matches = deps["find_target_matches"]
        create_page = deps["create_page_in_db"]
        update_page = deps["update_page_properties"]
        undo_stack = deps["undo_stack"]
        find_prop_key = deps["find_prop_key"]
        NOTION_DB = deps["NOTION_DATABASE_ID"]
    except Exception as e:
        print("handle_switch_on missing deps:", e)
        return

    matches = find_matches(keyword)
    if not matches:
        _safe_send(chat_id, f"❌ Không tìm thấy '{keyword}'")
        return

    page_id, title, props = matches[0]
    props = props or {}

    # send initial message and get message_id safely
    m = _safe_send(chat_id, f"🔄 Đang bật ON cho {title} ...")
    mid = _extract_mid(m)

    # ===== RESOLVE PROPERTY_IDs (must do this BEFORE building upd) =====
    try:
        status_key    = _find_prop_key(props, "trạng thái")
        ngay_dao_key  = _find_prop_key(props, "Ngày Đáo")
        ngay_xong_key = _find_prop_key(props, "ngày xong")
        qdt_key       = _find_prop_key(props, "Tổng Quan Đầu Tư")
        ttd_key       = _find_prop_key(props, "Tổng Thụ Động")
    except Exception as e:
        print("ERROR resolving property keys:", e)
        _safe_edit(chat_id, mid, "❌ Lỗi nội bộ: không thể đọc cấu trúc trang Notion.")
        return

    # ===== Resolve relation page ids from env/deps =====
    qdt_pid = _relation_page_id("Thụ động")
    g_pid   = _relation_page_id("G")

    # ===== BUILD upd PAYLOAD (define upd BEFORE use) =====
    upd: Dict[str, Any] = {
        status_key: {"select": {"name": "In progress"}},
        ngay_dao_key: {"date": {"start": _now_vn_date()}},
    }

    # include relation only if resolved; otherwise set explicit empty relation and warn
    if qdt_pid:
        upd[qdt_key] = {"relation": [{"id": qdt_pid}]}
    else:
        _safe_send(chat_id, "⚠️ Warning: SWITCH_QDT_PAGE_ID not set — 'Tổng Quan Đầu Tư' không được link.")
        upd[qdt_key] = {"relation": []}

    if g_pid:
        upd[ttd_key] = {"relation": [{"id": g_pid}]}
    else:
        _safe_send(chat_id, "⚠️ Warning: SWITCH_TTD_PAGE_ID not set — 'Tổng Thụ Động' không được link.")
        upd[ttd_key] = {"relation": []}

    # optional debug
    # print("DEBUG ON PATCH PAYLOAD:", upd)

    # ===== APPLY update (single PATCH) =====
    try:
        update_page(page_id, upd)
    except Exception as e:
        print("WARN update target on:", e)
        _safe_edit(chat_id, mid, "⚠️ Cảnh báo: không thể cập nhật trang mục tiêu. Tiếp tục tạo ngày nếu có.")

    _safe_edit(chat_id, mid, "⚙️ Đã cập nhật trạng thái & Ngày Đáo. Đang tạo ngày (nếu có)...")

    # ===== READ FIELDS (strict mapping) =====
    total_money = _parse_int_from_prop(props, "tiền", default=0)
    per_day     = _parse_int_from_prop(props, "G ngày", default=0)
    total_gop   = _parse_int_from_prop(props, "tổng ngày g", default=0)
    take_days   = _parse_int_from_prop(props, "ngày trước", default=0)
    truoc_val   = _parse_int_from_prop(props, "trước", default=0)
    ck_val      = _parse_int_from_prop(props, "CK", default=0)

    # ===== CREATE day pages =====
    created_pages: List[str] = []
    start_date = datetime.now(VN_TZ).date()
    for i in range(take_days):
        d = start_date + timedelta(days=i)
        page_payload = {
            "Name": {"title": [{"text": {"content": title}}]},
            "Ngày Góp": {"date": {"start": d.isoformat()}},
            "Tiền": {"number": per_day},
            "Đã Góp": {"checkbox": True},
            "Lịch G": {"relation": [{"id": page_id}]},
        }
        try:
            res = create_page(NOTION_DB, page_payload)
            created_id = None
            if isinstance(res, tuple) and len(res) >= 2:
                ok, body = res[0], res[1]
                if isinstance(body, dict) and body.get("id"):
                    created_id = body.get("id")
            elif isinstance(res, dict) and res.get("id"):
                created_id = res.get("id")
            if created_id:
                created_pages.append(created_id)
        except Exception as e:
            print("WARN create day page:", e)

        _safe_edit(chat_id, mid, f"📆 {i+1}/{max(1, take_days)} — {d.isoformat()}")
        time.sleep(0.12)

    # ===== PUSH UNDO =====
    try:
        undo_stack.setdefault(str(chat_id), []).append({
            "action": "switch_on",
            "target_id": page_id,
            "snapshot": {k: props.get(k) for k in (status_key, ngay_dao_key, qdt_key, ttd_key, ngay_xong_key)},
            "created_pages": created_pages,
        })
    except Exception:
        print("WARN: unable to push into undo_stack")

    # ===== FINAL REPORT =====
    lines: List[str] = []
    lines.append(f"🔔 Đã bật ON cho: {title}")
    lines.append(f"với số tiền {total_money:,} ngày {per_day:,} góp {total_gop} ngày")
    lines.append(f"💴 Lấy trước: {take_days} ngày {per_day:,} là {truoc_val:,}")
    lines.append("   ( từ hôm nay):")
    for idx in range(take_days):
        lines.append(f"{idx+1}. {(start_date + timedelta(days=idx)).isoformat()}")
    lines.append("")
    lines.append(f"🏛️ Tổng CK: ✅ {ck_val:,}")
    lines.append(f"📆 Đến ngày {(start_date + timedelta(days=take_days)).strftime('%d-%m-%Y')} bắt đầu góp lại")
    lines.append("")
    lines.append("🎉 Hoàn tất ON.")
    _safe_edit(chat_id, mid, "\n".join(lines))

# ---------------- Core: OFF ----------------
def handle_switch_off(chat_id: Optional[int], keyword: str):
    """
    OFF flow:
      - find target
      - snapshot properties
      - find & archive day pages related via Lịch G
      - create Lãi page if CK > 0 (uses create_lai_page)
      - update target: trạng thái = Done, ngày xong = today, clear relations
      - push undo log to deps['undo_stack']
      - animated telegram progress and final summary
    """
    try:
        send = deps["send_telegram"]
        find_matches = deps["find_target_matches"]
        query_calendar = deps["query_database_all"]
        archive_page = deps["archive_page"]
        create_lai = deps["create_lai_page"]
        update_page = deps["update_page_properties"]
        extract = deps["extract_prop_text"]
        parse_money = deps.get("parse_money_from_text")
        undo_stack = deps["undo_stack"]
        find_prop_key = deps["find_prop_key"]
        NOTION_DB = deps["NOTION_DATABASE_ID"]
    except Exception as e:
        print("handle_switch_off missing deps:", e)
        return

    matches = find_matches(keyword)
    if not matches:
        _safe_send(chat_id, f"❌ Không tìm thấy '{keyword}'")
        return

    page_id, title, props = matches[0]
    props = props or {}

    # ===== FIX: RESOLVE PROPERTY_ID FROM PROPS (CRITICAL) =====
    status_key   = deps["find_prop_key"](props, "trạng thái")
    ngay_dao_key = deps["find_prop_key"](props, "Ngày Đáo")
    ngay_xong_key = deps["find_prop_key"](props, "ngày xong")
    qdt_key      = deps["find_prop_key"](props, "Tổng Quan Đầu Tư")
    ttd_key      = deps["find_prop_key"](props, "Tổng Thụ Động")

    m = _safe_send(chat_id, f"⏳ Đang OFF {title} ...")
    mid = _extract_mid(m)

    # snapshot for undo
    snapshot: Dict[str, Any] = {}
    for name in ("trạng thái", "Ngày Đáo", "Tổng Quan Đầu Tư", "Tổng Thụ Động", "ngày xong"):
        key = _find_prop_key(props, name)
        if key in props:
            snapshot[key] = props[key]

    # gather calendar pages and archive those related to this target (Lịch G relation)
    archived_pages: List[str] = []
    try:
        pages = query_calendar(NOTION_DB)
        for p in pages:
            pprops = p.get("properties", {}) or {}
            rel_key = _find_prop_key(pprops, "Lịch G")
            rels = pprops.get(rel_key, {}).get("relation", []) if rel_key else []
            if any(r.get("id") == page_id for r in rels):
                try:
                    archive_page(p.get("id"))
                    archived_pages.append(p.get("id"))
                except Exception as e:
                    print("WARN archive:", e)
    except Exception as e:
        print("WARN query calendar for off:", e)

    # animate archive progress
    if archived_pages:
        total = len(archived_pages)
        for i, pid in enumerate(archived_pages, start=1):
            try:
                bar = "█" * int((i / total) * 10) + "░" * (10 - int((i / total) * 10))
                _safe_edit(chat_id, mid, f"🗑️ {i}/{total} [{bar}]")
            except Exception:
                pass
            time.sleep(0.12)

    # determine lai amount: prefer reading 'CK' formula or explicit fields
    try:
        lai_amt = _parse_int_from_prop(props, "CK", default=0)
    except Exception:
        lai_amt = 0

    lai_page_id: Optional[str] = None
    if lai_amt and lai_amt > 0:
        try:
            # create_lai_page should create in LA_NOTION_DATABASE_ID and link to target
            lai_page_id = create_lai(chat_id, title, lai_amt, page_id)
        except Exception as e:
            print("WARN create_lai_page:", e)

    # update target: Done, ngày xong = today, clear relations
    # ================== CORE: OFF — UPDATE TARGET ==================
    try:
        # 1️⃣ Resolve PROPERTY_ID (BẮT BUỘC)
        status_key   = deps["find_prop_key"](props, "trạng thái")
        ngay_xong_key = deps["find_prop_key"](props, "ngày xong")
        qdt_key      = deps["find_prop_key"](props, "Tổng Quan Đầu Tư")
        ttd_key      = deps["find_prop_key"](props, "Tổng Thụ Động")

        # 2️⃣ Build payload bằng PROPERTY_ID (KHÔNG DÙNG TÊN CỘT)
        upd = {
            status_key: {"select": {"name": "Done"}},
            ngay_xong_key: {"date": {"start": _now_vn_date()}},
            qdt_key: {"relation": []},   # clear Relation Thụ động
            ttd_key: {"relation": []},   # clear Relation G
        }

        # 3️⃣ Debug 1 lần (có thể xóa sau khi ổn)
        print("DEBUG OFF PATCH PAYLOAD:", upd)

        # 4️⃣ PATCH Notion
        update_page(page_id, upd)

    except Exception as e:
        print("❌ ERROR update target OFF:", e)
        traceback.print_exc()
    # ===============================================================


    # push undo record
    try:
        undo_stack.setdefault(str(chat_id), []).append({
            "action": "switch_off",
            "target_id": page_id,
            "snapshot": snapshot,
            "archived_pages": archived_pages,
            "lai_page_id": lai_page_id,
        })
    except Exception:
        pass

    _safe_edit(chat_id, mid, f"✅ Đã OFF {title}\n💰 Lãi tạo: {int(lai_amt) if lai_amt else 0}")


# ---------------- UNDO ----------------
def undo_switch(chat_id: Optional[int]):
    """
    Undo last switch_on or switch_off recorded in deps['undo_stack'] for this chat_id.
    Restores properties and created/archived pages accordingly.
    """
    try:
        undo_stack = deps["undo_stack"]
        update_page = deps["update_page_properties"]
        archive_page = deps["archive_page"]
        unarchive_page = deps["unarchive_page"]
        send = deps["send_telegram"]
    except Exception as e:
        print("undo_switch missing deps:", e)
        return

    stack = undo_stack.get(str(chat_id), [])
    if not stack:
        return send(chat_id, "❌ Không có thao tác để undo")

    log = stack.pop()

    # restore snapshot properties
    try:
        update_page(log["target_id"], log.get("snapshot", {}))
    except Exception as e:
        print("WARN undo restore props:", e)

    if log.get("action") == "switch_on":
        # archive created pages
        for pid in log.get("created_pages", []):
            try:
                archive_page(pid)
            except Exception:
                pass
        return send(chat_id, "♻️ Hoàn tác ON hoàn tất")

    if log.get("action") == "switch_off":
        # unarchive archived pages
        for pid in log.get("archived_pages", []):
            try:
                unarchive_page(pid)
            except Exception:
                pass
        # archive created lai page if exists
        if log.get("lai_page_id"):
            try:
                archive_page(log["lai_page_id"])
            except Exception:
                pass
        return send(chat_id, "♻️ Hoàn tác OFF hoàn tất")

    return send(chat_id, "♻️ Hoàn tác xong")

# Exports
__all__ = ["init_switch_deps", "handle_switch_on", "handle_switch_off", "undo_switch"]
