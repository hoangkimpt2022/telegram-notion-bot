# switch_app.py — FINAL VERSION (Aligned 100% with app.py)
import os
import time
import requests
import traceback
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional

VN_TZ = timezone(timedelta(hours=7))
deps: Dict[str, Any] = {}

# =========================================================
# INIT DEPENDENCIES (CALLED FROM app.py)
# =========================================================
def init_switch_deps(**kwargs):
    deps.update(kwargs)

# =========================================================
# UTILS (SAFE TELEGRAM)
# =========================================================
def _safe_send(chat_id, text):
    try:
        return deps["send_telegram"](chat_id, text)
    except Exception:
        return None

def _extract_mid(msg):
    if isinstance(msg, dict):
        return msg.get("result", {}).get("message_id")
    return None

def _safe_edit(chat_id, mid, text):
    if mid:
        try:
            deps["edit_telegram_message"](chat_id, mid, text)
            return
        except Exception:
            pass
    _safe_send(chat_id, text)

def _now_vn_date():
    return datetime.now(VN_TZ).date().isoformat()

# =========================================================
# NOTION HELPERS (CRITICAL)
# =========================================================
def _get_full_page(page_id: str) -> Dict[str, Any]:
    """ EXACTLY like app.py """
    r = requests.get(
        f"https://api.notion.com/v1/pages/{page_id}",
        headers=deps["NOTION_HEADERS"],
        timeout=15
    )
    r.raise_for_status()
    return r.json()

def _relation_page_id(kind: str) -> Optional[str]:
    if kind == "Thụ động":
        return os.getenv("SWITCH_QDT_PAGE_ID")
    if kind == "G":
        return os.getenv("SWITCH_TTD_PAGE_ID")
    return None

# =========================================================
# CORE: SWITCH ON
# =========================================================
def handle_switch_on(chat_id: int, keyword: str):
    try:
        matches = deps["find_target_matches"](keyword)
        if not matches:
            return _safe_send(chat_id, f"❌ Không tìm thấy {keyword}")

        page_id, title, _ = matches[0]

        # 🔴 GET FULL PAGE (ROOT FIX)
        page = _get_full_page(page_id)
        props = page["properties"]

        # Resolve PROPERTY_ID
        status_key   = deps["find_prop_key"](props, "trạng thái")
        ngay_dao_key = deps["find_prop_key"](props, "Ngày Đáo")
        qdt_key      = deps["find_prop_key"](props, "Tổng Quan Đầu Tư")
        ttd_key      = deps["find_prop_key"](props, "Tổng Thụ Động")

        # Telegram start
        msg = _safe_send(chat_id, f"🔄 Đang bật ON cho {title} ...")
        mid = _extract_mid(msg)

        # PATCH TARGET DB
        deps["update_page_properties"](page_id, {
            status_key: {"select": {"name": "In progress"}},
            ngay_dao_key: {"date": {"start": _now_vn_date()}},
            qdt_key: {"relation": [{"id": _relation_page_id("Thụ động")}]},
            ttd_key: {"relation": [{"id": _relation_page_id("G")}]},
        })

        # ===== READ FIELDS =====
        money = deps["parse_money_from_text"](deps["extract_prop_text"](props, "tiền"))
        per_day = deps["parse_money_from_text"](deps["extract_prop_text"](props, "G ngày"))
        total_g = int(deps["extract_prop_text"](props, "tổng ngày g") or 0)
        take_days = int(deps["extract_prop_text"](props, "ngày trước") or 0)
        truoc = deps["parse_money_from_text"](deps["extract_prop_text"](props, "trước"))
        ck = deps["parse_money_from_text"](deps["extract_prop_text"](props, "CK"))

        # CREATE DAY PAGES
        created = []
        start = datetime.now(VN_TZ).date()
        for i in range(take_days):
            d = start + timedelta(days=i)
            deps["create_page_in_db"](deps["NOTION_DATABASE_ID"], {
                "Name": {"title": [{"text": {"content": title}}]},
                "Ngày Góp": {"date": {"start": d.isoformat()}},
                "Tiền": {"number": per_day},
                "Đã Góp": {"checkbox": True},
                "Lịch G": {"relation": [{"id": page_id}]},
            })
            _safe_edit(chat_id, mid, f"📆 {i+1}/{take_days} — {d.isoformat()}")
            time.sleep(0.12)

        # FINAL MESSAGE
        lines = [
            f"🔔 Đã bật ON cho: {title}",
            f"với số tiền {money:,} ngày {per_day:,} góp {total_g}",
            f"💴 Lấy trước: {take_days} ngày {per_day:,} là {truoc:,}",
            "   ( từ hôm nay):",
        ]
        for i in range(take_days):
            lines.append(f"{i+1}. {(start + timedelta(days=i)).isoformat()}")
        lines += [
            "",
            f"🏛️ Tổng CK: ✅ {ck:,}",
            f"📆 Đến ngày {(start + timedelta(days=take_days)).strftime('%d-%m-%Y')} bắt đầu góp lại",
            "",
            "🎉 Hoàn tất ON."
        ]
        _safe_edit(chat_id, mid, "\n".join(lines))

    except Exception as e:
        traceback.print_exc()
        _safe_send(chat_id, "❌ Lỗi khi bật ON")

# =========================================================
# CORE: SWITCH OFF
# =========================================================
def handle_switch_off(chat_id: int, keyword: str):
    try:
        matches = deps["find_target_matches"](keyword)
        if not matches:
            return _safe_send(chat_id, f"❌ Không tìm thấy {keyword}")

        page_id, title, _ = matches[0]
        page = _get_full_page(page_id)
        props = page["properties"]

        status_key = deps["find_prop_key"](props, "trạng thái")
        ngay_xong_key = deps["find_prop_key"](props, "ngày xong")
        qdt_key = deps["find_prop_key"](props, "Tổng Quan Đầu Tư")
        ttd_key = deps["find_prop_key"](props, "Tổng Thụ Động")

        msg = _safe_send(chat_id, f"⏳ Đang OFF {title} ...")
        mid = _extract_mid(msg)

        # ARCHIVE DAY PAGES
        pages = deps["query_database_all"](deps["NOTION_DATABASE_ID"])
        related = []
        for p in pages:
            rels = p["properties"].get("Lịch G", {}).get("relation", [])
            if any(r["id"] == page_id for r in rels):
                related.append(p["id"])

        for i, pid in enumerate(related, 1):
            deps["archive_page"](pid)
            bar = "█" * int(i/len(related)*10)
            bar += "░" * (10-len(bar))
            _safe_edit(chat_id, mid, f"🗑️ {i}/{len(related)} [{bar}]")
            time.sleep(0.12)

        ck = deps["parse_money_from_text"](deps["extract_prop_text"](props, "CK"))
        if ck > 0:
            deps["create_lai_page"](chat_id, title, ck, page_id)

        deps["update_page_properties"](page_id, {
            status_key: {"select": {"name": "Done"}},
            ngay_xong_key: {"date": {"start": _now_vn_date()}},
            qdt_key: {"relation": []},
            ttd_key: {"relation": []},
        })

        _safe_edit(chat_id, mid, f"✅ Đã OFF {title}\n💰 Lãi tạo: {ck:,}")

    except Exception:
        traceback.print_exc()
        _safe_send(chat_id, "❌ Lỗi khi OFF")

# =========================================================
# EXPORT
# =========================================================
__all__ = [
    "init_switch_deps",
    "handle_switch_on",
    "handle_switch_off",
]
