# ===================== switch_app.py =====================
# FINAL VERSION – đúng nghiệp vụ – có UNDO – dùng ENV relation
# ========================================================

from datetime import datetime, timedelta, timezone
import time

VN_TZ = timezone(timedelta(hours=7))
deps = {}

# ================= INIT =================
def init_switch_deps(**kwargs):
    deps.update(kwargs)

# ================= UTILS =================
def today():
    return datetime.now(VN_TZ).date().isoformat()

def safe_send(chat_id, text):
    try:
        return deps["send_telegram"](chat_id, text)
    except Exception:
        return None

def extract_mid(msg):
    if isinstance(msg, dict):
        return msg.get("result", {}).get("message_id")
    return None

def safe_edit(chat_id, mid, text):
    if mid:
        try:
            deps["edit_telegram_message"](chat_id, mid, text)
            return
        except Exception:
            pass
    safe_send(chat_id, text)

def pk(props, name):
    return deps["find_prop_key"](props, name)

def num(props, name):
    v = deps["extract_prop_text"](props, name)
    if v is None:
        return 0
    return int(float(str(v).replace(",", "")))

# ================= ON =================
def handle_switch_on(chat_id, keyword):
    find = deps["find_target_matches"]
    update = deps["update_page_properties"]
    create_page = deps["create_page_in_db"]
    undo_stack = deps["undo_stack"]
    NOTION_DB = deps["NOTION_DATABASE_ID"]

    matches = find(keyword)
    if not matches:
        return send(chat_id, f"❌ Không tìm thấy {keyword}")

    pid, title, props = matches[0]

    msg = safe_send(chat_id, f"🔄 Đang bật ON cho {title} ...")
    mid = extract_mid(msg)


    # ===== SNAPSHOT for UNDO =====
    snapshot = {}
    for c in ["trạng thái", "Ngày Đáo", "Tổng Quan Đầu Tư", "Tổng Thụ Động", "ngày xong"]:
        k = pk(props, c)
        if k in props:
            snapshot[k] = props[k]

    # ===== READ DATA (ĐÚNG CỘT) =====
    tien = num(props, "tiền")
    g_ngay = num(props, "G ngày")
    tong_ngay_g = num(props, "tổng ngày g")
    ngay_truoc = num(props, "ngày trước")
    truoc = num(props, "trước")   # FORMULA
    ck = num(props, "CK")         # FORMULA

    # ===== UPDATE TARGET =====
    update(pid, {
        pk(props, "trạng thái"): {"select": {"name": "In progress"}},
        pk(props, "Ngày Đáo"): {"date": {"start": today()}},
        pk(props, "Tổng Quan Đầu Tư"): {
            "relation": [{"id": deps["SWITCH_QDT_PAGE_ID"]}]
        },
        pk(props, "Tổng Thụ Động"): {
            "relation": [{"id": deps["SWITCH_TTD_PAGE_ID"]}]
        },
    })

    # ===== CREATE DAYS =====
    start = datetime.now(VN_TZ).date()
    created_pages = []

    for i in range(ngay_truoc):
        d = start + timedelta(days=i)
        ok, res = create_page(NOTION_DB, {
            "Name": {"title": [{"text": {"content": title}}]},
            "Ngày Góp": {"date": {"start": d.isoformat()}},
            "Tiền": {"number": g_ngay},
            "Đã Góp": {"checkbox": True},
            "Lịch G": {"relation": [{"id": pid}]},
        })
        if ok and res.get("id"):
            created_pages.append(res["id"])
        edit(chat_id, mid, f"📆 {i+1}/{ngay_truoc} — {d.isoformat()}")
        time.sleep(0.15)

    # ===== SAVE UNDO =====
    undo_stack.setdefault(str(chat_id), []).append({
        "action": "switch_on",
        "target_id": pid,
        "snapshot": snapshot,
        "created_pages": created_pages,
    })

    # ===== FINAL REPORT =====
    report = [
        f"🔔 Đã bật ON cho: {title}",
        f"với số tiền {tien:,} ngày {g_ngay:,} góp {tong_ngay_g}",
        f"💴 Lấy trước: {ngay_truoc} ngày {g_ngay:,} là {truoc:,}",
        "   ( từ hôm nay):"
    ]
    for i in range(ngay_truoc):
        report.append(f"{i+1}. {(start + timedelta(days=i)).isoformat()}")
    report += [
        "",
        f"🏛️ Tổng CK: ✅ {ck:,}",
        f"📆 Đến ngày {(start + timedelta(days=ngay_truoc)).strftime('%d-%m-%Y')} bắt đầu góp lại",
        "",
        "🎉 Hoàn tất ON."
    ]

    edit(chat_id, mid, "\n".join(report))

# ================= OFF =================
def handle_switch_off(chat_id, keyword):
    find = deps["find_target_matches"]
    update = deps["update_page_properties"]
    archive = deps["archive_page"]
    create_lai = deps["create_lai_page"]
    query = deps["query_database_all"]
    undo_stack = deps["undo_stack"]
    NOTION_DB = deps["NOTION_DATABASE_ID"]

    matches = find(keyword)
    if not matches:
        return send(chat_id, f"❌ Không tìm thấy {keyword}")

    pid, title, props = matches[0]
    msg = send(chat_id, f"⏳ Đang OFF {title} ...")
    mid = msg.get("result", {}).get("message_id")

    snapshot = {}
    for c in ["trạng thái", "Ngày Đáo", "Tổng Quan Đầu Tư", "Tổng Thụ Động", "ngày xong"]:
        k = pk(props, c)
        if k in props:
            snapshot[k] = props[k]

    archived_pages = []

    for p in query(NOTION_DB):
        rel = p.get("properties", {}).get(pk(p["properties"], "Lịch G"), {}).get("relation", [])
        if any(r["id"] == pid for r in rel):
            archive(p["id"])
            archived_pages.append(p["id"])
            edit(chat_id, mid, f"🗑️ {len(archived_pages)}")

    ck = num(props, "CK")
    lai_page_id = None
    if ck > 0:
        lai_page_id = create_lai(chat_id, title, ck, pid)

    update(pid, {
        pk(props, "trạng thái"): {"select": {"name": "Done"}},
        pk(props, "ngày xong"): {"date": {"start": today()}},
        pk(props, "Tổng Quan Đầu Tư"): {"relation": []},
        pk(props, "Tổng Thụ Động"): {"relation": []},
    })

    undo_stack.setdefault(str(chat_id), []).append({
        "action": "switch_off",
        "target_id": pid,
        "snapshot": snapshot,
        "archived_pages": archived_pages,
        "lai_page_id": lai_page_id,
    })

    edit(chat_id, mid, f"✅ Đã OFF {title}")

# ================= UNDO =================
def undo_switch(chat_id):
    undo_stack = deps["undo_stack"]
    update = deps["update_page_properties"]
    archive = deps["archive_page"]
    unarchive = deps["unarchive_page"]

    stack = undo_stack.get(str(chat_id))
    if not stack:
        return send(chat_id, "❌ Không có thao tác để undo")

    log = stack.pop()
    update(log["target_id"], log["snapshot"])

    if log["action"] == "switch_on":
        for pid in log["created_pages"]:
            archive(pid)
        return send(chat_id, "♻️ Đã undo ON")

    if log["action"] == "switch_off":
        for pid in log["archived_pages"]:
            unarchive(pid)
        if log["lai_page_id"]:
            archive(log["lai_page_id"])
        return send(chat_id, "♻️ Đã undo OFF")

__all__ = ["init_switch_deps", "handle_switch_on", "handle_switch_off", "undo_switch"]
