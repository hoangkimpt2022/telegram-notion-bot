# switch_app.py
# Plugin ON/OFF cho app.py với hệ thống undo log
import time
import traceback
from datetime import datetime, timedelta, timezone

# ===== DEPENDENCIES (được inject từ app.py) =====
send_telegram = None
edit_telegram_message = None
find_target_matches = None
extract_prop_text = None
parse_money_from_text = None
create_page_in_db = None
archive_page = None
unarchive_page = None
update_page_properties = None
create_lai_page = None
query_database_all = None
undo_stack = None
NOTION_DATABASE_ID = None
find_prop_key = None

VN_TZ = timezone(timedelta(hours=7))

def init_switch_deps(**kwargs):
    """Khởi tạo dependencies từ app.py"""
    global send_telegram, edit_telegram_message, find_target_matches
    global extract_prop_text, parse_money_from_text, create_page_in_db
    global archive_page, unarchive_page, update_page_properties
    global create_lai_page, query_database_all, undo_stack
    global NOTION_DATABASE_ID, find_prop_key
    
    send_telegram = kwargs.get("send_telegram")
    edit_telegram_message = kwargs.get("edit_telegram_message")
    find_target_matches = kwargs.get("find_target_matches")
    extract_prop_text = kwargs.get("extract_prop_text")
    parse_money_from_text = kwargs.get("parse_money_from_text")
    create_page_in_db = kwargs.get("create_page_in_db")
    archive_page = kwargs.get("archive_page")
    unarchive_page = kwargs.get("unarchive_page")
    update_page_properties = kwargs.get("update_page_properties")
    create_lai_page = kwargs.get("create_lai_page")
    query_database_all = kwargs.get("query_database_all")
    undo_stack = kwargs.get("undo_stack")
    NOTION_DATABASE_ID = kwargs.get("NOTION_DATABASE_ID")
    find_prop_key = kwargs.get("find_prop_key")


def _num(props, key_like):
    """Helper: parse number from property"""
    return parse_money_from_text(extract_prop_text(props, key_like)) or 0


# ================================================================
# 🟢 HANDLE ON
# ================================================================
def handle_switch_on(chat_id: int, keyword: str):
    """
    Xử lý lệnh ON:
    1. Tìm target trong TARGET_NOTION_DATABASE_ID
    2. Cập nhật: trạng thái → In progress, Ngày Đáo → hôm nay
    3. Tạo N ngày trong NOTION_DATABASE_ID (N = ngày trước)
    4. Ghi undo log
    """
    try:
        msg = send_telegram(chat_id, f"🟢 Đang xử lý ON cho '{keyword}' ...")
        message_id = msg.get("result", {}).get("message_id")
        # ===== DEBUG =====
        print(f"[DEBUG switch_on] keyword='{keyword}' | find_target_matches={find_target_matches}")
        # =================
        def update(text):
            if message_id:
                try:
                    edit_telegram_message(chat_id, message_id, text)
                    return
                except:
                    pass
            send_telegram(chat_id, text)

        # ---- BƯỚC 1: TÌM TARGET ----
        update(f"🔍 Đang tìm '{keyword}' trong TARGET DB ...")
        matches = find_target_matches(keyword)

        if not matches:
            update(f"❌ Không tìm thấy '{keyword}' trong TARGET DB.")
            return

        if len(matches) > 1:
            update(f"⚠️ Tìm thấy {len(matches)} kết quả cho '{keyword}'. Vui lòng nhập chính xác hơn.")
            return

        target_id, title, props = matches[0]
        update(f"✅ Đã tìm thấy: {title}")
        time.sleep(0.3)
        
        time.sleep(0.3)
        # Tiếp tục các bước sau...
        # ---- ĐỌC DỮ LIỆU ----
        total_money = _num(props, "tiền")
        per_day = _num(props, "G ngày")
        total_days = _num(props, "tổng ngày g")
        take_days = int(_num(props, "ngày trước"))
        truoc_val = _num(props, "trước")
        ck_val = _num(props, "CK")

        if take_days <= 0:
            update(f"⚠️ 'ngày trước' = 0 → Không tạo ngày nào.")
            return

        # ---- BƯỚC 2: CẬP NHẬT TARGET DB (FAIL-SOFT) ----
        update("📝 Đang cập nhật trạng thái TARGET → In progress ...")
        today_vn = datetime.now(VN_TZ).date().isoformat()
        try:
            status_key = find_prop_key(props, "trạng thái")
            ngaydao_key = find_prop_key(props, "Ngày Đáo") or find_prop_key(props, "ngày đáo")
            update_props = {}
            if status_key:
                update_props[status_key] = {
                    "status": {"name": "In progress"}
                }
            if ngaydao_key:
                update_props[ngaydao_key] = {
                    "date": {"start": today_vn}
                }
            if not update_props:
                update("⚠️ Không tìm thấy property hợp lệ → bỏ qua cập nhật TARGET.")
            else:
                ok, res = update_page_properties(target_id, update_props)
                if not ok:
                    update(f"⚠️ Cập nhật TARGET thất bại (bỏ qua): {res}")
                else:
                    update("✅ TARGET đã chuyển sang In progress.")
        except Exception as e:
            update(f"⚠️ Lỗi cập nhật TARGET (bỏ qua): {e}")
        time.sleep(0.3)

        # ---- BƯỚC 3: TẠO CÁC NGÀY TRONG CALENDAR DB ----
        update(f"🛠️ Đang tạo {take_days} ngày trong CALENDAR DB ...")
        time.sleep(0.3)

        start_date = datetime.now(VN_TZ).date()
        days = [start_date + timedelta(days=i) for i in range(take_days)]
        created_pages = []

        for idx, d in enumerate(days, start=1):
            props_payload = {
                "Name": {"title": [{"type": "text", "text": {"content": title}}]},
                "Ngày Góp": {"date": {"start": d.isoformat()}},
                "Tiền": {"number": per_day},
                "Đã Góp": {"checkbox": True},
                "Lịch G": {"relation": [{"id": target_id}]}
            }

            ok, res = create_page_in_db(NOTION_DATABASE_ID, props_payload)
            if ok:
                created_pages.append(res.get("id"))
            else:
                update(f"⚠️ Lỗi tạo ngày {idx}: {res}")

            # Progress bar
            bar = int((idx / take_days) * 10)
            progress = "▬" * bar + "▭" * (10 - bar)
            update(f"📅 Tạo ngày {idx}/{take_days} [{progress}] – {d.isoformat()}")
            time.sleep(0.25)

        update(f"✅ Đã tạo {len(created_pages)} ngày mới cho '{title}' 🎉")
        time.sleep(0.4)

        # ---- BƯỚC 4: THÔNG BÁO KẾT QUẢ ----
        lines = []
        lines.append(f"🔔 Đã bật ON cho: {title}")
        lines.append(f"với số tiền {int(total_money):,} ngày {int(per_day):,} góp {int(total_days)} ngày")
        lines.append(f"💴 Lấy trước: {take_days} ngày {int(per_day):,} là {int(truoc_val):,}")
        lines.append("   ( từ hôm nay):")
        for i, d in enumerate(days, start=1):
            lines.append(f"{i}. {d.isoformat()}")
        lines.append("")
        lines.append(f"🏛️ Tổng CK: ✅ {int(ck_val):,}")
        next_start = (start_date + timedelta(days=take_days)).strftime("%d-%m-%Y")
        lines.append(f"📆 Đến ngày {next_start} bắt đầu góp lại")
        lines.append("")
        lines.append("🎉 Hoàn tất ON.")

        update("\n".join(lines))

        # ---- BƯỚC 5: GHI UNDO LOG ----
        undo_stack.setdefault(str(chat_id), []).append({
            "action": "switch_on",
            "target_id": target_id,
            "title": title,
            "created_pages": created_pages,
            "old_trangthai": extract_prop_text(props, "trạng thái") or "",
            "old_ngaydao": extract_prop_text(props, "Ngày Đáo") or ""
        })
        
    except Exception as e:
        traceback.print_exc()
        send_telegram(chat_id, f"❌ Lỗi ON: {e}")


# ================================================================
# 🔴 HANDLE OFF
# ================================================================
def handle_switch_off(chat_id: int, keyword: str):
    """
    Xử lý lệnh OFF:
    1. Tìm target trong TARGET_NOTION_DATABASE_ID
    2. Xóa toàn bộ ngày trong NOTION_DATABASE_ID
    3. Tạo Lãi trong LA_NOTION_DATABASE_ID
    4. Cập nhật: trạng thái → Done, ngày xong → hôm nay
    5. Ghi undo log
    """
    try:
        msg = send_telegram(chat_id, f"🔴 Đang xử lý OFF cho '{keyword}' ...")
        message_id = msg.get("result", {}).get("message_id")

        def update(text):
            if message_id:
                try:
                    edit_telegram_message(chat_id, message_id, text)
                    return
                except:
                    pass
            send_telegram(chat_id, text)

        # ---- BƯỚC 1: TÌM TARGET ----
        update(f"🔍 Đang tìm '{keyword}' trong TARGET DB ...")
        matches = find_target_matches(keyword)

        if not matches:
            update(f"❌ Không tìm thấy '{keyword}' trong TARGET DB.")
            return

        if len(matches) > 1:
            update(f"⚠️ Tìm thấy {len(matches)} kết quả. Vui lòng nhập chính xác hơn.")
            return

        target_id, title, props = matches[0]
        update(f"✅ Đã tìm thấy: {title}")
        time.sleep(0.3)
        
        # ---- BƯỚC 2: TÌM VÀ XÓA CÁC NGÀY TRONG CALENDAR DB ----
        update(f"🧹 Đang tìm các ngày của '{title}' trong CALENDAR DB ...")
        time.sleep(0.3)

        calendar_pages = query_database_all(NOTION_DATABASE_ID, page_size=500)
        children = []

        for p in calendar_pages:
            props_p = p.get("properties", {})
            rel_key = find_prop_key(props_p, "Lịch G")
            if not rel_key:
                continue

            rel_arr = props_p.get(rel_key, {}).get("relation", [])
            if any(r.get("id") == target_id for r in rel_arr):
                children.append(p.get("id"))

        total = len(children)

        if total == 0:
            update(f"🧹 Không có ngày nào để xóa cho '{title}'.")
            time.sleep(0.3)
        else:
            update(f"🧹 Bắt đầu xóa {total} ngày ...")
            time.sleep(0.3)

            for idx, day_id in enumerate(children, start=1):
                archive_page(day_id)

                bar = int((idx / total) * 10)
                progress = "▬" * bar + "▭" * (10 - bar)
                update(f"🧹 Xóa {idx}/{total} [{progress}]")
                time.sleep(0.25)

            update(f"✅ Đã xóa toàn bộ {total} ngày 🎉")
            time.sleep(0.4)

        # ---- BƯỚC 3: TẠO LÃI ----
        lai_text = (
            extract_prop_text(props, "Lai lịch g")
            or extract_prop_text(props, "Lãi")
            or extract_prop_text(props, "Lai")
            or ""
        )
        lai_amt = parse_money_from_text(lai_text) or 0
        lai_page_id = None

        if lai_amt > 0:
            update(f"💰 Đang tạo Lãi {int(lai_amt):,} ...")
            lai_page_id = create_lai_page(chat_id, title, lai_amt, target_id)
            update(f"✅ Đã tạo Lãi cho {title}.")
        else:
            update("ℹ️ Không có giá trị Lãi hoặc chưa cấu hình LA_NOTION_DATABASE_ID.")

        time.sleep(0.3)

        # ---- BƯỚC 4: CẬP NHẬT TARGET DB ----
        update("📝 Đang cập nhật trạng thái TARGET → Done ...")
        today_vn = datetime.now(VN_TZ).date().isoformat()
        try:
            status_key = find_prop_key(props, "trạng thái")
            ngayxong_key = find_prop_key(props, "ngày xong")
            update_props = {}
            if status_key:
                update_props[status_key] = {
                    "status": {"name": "Done"}
                }
            if ngayxong_key:
                update_props[ngayxong_key] = {
                    "date": {"start": today_vn}
                }
            if not update_props:
                update("⚠️ Không tìm thấy property hợp lệ → bỏ qua cập nhật TARGET.")
            else:
                ok, res = update_page_properties(target_id, update_props)
                if not ok:
                    update(f"⚠️ Cập nhật TARGET thất bại (bỏ qua): {res}")
                else:
                    update("✅ TARGET đã chuyển sang Done.")
        except Exception as e:
            update(f"⚠️ Lỗi cập nhật TARGET (bỏ qua): {e}")
        time.sleep(0.3)

        # ---- BƯỚC 5: THÔNG BÁO KẾT QUẢ ----
        update(f"🎉 Hoàn tất OFF cho: {title}")

        # ---- BƯỚC 6: GHI UNDO LOG ----
        undo_stack.setdefault(str(chat_id), []).append({
            "action": "switch_off",
            "target_id": target_id,
            "title": title,
            "archived_pages": children,
            "lai_page": lai_page_id,
            "old_trangthai": extract_prop_text(props, "trạng thái") or "",
            "old_ngayxong": extract_prop_text(props, "ngày xong") or ""
        })

    except Exception as e:
        traceback.print_exc()
        send_telegram(chat_id, f"❌ Lỗi OFF: {e}")


# ================================================================
# ♻️ UNDO SWITCH
# ================================================================
def undo_switch(chat_id: int):
    """
    Hoàn tác thao tác ON hoặc OFF gần nhất
    """
    chat_key = str(chat_id)

    if not undo_stack.get(chat_key):
        send_telegram(chat_id, "❌ Không có hành động nào để hoàn tác.")
        return

    log = undo_stack[chat_key].pop()
    action = log.get("action")

    # ---- UNDO ON ----
    if action == "switch_on":
        msg = send_telegram(chat_id, "♻️ Đang hoàn tác ON...")
        message_id = msg.get("result", {}).get("message_id")

        # Xóa các ngày đã tạo
        created = log.get("created_pages", [])
        total = len(created)
        
        for idx, pid in enumerate(created, start=1):
            try:
                archive_page(pid)
                bar = int((idx / total) * 10) if total > 0 else 0
                progress = "▬" * bar + "▭" * (10 - bar)
                if message_id:
                    edit_telegram_message(chat_id, message_id, 
                        f"♻️ Xóa ngày {idx}/{total} [{progress}]")
                time.sleep(0.25)
            except Exception as e:
                print(f"⚠️ Lỗi xóa page: {pid} – {e}")

        # Khôi phục trạng thái cũ
        target_id = log.get("target_id")
        old_tt = log.get("old_trangthai")
        old_nd = log.get("old_ngaydao")

        restore_props = {}
        if old_tt:
            restore_props["trạng thái"] = {"status": {"name": old_tt}}
        if old_nd:
            restore_props["Ngày Đáo"] = {"date": {"start": old_nd}}

        if restore_props:
            update_page_properties(target_id, restore_props)

        final_msg = f"✅ Đã hoàn tác ON cho: {log.get('title')}"
        if message_id:
            edit_telegram_message(chat_id, message_id, final_msg)
        else:
            send_telegram(chat_id, final_msg)
        return

    # ---- UNDO OFF ----
    if action == "switch_off":
        msg = send_telegram(chat_id, "♻️ Đang hoàn tác OFF...")
        message_id = msg.get("result", {}).get("message_id")

        # Khôi phục các ngày đã xóa
        archived = log.get("archived_pages", [])
        total = len(archived)
        
        for idx, pid in enumerate(archived, start=1):
            try:
                unarchive_page(pid)
                bar = int((idx / total) * 10) if total > 0 else 0
                progress = "▬" * bar + "▭" * (10 - bar)
                if message_id:
                    edit_telegram_message(chat_id, message_id,
                        f"♻️ Khôi phục {idx}/{total} [{progress}]")
                time.sleep(0.25)
            except Exception as e:
                print(f"⚠️ Lỗi khôi phục page: {pid} – {e}")

        # Xóa trang Lãi
        lai_page = log.get("lai_page")
        if lai_page:
            try:
                archive_page(lai_page)
            except Exception as e:
                print(f"⚠️ Lỗi xóa Lãi: {lai_page} – {e}")

        # Khôi phục trạng thái cũ
        target_id = log.get("target_id")
        old_tt = log.get("old_trangthai")
        old_nx = log.get("old_ngayxong")

        restore_props = {}
        if old_tt:
            restore_props["trạng thái"] = {"status": {"name": old_tt}}
        if old_nx:
            restore_props["ngày xong"] = {"date": {"start": old_nx}}

        if restore_props:
            update_page_properties(target_id, restore_props)

        final_msg = f"✅ Đã hoàn tác OFF cho: {log.get('title')}"
        if message_id:
            edit_telegram_message(chat_id, message_id, final_msg)
        else:
            send_telegram(chat_id, final_msg)
        return

    send_telegram(chat_id, f"⚠️ Không hỗ trợ undo cho action '{action}'.")
