"""
Eslatma + Murojaat boti — BITTA FAYL.

ISHGA TUSHIRISH:
  1) pip install aiogram APScheduler
  2) Pastdagi BOT_TOKEN va ADMIN_ID ni to'ldiring
  3) python main.py
"""

import os
import asyncio
import sqlite3
import datetime as dt

from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton,
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.cron import CronTrigger

from aiohttp import web
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application

# ==========================================================
#                     SOZLAMALAR
# ==========================================================
BOT_TOKEN = os.getenv("BOT_TOKEN", "")  # Render Environment: BOT_TOKEN
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))  # Render Environment: ADMIN_ID

DB_PATH = "bot.db"
INTERVAL_OPTIONS = [20, 30, 40, 50, 60, 90, 120, 180]   # daqiqa
DEFAULT_QUIET_START = 23
DEFAULT_QUIET_END = 7
WEEKDAYS = ["Dushanba", "Seshanba", "Chorshanba", "Payshanba", "Juma", "Shanba", "Yakshanba"]


# ==========================================================
#                     BAZA (SQLite)
# ==========================================================
_conn = None


def conn():
    global _conn
    if _conn is None:
        _conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        _conn.row_factory = sqlite3.Row
        _conn.execute("PRAGMA journal_mode=WAL")
    return _conn


def init_db():
    conn().executescript("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY, username TEXT, full_name TEXT,
            joined_at TEXT, quiet_start INTEGER, quiet_end INTEGER);

        CREATE TABLE IF NOT EXISTS reminders (
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL,
            kind TEXT NOT NULL, text TEXT, voice_id TEXT, interval_min INTEGER,
            run_at TEXT, hour INTEGER, minute INTEGER, weekday INTEGER,
            max_count INTEGER, sent_count INTEGER DEFAULT 0,
            active INTEGER DEFAULT 1, created_at TEXT);

        CREATE TABLE IF NOT EXISTS support (
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL,
            username TEXT, full_name TEXT, content TEXT, media_type TEXT,
            file_id TEXT, status TEXT DEFAULT 'new', created_at TEXT);
    """)
    conn().commit()


def now_iso():
    return dt.datetime.now().isoformat(timespec="seconds")


def upsert_user(uid, username, name):
    c = conn()
    if c.execute("SELECT 1 FROM users WHERE user_id=?", (uid,)).fetchone() is None:
        c.execute("INSERT INTO users VALUES(?,?,?,?,?,?)",
                  (uid, username, name, now_iso(), DEFAULT_QUIET_START, DEFAULT_QUIET_END))
    else:
        c.execute("UPDATE users SET username=?, full_name=? WHERE user_id=?", (username, name, uid))
    c.commit()


def get_user(uid):
    return conn().execute("SELECT * FROM users WHERE user_id=?", (uid,)).fetchone()


def set_quiet(uid, s, e):
    conn().execute("UPDATE users SET quiet_start=?, quiet_end=? WHERE user_id=?", (s, e, uid))
    conn().commit()


def all_users():
    return conn().execute("SELECT * FROM users ORDER BY joined_at DESC").fetchall()


def add_reminder(**k):
    cols = ["user_id", "kind", "text", "voice_id", "interval_min", "run_at",
            "hour", "minute", "weekday", "max_count"]
    c = conn()
    cur = c.execute(
        f"INSERT INTO reminders({','.join(cols)}, created_at) VALUES({','.join('?'*len(cols))}, ?)",
        (*[k.get(x) for x in cols], now_iso()))
    c.commit()
    return cur.lastrowid


def get_reminder(rid):
    return conn().execute("SELECT * FROM reminders WHERE id=?", (rid,)).fetchone()


def user_reminders(uid):
    return conn().execute("SELECT * FROM reminders WHERE user_id=? ORDER BY id DESC", (uid,)).fetchall()


def all_active_reminders():
    return conn().execute("SELECT * FROM reminders WHERE active=1").fetchall()


def set_active(rid, a):
    conn().execute("UPDATE reminders SET active=? WHERE id=?", (1 if a else 0, rid))
    conn().commit()


def inc_sent(rid):
    conn().execute("UPDATE reminders SET sent_count=sent_count+1 WHERE id=?", (rid,))
    conn().commit()


def delete_reminder(rid):
    conn().execute("DELETE FROM reminders WHERE id=?", (rid,))
    conn().commit()


def add_support(uid, username, name, content, mtype, fid):
    c = conn()
    cur = c.execute(
        "INSERT INTO support(user_id,username,full_name,content,media_type,file_id,status,created_at)"
        " VALUES(?,?,?,?,?,?, 'new', ?)", (uid, username, name, content, mtype, fid, now_iso()))
    c.commit()
    return cur.lastrowid


def get_support(sid):
    return conn().execute("SELECT * FROM support WHERE id=?", (sid,)).fetchone()


def set_support_answered(sid):
    conn().execute("UPDATE support SET status='answered' WHERE id=?", (sid,))
    conn().commit()


def support_list(status=None, limit=20):
    if status:
        return conn().execute("SELECT * FROM support WHERE status=? ORDER BY id DESC LIMIT ?",
                              (status, limit)).fetchall()
    return conn().execute("SELECT * FROM support ORDER BY id DESC LIMIT ?", (limit,)).fetchall()


def stats():
    c = conn()
    g = lambda q: c.execute(q).fetchone()[0]
    return {
        "users": g("SELECT COUNT(*) FROM users"),
        "reminders": g("SELECT COUNT(*) FROM reminders"),
        "reminders_active": g("SELECT COUNT(*) FROM reminders WHERE active=1"),
        "support": g("SELECT COUNT(*) FROM support"),
        "support_new": g("SELECT COUNT(*) FROM support WHERE status='new'"),
    }


# ==========================================================
#                     KLAVIATURALAR
# ==========================================================
def main_menu(uid):
    rows = [
        [KeyboardButton(text="➕ Yangi eslatma"), KeyboardButton(text="📋 Eslatmalarim")],
        [KeyboardButton(text="✍️ Murojaat"), KeyboardButton(text="🌙 Tinch soatlar")],
    ]
    if uid == ADMIN_ID:
        rows.append([KeyboardButton(text="🛠 Admin panel")])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)


def kind_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⏰ Interval (har necha daqiqada)", callback_data="kind:interval")],
        [InlineKeyboardButton(text="📅 Aniq vaqtga (bir marta)", callback_data="kind:once")],
        [InlineKeyboardButton(text="🔁 Har kuni", callback_data="kind:daily")],
        [InlineKeyboardButton(text="🗓 Har hafta", callback_data="kind:weekly")],
        [InlineKeyboardButton(text="⬅️ Bekor qilish", callback_data="cancel")],
    ])


def interval_menu():
    row, rows = [], []
    for m in INTERVAL_OPTIONS:
        label = f"{m} daq" if m < 60 else f"{m//60} soat" + (f" {m%60}daq" if m % 60 else "")
        row.append(InlineKeyboardButton(text=label, callback_data=f"int:{m}"))
        if len(row) == 3:
            rows.append(row); row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton(text="⬅️ Bekor qilish", callback_data="cancel")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def count_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="♾ Cheksiz", callback_data="cnt:0"),
         InlineKeyboardButton(text="5 marta", callback_data="cnt:5")],
        [InlineKeyboardButton(text="10 marta", callback_data="cnt:10"),
         InlineKeyboardButton(text="20 marta", callback_data="cnt:20")],
        [InlineKeyboardButton(text="⬅️ Bekor qilish", callback_data="cancel")],
    ])


def weekday_menu():
    rows = [[InlineKeyboardButton(text=WEEKDAYS[i], callback_data=f"wd:{i}")] for i in range(7)]
    rows.append([InlineKeyboardButton(text="⬅️ Bekor qilish", callback_data="cancel")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def reminder_actions(rid, active):
    t = ("⏸ Pauza", f"pause:{rid}") if active else ("▶️ Davom", f"resume:{rid}")
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text=t[0], callback_data=t[1]),
        InlineKeyboardButton(text="🗑 O'chirish", callback_data=f"del:{rid}")]])


def admin_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✉️ Bitta odamga yozish", callback_data="adm:dm")],
        [InlineKeyboardButton(text="📊 Statistika", callback_data="adm:stats")],
        [InlineKeyboardButton(text="🆕 Yangi murojaatlar", callback_data="adm:support_new")],
        [InlineKeyboardButton(text="📨 Barcha murojaatlar", callback_data="adm:support_all")],
        [InlineKeyboardButton(text="👥 Foydalanuvchilar", callback_data="adm:users")],
    ])


def support_reply_btn(sid):
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="↩️ Javob berish", callback_data=f"reply:{sid}")]])


# ==========================================================
#                     PLANLASHTIRUVCHI
# ==========================================================
scheduler = AsyncIOScheduler()


def in_quiet_hours(uid):
    u = get_user(uid)
    if not u or u["quiet_start"] is None or u["quiet_end"] is None:
        return False
    s, e = u["quiet_start"], u["quiet_end"]
    if s == e:
        return False
    h = dt.datetime.now().hour
    return (s <= h < e) if s < e else (h >= s or h < e)


async def fire(rid):
    r = get_reminder(rid)
    if r is None or not r["active"]:
        remove_job(rid); return
    if in_quiet_hours(r["user_id"]):
        return
    try:
        if r["voice_id"]:
            await bot.send_voice(r["user_id"], r["voice_id"],
                                 caption="🔔 Eslatma" + (f": {r['text']}" if r["text"] else ""))
        else:
            await bot.send_message(r["user_id"], f"🔔 Eslatma: {r['text']}")
    except Exception as ex:
        print("send error:", ex); return

    inc_sent(rid)
    r = get_reminder(rid)
    if r["max_count"] and r["sent_count"] >= r["max_count"]:
        set_active(rid, False); remove_job(rid)
        try:
            await bot.send_message(r["user_id"], f"✅ Eslatma tugadi ({r['max_count']} marta).")
        except Exception:
            pass
    if r["kind"] == "once":
        set_active(rid, False); remove_job(rid)


def job_id(rid):
    return f"rem_{rid}"


def remove_job(rid):
    try:
        scheduler.remove_job(job_id(rid))
    except Exception:
        pass


def schedule_reminder(r):
    rid = r["id"]
    remove_job(rid)
    k = r["kind"]
    if k == "interval":
        trig = IntervalTrigger(minutes=r["interval_min"])
    elif k == "once":
        run_at = dt.datetime.fromisoformat(r["run_at"])
        if run_at <= dt.datetime.now():
            return
        trig = DateTrigger(run_date=run_at)
    elif k == "daily":
        trig = CronTrigger(hour=r["hour"], minute=r["minute"])
    elif k == "weekly":
        trig = CronTrigger(day_of_week=r["weekday"], hour=r["hour"], minute=r["minute"])
    else:
        return
    scheduler.add_job(fire, trig, args=[rid], id=job_id(rid), replace_existing=True)


# ==========================================================
#                     BOT + HANDLERLAR
# ==========================================================
bot = Bot(BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())


class NewReminder(StatesGroup):
    kind = State(); content = State(); interval = State()
    once_time = State(); daily_time = State()
    weekly_day = State(); weekly_time = State(); count = State()


class Support(StatesGroup):
    waiting = State()


class Quiet(StatesGroup):
    waiting = State()


class AdminReply(StatesGroup):
    waiting = State()
    dm_id = State()
    dm_msg = State()


@dp.message(CommandStart())
async def start(m: Message, state: FSMContext):
    await state.clear()
    upsert_user(m.from_user.id, m.from_user.username, m.from_user.full_name)
    await m.answer(
        "Assalomu alaykum! Men eslatma botiman.\n\n"
        "• ➕ Yangi eslatma — matn yoki ovozli, kerakli oraliqda takrorlab turaman\n"
        "• 📋 Eslatmalarim — pauza / o'chirish\n"
        "• ✍️ Murojaat — savol yoki taklifingizni yuboring\n"
        "• 🌙 Tinch soatlar — shu oraliqda bezovta qilmayman",
        reply_markup=main_menu(m.from_user.id))


# ---- yangi eslatma ----
@dp.message(F.text == "➕ Yangi eslatma")
async def new_rem(m: Message, state: FSMContext):
    await state.set_state(NewReminder.kind)
    await m.answer("Eslatma turini tanlang:", reply_markup=kind_menu())


@dp.callback_query(F.data == "cancel")
async def cancel_cb(c: CallbackQuery, state: FSMContext):
    await state.clear()
    await c.message.edit_text("Bekor qilindi.")
    await c.answer()


@dp.callback_query(NewReminder.kind, F.data.startswith("kind:"))
async def pick_kind(c: CallbackQuery, state: FSMContext):
    await state.update_data(kind=c.data.split(":")[1])
    await state.set_state(NewReminder.content)
    await c.message.edit_text("Eslatma matnini yozing yoki ovozli xabar yuboring 🎤")
    await c.answer()


@dp.message(NewReminder.content, F.voice)
async def content_voice(m: Message, state: FSMContext):
    await state.update_data(voice_id=m.voice.file_id, text=m.caption or "")
    await after_content(m, state)


@dp.message(NewReminder.content, F.text)
async def content_text(m: Message, state: FSMContext):
    await state.update_data(voice_id=None, text=m.text)
    await after_content(m, state)


async def after_content(m: Message, state: FSMContext):
    kind = (await state.get_data())["kind"]
    if kind == "interval":
        await state.set_state(NewReminder.interval)
        await m.answer("Necha daqiqada bir eslatib turay?", reply_markup=interval_menu())
    elif kind == "once":
        await state.set_state(NewReminder.once_time)
        await m.answer("Sana va vaqtni yozing: `YYYY-MM-DD HH:MM`\n(masalan 2026-09-11 09:30)",
                       parse_mode="Markdown")
    elif kind == "daily":
        await state.set_state(NewReminder.daily_time)
        await m.answer("Har kuni soat nechada? `HH:MM` (masalan 07:00)", parse_mode="Markdown")
    elif kind == "weekly":
        await state.set_state(NewReminder.weekly_day)
        await m.answer("Qaysi kuni?", reply_markup=weekday_menu())


@dp.callback_query(NewReminder.interval, F.data.startswith("int:"))
async def pick_interval(c: CallbackQuery, state: FSMContext):
    await state.update_data(interval_min=int(c.data.split(":")[1]))
    await state.set_state(NewReminder.count)
    await c.message.edit_text("Necha marta yuboray?", reply_markup=count_menu())
    await c.answer()


@dp.message(NewReminder.once_time, F.text)
async def once_time(m: Message, state: FSMContext):
    try:
        run_at = dt.datetime.strptime(m.text.strip(), "%Y-%m-%d %H:%M")
    except ValueError:
        return await m.answer("Format noto'g'ri. Namuna: 2026-09-11 09:30")
    if run_at <= dt.datetime.now():
        return await m.answer("Bu vaqt o'tib ketgan.")
    await state.update_data(run_at=run_at.isoformat(), max_count=0)
    await save(m, state)


@dp.message(NewReminder.daily_time, F.text)
async def daily_time(m: Message, state: FSMContext):
    hm = parse_hm(m.text)
    if not hm:
        return await m.answer("Format noto'g'ri. Namuna: 07:00")
    await state.update_data(hour=hm[0], minute=hm[1], max_count=0)
    await save(m, state)


@dp.callback_query(NewReminder.weekly_day, F.data.startswith("wd:"))
async def weekly_day(c: CallbackQuery, state: FSMContext):
    await state.update_data(weekday=int(c.data.split(":")[1]))
    await state.set_state(NewReminder.weekly_time)
    await c.message.edit_text("Soat nechada? `HH:MM`", parse_mode="Markdown")
    await c.answer()


@dp.message(NewReminder.weekly_time, F.text)
async def weekly_time(m: Message, state: FSMContext):
    hm = parse_hm(m.text)
    if not hm:
        return await m.answer("Format noto'g'ri. Namuna: 07:00")
    await state.update_data(hour=hm[0], minute=hm[1], max_count=0)
    await save(m, state)


@dp.callback_query(NewReminder.count, F.data.startswith("cnt:"))
async def pick_count(c: CallbackQuery, state: FSMContext):
    await state.update_data(max_count=int(c.data.split(":")[1]))
    await save(c.message, state, user_id=c.from_user.id)
    await c.answer()


def parse_hm(text):
    try:
        h, mm = text.strip().split(":")
        h, mm = int(h), int(mm)
        if 0 <= h < 24 and 0 <= mm < 60:
            return h, mm
    except Exception:
        pass
    return None


async def save(m: Message, state: FSMContext, user_id=None):
    d = await state.get_data()
    uid = user_id or m.from_user.id
    rid = add_reminder(user_id=uid, kind=d["kind"], text=d.get("text"), voice_id=d.get("voice_id"),
                       interval_min=d.get("interval_min"), run_at=d.get("run_at"),
                       hour=d.get("hour"), minute=d.get("minute"), weekday=d.get("weekday"),
                       max_count=d.get("max_count", 0))
    schedule_reminder(get_reminder(rid))
    await state.clear()
    await m.answer("✅ Eslatma yaratildi!", reply_markup=main_menu(uid))


# ---- eslatmalarim ----
def describe(r):
    body = ("🎤 ovoz " + (r["text"] or "")) if r["voice_id"] else r["text"]
    if r["kind"] == "interval":
        when = f"har {r['interval_min']} daq"
        if r["max_count"]:
            when += f" ({r['sent_count']}/{r['max_count']})"
    elif r["kind"] == "once":
        when = "bir marta: " + r["run_at"].replace("T", " ")
    elif r["kind"] == "daily":
        when = f"har kuni {r['hour']:02d}:{r['minute']:02d}"
    else:
        when = f"{WEEKDAYS[r['weekday']]} {r['hour']:02d}:{r['minute']:02d}"
    return f"{'🟢' if r['active'] else '⏸'} #{r['id']} — {body}\n    {when}"


@dp.message(F.text == "📋 Eslatmalarim")
async def my_rems(m: Message):
    rems = user_reminders(m.from_user.id)
    if not rems:
        return await m.answer("Hozircha eslatmangiz yo'q.")
    for r in rems:
        await m.answer(describe(r), reply_markup=reminder_actions(r["id"], r["active"]))


@dp.callback_query(F.data.startswith("pause:"))
async def pause(c: CallbackQuery):
    rid = int(c.data.split(":")[1])
    set_active(rid, False); remove_job(rid)
    await c.message.edit_text(describe(get_reminder(rid)), reply_markup=reminder_actions(rid, False))
    await c.answer("Pauza")


@dp.callback_query(F.data.startswith("resume:"))
async def resume(c: CallbackQuery):
    rid = int(c.data.split(":")[1])
    set_active(rid, True); schedule_reminder(get_reminder(rid))
    await c.message.edit_text(describe(get_reminder(rid)), reply_markup=reminder_actions(rid, True))
    await c.answer("Davom")


@dp.callback_query(F.data.startswith("del:"))
async def delete(c: CallbackQuery):
    rid = int(c.data.split(":")[1])
    remove_job(rid); delete_reminder(rid)
    await c.message.edit_text("🗑 O'chirildi.")
    await c.answer()


# ---- tinch soatlar ----
@dp.message(F.text == "🌙 Tinch soatlar")
async def quiet(m: Message, state: FSMContext):
    u = get_user(m.from_user.id)
    cur = f"{u['quiet_start']:02d}:00–{u['quiet_end']:02d}:00" if u["quiet_start"] is not None else "o'chirilgan"
    await state.set_state(Quiet.waiting)
    await m.answer(f"Joriy: {cur}\n\nYangi oraliq: `HH-HH` (masalan 23-07)\nO'chirish: 0",
                   parse_mode="Markdown")


@dp.message(Quiet.waiting, F.text)
async def quiet_set(m: Message, state: FSMContext):
    t = m.text.strip()
    if t == "0":
        set_quiet(m.from_user.id, None, None)
        await state.clear()
        return await m.answer("Tinch soatlar o'chirildi.", reply_markup=main_menu(m.from_user.id))
    try:
        s, e = t.split("-"); s, e = int(s), int(e)
        assert 0 <= s < 24 and 0 <= e < 24
    except Exception:
        return await m.answer("Format noto'g'ri. Namuna: 23-07")
    set_quiet(m.from_user.id, s, e)
    await state.clear()
    await m.answer(f"✅ Tinch soatlar: {s:02d}:00–{e:02d}:00", reply_markup=main_menu(m.from_user.id))


# ---- murojaat ----
@dp.message(F.text == "✍️ Murojaat")
async def support(m: Message, state: FSMContext):
    await state.set_state(Support.waiting)
    await m.answer("Xabaringizni yozing (matn, rasm, ovoz yoki fayl). Adminga yetkazaman.")


@dp.message(Support.waiting)
async def support_recv(m: Message, state: FSMContext):
    mtype, fid, content = "text", None, m.text or ""
    if m.photo:
        mtype, fid, content = "photo", m.photo[-1].file_id, m.caption or ""
    elif m.voice:
        mtype, fid, content = "voice", m.voice.file_id, m.caption or ""
    elif m.document:
        mtype, fid, content = "document", m.document.file_id, m.caption or ""

    sid = add_support(m.from_user.id, m.from_user.username, m.from_user.full_name, content, mtype, fid)
    await state.clear()
    await m.answer("✅ Murojaatingiz yuborildi.", reply_markup=main_menu(m.from_user.id))

    head = (f"🆕 Murojaat #{sid}\n👤 {m.from_user.full_name} "
            f"(@{m.from_user.username or '—'}, id {m.from_user.id})\n")
    try:
        if mtype == "text":
            await bot.send_message(ADMIN_ID, head + f"\n{content}", reply_markup=support_reply_btn(sid))
        elif mtype == "photo":
            await bot.send_photo(ADMIN_ID, fid, caption=head + content, reply_markup=support_reply_btn(sid))
        elif mtype == "voice":
            await bot.send_voice(ADMIN_ID, fid, caption=head + content, reply_markup=support_reply_btn(sid))
        else:
            await bot.send_document(ADMIN_ID, fid, caption=head + content, reply_markup=support_reply_btn(sid))
    except Exception as ex:
        print("admin notify error:", ex)


@dp.callback_query(F.data.startswith("reply:"))
async def reply_start(c: CallbackQuery, state: FSMContext):
    if c.from_user.id != ADMIN_ID:
        return await c.answer("Ruxsat yo'q", show_alert=True)
    sid = int(c.data.split(":")[1])
    await state.set_state(AdminReply.waiting)
    await state.update_data(sid=sid)
    await c.message.answer(f"#{sid} murojaatga javobingizni yozing:")
    await c.answer()


@dp.message(AdminReply.waiting)
async def reply_send(m: Message, state: FSMContext):
    if m.from_user.id != ADMIN_ID:
        return
    sid = (await state.get_data())["sid"]
    s = get_support(sid)
    await state.clear()
    if not s:
        return await m.answer("Murojaat topilmadi.")
    uid = s["user_id"]
    cap = "📩 Admin javobi:"
    try:
        if m.photo:
            await bot.send_photo(uid, m.photo[-1].file_id,
                                 caption=cap + (f"\n\n{m.caption}" if m.caption else ""))
        elif m.voice:
            await bot.send_voice(uid, m.voice.file_id,
                                 caption=cap + (f"\n\n{m.caption}" if m.caption else ""))
        elif m.video:
            await bot.send_video(uid, m.video.file_id,
                                 caption=cap + (f"\n\n{m.caption}" if m.caption else ""))
        elif m.document:
            await bot.send_document(uid, m.document.file_id,
                                    caption=cap + (f"\n\n{m.caption}" if m.caption else ""))
        else:
            await bot.send_message(uid, f"{cap}\n\n{m.text}")
        set_support_answered(sid)
        await m.answer("✅ Javob yuborildi.")
    except Exception as ex:
        await m.answer(f"Yuborilmadi: {ex}")


# ---- admin panel ----
@dp.message(F.text == "🛠 Admin panel")
async def admin(m: Message):
    if m.from_user.id != ADMIN_ID:
        return
    await m.answer("Admin panel:", reply_markup=admin_menu())


# ---- admin: bitta odamga yozish ----
@dp.callback_query(F.data == "adm:dm")
async def dm_start(c: CallbackQuery, state: FSMContext):
    if c.from_user.id != ADMIN_ID:
        return await c.answer("Ruxsat yo'q", show_alert=True)
    await state.set_state(AdminReply.dm_id)
    await c.message.answer("Kimga yozamiz? Foydalanuvchi ID sini yuboring.\n"
                           "(👥 Foydalanuvchilar ro'yxatidan ID ni ko'rishingiz mumkin.)")
    await c.answer()


@dp.message(AdminReply.dm_id, F.text)
async def dm_get_id(m: Message, state: FSMContext):
    if m.from_user.id != ADMIN_ID:
        return
    try:
        uid = int(m.text.strip())
    except ValueError:
        return await m.answer("ID faqat raqam bo'ladi. Qaytadan yuboring.")
    await state.update_data(dm_uid=uid)
    await state.set_state(AdminReply.dm_msg)
    await m.answer(f"#{uid} ga xabaringizni yuboring (matn, rasm, ovoz yoki fayl).")


@dp.message(AdminReply.dm_msg)
async def dm_send(m: Message, state: FSMContext):
    if m.from_user.id != ADMIN_ID:
        return
    uid = (await state.get_data())["dm_uid"]
    await state.clear()
    cap = "📩 Admin xabari:"
    try:
        if m.photo:
            await bot.send_photo(uid, m.photo[-1].file_id,
                                 caption=cap + (f"\n\n{m.caption}" if m.caption else ""))
        elif m.voice:
            await bot.send_voice(uid, m.voice.file_id,
                                 caption=cap + (f"\n\n{m.caption}" if m.caption else ""))
        elif m.video:
            await bot.send_video(uid, m.video.file_id,
                                 caption=cap + (f"\n\n{m.caption}" if m.caption else ""))
        elif m.document:
            await bot.send_document(uid, m.document.file_id,
                                    caption=cap + (f"\n\n{m.caption}" if m.caption else ""))
        else:
            await bot.send_message(uid, f"{cap}\n\n{m.text}")
        await m.answer("✅ Yuborildi.", reply_markup=main_menu(m.from_user.id))
    except Exception as ex:
        await m.answer(f"Yuborilmadi: {ex}\n(Foydalanuvchi botga /start bosмаган bo'lishi mumkin.)",
                       reply_markup=main_menu(m.from_user.id))


@dp.callback_query(F.data == "adm:stats")
async def adm_stats(c: CallbackQuery):
    if c.from_user.id != ADMIN_ID:
        return await c.answer()
    s = stats()
    await c.message.answer(
        f"📊 Statistika\n👥 Foydalanuvchilar: {s['users']}\n"
        f"⏰ Eslatmalar: {s['reminders']} (faol {s['reminders_active']})\n"
        f"📨 Murojaatlar: {s['support']} (yangi {s['support_new']})")
    await c.answer()


@dp.callback_query(F.data.in_({"adm:support_new", "adm:support_all"}))
async def adm_support(c: CallbackQuery):
    if c.from_user.id != ADMIN_ID:
        return await c.answer()
    status = "new" if c.data.endswith("new") else None
    items = support_list(status=status)
    if not items:
        await c.message.answer("Bo'sh.")
        return await c.answer()
    for s in items:
        await c.message.answer(
            f"#{s['id']} [{s['status']}] {s['media_type']}\n"
            f"👤 {s['full_name']} (@{s['username'] or '—'}, id {s['user_id']})\n"
            f"{s['created_at']}\n\n{s['content'] or '—'}",
            reply_markup=support_reply_btn(s["id"]))
    await c.answer()


@dp.callback_query(F.data == "adm:users")
async def adm_users(c: CallbackQuery):
    if c.from_user.id != ADMIN_ID:
        return await c.answer()
    lines = [f"• {u['full_name']} (@{u['username'] or '—'}, id {u['user_id']})" for u in all_users()[:50]]
    await c.message.answer("👥 Foydalanuvchilar:\n" + ("\n".join(lines) or "—"))
    await c.answer()


# ==========================================================
#                        RUN (webhook — bepul Web Service uchun)
# ==========================================================
PORT = int(os.getenv("PORT", "10000"))
# Render avtomatik beradi, masalan: https://my-bot.onrender.com
BASE_URL = os.getenv("RENDER_EXTERNAL_URL", os.getenv("BASE_URL", ""))
WEBHOOK_PATH = "/webhook"


async def health(request):
    # cron-job.org shu manzilni ping qilib botni uxlatmaydi
    return web.Response(text="ok")


async def on_startup(app):
    init_db()
    for r in all_active_reminders():
        schedule_reminder(r)
    scheduler.start()
    if BASE_URL:
        await bot.set_webhook(BASE_URL.rstrip("/") + WEBHOOK_PATH,
                              drop_pending_updates=True)
        print("Webhook o'rnatildi:", BASE_URL.rstrip("/") + WEBHOOK_PATH)
    else:
        print("DIQQAT: BASE_URL/RENDER_EXTERNAL_URL yo'q — webhook o'rnatilmadi.")
    print("Bot ishga tushdi.")


def build_app():
    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_get("/health", health)
    SimpleRequestHandler(dispatcher=dp, bot=bot).register(app, path=WEBHOOK_PATH)
    setup_application(app, dp, bot=bot)
    app.on_startup.append(on_startup)
    return app


if __name__ == "__main__":
    web.run_app(build_app(), host="0.0.0.0", port=PORT)
