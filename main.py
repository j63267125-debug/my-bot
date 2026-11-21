"""
Uy navbatchiligi boti — BITTA FAYL.

ISHGA TUSHIRISH:
  1) pip install aiogram APScheduler tzdata
  2) Environment:
       BOT_TOKEN, ADMIN_ID
       TZ_NAME   (ixtiyoriy, standart Asia/Tashkent)
       DB_PATH   (ixtiyoriy, standart navbat.db)
       BASE_URL yoki RENDER_EXTERNAL_URL — bo'lsa webhook, bo'lmasa polling
       PORT      (webhook uchun, standart 10000)
  3) python main.py
"""

import os
import random
import asyncio
import logging
import sqlite3
import datetime as dt
from zoneinfo import ZoneInfo

from aiohttp import web
from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton,
)
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("navbat")

# ==========================================================
#                     SOZLAMALAR
# ==========================================================
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
TZ = ZoneInfo(os.getenv("TZ_NAME", "Asia/Tashkent"))
DB_PATH = os.getenv("DB_PATH", "navbat.db")
PORT = int(os.getenv("PORT", "10000"))
BASE_URL = os.getenv("RENDER_EXTERNAL_URL", os.getenv("BASE_URL", ""))
WEBHOOK_PATH = "/webhook"

WD = ["Du", "Se", "Ch", "Pa", "Ju", "Sh", "Ya"]
MONTHS = ["Yanvar", "Fevral", "Mart", "Aprel", "May", "Iyun",
          "Iyul", "Avgust", "Sentyabr", "Oktyabr", "Noyabr", "Dekabr"]

DEFAULT_SETTINGS = {
    "remind_hours": "8,13,18,21",
    "deadline": "22",
    "tomorrow_hour": "21",
    "week_gen_hour": "20",
    "rate_hours": "24",
    "p_done": "10",
    "p_allday": "5",
    "p_like": "1",
    "p_dislike": "-1",
    "p_missed": "-10",
    "p_fine": "-15",
    "discount": "100000",
    "award_hour": "23",
}

SETTING_INFO = {
    "remind_hours": "Eslatma soatlari (birinchisi — ertalabki xabar)",
    "deadline": "Muddat soati",
    "tomorrow_hour": "«Ertaga navbatchisiz» xabari soati",
    "week_gen_hour": "Yakshanba: keyingi hafta jadvali tuziladigan soat",
    "rate_hours": "Baholash muddati (soat)",
    "p_done": "Ish bajarilsa (ball)",
    "p_allday": "Kunning hamma ishi bajarilsa (bonus)",
    "p_like": "Har 👍 uchun",
    "p_dislike": "Har 👎 uchun",
    "p_missed": "Ish bajarilmasa",
    "p_fine": "Jarima yozilsa",
    "discount": "Oy g'olibiga chegirma (so'm)",
    "award_hour": "Oyning 1-kuni g'olib aniqlanadigan soat",
}

DEFAULT_QUOTES = [
    "🌿 «Poklik imonning yarmidir.» (Sahih Muslim)",
    "🤲 «Albatta, Alloh tavba qiluvchilarni va poklanuvchilarni sevadi.» (Baqara, 222)",
    "✨ Toza uy — xotirjam yurak. Bugun hammamiz sizning mehnatingizdan bahramand bo'lamiz.",
    "💪 Bir soatlik mehnat — butun kunlik farovonlik. Omad!",
    "🏠 Siz tozalagan uyda boshqalar ham sizni duo qiladi.",
]

OATH_TEXT = ("⚠️ Rasm ish tugagandan keyin, HOZIR olingan bo'lishi shart.\n"
             "Agar oldin olingan rasm yuborsangiz — Alloh jazoingizni bersin.\n\n"
             "Rozi bo'lsangiz, pastdagi tugmani bosing va rasm yuboring.")

bot = Bot(BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())
scheduler = AsyncIOScheduler(timezone=TZ)
IsAdmin = F.from_user.id == ADMIN_ID


# ==========================================================
#                     YORDAMCHILAR
# ==========================================================
def now():
    return dt.datetime.now(TZ).replace(tzinfo=None)


def today():
    return now().date()


def iso(d):
    return d if isinstance(d, str) else d.isoformat()


def ts():
    return now().isoformat(timespec="seconds")


def fmt(n):
    return f"{int(n):,}".replace(",", " ")


def short(s, n):
    s = (s or "").replace("\n", " ").strip()
    return s if len(s) <= n else s[:n - 1] + "…"


def to_int(s):
    return int((s or "").replace(" ", "").replace(",", ""))


def ikb(rows):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t, callback_data=d) for t, d in row] for row in rows])


def monday_of(d):
    return d - dt.timedelta(days=d.weekday())


def week_days(mon):
    return [mon + dt.timedelta(days=i) for i in range(7)]


def month_name(month):
    return f"{MONTHS[int(month[5:7]) - 1]} {month[:4]}"


# ==========================================================
#                     BAZA (SQLite)
# ==========================================================
_conn = None


def db():
    global _conn
    if _conn is None:
        _conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        _conn.row_factory = sqlite3.Row
        _conn.execute("PRAGMA journal_mode=WAL")
    return _conn


def q(sql, a=()):
    return db().execute(sql, a).fetchall()


def q1(sql, a=()):
    return db().execute(sql, a).fetchone()


def run(sql, a=()):
    c = db()
    cur = c.execute(sql, a)
    c.commit()
    return cur


def init_db():
    c = db()
    c.executescript("""
        CREATE TABLE IF NOT EXISTS users(
            user_id INTEGER PRIMARY KEY, username TEXT, full_name TEXT,
            status TEXT, joined_at TEXT);
        CREATE TABLE IF NOT EXISTS tasks(
            id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, descr TEXT,
            fine INTEGER DEFAULT 0, active INTEGER DEFAULT 1);
        CREATE TABLE IF NOT EXISTS schedule(day TEXT PRIMARY KEY, user_id INTEGER);
        CREATE TABLE IF NOT EXISTS completions(
            id INTEGER PRIMARY KEY AUTOINCREMENT, day TEXT, task_id INTEGER, user_id INTEGER,
            photo_id TEXT, created_at TEXT, status TEXT DEFAULT 'rating', decision TEXT,
            UNIQUE(day, task_id));
        CREATE TABLE IF NOT EXISTS votes(
            completion_id INTEGER, voter_id INTEGER, value INTEGER, pts INTEGER,
            cancelled INTEGER DEFAULT 0, created_at TEXT,
            PRIMARY KEY(completion_id, voter_id));
        CREATE TABLE IF NOT EXISTS missed(
            day TEXT, task_id INTEGER, user_id INTEGER, decision TEXT,
            PRIMARY KEY(day, task_id));
        CREATE TABLE IF NOT EXISTS fines(
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, amount INTEGER,
            reason TEXT, day TEXT, paid INTEGER DEFAULT 0, created_at TEXT);
        CREATE TABLE IF NOT EXISTS points(
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, delta INTEGER,
            reason TEXT, ref TEXT UNIQUE, day TEXT, created_at TEXT);
        CREATE TABLE IF NOT EXISTS quotes(id INTEGER PRIMARY KEY AUTOINCREMENT, text TEXT);
        CREATE TABLE IF NOT EXISTS awards(
            month TEXT PRIMARY KEY, user_id INTEGER, points INTEGER, amount INTEGER, status TEXT);
        CREATE TABLE IF NOT EXISTS support(
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, created_at TEXT);
        CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY, value TEXT);
    """)
    for k, v in DEFAULT_SETTINGS.items():
        c.execute("INSERT OR IGNORE INTO settings VALUES(?,?)", (k, v))
    if c.execute("SELECT COUNT(*) FROM quotes").fetchone()[0] == 0:
        c.executemany("INSERT INTO quotes(text) VALUES(?)", [(t,) for t in DEFAULT_QUOTES])
    c.commit()


# ---- sozlamalar ----
def sget(k):
    r = q1("SELECT value FROM settings WHERE key=?", (k,))
    return r[0] if r else DEFAULT_SETTINGS.get(k, "")


def sint(k):
    return int(sget(k))


def sset(k, v):
    run("INSERT OR REPLACE INTO settings VALUES(?,?)", (k, str(v)))


def remind_hours():
    return sorted(int(h) for h in sget("remind_hours").split(",") if h.strip())


# ---- ma'lumotlar ----
def get_user(uid):
    return q1("SELECT * FROM users WHERE user_id=?", (uid,))


def is_active(uid):
    r = get_user(uid)
    return bool(r) and r["status"] == "active"


def nm(uid):
    r = get_user(uid)
    return (r["full_name"] or str(uid)) if r else str(uid)


def active_ids():
    return [r[0] for r in q("SELECT user_id FROM users WHERE status='active' ORDER BY joined_at")]


def active_tasks():
    return q("SELECT * FROM tasks WHERE active=1 ORDER BY id")


def get_task(tid):
    return q1("SELECT * FROM tasks WHERE id=?", (tid,))


def get_comp(cid):
    return q1("SELECT * FROM completions WHERE id=?", (cid,))


def duty(d):
    r = q1("SELECT user_id FROM schedule WHERE day=?", (iso(d),))
    return r[0] if r else None


def done_ids(d):
    return {r[0] for r in q("SELECT task_id FROM completions WHERE day=?", (iso(d),))}


def remaining(d):
    done = done_ids(d)
    return [t for t in active_tasks() if t["id"] not in done]


def add_points(uid, delta, reason, ref=None, day=None):
    """ref takrorlanmas — bir hodisa uchun ball ikki marta yozilmaydi."""
    if not delta:
        return False
    cur = run("INSERT OR IGNORE INTO points(user_id,delta,reason,ref,day,created_at) VALUES(?,?,?,?,?,?)",
              (uid, delta, reason, ref, iso(day or today()), ts()))
    return cur.rowcount == 1


def next_quote():
    rows = q("SELECT id, text FROM quotes")
    if not rows:
        return ""
    last = sget("last_quote")
    pool = [r for r in rows if str(r[0]) != last] or rows
    r = random.choice(pool)
    sset("last_quote", r[0])
    return r[1]


def harsh(voter):
    """Oxirgi ovozlarining 80%+ i 👎 bo'lsa — shubhali."""
    rows = q("SELECT value FROM votes WHERE voter_id=? AND cancelled=0 ORDER BY created_at DESC LIMIT 10",
             (voter,))
    return len(rows) >= 5 and sum(1 for r in rows if r[0] == -1) / len(rows) >= 0.8


def ranking(month):
    """[(uid, ball, jarima_soni, like_soni)] — tartiblangan."""
    users = active_ids()
    pts = {r[0]: r[1] for r in q(
        "SELECT user_id, SUM(delta) FROM points WHERE substr(day,1,7)=? GROUP BY user_id", (month,))}
    fn = {r[0]: r[1] for r in q(
        "SELECT user_id, COUNT(*) FROM fines WHERE substr(day,1,7)=? GROUP BY user_id", (month,))}
    lk = {r[0]: r[1] for r in q(
        "SELECT c.user_id, COUNT(*) FROM votes v JOIN completions c ON c.id=v.completion_id "
        "WHERE v.value=1 AND v.cancelled=0 AND substr(c.day,1,7)=? GROUP BY c.user_id", (month,))}
    rows = [(u, pts.get(u) or 0, fn.get(u, 0), lk.get(u, 0)) for u in users]
    rows.sort(key=lambda r: (-r[1], r[2], -r[3]))
    return rows


# ==========================================================
#                     NAVBAT TAQSIMLASH
# ==========================================================
def assign(days):
    """Adolatli random: kam navbatchi bo'lganlarga ortiqcha kun beriladi,
    bir kishi ketma-ket 2 kun navbatchi bo'lmaydi."""
    users = active_ids()
    days = sorted(days)
    if not users or not days:
        return {}
    since = iso(days[0] - dt.timedelta(days=28))
    hist = {r[0]: r[1] for r in q(
        "SELECT user_id, COUNT(*) FROM schedule WHERE day>=? AND day<? GROUP BY user_id",
        (since, iso(days[0])))}
    base, extra = divmod(len(days), len(users))
    order = sorted(users, key=lambda u: (hist.get(u, 0), random.random()))
    pool = []
    for i, u in enumerate(order):
        pool += [u] * (base + (1 if i < extra else 0))

    def ok(m):
        for d, u in m.items():
            for n in (d - dt.timedelta(days=1), d + dt.timedelta(days=1)):
                other = m[n] if n in m else duty(n)
                if other == u:
                    return False
        return True

    best = None
    for _ in range(3000):
        random.shuffle(pool)
        m = dict(zip(days, pool))
        if ok(m):
            best = m
            break
    if best is None:
        best = dict(zip(days, pool))
    for d, u in best.items():
        run("INSERT OR REPLACE INTO schedule(day,user_id) VALUES(?,?)", (iso(d), u))
    return best


def ensure_current():
    d = today()
    missing = [x for x in week_days(monday_of(d)) if x >= d and duty(x) is None]
    if missing:
        assign(missing)


def schedule_text(mon):
    lines = []
    for d in week_days(mon):
        u = duty(d)
        mark = "👉 " if d == today() else ""
        lines.append(f"{mark}{WD[d.weekday()]} {d:%d.%m} — {nm(u) if u else '—'}")
    return "\n".join(lines)


# ==========================================================
#                     XABAR YUBORISH
# ==========================================================
async def send(uid, text, **kw):
    try:
        return await bot.send_message(uid, text, **kw)
    except Exception as e:
        log.warning("send %s: %s", uid, e)


async def broadcast(text, admin=False, exclude=()):
    ids = [u for u in active_ids() if u not in exclude]
    if admin:
        ids.append(ADMIN_ID)
    for u in ids:
        await send(u, text)
        await asyncio.sleep(0.05)


async def safe_markup(msg, kb):
    try:
        await msg.edit_reply_markup(reply_markup=kb)
    except Exception:
        pass


def tasks_kb(rem):
    return ikb([[(f"✅ {short(t['name'], 30)}", f"do:{t['id']}")] for t in rem]) if rem else None


def vote_kb(cid, val=0):
    return ikb([[("👍 ✓" if val == 1 else "👍", f"v:{cid}:1"),
                 ("👎 ✓" if val == -1 else "👎", f"v:{cid}:-1")]])


def duty_message(uid, d, greet=True):
    tasks = active_tasks()
    done = done_ids(d)
    rem = [t for t in tasks if t["id"] not in done]
    text = ""
    if greet:
        text = f"🌅 Assalomu alaykum, {nm(uid)}!\nBugun siz navbatchisiz.\n\n{next_quote()}\n\n"
    text += "🧹 Bugungi ishlar:\n"
    for t in tasks:
        text += f"{'✅' if t['id'] in done else '⬜'} {t['name']}"
        text += f" — {t['descr']}\n" if t["descr"] else "\n"
    if not tasks:
        text += "Ro'yxat hali bo'sh.\n"
    if rem:
        text += f"\n⏰ Muddat: {sint('deadline')}:00 gacha.\nIshni bajargach, pastdagi tugmani bosing."
    elif tasks:
        text += "\n🎉 Bugungi barcha ishlar bajarilgan."
    return text, tasks_kb(rem)


async def notify_today_change(old):
    d = today()
    new = duty(d)
    hrs = remind_hours()
    h = now().hour
    if new and new != old and hrs and hrs[0] <= h < sint("deadline"):
        text, kb = duty_message(new, d)
        await send(new, text, reply_markup=kb)


# ==========================================================
#                     JARIMA VA BAHO
# ==========================================================
async def fine_user(uid, amount, reason, day):
    cur = run("INSERT INTO fines(user_id,amount,reason,day,created_at) VALUES(?,?,?,?,?)",
              (uid, amount, reason, iso(day), ts()))
    add_points(uid, sint("p_fine"), f"Jarima: {reason}", ref=f"fine:{cur.lastrowid}", day=day)
    await send(uid, f"💸 Sizga {fmt(amount)} so'm jarima yozildi.\nSabab: {reason}")


async def decide_comp(cid, amount):
    c = get_comp(cid)
    if not c or c["decision"]:
        return False
    run("UPDATE completions SET decision=? WHERE id=?", ("fined" if amount else "ok", cid))
    if amount:
        t = get_task(c["task_id"])
        await fine_user(c["user_id"], amount, f"{c['day']} — {t['name']}: yomon baho", c["day"])
    return True


async def decide_missed(day, tid, amount):
    r = q1("SELECT * FROM missed WHERE day=? AND task_id=?", (day, tid))
    if not r or r["decision"]:
        return False
    run("UPDATE missed SET decision=? WHERE day=? AND task_id=?", ("fined" if amount else "ok", day, tid))
    if amount:
        t = get_task(tid)
        await fine_user(r["user_id"], amount, f"{day} — {t['name']}: bajarilmadi", day)
    return True


def result_kb(cid):
    c = get_comp(cid)
    t = get_task(c["task_id"])
    rows = []
    if not c["decision"]:
        rows.append([(f"💸 Jarima {fmt(t['fine'])}", f"fc:{cid}"), ("✏️ Boshqa summa", f"fa:{cid}")])
        rows.append([("✅ Jarimasiz", f"fo:{cid}")])
    for v in q("SELECT voter_id FROM votes WHERE completion_id=? AND value=-1 AND cancelled=0", (cid,)):
        rows.append([(f"🚫 {short(nm(v[0]), 18)} 👎 ni bekor qilish", f"vc:{cid}:{v[0]}")])
    return ikb(rows) if rows else None


def missed_kb(day, tid):
    t = get_task(tid)
    return ikb([[(f"💸 Jarima {fmt(t['fine'])}", f"mf:{day}:{tid}"), ("✏️ Boshqa summa", f"ma:{day}:{tid}")],
                [("✅ Kechirish", f"mo:{day}:{tid}")]])


async def close_comp(cid):
    c = get_comp(cid)
    if not c or c["status"] != "rating":
        return
    run("UPDATE completions SET status='closed' WHERE id=?", (cid,))
    pl, pdl = sint("p_like"), sint("p_dislike")
    likes, dis = [], []
    for v in q("SELECT * FROM votes WHERE completion_id=? AND cancelled=0", (cid,)):
        pts = pl if v["value"] == 1 else pdl
        run("UPDATE votes SET pts=? WHERE completion_id=? AND voter_id=?", (pts, cid, v["voter_id"]))
        add_points(c["user_id"], pts, "Baho", ref=f"vote:{cid}:{v['voter_id']}", day=c["day"])
        (likes if v["value"] == 1 else dis).append(v["voter_id"])
    t = get_task(c["task_id"])
    bad = len(dis) > len(likes)
    like_names = ", ".join(nm(u) for u in likes) or "—"
    dis_names = ", ".join(nm(u) for u in dis) or "—"
    cap = (f"{'⚠️ YOMON BAHO' if bad else '✅ Baholash yakunlandi'}\n"
           f"📅 {c['day']}\n👤 {nm(c['user_id'])}\n🧹 {t['name']}\n\n"
           f"👍 {len(likes)}: {like_names}\n👎 {len(dis)}: {dis_names}")
    flags = [nm(u) for u in dis if harsh(u)]
    if flags:
        cap += "\n\n🚩 Doim 👎 bosadi: " + ", ".join(flags)
    cap += "\n\nJarima yozilsinmi?"
    try:
        await bot.send_photo(ADMIN_ID, c["photo_id"], caption=cap[:1024], reply_markup=result_kb(cid))
    except Exception as e:
        log.warning("admin result: %s", e)
    await send(c["user_id"], f"📊 «{t['name']}» ishingiz baholandi: 👍 {len(likes)} · 👎 {len(dis)}")


# ==========================================================
#                     REJALI ISHLAR
# ==========================================================
async def morning():
    d = today()
    uid = duty(d)
    if not uid:
        return
    text, kb = duty_message(uid, d)
    await send(uid, text, reply_markup=kb)
    await broadcast(f"📅 Bugun navbatchi: {nm(uid)}", exclude=(uid,))


async def remind():
    d = today()
    uid = duty(d)
    if not uid:
        return
    rem = remaining(d)
    if not rem:
        return
    left = sint("deadline") - now().hour
    text = (f"⏰ Eslatma: hali {len(rem)} ta ish bajarilmagan.\n"
            f"Muddatga {left} soat qoldi.")
    if left <= 1:
        text += f"\n{sint('deadline')}:00 dan keyin jarima masalasi ko'riladi."
    await send(uid, text, reply_markup=tasks_kb(rem))


async def deadline_check():
    d = today()
    uid = duty(d)
    if not uid:
        return
    rem = remaining(d)
    names = []
    for t in rem:
        cur = run("INSERT OR IGNORE INTO missed(day,task_id,user_id) VALUES(?,?,?)", (iso(d), t["id"], uid))
        if cur.rowcount != 1:
            continue
        names.append(t["name"])
        add_points(uid, sint("p_missed"), f"Bajarilmadi: {t['name']}", ref=f"missed:{iso(d)}:{t['id']}", day=d)
        await send(ADMIN_ID, f"❌ Bajarilmadi\n📅 {iso(d)}\n👤 {nm(uid)}\n🧹 {t['name']}\n\nJarima yozilsinmi?",
                   reply_markup=missed_kb(iso(d), t["id"]))
    if names:
        await send(uid, "❌ Bugun bajarilmagan ishlar:\n" + "\n".join(f"• {n}" for n in names) +
                   "\n\nJarima masalasini uy egasi ko'rib chiqadi.")


async def tomorrow_warn():
    uid = duty(today() + dt.timedelta(days=1))
    if uid:
        await send(uid, "🔔 Ertaga siz navbatchisiz. Ertalab ishlar ro'yxatini yuboraman.")


async def gen_next_week():
    mon = monday_of(today()) + dt.timedelta(days=7)
    missing = [d for d in week_days(mon) if duty(d) is None]
    if missing:
        assign(missing)
    await broadcast("📅 Keyingi hafta navbatchilari:\n\n" + schedule_text(mon), admin=True)


async def weekly_best():
    mon = monday_of(today())
    rows = q("SELECT c.user_id, COUNT(*) n FROM votes v JOIN completions c ON c.id=v.completion_id "
             "WHERE v.value=1 AND v.cancelled=0 AND c.day>=? AND c.day<=? GROUP BY c.user_id ORDER BY n DESC",
             (iso(mon), iso(today())))
    rows = [r for r in rows if is_active(r[0])]
    if not rows:
        return
    await broadcast(f"🏆 Haftaning eng toza navbatchisi: {nm(rows[0][0])} (👍 {rows[0][1]})\nBarakalla!",
                    admin=True)


async def monthly_award():
    month = (today().replace(day=1) - dt.timedelta(days=1)).strftime("%Y-%m")
    if q1("SELECT 1 FROM awards WHERE month=?", (month,)):
        return
    rows = ranking(month)
    if not rows:
        return
    u, p = rows[0][0], rows[0][1]
    amount = sint("discount")
    run("INSERT INTO awards(month,user_id,points,amount,status) VALUES(?,?,?,?, 'pending')", (month, u, p, amount))
    top = "\n".join(f"{i + 1}. {nm(r[0])} — {r[1]} ball" for i, r in enumerate(rows[:3]))
    await send(ADMIN_ID,
               f"🏆 {month_name(month)} yakunlari:\n\n{top}\n\n"
               f"G'olib: {nm(u)} — {p} ball.\n{fmt(amount)} so'm chegirma tasdiqlansinmi?",
               reply_markup=ikb([[("✅ Tasdiqlash", f"aw:{month}"), ("❌ Bekor", f"an:{month}")]]))


async def close_due():
    lim = (now() - dt.timedelta(hours=sint("rate_hours"))).isoformat(timespec="seconds")
    for r in q("SELECT id FROM completions WHERE status='rating' AND created_at<=?", (lim,)):
        await close_comp(r[0])


async def tick():
    """Har soat boshida ishlaydi."""
    try:
        h, d = now().hour, today()
        await close_due()
        if d.weekday() == 6 and h == sint("week_gen_hour"):
            await gen_next_week()
        hrs = [x for x in remind_hours() if x < sint("deadline")]
        if hrs and h == hrs[0]:
            await morning()
        elif h in hrs:
            await remind()
        if h == sint("deadline"):
            await deadline_check()
            if d.weekday() == 6:
                await weekly_best()
        if h == sint("tomorrow_hour"):
            await tomorrow_warn()
        if d.day == 1 and h == sint("award_hour"):
            await monthly_award()
    except Exception:
        log.exception("tick")


# ==========================================================
#                     KLAVIATURALAR
# ==========================================================
T_TODAY, T_SCHED, T_RANK = "🧹 Bugungi ishlar", "📅 Navbat jadvali", "🏆 Reyting"
T_ME, T_SUPPLY, T_SUPPORT = "📊 Mening holatim", "🧴 Vosita tugadi", "✍️ Murojaat"
A_TASKS, A_USERS, A_SCHED = "🧹 Ishlar", "👥 Ijarachilar", "📅 Navbat"
A_DEBT, A_PTS, A_QUOTES = "💰 Qarzlar", "➕ Ball", "💬 Gaplar"
A_STATS, A_SET = "📊 Statistika", "⚙️ Sozlamalar"


def kb(rows):
    return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text=t) for t in r] for r in rows],
                               resize_keyboard=True)


def tenant_kb():
    return kb([[T_TODAY, T_SCHED], [T_RANK, T_ME], [T_SUPPLY, T_SUPPORT]])


def admin_kb():
    return kb([[A_TASKS, A_USERS, A_SCHED], [A_DEBT, T_RANK, A_PTS], [A_QUOTES, A_STATS, A_SET]])


# ==========================================================
#                     HOLATLAR
# ==========================================================
class DoTask(StatesGroup):
    photo = State()


class Supply(StatesGroup):
    waiting = State()


class Support(StatesGroup):
    waiting = State()


class Adm(StatesGroup):
    add_task = State()
    task_fine = State()
    fine_amt = State()
    pts = State()
    add_quote = State()
    reply = State()


# ==========================================================
#                     START / BEKOR
# ==========================================================
@dp.message(CommandStart())
async def start(m: Message, state: FSMContext):
    await state.clear()
    u = m.from_user
    if u.id == ADMIN_ID:
        return await m.answer("🛠 Uy egasi paneli", reply_markup=admin_kb())
    r = get_user(u.id)
    if r and r["status"] == "active":
        run("UPDATE users SET username=?, full_name=? WHERE user_id=?", (u.username, u.full_name, u.id))
        return await m.answer("Asosiy menyu", reply_markup=tenant_kb())
    if r and r["status"] == "pending":
        return await m.answer("⏳ So'rovingiz uy egasiga yuborilgan. Tasdiqlashini kuting.")
    run("INSERT OR REPLACE INTO users(user_id,username,full_name,status,joined_at) VALUES(?,?,?,'pending',?)",
        (u.id, u.username, u.full_name, ts()))
    await m.answer("⏳ So'rovingiz uy egasiga yuborildi. Tasdiqlangach xabar beraman.")
    await send(ADMIN_ID, f"🆕 Yangi ijarachi so'rovi:\n👤 {u.full_name} (@{u.username or '—'}, id {u.id})",
               reply_markup=ikb([[("✅ Qabul qilish", f"ap:{u.id}"), ("❌ Rad etish", f"rj:{u.id}")]]))


@dp.callback_query(F.data == "cancel")
async def cb_cancel(c: CallbackQuery, state: FSMContext):
    await state.clear()
    try:
        await c.message.edit_text("Bekor qilindi.")
    except Exception:
        pass
    await c.answer()


# ==========================================================
#                     IJARACHI MENYUSI
# ==========================================================
@dp.message(F.text == T_TODAY)
async def m_today(m: Message, state: FSMContext):
    await state.clear()
    uid = m.from_user.id
    if not is_active(uid):
        return
    d = today()
    du = duty(d)
    if du == uid:
        text, k = duty_message(uid, d, greet=False)
        return await m.answer(text, reply_markup=k)
    tasks = active_tasks()
    done = done_ids(d)
    lines = "\n".join(f"{'✅' if t['id'] in done else '⬜'} {t['name']}" for t in tasks) or "—"
    await m.answer(f"📅 Bugun navbatchi: {nm(du) if du else '—'}\n\n{lines}")


@dp.message(F.text == T_SCHED)
async def m_sched(m: Message, state: FSMContext):
    await state.clear()
    if not is_active(m.from_user.id):
        return
    mon = monday_of(today())
    text = "📅 Shu hafta:\n" + schedule_text(mon)
    nxt = mon + dt.timedelta(days=7)
    if duty(nxt):
        text += "\n\n📅 Keyingi hafta:\n" + schedule_text(nxt)
    await m.answer(text)


def ranking_text(month=None):
    month = month or today().strftime("%Y-%m")
    medals = ["🥇", "🥈", "🥉"]
    lines = []
    for i, r in enumerate(ranking(month)):
        mark = medals[i] if i < 3 else f"{i + 1}."
        lines.append(f"{mark} {nm(r[0])} — {r[1]} ball")
    return (f"🏆 {month_name(month)} reytingi\n\n" + ("\n".join(lines) or "Hali ma'lumot yo'q") +
            f"\n\nOy oxirida 1-o'rin egasiga ijaradan {fmt(sint('discount'))} so'm chegirma.")


@dp.message(F.text == T_RANK)
async def m_rank(m: Message, state: FSMContext):
    await state.clear()
    if m.from_user.id != ADMIN_ID and not is_active(m.from_user.id):
        return
    await m.answer(ranking_text())


@dp.message(F.text == T_ME)
async def m_me(m: Message, state: FSMContext):
    await state.clear()
    uid = m.from_user.id
    if not is_active(uid):
        return
    month = today().strftime("%Y-%m")
    rows = ranking(month)
    place = next((i + 1 for i, r in enumerate(rows) if r[0] == uid), "—")
    pts = next((r[1] for r in rows if r[0] == uid), 0)
    debt = q1("SELECT COALESCE(SUM(amount),0) FROM fines WHERE user_id=? AND paid=0", (uid,))[0]
    nd = q1("SELECT day FROM schedule WHERE user_id=? AND day>=? ORDER BY day LIMIT 1", (uid, iso(today())))
    nd_text = "—"
    if nd:
        d = dt.date.fromisoformat(nd[0])
        nd_text = f"{WD[d.weekday()]} {d:%d.%m}" + (" (bugun)" if d == today() else "")
    await m.answer(f"📊 {nm(uid)}\n\n🏆 {month_name(month)}: {pts} ball, {place}-o'rin\n"
                   f"💸 To'lanmagan jarima: {fmt(debt)} so'm\n📅 Keyingi navbat: {nd_text}")


@dp.message(F.text == T_SUPPLY)
async def m_supply(m: Message, state: FSMContext):
    await state.clear()
    if not is_active(m.from_user.id):
        return
    await state.set_state(Supply.waiting)
    await m.answer("Nima tugadi? Yozing (masalan: idish yuvish vositasi, axlat paketi).")


@dp.message(F.text == T_SUPPORT)
async def m_support(m: Message, state: FSMContext):
    await state.clear()
    if not is_active(m.from_user.id):
        return
    await state.set_state(Support.waiting)
    await m.answer("Xabaringizni yozing (matn, rasm, ovoz yoki fayl). Uy egasiga yetkazaman.")


# ==========================================================
#                     ADMIN MENYUSI
# ==========================================================
def tasks_view():
    ts_ = active_tasks()
    lines = []
    for i, t in enumerate(ts_, 1):
        lines.append(f"{i}. {t['name']} — jarima {fmt(t['fine'])} so'm" + (f"\n   {t['descr']}" if t["descr"] else ""))
    rows = [[(f"🗑 {short(t['name'], 18)}", f"td:{t['id']}"), ("💸 Jarima", f"tf:{t['id']}")] for t in ts_]
    rows.append([("➕ Ish qo'shish", "ta")])
    return "🧹 Kunlik ishlar:\n\n" + ("\n".join(lines) or "Hali ish qo'shilmagan."), ikb(rows)


def users_view():
    pend = q("SELECT * FROM users WHERE status='pending' ORDER BY joined_at")
    act = q("SELECT * FROM users WHERE status='active' ORDER BY joined_at")
    text = "👥 Ijarachilar:\n\n"
    text += "\n".join(f"• {u['full_name']} (@{u['username'] or '—'})" for u in act) or "Hali yo'q."
    if pend:
        text += "\n\n⏳ Kutayotganlar:\n" + "\n".join(f"• {u['full_name']} (@{u['username'] or '—'})" for u in pend)
    rows = [[(f"✅ {short(u['full_name'], 20)}", f"ap:{u['user_id']}"), ("❌", f"rj:{u['user_id']}")] for u in pend]
    rows += [[(f"🚪 {short(u['full_name'], 20)} — chiqarish", f"rm:{u['user_id']}")] for u in act]
    return text, (ikb(rows) if rows else None)


def sched_view():
    mon = monday_of(today())
    nxt = mon + dt.timedelta(days=7)
    text = "📅 Shu hafta:\n" + schedule_text(mon)
    days = [d for d in week_days(mon) if d >= today()]
    if duty(nxt):
        text += "\n\n📅 Keyingi hafta:\n" + schedule_text(nxt)
        days += week_days(nxt)
    text += "\n\nKunni o'zgartirish uchun tanlang:"
    btns = [(f"{WD[d.weekday()]} {d:%d.%m}", f"sd:{iso(d)}") for d in days]
    rows = [btns[i:i + 3] for i in range(0, len(btns), 3)]
    rows.append([("🔀 Shu haftani qayta taqsimlash", "rg")])
    return text, ikb(rows)


def debts_view():
    rows = q("SELECT user_id, SUM(amount) s FROM fines WHERE paid=0 GROUP BY user_id HAVING s>0 ORDER BY s DESC")
    text = "💰 To'lanmagan jarimalar:\n\n" + ("\n".join(f"• {nm(r[0])} — {fmt(r[1])} so'm" for r in rows) or "Qarz yo'q.")
    k = ikb([[(f"✅ {short(nm(r[0]), 20)} to'ladi", f"pd:{r[0]}")] for r in rows]) if rows else None
    return text, k


def quotes_view():
    rows = q("SELECT * FROM quotes ORDER BY id")
    text = "💬 Navbatchiga yuboriladigan gaplar:\n\n" + ("\n\n".join(f"{i}. {r['text']}" for i, r in enumerate(rows, 1)) or "—")
    btns = [(f"🗑 {i}", f"qd:{r['id']}") for i, r in enumerate(rows, 1)]
    k = [btns[i:i + 5] for i in range(0, len(btns), 5)]
    k.append([("➕ Gap qo'shish", "qa")])
    return text[:4000], ikb(k)


def stats_text():
    month = today().strftime("%Y-%m")
    pts = {r[0]: r[1] for r in ranking(month)}
    lines = []
    for u in active_ids():
        done = q1("SELECT COUNT(*) FROM completions WHERE user_id=?", (u,))[0]
        lk = q1("SELECT COUNT(*) FROM votes v JOIN completions c ON c.id=v.completion_id "
                "WHERE c.user_id=? AND v.value=1 AND v.cancelled=0", (u,))[0]
        dl = q1("SELECT COUNT(*) FROM votes v JOIN completions c ON c.id=v.completion_id "
                "WHERE c.user_id=? AND v.value=-1 AND v.cancelled=0", (u,))[0]
        fc, fs = q1("SELECT COUNT(*), COALESCE(SUM(amount),0) FROM fines WHERE user_id=?", (u,))
        ms = q1("SELECT COUNT(*) FROM missed WHERE user_id=?", (u,))[0]
        lines.append(f"👤 {nm(u)}\n   ✅ {done} ish · ❌ {ms} bajarilmagan · 👍 {lk} · 👎 {dl}\n"
                     f"   💸 {fc} jarima ({fmt(fs)} so'm) · 🏆 shu oy {pts.get(u, 0)} ball")
    return "📊 Statistika (umumiy)\n\n" + ("\n\n".join(lines) or "Ijarachi yo'q.")


def settings_text():
    lines = [f"• {k} = {sget(k)}\n   {v}" for k, v in SETTING_INFO.items()]
    return ("⚙️ Sozlamalar\n\n" + "\n".join(lines) +
            "\n\nO'zgartirish: /set kalit qiymat\nMasalan: /set discount 150000\n/set remind_hours 8,12,18,21")


@dp.message(IsAdmin, F.text == A_TASKS)
async def a_tasks(m: Message, state: FSMContext):
    await state.clear()
    text, k = tasks_view()
    await m.answer(text, reply_markup=k)


@dp.message(IsAdmin, F.text == A_USERS)
async def a_users(m: Message, state: FSMContext):
    await state.clear()
    text, k = users_view()
    await m.answer(text, reply_markup=k)


@dp.message(IsAdmin, F.text == A_SCHED)
async def a_sched(m: Message, state: FSMContext):
    await state.clear()
    text, k = sched_view()
    await m.answer(text, reply_markup=k)


@dp.message(IsAdmin, F.text == A_DEBT)
async def a_debt(m: Message, state: FSMContext):
    await state.clear()
    text, k = debts_view()
    await m.answer(text, reply_markup=k)


@dp.message(IsAdmin, F.text == A_PTS)
async def a_pts(m: Message, state: FSMContext):
    await state.clear()
    ids = active_ids()
    if not ids:
        return await m.answer("Ijarachi yo'q.")
    await m.answer("Kimga ball qo'shamiz yoki ayiramiz?",
                   reply_markup=ikb([[(nm(u), f"pt:{u}")] for u in ids]))


@dp.message(IsAdmin, F.text == A_QUOTES)
async def a_quotes(m: Message, state: FSMContext):
    await state.clear()
    text, k = quotes_view()
    await m.answer(text, reply_markup=k)


@dp.message(IsAdmin, F.text == A_STATS)
async def a_stats(m: Message, state: FSMContext):
    await state.clear()
    await m.answer(stats_text()[:4000])


@dp.message(IsAdmin, F.text == A_SET)
async def a_set(m: Message, state: FSMContext):
    await state.clear()
    await m.answer(settings_text())


@dp.message(IsAdmin, Command("set"))
async def cmd_set(m: Message, state: FSMContext):
    await state.clear()
    parts = (m.text or "").split(maxsplit=2)
    if len(parts) < 3 or parts[1] not in SETTING_INFO:
        return await m.answer("Format: /set kalit qiymat\nKalitlar: " + ", ".join(SETTING_INFO))
    k, v = parts[1], parts[2].strip()
    try:
        if k == "remind_hours":
            hs = sorted({int(h) for h in v.split(",")})
            assert hs and all(0 <= h < 24 for h in hs)
            v = ",".join(map(str, hs))
        else:
            n = int(v)
            if k in ("deadline", "tomorrow_hour", "week_gen_hour", "award_hour"):
                assert 0 <= n < 24
            if k == "rate_hours":
                assert 1 <= n <= 72
            v = str(n)
    except Exception:
        return await m.answer("Qiymat noto'g'ri.")
    sset(k, v)
    await m.answer(f"✅ {k} = {v}")


# ==========================================================
#                     NAVBATCHI: ISH BAJARISH
# ==========================================================
def can_do(uid, tid):
    t = get_task(tid) if tid else None
    if not t or not t["active"]:
        return "Bu ish topilmadi.", None
    d = today()
    if duty(d) != uid:
        return "Bugun siz navbatchi emassiz.", None
    if now().hour >= sint("deadline"):
        return f"⏰ Bugungi muddat ({sint('deadline')}:00) tugagan.", None
    if tid in done_ids(d):
        return "Bu ish allaqachon yuborilgan.", None
    return None, t


@dp.callback_query(F.data.startswith("do:"))
async def cb_do(c: CallbackQuery, state: FSMContext):
    err, t = can_do(c.from_user.id, int(c.data.split(":")[1]))
    if err:
        return await c.answer(err, show_alert=True)
    await c.message.answer(f"🧹 {t['name']}\n\n{OATH_TEXT}", reply_markup=ikb([
        [("🤲 Rasm hozir olingan, qasam ichaman", f"oath:{t['id']}")],
        [("⬅️ Bekor qilish", "cancel")]]))
    await c.answer()


@dp.callback_query(F.data.startswith("oath:"))
async def cb_oath(c: CallbackQuery, state: FSMContext):
    err, t = can_do(c.from_user.id, int(c.data.split(":")[1]))
    if err:
        return await c.answer(err, show_alert=True)
    await state.set_state(DoTask.photo)
    await state.update_data(tid=t["id"])
    try:
        await c.message.edit_text(f"🧹 {t['name']}\n\n📸 Rasmni yuboring:")
    except Exception:
        await c.message.answer(f"📸 {t['name']} — rasmni yuboring:")
    await c.answer()


@dp.message(DoTask.photo, F.photo)
async def got_photo(m: Message, state: FSMContext):
    if m.forward_origin:
        return await m.answer("❌ Forward qilingan rasm qabul qilinmaydi. Rasmni o'zingiz yuboring.")
    uid = m.from_user.id
    tid = (await state.get_data()).get("tid")
    err, t = can_do(uid, tid)
    if err:
        await state.clear()
        return await m.answer(err)
    d = today()
    photo = m.photo[-1].file_id
    try:
        cur = run("INSERT INTO completions(day,task_id,user_id,photo_id,created_at) VALUES(?,?,?,?,?)",
                  (iso(d), tid, uid, photo, ts()))
    except sqlite3.IntegrityError:
        await state.clear()
        return await m.answer("Bu ish allaqachon yuborilgan.")
    cid = cur.lastrowid
    await state.clear()
    add_points(uid, sint("p_done"), f"Bajarildi: {t['name']}", ref=f"done:{cid}", day=d)

    rem = remaining(d)
    head = f"✅ «{t['name']}» qabul qilindi (+{sint('p_done')} ball). Boshqalar baholaydi."
    if rem:
        await m.answer(head + "\n\nQolgan ishlar:", reply_markup=tasks_kb(rem))
    else:
        await m.answer(head)
        if add_points(uid, sint("p_allday"), "Kunlik bonus", ref=f"allday:{iso(d)}", day=d):
            await m.answer(f"🎉 Rahmat! Bugungi barcha ishlar bajarildi. +{sint('p_allday')} bonus ball.\n\n"
                           f"{next_quote()}")

    for v in active_ids():
        if v == uid:
            continue
        try:
            await bot.send_photo(v, photo, caption=f"🧹 {nm(uid)} — {t['name']}\n📅 {d:%d.%m}\n\nIshni baholang:",
                                 reply_markup=vote_kb(cid))
        except Exception as e:
            log.warning("vote send %s: %s", v, e)
        await asyncio.sleep(0.05)


@dp.message(DoTask.photo)
async def need_photo(m: Message):
    await m.answer("📸 Rasm yuboring. Bekor qilish: /start")


# ==========================================================
#                     BAHOLASH
# ==========================================================
@dp.callback_query(F.data.startswith("v:"))
async def cb_vote(c: CallbackQuery):
    _, cid, val = c.data.split(":")
    cid, val = int(cid), (1 if val == "1" else -1)
    uid = c.from_user.id
    if not is_active(uid):
        return await c.answer("Siz ovoz bera olmaysiz.", show_alert=True)
    comp = get_comp(cid)
    if not comp:
        return await c.answer("Topilmadi")
    if comp["user_id"] == uid:
        return await c.answer("O'z ishingizni baholay olmaysiz.", show_alert=True)
    if comp["status"] != "rating":
        await safe_markup(c.message, None)
        return await c.answer("Baholash yakunlangan.")
    old = q1("SELECT cancelled FROM votes WHERE completion_id=? AND voter_id=?", (cid, uid))
    if old and old["cancelled"]:
        return await c.answer("Ovozingiz uy egasi tomonidan bekor qilingan.", show_alert=True)
    run("INSERT OR REPLACE INTO votes(completion_id,voter_id,value,pts,cancelled,created_at) "
        "VALUES(?,?,?,NULL,0,?)", (cid, uid, val, ts()))
    await c.answer("Qabul qilindi 👍" if val == 1 else "Qabul qilindi 👎")
    await safe_markup(c.message, vote_kb(cid, val))
    eligible = [u for u in active_ids() if u != comp["user_id"]]
    voted = {r[0] for r in q("SELECT voter_id FROM votes WHERE completion_id=?", (cid,))}
    if all(u in voted for u in eligible):
        await close_comp(cid)


# ==========================================================
#                     ADMIN: QARORLAR
# ==========================================================
@dp.callback_query(IsAdmin, F.data.startswith("fc:"))
async def cb_fc(c: CallbackQuery):
    cid = int(c.data.split(":")[1])
    comp = get_comp(cid)
    if not comp:
        return await c.answer("Topilmadi")
    ok = await decide_comp(cid, get_task(comp["task_id"])["fine"])
    await c.answer("Jarima yozildi" if ok else "Allaqachon hal qilingan")
    await safe_markup(c.message, result_kb(cid))


@dp.callback_query(IsAdmin, F.data.startswith("fo:"))
async def cb_fo(c: CallbackQuery):
    cid = int(c.data.split(":")[1])
    ok = await decide_comp(cid, None)
    await c.answer("Jarimasiz" if ok else "Allaqachon hal qilingan")
    if get_comp(cid):
        await safe_markup(c.message, result_kb(cid))


@dp.callback_query(IsAdmin, F.data.startswith("fa:"))
async def cb_fa(c: CallbackQuery, state: FSMContext):
    cid = int(c.data.split(":")[1])
    await state.set_state(Adm.fine_amt)
    await state.update_data(k="c", cid=cid, mid=c.message.message_id)
    await c.message.answer("Jarima summasini yozing (so'm):")
    await c.answer()


@dp.callback_query(IsAdmin, F.data.startswith("vc:"))
async def cb_vc(c: CallbackQuery):
    _, cid, voter = c.data.split(":")
    cid, voter = int(cid), int(voter)
    v = q1("SELECT * FROM votes WHERE completion_id=? AND voter_id=?", (cid, voter))
    if not v or v["cancelled"]:
        return await c.answer("Allaqachon bekor qilingan")
    run("UPDATE votes SET cancelled=1 WHERE completion_id=? AND voter_id=?", (cid, voter))
    comp = get_comp(cid)
    if v["pts"] is not None:
        add_points(comp["user_id"], -v["pts"], "Ovoz bekor qilindi", ref=f"vc:{cid}:{voter}", day=comp["day"])
    await c.answer(f"{nm(voter)} ovozi bekor qilindi")
    await safe_markup(c.message, result_kb(cid))


@dp.callback_query(IsAdmin, F.data.startswith(("mf:", "mo:", "ma:")))
async def cb_missed(c: CallbackQuery, state: FSMContext):
    act, day, tid = c.data.split(":")
    tid = int(tid)
    if act == "ma":
        await state.set_state(Adm.fine_amt)
        await state.update_data(k="m", day=day, tid=tid, mid=c.message.message_id)
        await c.message.answer("Jarima summasini yozing (so'm):")
        return await c.answer()
    amount = get_task(tid)["fine"] if act == "mf" else None
    ok = await decide_missed(day, tid, amount)
    await c.answer(("Jarima yozildi" if amount else "Kechirildi") if ok else "Allaqachon hal qilingan")
    await safe_markup(c.message, None)


@dp.message(Adm.fine_amt, IsAdmin)
async def st_fine_amt(m: Message, state: FSMContext):
    try:
        amount = to_int(m.text)
        assert amount > 0
    except Exception:
        return await m.answer("Faqat musbat son yozing, masalan 30000")
    d = await state.get_data()
    await state.clear()
    if d["k"] == "c":
        ok = await decide_comp(d["cid"], amount)
        k = result_kb(d["cid"])
    else:
        ok = await decide_missed(d["day"], d["tid"], amount)
        k = None
    try:
        await bot.edit_message_reply_markup(chat_id=ADMIN_ID, message_id=d["mid"], reply_markup=k)
    except Exception:
        pass
    await m.answer("✅ Jarima yozildi." if ok else "Allaqachon hal qilingan.", reply_markup=admin_kb())


# ==========================================================
#                     ADMIN: ISHLAR
# ==========================================================
@dp.callback_query(IsAdmin, F.data == "ta")
async def cb_ta(c: CallbackQuery, state: FSMContext):
    await state.set_state(Adm.add_task)
    await c.message.answer("Format: Nomi | jarima | tavsif (ixtiyoriy)\n"
                           "Masalan: Oshxona | 20000 | idish, plita, pol")
    await c.answer()


@dp.message(Adm.add_task, IsAdmin)
async def st_add_task(m: Message, state: FSMContext):
    parts = [p.strip() for p in (m.text or "").split("|")]
    try:
        name, fine = parts[0], to_int(parts[1])
        assert name and fine >= 0
    except Exception:
        return await m.answer("Format noto'g'ri. Namuna: Oshxona | 20000 | idish, plita, pol")
    descr = parts[2] if len(parts) > 2 else ""
    run("INSERT INTO tasks(name,descr,fine) VALUES(?,?,?)", (name, descr, fine))
    await state.clear()
    text, k = tasks_view()
    await m.answer("✅ Qo'shildi.\n\n" + text, reply_markup=k)


@dp.callback_query(IsAdmin, F.data.startswith("td:"))
async def cb_td(c: CallbackQuery):
    run("UPDATE tasks SET active=0 WHERE id=?", (int(c.data.split(":")[1]),))
    text, k = tasks_view()
    try:
        await c.message.edit_text(text, reply_markup=k)
    except Exception:
        pass
    await c.answer("O'chirildi")


@dp.callback_query(IsAdmin, F.data.startswith("tf:"))
async def cb_tf(c: CallbackQuery, state: FSMContext):
    tid = int(c.data.split(":")[1])
    await state.set_state(Adm.task_fine)
    await state.update_data(tid=tid)
    await c.message.answer(f"«{get_task(tid)['name']}» uchun yangi jarima summasi (so'm):")
    await c.answer()


@dp.message(Adm.task_fine, IsAdmin)
async def st_task_fine(m: Message, state: FSMContext):
    try:
        fine = to_int(m.text)
        assert fine >= 0
    except Exception:
        return await m.answer("Faqat son yozing.")
    tid = (await state.get_data())["tid"]
    run("UPDATE tasks SET fine=? WHERE id=?", (fine, tid))
    await state.clear()
    text, k = tasks_view()
    await m.answer("✅ Yangilandi.\n\n" + text, reply_markup=k)


# ==========================================================
#                     ADMIN: IJARACHILAR
# ==========================================================
@dp.callback_query(IsAdmin, F.data.startswith("ap:"))
async def cb_ap(c: CallbackQuery):
    uid = int(c.data.split(":")[1])
    r = get_user(uid)
    if not r or r["status"] == "active":
        return await c.answer("Allaqachon hal qilingan")
    run("UPDATE users SET status='active' WHERE user_id=?", (uid,))
    ensure_current()
    await send(uid, "✅ Uy egasi sizni tasdiqladi. Xush kelibsiz!", reply_markup=tenant_kb())
    await c.answer("Qabul qilindi")
    try:
        await c.message.edit_text(f"✅ {nm(uid)} qabul qilindi.\n"
                                  f"Navbatga qo'shish uchun: 📅 Navbat → 🔀 Qayta taqsimlash")
    except Exception:
        pass


@dp.callback_query(IsAdmin, F.data.startswith("rj:"))
async def cb_rj(c: CallbackQuery):
    uid = int(c.data.split(":")[1])
    run("UPDATE users SET status='rejected' WHERE user_id=? AND status='pending'", (uid,))
    await send(uid, "❌ So'rovingiz rad etildi.")
    await c.answer("Rad etildi")
    try:
        await c.message.edit_text(f"❌ {nm(uid)} rad etildi.")
    except Exception:
        pass


@dp.callback_query(IsAdmin, F.data.startswith("rm:"))
async def cb_rm(c: CallbackQuery):
    uid = int(c.data.split(":")[1])
    await c.message.answer(f"{nm(uid)} ro'yxatdan chiqarilsinmi?",
                           reply_markup=ikb([[("✅ Ha", f"rmy:{uid}"), ("⬅️ Yo'q", "cancel")]]))
    await c.answer()


@dp.callback_query(IsAdmin, F.data.startswith("rmy:"))
async def cb_rmy(c: CallbackQuery):
    uid = int(c.data.split(":")[1])
    run("UPDATE users SET status='removed' WHERE user_id=?", (uid,))
    d = today()
    start_ = d + dt.timedelta(days=1) if done_ids(d) else d
    old = duty(d)
    days = [dt.date.fromisoformat(r[0]) for r in
            q("SELECT day FROM schedule WHERE user_id=? AND day>=?", (uid, iso(start_)))]
    run("DELETE FROM schedule WHERE user_id=? AND day>=?", (uid, iso(start_)))
    assign(days)
    await notify_today_change(old)
    await send(uid, "Siz navbatchilik ro'yxatidan chiqarildingiz.")
    await c.answer("Chiqarildi")
    try:
        await c.message.edit_text(f"🚪 {nm(uid)} chiqarildi. Uning navbatlari qayta taqsimlandi.")
    except Exception:
        pass


# ==========================================================
#                     ADMIN: NAVBAT
# ==========================================================
@dp.callback_query(IsAdmin, F.data.startswith("sd:"))
async def cb_sd(c: CallbackQuery):
    day = c.data[3:]
    ids = active_ids()
    if not ids:
        return await c.answer("Ijarachi yo'q")
    d = dt.date.fromisoformat(day)
    await c.message.answer(f"{WD[d.weekday()]} {d:%d.%m} kuni kim navbatchi?",
                           reply_markup=ikb([[(nm(u), f"su:{day}:{u}")] for u in ids]))
    await c.answer()


@dp.callback_query(IsAdmin, F.data.startswith("su:"))
async def cb_su(c: CallbackQuery):
    _, day, uid = c.data.split(":")
    uid = int(uid)
    d = dt.date.fromisoformat(day)
    old_day = duty(d)
    old_today = duty(today())
    run("INSERT OR REPLACE INTO schedule(day,user_id) VALUES(?,?)", (day, uid))
    label = f"{WD[d.weekday()]} {d:%d.%m}"
    if old_day and old_day != uid:
        await send(old_day, f"📅 {label} kungi navbatingiz {nm(uid)} ga o'tkazildi.")
    if d == today():
        await notify_today_change(old_today)
    elif old_day != uid:
        await send(uid, f"📅 Siz {label} kuni navbatchi etib belgilandingiz.")
    await c.answer("Saqlandi")
    try:
        await c.message.edit_text(f"✅ {label} — {nm(uid)}")
    except Exception:
        pass


@dp.callback_query(IsAdmin, F.data == "rg")
async def cb_rg(c: CallbackQuery):
    d = today()
    start_ = d + dt.timedelta(days=1) if done_ids(d) else d
    days = [x for x in week_days(monday_of(d)) if x >= start_]
    if not days:
        return await c.answer("Bu haftada qayta taqsimlanadigan kun qolmadi", show_alert=True)
    if not active_ids():
        return await c.answer("Ijarachi yo'q", show_alert=True)
    old = duty(d)
    assign(days)
    await notify_today_change(old)
    await broadcast("📅 Navbat jadvali yangilandi:\n\n" + schedule_text(monday_of(d)))
    text, k = sched_view()
    try:
        await c.message.edit_text(text, reply_markup=k)
    except Exception:
        pass
    await c.answer("Qayta taqsimlandi")


# ==========================================================
#                     ADMIN: QARZ, BALL, GAPLAR, CHEGIRMA
# ==========================================================
@dp.callback_query(IsAdmin, F.data.startswith("pd:"))
async def cb_pd(c: CallbackQuery):
    uid = int(c.data.split(":")[1])
    total = q1("SELECT COALESCE(SUM(amount),0) FROM fines WHERE user_id=? AND paid=0", (uid,))[0]
    run("UPDATE fines SET paid=1 WHERE user_id=? AND paid=0", (uid,))
    if total:
        await send(uid, f"✅ {fmt(total)} so'm jarima to'langan deb belgilandi.")
    text, k = debts_view()
    try:
        await c.message.edit_text(text, reply_markup=k)
    except Exception:
        pass
    await c.answer("Belgilandi")


@dp.callback_query(IsAdmin, F.data.startswith("pt:"))
async def cb_pt(c: CallbackQuery, state: FSMContext):
    uid = int(c.data.split(":")[1])
    await state.set_state(Adm.pts)
    await state.update_data(uid=uid)
    await c.message.answer(f"{nm(uid)}: ball va sababni yozing.\n"
                           f"Masalan: 20 Hovlini tozaladi\nAyirish: -10 Idishni qoldirgan")
    await c.answer()


@dp.message(Adm.pts, IsAdmin)
async def st_pts(m: Message, state: FSMContext):
    parts = (m.text or "").split(maxsplit=1)
    try:
        n = int(parts[0])
        assert n != 0
    except Exception:
        return await m.answer("Format: 20 sabab  yoki  -10 sabab")
    reason = parts[1] if len(parts) > 1 else "Uy egasi qarori"
    uid = (await state.get_data())["uid"]
    await state.clear()
    add_points(uid, n, f"Uy egasi: {reason}", day=today())
    await send(uid, f"{'➕' if n > 0 else '➖'} {abs(n)} ball: {reason}")
    await m.answer(f"✅ {nm(uid)}: {n:+d} ball", reply_markup=admin_kb())


@dp.callback_query(IsAdmin, F.data == "qa")
async def cb_qa(c: CallbackQuery, state: FSMContext):
    await state.set_state(Adm.add_quote)
    await c.message.answer("Yangi gapni yozing:")
    await c.answer()


@dp.message(Adm.add_quote, IsAdmin)
async def st_add_quote(m: Message, state: FSMContext):
    if not m.text:
        return await m.answer("Matn yozing.")
    run("INSERT INTO quotes(text) VALUES(?)", (m.text.strip(),))
    await state.clear()
    text, k = quotes_view()
    await m.answer("✅ Qo'shildi.\n\n" + text, reply_markup=k)


@dp.callback_query(IsAdmin, F.data.startswith("qd:"))
async def cb_qd(c: CallbackQuery):
    run("DELETE FROM quotes WHERE id=?", (int(c.data.split(":")[1]),))
    text, k = quotes_view()
    try:
        await c.message.edit_text(text, reply_markup=k)
    except Exception:
        pass
    await c.answer("O'chirildi")


@dp.callback_query(IsAdmin, F.data.startswith(("aw:", "an:")))
async def cb_award(c: CallbackQuery):
    act, month = c.data.split(":")
    a = q1("SELECT * FROM awards WHERE month=?", (month,))
    if not a or a["status"] != "pending":
        return await c.answer("Allaqachon hal qilingan")
    if act == "aw":
        run("UPDATE awards SET status='confirmed' WHERE month=?", (month,))
        await broadcast(f"🏆 {month_name(month)} oyining eng toza ijarachisi: {nm(a['user_id'])} — "
                        f"{a['points']} ball!\nUnga ijaradan {fmt(a['amount'])} so'm chegirma.\n\n"
                        f"Yangi oyda ballar 0 dan boshlandi. Omad!")
        await c.answer("Tasdiqlandi")
    else:
        run("UPDATE awards SET status='cancelled' WHERE month=?", (month,))
        await c.answer("Bekor qilindi")
    await safe_markup(c.message, None)


# ==========================================================
#                     VOSITA VA MUROJAAT
# ==========================================================
@dp.message(Supply.waiting)
async def st_supply(m: Message, state: FSMContext):
    await state.clear()
    await send(ADMIN_ID, f"🧴 {nm(m.from_user.id)}: tugadi 👇")
    try:
        await bot.copy_message(ADMIN_ID, m.chat.id, m.message_id)
    except Exception as e:
        log.warning("supply copy: %s", e)
    await m.answer("✅ Uy egasiga yetkazildi.", reply_markup=tenant_kb())


@dp.message(Support.waiting)
async def st_support(m: Message, state: FSMContext):
    await state.clear()
    uid = m.from_user.id
    sid = run("INSERT INTO support(user_id,created_at) VALUES(?,?)", (uid, ts())).lastrowid
    await send(ADMIN_ID, f"✍️ Murojaat #{sid}\n👤 {nm(uid)} (id {uid}) 👇",
               reply_markup=ikb([[("↩️ Javob berish", f"rp:{sid}")]]))
    try:
        await bot.copy_message(ADMIN_ID, m.chat.id, m.message_id)
    except Exception as e:
        log.warning("support copy: %s", e)
    await m.answer("✅ Murojaatingiz uy egasiga yuborildi.", reply_markup=tenant_kb())


@dp.callback_query(IsAdmin, F.data.startswith("rp:"))
async def cb_rp(c: CallbackQuery, state: FSMContext):
    sid = int(c.data.split(":")[1])
    await state.set_state(Adm.reply)
    await state.update_data(sid=sid)
    await c.message.answer(f"#{sid} murojaatga javobingizni yozing:")
    await c.answer()


@dp.message(Adm.reply, IsAdmin)
async def st_reply(m: Message, state: FSMContext):
    sid = (await state.get_data())["sid"]
    await state.clear()
    r = q1("SELECT user_id FROM support WHERE id=?", (sid,))
    if not r:
        return await m.answer("Murojaat topilmadi.", reply_markup=admin_kb())
    try:
        await bot.send_message(r[0], "📩 Uy egasi javobi:")
        await bot.copy_message(r[0], m.chat.id, m.message_id)
        await m.answer("✅ Javob yuborildi.", reply_markup=admin_kb())
    except Exception as e:
        await m.answer(f"Yuborilmadi: {e}", reply_markup=admin_kb())


# ==========================================================
#                     QOLGAN XABARLAR
# ==========================================================
@dp.message()
async def fallback(m: Message):
    uid = m.from_user.id
    if uid == ADMIN_ID:
        return await m.answer("Menyudan tanlang.", reply_markup=admin_kb())
    if is_active(uid):
        return await m.answer("Menyudan tanlang.", reply_markup=tenant_kb())
    await m.answer("Boshlash uchun /start bosing.")


# ==========================================================
#                     ISHGA TUSHIRISH
# ==========================================================
async def on_startup(bot: Bot):
    ensure_current()
    d = today()
    if d.weekday() == 6 and now().hour >= sint("week_gen_hour"):
        nxt = monday_of(d) + dt.timedelta(days=7)
        if any(duty(x) is None for x in week_days(nxt)):
            await gen_next_week()
    scheduler.add_job(tick, CronTrigger(minute=0, timezone=TZ), id="tick",
                      replace_existing=True, misfire_grace_time=600, coalesce=True)
    if not scheduler.running:
        scheduler.start()
    if BASE_URL:
        url = BASE_URL.rstrip("/") + WEBHOOK_PATH
        await bot.set_webhook(url, drop_pending_updates=True,
                              allowed_updates=dp.resolve_used_update_types())
        log.info("Webhook: %s", url)
    log.info("Bot ishga tushdi.")


async def health(request):
    return web.Response(text="ok")


async def polling():
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


def main():
    init_db()
    dp.startup.register(on_startup)
    if BASE_URL:
        app = web.Application()
        app.router.add_get("/", health)
        app.router.add_get("/health", health)
        SimpleRequestHandler(dispatcher=dp, bot=bot).register(app, path=WEBHOOK_PATH)
        setup_application(app, dp, bot=bot)
        web.run_app(app, host="0.0.0.0", port=PORT)
    else:
        asyncio.run(polling())


if __name__ == "__main__":
    main()
