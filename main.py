"""
Eslatma + Murojaat boti — BITTA FAYL (Telegram Widgets, Bot API 10.4).

ISHGA TUSHIRISH:
  1) pip install aiogram APScheduler tzdata
  2) Environment: BOT_TOKEN, ADMIN_ID, (ixtiyoriy) TZ_NAME=Asia/Tashkent
  3) python main.py

Widgetlar xabar ichida ishlaydi (Mini App emas). Eski Telegram'da ko'rinmasa — /oddiy.
"""

import os
import json
import asyncio
import sqlite3
import datetime as dt
from zoneinfo import ZoneInfo

import aiohttp
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
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.cron import CronTrigger

from aiohttp import web
from aiogram.webhook.aiohttp_server import setup_application

# ==========================================================
#                     SOZLAMALAR
# ==========================================================
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
TZ = ZoneInfo(os.getenv("TZ_NAME", "Asia/Tashkent"))
API = f"https://api.telegram.org/bot{BOT_TOKEN}"

DB_PATH = "bot.db"
INTERVAL_OPTIONS = [20, 30, 40, 50, 60, 90, 120, 180]   # daqiqa
DEFAULT_QUIET_START = 23
DEFAULT_QUIET_END = 7
SNOOZE_MIN = 10
WEEKDAYS = ["Dushanba", "Seshanba", "Chorshanba", "Payshanba", "Juma", "Shanba", "Yakshanba"]


def now():
    return dt.datetime.now(TZ).replace(tzinfo=None)


def jd(o):
    return json.dumps(o, ensure_ascii=False, separators=(",", ":"))


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

        CREATE TABLE IF NOT EXISTS drafts (
            user_id INTEGER, msg_id INTEGER, text TEXT, voice_id TEXT, rid INTEGER,
            PRIMARY KEY (user_id, msg_id));
    """)
    for table, col, typ in [("users", "classic", "INTEGER DEFAULT 0"),
                            ("support", "user_msg_id", "INTEGER")]:
        try:
            conn().execute(f"ALTER TABLE {table} ADD COLUMN {col} {typ}")
        except sqlite3.OperationalError:
            pass
    conn().commit()


def now_iso():
    return now().isoformat(timespec="seconds")


def upsert_user(uid, username, name):
    c = conn()
    if c.execute("SELECT 1 FROM users WHERE user_id=?", (uid,)).fetchone() is None:
        c.execute("INSERT INTO users(user_id,username,full_name,joined_at,quiet_start,quiet_end,classic)"
                  " VALUES(?,?,?,?,?,?,0)",
                  (uid, username, name, now_iso(), DEFAULT_QUIET_START, DEFAULT_QUIET_END))
    else:
        c.execute("UPDATE users SET username=?, full_name=? WHERE user_id=?", (username, name, uid))
    c.commit()


def get_user(uid):
    return conn().execute("SELECT * FROM users WHERE user_id=?", (uid,)).fetchone()


def set_quiet(uid, s, e):
    conn().execute("UPDATE users SET quiet_start=?, quiet_end=? WHERE user_id=?", (s, e, uid))
    conn().commit()


def set_classic(uid, v):
    conn().execute("UPDATE users SET classic=? WHERE user_id=?", (1 if v else 0, uid))
    conn().commit()


def wmode(uid):
    u = get_user(uid)
    return not (u and u["classic"])


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


def add_draft(uid, msg_id, text, voice_id):
    conn().execute("INSERT OR REPLACE INTO drafts VALUES(?,?,?,?,NULL)", (uid, msg_id, text, voice_id))
    conn().commit()


def get_draft(uid, msg_id):
    return conn().execute("SELECT * FROM drafts WHERE user_id=? AND msg_id=?", (uid, msg_id)).fetchone()


def set_draft_rid(uid, msg_id, rid):
    conn().execute("UPDATE drafts SET rid=? WHERE user_id=? AND msg_id=?", (rid, uid, msg_id))
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


def set_support_status(sid, st):
    conn().execute("UPDATE support SET status=? WHERE id=?", (st, sid))
    conn().commit()


def set_support_msg(sid, mid):
    conn().execute("UPDATE support SET user_msg_id=? WHERE id=?", (mid, sid))
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
#                     WIDGETLAR (HTML)
# ==========================================================
# Har biri: ≤32 KB HTML, ≤4 KB siqilgan. Holat (chat_data) ≤1024 bayt — uni faqat bot yozadi.
CSS = """<style>
body{margin:0;padding:10px 12px 12px;font:15px/1.35 system-ui,sans-serif}
.r{display:flex;gap:6px;margin:6px 0;align-items:center}
.w{flex-wrap:wrap}.w button{flex:0 0 calc(25% - 5px)}
button{flex:1;min-width:0;padding:8px 4px;border:0;border-radius:10px;font:inherit;font-size:14px;
background:var(--tg-theme-secondary-bg-color);color:var(--tg-theme-text-color)}
button.on{background:var(--tg-theme-button-color);color:var(--tg-theme-button-text-color)}
.s{flex:0 0 38px}.x{color:var(--tg-theme-destructive-text-color)}
.h{color:var(--tg-theme-hint-color);font-size:13px}
.n{flex:1;font-size:24px;font-weight:600;font-variant-numeric:tabular-nums;text-align:center}
.t{font-weight:600;margin:2px 0 4px}
.b{height:5px;border-radius:3px;background:var(--tg-theme-secondary-bg-color);overflow:hidden;margin:4px 0}
.b i{display:block;height:100%;background:var(--tg-theme-button-color)}
.e{white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.l{padding:6px 0;border-top:1px solid var(--tg-theme-section-separator-color)}
</style>"""

JS0 = r"""var W=TelegramWidget,M='';
function j(s){try{return JSON.parse(s)}catch(e){return null}}
function E(s){return String(s==null?'':s).replace(/[&<>"']/g,function(c){return'&#'+c.charCodeAt(0)+';'})}
function z(n){return(n<10?'0':'')+n}
function B(t,a,on,c){return'<button class="'+(on?'on ':'')+(c||'')+'" onclick="'+a+'">'+t+'</button>'}
function bar(c,m){return'<div class="b"><i style="width:'+Math.min(100,c*100/m)+'%"></i></div>'}
function pg(p){return V.n>1?'<div class="r">'+B('‹',V.p>0?"q('"+p+V.t+":"+(V.p-1)+"')":'')+'<div class="h" style="flex:1;text-align:center">'+(V.p+1)+'/'+V.n+'</div>'+B('›',V.p<V.n-1?"q('"+p+V.t+":"+(V.p+1)+"')":'')+'</div>':''}
function Q(d,ok,s){if(!s){M='⏳';R()}try{W.request(d).then(function(a){var v=j(a.result)||{};if(v.e)M='⚠️ '+v.e;else{ok(v);M=v.m||''}R()},function(e){if(!s){M='⚠️ '+e.code;R()}})}catch(e){}}
"""


def _w(body_js):
    return CSS + "<script>" + JS0 + body_js + "\nR();</script>"


# 1) Eslatma sozlash
W_BUILDER = _w(r"""
var D=j(W.chatData)||{},S=j(localStorage.getItem('s'))||{k:'i',v:30,c:0,h:9,m:0,d:0,w:0};
var K={i:'Interval',o:'Bir marta',d:'Har kuni',w:'Har hafta'},IV=[20,30,40,50,60,90,120,180],WD=['Du','Se','Ch','Pa','Ju','Sh','Ya'];
function P(){try{localStorage.setItem('s',JSON.stringify(S))}catch(e){}}
function set(k,v){S[k]=v;M='';P();R()}
function add(k,d,lo,hi,wr){var v=S[k]+d;if(v<lo)v=wr?hi:lo;if(v>hi)v=wr?lo:hi;S[k]=v;M='';P();R()}
function dl(){var t=(D.t||'2026-01-01').split('-'),x=new Date(Date.UTC(+t[0],t[1]-1,+t[2]+S.d));
return z(x.getUTCDate())+'.'+z(x.getUTCMonth()+1)+' '+WD[(x.getUTCDay()+6)%7]+(S.d==0?' · bugun':S.d==1?' · ertaga':'')}
function tm(){return'<div class="r">'+B('−',"add('h',-1,0,23,1)",0,'s')+'<div class="n">'+z(S.h)+'</div>'+B('+',"add('h',1,0,23,1)",0,'s')+'<b>:</b>'+B('−',"add('m',-5,0,55,1)",0,'s')+'<div class="n">'+z(S.m)+'</div>'+B('+',"add('m',5,0,55,1)",0,'s')+'</div>'}
function sv(){var t=z(S.h)+z(S.m),p=S.k=='i'?'B:i:'+S.v+':'+S.c:S.k=='o'?'B:o:'+S.d+':'+t:S.k=='d'?'B:d:'+t:'B:w:'+S.w+':'+t;
Q(p,function(v){D=v})}
function R(){var h;
if(D.ok){h='<div class="t">✅ Eslatma #'+D.id+' saqlandi</div><div class="e">'+E(D.x)+'</div><div class="h">'+E(D.w)+'</div>'}
else{h='<div class="h e">'+E(D.x)+'</div><div class="r w">';for(var k in K)h+=B(K[k],"set('k','"+k+"')",S.k==k);h+='</div>';
if(S.k=='i'){h+='<div class="h">Har necha vaqtda:</div><div class="r w">';IV.forEach(function(v){h+=B(v<60?v+' daq':v/60+' soat',"set('v',"+v+")",S.v==v)});
h+='</div><div class="h">Necha marta:</div><div class="r">';[0,5,10,20].forEach(function(c){h+=B(c?c+' ta':'∞',"set('c',"+c+")",S.c==c)});h+='</div>'}
else{if(S.k=='o')h+='<div class="r">'+B('−',"add('d',-1,0,365)",0,'s')+'<div class="n" style="font-size:17px">'+dl()+'</div>'+B('+',"add('d',1,0,365)",0,'s')+'</div>';
if(S.k=='w'){h+='<div class="r">';WD.forEach(function(w,i){h+=B(w,"set('w',"+i+")",S.w==i)});h+='</div>'}
h+=tm()}
h+='<div class="r">'+B(M?E(M):'💾 Saqlash','sv()',1)+'</div>'}
document.body.innerHTML=h}
W.onEvent('chatDataChanged',function(){D=j(W.chatData)||D;R()});""")

# 2) Eslatmalarim doskasi
W_BOARD = _w(r"""
var V=j(W.chatData)||{t:'a',p:0,n:1,i:[],s:[0,0,0,0]},C=0,T={a:'Faol',p:'Pauzada',s:'Natija'};
function q(d,s){C=0;Q(d,function(v){V=v},s)}
function R(){var h='<div class="r">';for(var k in T)h+=B(T[k]+(k=='s'?'':' '+V.s[k=='a'?1:2]),"q('L:t:"+k+":0')",V.t==k);h+='</div>';
if(V.t=='s'){[['Jami eslatma',V.s[0]],['Faol',V.s[1]],['Pauzada',V.s[2]],['Yuborilgan eslatmalar',V.s[3]]].forEach(function(x){h+='<div class="r l"><span style="flex:1">'+x[0]+'</span><b>'+x[1]+'</b></div>'})}
else{if(!V.i.length)h+='<div class="h" style="padding:14px 0;text-align:center">Bo‘sh</div>';
V.i.forEach(function(it){var id=it[0],a=it[3],g=':'+V.t+':'+V.p,c=C==id;
h+='<div class="l"><div class="r"><div style="flex:1;min-width:0"><div class="e">'+E(it[1])+'</div><div class="h e">'+E(it[2])+'</div></div>'
+B(a?'⏸':'▶️',"q('L:"+(a?'p':'r')+":"+id+g+"')",0,'s')
+B(c?'Ha?':'🗑',c?"q('L:d:"+id+g+"')":'C='+id+';R()',0,'s'+(c?' x':''))+'</div>'+(it[5]?bar(it[4],it[5]):'')+'</div>'});
h+=pg('L:t:')}
if(M)h+='<div class="h">'+E(M)+'</div>';document.body.innerHTML=h}
W.onEvent('chatDataChanged',function(){V=j(W.chatData)||V;R()});
q('L:t:'+V.t+':'+V.p,1);""")

# 3) Kelgan eslatma kartasi
W_CARD = _w(r"""
var D=j(W.chatData)||{},ST={d:'✅ Bajarildi',z:'⏰ 10 daqiqadan keyin yana eslataman',p:'⏸ To‘xtatildi'};
function q(a){Q('C:'+a+':'+D.r,function(v){D=v})}
function R(){var h='<div class="t">🔔 Eslatma</div><div style="white-space:pre-wrap;margin-bottom:6px">'+E(D.x)+'</div>';
if(D.m)h+='<div class="h">'+D.s+' / '+D.m+'</div>'+bar(D.s,D.m);
if(D.st)h+='<div class="h" style="padding:6px 0">'+ST[D.st]+'</div>';
else h+='<div class="r">'+B('✅ Bajarildi',"q('d')",1)+B('⏰ +10 daq',"q('z')")+(D.o?'':B('⏸',"q('p')",0,'s'))+'</div>';
if(M)h+='<div class="h">'+E(M)+'</div>';document.body.innerHTML=h}
W.onEvent('chatDataChanged',function(){D=j(W.chatData)||D;R()});""")

# 4) Tinch soatlar
W_QUIET = _w(r"""
var D=j(W.chatData)||{a:23,b:7,on:1};
function ad(k,d){D[k]=(D[k]+d+24)%24;M='';R()}
function q(p){Q(p,function(v){D=v;v.m=v.m||'✅ Saqlandi'})}
function row(t,k){return'<div class="r"><span style="flex:1">'+t+'</span>'+B('−',"ad('"+k+"',-1)",0,'s')+'<div class="n">'+z(D[k])+':00</div>'+B('+',"ad('"+k+"',1)",0,'s')+'</div>'}
function R(){document.body.innerHTML='<div class="t">🌙 Tinch soatlar</div><div class="h">'+(D.on?'Shu oraliqda eslatma yubormayman':'Hozir o‘chirilgan')+'</div>'
+row('Boshlanish','a')+row('Tugash','b')+'<div class="r">'+B(M?E(M):'💾 Saqlash',"q('Q:'+D.a+':'+D.b)",1)+B('O‘chirish',"q('Q:off')")+'</div>'}
W.onEvent('chatDataChanged',function(){D=j(W.chatData)||D;R()});""")

# 5) Murojaat holati (so'rovsiz, faqat bot yangilaydi)
W_SUPPORT = _w(r"""
var D=j(W.chatData)||{n:0,s:0},N=['Qabul qilindi','Ko‘rilmoqda','Javob berildi'];
function R(){var h='<div class="t">📨 Murojaat #'+D.n+'</div><div class="r">';
N.forEach(function(x,i){h+='<div style="flex:1;height:5px;border-radius:3px;background:var(--tg-theme-'+(i<=D.s?'button':'secondary-bg')+'-color)"></div>'});
h+='</div><div class="r h">';N.forEach(function(x,i){h+='<span style="flex:1'+(i==D.s?';color:var(--tg-theme-text-color);font-weight:600':'')+'">'+x+'</span>'});
document.body.innerHTML=h+'</div>'+(D.s==2?'<div class="h">Javob quyidagi xabarda 👇</div>':'')}
W.onEvent('chatDataChanged',function(){D=j(W.chatData)||D;R()});""")

# 6) Admin panel
W_ADMIN = _w(r"""
var V=j(W.chatData)||{t:'s',p:0,n:1,i:[],s:[0,0,0,0,0]},SI=['🆕','👁','✅'];
function q(d,s){Q(d,function(v){V=v},s)}
function R(){var s=V.s,h='<div class="r">'+B('Statistika',"q('A:t:s:0')",V.t=='s')+B('Murojaat '+s[4],"q('A:t:m:0')",V.t=='m')+B('Odamlar',"q('A:t:u:0')",V.t=='u')+'</div>';
if(V.t=='s')[['👥 Foydalanuvchilar',s[0]],['⏰ Eslatmalar',s[1]+' (faol '+s[2]+')'],['📨 Murojaatlar',s[3]+' (yangi '+s[4]+')']].forEach(function(x){h+='<div class="r l"><span style="flex:1">'+x[0]+'</span><b>'+x[1]+'</b></div>'});
else{if(!V.i.length)h+='<div class="h" style="padding:14px 0;text-align:center">Bo‘sh</div>';
V.i.forEach(function(it){h+='<div class="r l"><div style="flex:1;min-width:0"><div class="e">'+(V.t=='m'?SI[it[3]]+' #'+it[0]+' ':'')+E(it[1])+'</div><div class="h e">'+E(it[2])+'</div></div>'
+(V.t=='m'?B('↩️',"q('A:r:"+it[0]+":m:"+V.p+"')",0,'s'):B('📋',"W.copyText('"+it[0]+"')",0,'s'))+'</div>'});
h+=pg('A:t:')}
if(M)h+='<div class="h">'+E(M)+'</div>';document.body.innerHTML=h}
W.onEvent('chatDataChanged',function(){V=j(W.chatData)||V;R()});
q('A:t:'+V.t+':'+V.p,1);""")


# ==========================================================
#                     BOT API (to'g'ridan-to'g'ri)
# ==========================================================
HTTP: aiohttp.ClientSession | None = None


async def tg(method, **p):
    p = {k: v for k, v in p.items() if v is not None}
    async with HTTP.post(f"{API}/{method}", json=p) as r:
        res = await r.json()
    if not res.get("ok"):
        d = res.get("description", "")
        if "not modified" in d:
            return None
        raise RuntimeError(f"{method}: {d}")
    return res["result"]


def ikb(rows):
    return {"inline_keyboard": [[{"text": t, "callback_data": d} for t, d in row] for row in rows]}


async def send_widget(uid, html, caption, data=None, queries=True, kb=None):
    """Muvaffaqiyatli bo'lsa message dict, bo'lmasa None (shunda eski usulga o'tamiz)."""
    try:
        return await tg("sendWidget", chat_id=uid, html=html, caption=caption[:1024],
                        chat_data=jd(data) if data is not None else None,
                        allow_queries=queries, reply_markup=kb)
    except Exception as ex:
        print("sendWidget error:", ex)
        return None


async def edit_widget(chat_id, msg_id, data):
    try:
        await tg("editMessageWidgetData", chat_id=chat_id, message_id=msg_id, chat_data=jd(data))
    except Exception as ex:
        print("editMessageWidgetData error:", ex)


def fit(v, limit=1000):
    """chat_data 1024 baytdan oshmasin: avval matnlarni qisqartiradi, keyin elementlarni kamaytiradi."""
    while len(jd(v).encode()) > limit:
        cut = False
        for it in v.get("i", []):
            for k in (1, 2):
                if isinstance(it[k], str) and len(it[k]) > 8:
                    it[k] = it[k][:len(it[k]) - 4]
                    cut = True
        if not cut:
            if v.get("i"):
                v["i"].pop()
            else:
                break
    return v


def short(s, n):
    s = (s or "").replace("\n", " ").strip()
    return s if len(s) <= n else s[:n - 1] + "…"


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


def fired_kb(rid):
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text=f"⏰ +{SNOOZE_MIN} daq", callback_data=f"snz:{rid}"),
        InlineKeyboardButton(text="⏸ To'xtatish", callback_data=f"pause:{rid}")]])


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


CLASSIC_KB = ikb([[("⌨️ Ko'rinmayaptimi? Oddiy rejim", "classic")]])


# ==========================================================
#                     PLANLASHTIRUVCHI
# ==========================================================
scheduler = AsyncIOScheduler(timezone=TZ)


def in_quiet_hours(uid):
    u = get_user(uid)
    if not u or u["quiet_start"] is None or u["quiet_end"] is None:
        return False
    s, e = u["quiet_start"], u["quiet_end"]
    if s == e:
        return False
    h = now().hour
    return (s <= h < e) if s < e else (h >= s or h < e)


def card_data(r, sent, st=""):
    d = {"r": r["id"], "x": r["text"] or "", "s": sent, "m": r["max_count"] or 0,
         "o": 1 if r["kind"] == "once" else 0, "st": st}
    while len(jd(d).encode()) > 1000:
        d["x"] = d["x"][:len(d["x"]) - 20] + "…"
    return d


async def fire(rid, snooze=False):
    r = get_reminder(rid)
    if r is None:
        remove_job(rid); return
    if not snooze and not r["active"]:
        remove_job(rid); return
    if in_quiet_hours(r["user_id"]):
        return
    uid = r["user_id"]
    sent = r["sent_count"] + (0 if snooze else 1)
    try:
        if r["voice_id"]:
            await bot.send_voice(uid, r["voice_id"], reply_markup=fired_kb(rid),
                                 caption="🔔 Eslatma" + (f": {r['text']}" if r["text"] else ""))
        else:
            ok = None
            if wmode(uid):
                ok = await send_widget(uid, W_CARD, f"🔔 {r['text']}", card_data(r, sent),
                                       kb=CLASSIC_KB if sent == 1 else None)
            if not ok:
                await bot.send_message(uid, f"🔔 Eslatma: {r['text']}", reply_markup=fired_kb(rid))
    except Exception as ex:
        print("send error:", ex); return
    if snooze:
        return

    inc_sent(rid)
    r = get_reminder(rid)
    if r["max_count"] and r["sent_count"] >= r["max_count"]:
        set_active(rid, False); remove_job(rid)
        try:
            await bot.send_message(uid, f"✅ Eslatma tugadi ({r['max_count']} marta).")
        except Exception:
            pass
    if r["kind"] == "once":
        set_active(rid, False); remove_job(rid)


def snooze(rid):
    scheduler.add_job(fire, DateTrigger(run_date=now() + dt.timedelta(minutes=SNOOZE_MIN), timezone=TZ),
                      args=[rid, True], id=f"snz_{rid}", replace_existing=True)


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
        trig = IntervalTrigger(minutes=r["interval_min"], timezone=TZ)
    elif k == "once":
        run_at = dt.datetime.fromisoformat(r["run_at"])
        if run_at <= now():
            return
        trig = DateTrigger(run_date=run_at, timezone=TZ)
    elif k == "daily":
        trig = CronTrigger(hour=r["hour"], minute=r["minute"], timezone=TZ)
    elif k == "weekly":
        trig = CronTrigger(day_of_week=r["weekday"], hour=r["hour"], minute=r["minute"], timezone=TZ)
    else:
        return
    scheduler.add_job(fire, trig, args=[rid], id=job_id(rid), replace_existing=True)


# ==========================================================
#                     KO'RINISHLAR (widget holati)
# ==========================================================
def when_text(r):
    if r["kind"] == "interval":
        w = f"har {r['interval_min']} daq"
        if r["max_count"]:
            w += f" ({r['sent_count']}/{r['max_count']})"
        return w
    if r["kind"] == "once":
        return "bir marta: " + r["run_at"].replace("T", " ")[:16]
    if r["kind"] == "daily":
        return f"har kuni {r['hour']:02d}:{r['minute']:02d}"
    return f"{WEEKDAYS[r['weekday']]} {r['hour']:02d}:{r['minute']:02d}"


def label(r):
    return ("🎤 " + (r["text"] or "ovoz")) if r["voice_id"] else (r["text"] or "")


def paginate(lst, page, per):
    n = max(1, -(-len(lst) // per))
    page = max(0, min(page, n - 1))
    return lst[page * per:(page + 1) * per], page, n


def board_view(uid, tab="a", page=0):
    rems = user_reminders(uid)
    act = [r for r in rems if r["active"]]
    pau = [r for r in rems if not r["active"]]
    lst = act if tab == "a" else pau if tab == "p" else []
    chunk, page, n = paginate(lst, page, 6)
    items = [[r["id"], short(label(r), 28), when_text(r), r["active"], r["sent_count"], r["max_count"] or 0]
             for r in chunk]
    return fit({"t": tab, "p": page, "n": n, "i": items,
                "s": [len(rems), len(act), len(pau), sum(r["sent_count"] for r in rems)]})


def admin_view(tab="s", page=0):
    s = stats()
    items, n = [], 1
    if tab == "m":
        chunk, page, n = paginate(support_list(limit=1000), page, 5)
        st = {"new": 0, "seen": 1, "answered": 2}
        items = [[x["id"], short(x["full_name"], 22),
                  short(x["content"] or f"[{x['media_type']}]", 40), st.get(x["status"], 0)] for x in chunk]
    elif tab == "u":
        chunk, page, n = paginate(all_users(), page, 7)
        items = [[u["user_id"], short(u["full_name"], 24),
                  f"@{u['username'] or '—'} · {u['user_id']}"] for u in chunk]
    else:
        page = 0
    return fit({"t": tab, "p": page, "n": n, "i": items,
                "s": [s["users"], s["reminders"], s["reminders_active"], s["support"], s["support_new"]]})


def quiet_data(uid):
    u = get_user(uid)
    on = u is not None and u["quiet_start"] is not None
    return {"a": u["quiet_start"] if on else DEFAULT_QUIET_START,
            "b": u["quiet_end"] if on else DEFAULT_QUIET_END, "on": 1 if on else 0}


async def support_widget_status(sid, st):
    s = get_support(sid)
    if s and s["user_msg_id"]:
        await edit_widget(s["user_id"], s["user_msg_id"], {"n": sid, "s": st})


# ==========================================================
#                     WIDGET SO'ROVLARI (widget_query)
# ==========================================================
async def wq_builder(uid, mid, p):
    d = get_draft(uid, mid)
    if d is None:
        return {"e": "Qoralama topilmadi, qaytadan boshlang"}
    base = {"t": now().date().isoformat(), "x": short(d["text"] or "🎤 ovozli xabar", 60)}
    if d["rid"]:
        r = get_reminder(d["rid"])
        return {**base, "ok": 1, "id": d["rid"], "w": when_text(r) if r else ""}
    k = p[0]
    rec = dict(user_id=uid, text=d["text"], voice_id=d["voice_id"], max_count=0)
    if k == "i":
        iv, cnt = int(p[1]), int(p[2])
        if iv not in INTERVAL_OPTIONS or cnt not in (0, 5, 10, 20):
            return {"e": "Noto‘g‘ri qiymat"}
        rec.update(kind="interval", interval_min=iv, max_count=cnt)
    else:
        hm = p[-1]
        h, m = int(hm[:2]), int(hm[2:])
        if not (0 <= h < 24 and 0 <= m < 60):
            return {"e": "Noto‘g‘ri vaqt"}
        if k == "o":
            day = now().date() + dt.timedelta(days=int(p[1]))
            run_at = dt.datetime.combine(day, dt.time(h, m))
            if run_at <= now():
                return {"e": "Bu vaqt o‘tib ketgan"}
            rec.update(kind="once", run_at=run_at.isoformat())
        elif k == "d":
            rec.update(kind="daily", hour=h, minute=m)
        elif k == "w":
            rec.update(kind="weekly", weekday=int(p[1]) % 7, hour=h, minute=m)
        else:
            return {"e": "Noma’lum tur"}
    rid = add_reminder(**rec)
    set_draft_rid(uid, mid, rid)
    r = get_reminder(rid)
    schedule_reminder(r)
    return {**base, "ok": 1, "id": rid, "w": when_text(r)}


def wq_board(uid, p):
    act = p[0]
    if act == "t":
        return board_view(uid, p[1], int(p[2]))
    rid, tab, page = int(p[1]), p[2], int(p[3])
    r = get_reminder(rid)
    if r is None or r["user_id"] != uid:
        return board_view(uid, tab, page)
    if act == "p":
        set_active(rid, False); remove_job(rid)
    elif act == "r":
        set_active(rid, True); schedule_reminder(get_reminder(rid))
    elif act == "d":
        remove_job(rid); delete_reminder(rid)
    return board_view(uid, tab, page)


def wq_card(uid, p):
    act, rid = p[0], int(p[1])
    r = get_reminder(rid)
    if r is None or r["user_id"] != uid:
        return {"e": "Eslatma topilmadi"}
    if act == "z":
        snooze(rid)
    elif act == "p":
        set_active(rid, False); remove_job(rid)
    elif act != "d":
        return {"e": "?"}
    return card_data(r, r["sent_count"], act)


def wq_quiet(uid, p):
    if p[0] == "off":
        set_quiet(uid, None, None)
        return {**quiet_data(uid), "m": "O‘chirildi"}
    s, e = int(p[0]) % 24, int(p[1]) % 24
    if s == e:
        return {"e": "Boshlanish va tugash bir xil bo‘lmasin"}
    set_quiet(uid, s, e)
    return quiet_data(uid)


async def wq_admin(uid, p):
    if uid != ADMIN_ID:
        return {"e": "Ruxsat yo‘q"}
    if p[0] == "t":
        return admin_view(p[1], int(p[2]))
    if p[0] == "r":
        sid = int(p[1])
        if not get_support(sid):
            return {"e": "Murojaat topilmadi"}
        await start_admin_reply(sid)
        return {**admin_view(p[2], int(p[3])), "m": f"#{sid} javobini chatga yozing 👇"}
    return {"e": "?"}


async def on_widget_query(q):
    qid = q["id"]
    uid = q["from"]["id"]
    chat_id = (q.get("chat") or {}).get("id")
    mid = q.get("message_id")
    p = (q.get("data") or "").split(":")
    try:
        if p[0] == "B":
            ans = await wq_builder(uid, mid, p[1:])
        elif p[0] == "L":
            ans = wq_board(uid, p[1:])
        elif p[0] == "C":
            ans = wq_card(uid, p[1:])
        elif p[0] == "Q":
            ans = wq_quiet(uid, p[1:])
        elif p[0] == "A":
            ans = await wq_admin(uid, p[1:])
        else:
            ans = {"e": "Noma’lum so‘rov"}
    except Exception as ex:
        print("widget_query error:", ex)
        ans = {"e": "Xatolik"}
    try:
        await tg("answerWidgetQuery", widget_query_id=qid, data=jd(ans))
    except Exception as ex:
        print("answerWidgetQuery error:", ex)
    if chat_id and mid and "e" not in ans:
        await edit_widget(chat_id, mid, {k: v for k, v in ans.items() if k != "m"})


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
        "• 🌙 Tinch soatlar — shu oraliqda bezovta qilmayman\n\n"
        "Interaktiv kartalar ko'rinmasa — Telegram'ni yangilang yoki /oddiy",
        reply_markup=main_menu(m.from_user.id))


@dp.message(Command("oddiy"))
async def cmd_classic(m: Message):
    upsert_user(m.from_user.id, m.from_user.username, m.from_user.full_name)
    set_classic(m.from_user.id, True)
    await m.answer("⌨️ Oddiy rejim yoqildi. Kartalarga qaytish: /widget", reply_markup=main_menu(m.from_user.id))


@dp.message(Command("widget"))
async def cmd_widget(m: Message):
    upsert_user(m.from_user.id, m.from_user.username, m.from_user.full_name)
    set_classic(m.from_user.id, False)
    await m.answer("✨ Interaktiv kartalar yoqildi.", reply_markup=main_menu(m.from_user.id))


@dp.callback_query(F.data == "classic")
async def cb_classic(c: CallbackQuery, state: FSMContext):
    await state.clear()
    upsert_user(c.from_user.id, c.from_user.username, c.from_user.full_name)
    set_classic(c.from_user.id, True)
    await c.message.answer("⌨️ Oddiy rejim yoqildi. Kerakli tugmani qayta bosing.\nKartalarga qaytish: /widget",
                           reply_markup=main_menu(c.from_user.id))
    await c.answer()


# ---- yangi eslatma ----
@dp.message(F.text == "➕ Yangi eslatma")
async def new_rem(m: Message, state: FSMContext):
    await state.clear()
    await state.set_state(NewReminder.content)
    await m.answer("Eslatma matnini yozing yoki ovozli xabar yuboring 🎤")


@dp.callback_query(F.data == "cancel")
async def cancel_cb(c: CallbackQuery, state: FSMContext):
    await state.clear()
    await c.message.edit_text("Bekor qilindi.")
    await c.answer()


@dp.message(NewReminder.content, F.voice)
async def content_voice(m: Message, state: FSMContext):
    await state.update_data(voice_id=m.voice.file_id, text=m.caption or "")
    await after_content_input(m, state)


@dp.message(NewReminder.content, F.text)
async def content_text(m: Message, state: FSMContext):
    await state.update_data(voice_id=None, text=m.text)
    await after_content_input(m, state)


async def after_content_input(m: Message, state: FSMContext):
    uid = m.from_user.id
    d = await state.get_data()
    if wmode(uid):
        title = d.get("text") or "🎤 ovozli xabar"
        msg = await send_widget(
            uid, W_BUILDER,
            f"📝 {short(title, 200)}\nTurini va vaqtini tanlab, «Saqlash»ni bosing.",
            {"t": now().date().isoformat(), "x": short(title, 60)}, kb=CLASSIC_KB)
        if msg:
            add_draft(uid, msg["message_id"], d.get("text"), d.get("voice_id"))
            await state.clear()
            return
    await state.set_state(NewReminder.kind)
    await m.answer("Eslatma turini tanlang:", reply_markup=kind_menu())


@dp.callback_query(NewReminder.kind, F.data.startswith("kind:"))
async def pick_kind(c: CallbackQuery, state: FSMContext):
    await state.update_data(kind=c.data.split(":")[1])
    await c.message.edit_reply_markup(reply_markup=None)
    await after_content(c.message, state)
    await c.answer()


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
    if run_at <= now():
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
    return f"{'🟢' if r['active'] else '⏸'} #{r['id']} — {label(r)}\n    {when_text(r)}"


@dp.message(F.text == "📋 Eslatmalarim")
async def my_rems(m: Message):
    uid = m.from_user.id
    if wmode(uid) and await send_widget(uid, W_BOARD, "📋 Eslatmalarim", board_view(uid)):
        return
    rems = user_reminders(uid)
    if not rems:
        return await m.answer("Hozircha eslatmangiz yo'q.")
    for r in rems:
        await m.answer(describe(r), reply_markup=reminder_actions(r["id"], r["active"]))


@dp.callback_query(F.data.startswith("pause:"))
async def pause(c: CallbackQuery):
    rid = int(c.data.split(":")[1])
    r = get_reminder(rid)
    if r is None or r["user_id"] != c.from_user.id:
        return await c.answer("Topilmadi")
    set_active(rid, False); remove_job(rid)
    if c.message.text and c.message.text.startswith(("🟢", "⏸")):
        await c.message.edit_text(describe(get_reminder(rid)), reply_markup=reminder_actions(rid, False))
    else:
        await c.message.edit_reply_markup(reply_markup=None)
    await c.answer("Pauza")


@dp.callback_query(F.data.startswith("resume:"))
async def resume(c: CallbackQuery):
    rid = int(c.data.split(":")[1])
    r = get_reminder(rid)
    if r is None or r["user_id"] != c.from_user.id:
        return await c.answer("Topilmadi")
    set_active(rid, True); schedule_reminder(get_reminder(rid))
    await c.message.edit_text(describe(get_reminder(rid)), reply_markup=reminder_actions(rid, True))
    await c.answer("Davom")


@dp.callback_query(F.data.startswith("del:"))
async def delete(c: CallbackQuery):
    rid = int(c.data.split(":")[1])
    r = get_reminder(rid)
    if r is None or r["user_id"] != c.from_user.id:
        return await c.answer("Topilmadi")
    remove_job(rid); delete_reminder(rid)
    await c.message.edit_text("🗑 O'chirildi.")
    await c.answer()


@dp.callback_query(F.data.startswith("snz:"))
async def snooze_cb(c: CallbackQuery):
    rid = int(c.data.split(":")[1])
    r = get_reminder(rid)
    if r is None or r["user_id"] != c.from_user.id:
        return await c.answer("Topilmadi")
    snooze(rid)
    await c.message.edit_reply_markup(reply_markup=None)
    await c.answer(f"{SNOOZE_MIN} daqiqadan keyin eslataman")


# ---- tinch soatlar ----
@dp.message(F.text == "🌙 Tinch soatlar")
async def quiet(m: Message, state: FSMContext):
    uid = m.from_user.id
    upsert_user(uid, m.from_user.username, m.from_user.full_name)
    if wmode(uid) and await send_widget(uid, W_QUIET, "🌙 Tinch soatlar", quiet_data(uid)):
        return
    u = get_user(uid)
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

    uid = m.from_user.id
    sid = add_support(uid, m.from_user.username, m.from_user.full_name, content, mtype, fid)
    await state.clear()
    msg = None
    if wmode(uid):
        msg = await send_widget(uid, W_SUPPORT, f"📨 Murojaat #{sid} qabul qilindi", {"n": sid, "s": 0},
                                queries=False)
    if msg:
        set_support_msg(sid, msg["message_id"])
    else:
        await m.answer("✅ Murojaatingiz yuborildi.", reply_markup=main_menu(uid))

    head = (f"🆕 Murojaat #{sid}\n👤 {m.from_user.full_name} "
            f"(@{m.from_user.username or '—'}, id {uid})\n")
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


async def start_admin_reply(sid):
    ctx = dp.fsm.get_context(bot=bot, chat_id=ADMIN_ID, user_id=ADMIN_ID)
    await ctx.set_state(AdminReply.waiting)
    await ctx.update_data(sid=sid)
    s = get_support(sid)
    if s and s["status"] == "new":
        set_support_status(sid, "seen")
        await support_widget_status(sid, 1)
    await bot.send_message(ADMIN_ID, f"#{sid} murojaatga javobingizni yozing:")


@dp.callback_query(F.data.startswith("reply:"))
async def reply_start(c: CallbackQuery):
    if c.from_user.id != ADMIN_ID:
        return await c.answer("Ruxsat yo'q", show_alert=True)
    await start_admin_reply(int(c.data.split(":")[1]))
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
        set_support_status(sid, "answered")
        await support_widget_status(sid, 2)
        await m.answer("✅ Javob yuborildi.")
    except Exception as ex:
        await m.answer(f"Yuborilmadi: {ex}")


# ---- admin panel ----
@dp.message(F.text == "🛠 Admin panel")
async def admin(m: Message):
    if m.from_user.id != ADMIN_ID:
        return
    kb = ikb([[("✉️ Bitta odamga yozish", "adm:dm")], [("📨 Yangi murojaatlar (to'liq)", "adm:support_new")]])
    if wmode(ADMIN_ID) and await send_widget(ADMIN_ID, W_ADMIN, "🛠 Admin panel", admin_view(), kb=kb):
        return
    await m.answer("Admin panel:", reply_markup=admin_menu())


@dp.callback_query(F.data == "adm:dm")
async def dm_start(c: CallbackQuery, state: FSMContext):
    if c.from_user.id != ADMIN_ID:
        return await c.answer("Ruxsat yo'q", show_alert=True)
    await state.set_state(AdminReply.dm_id)
    await c.message.answer("Kimga yozamiz? Foydalanuvchi ID sini yuboring.\n"
                           "(«Odamlar» bo'limida 📋 bosilsa ID nusxalanadi.)")
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
        await m.answer(f"Yuborilmadi: {ex}\n(Foydalanuvchi botga /start bosmagan bo'lishi mumkin.)",
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
#                        RUN (webhook)
# ==========================================================
PORT = int(os.getenv("PORT", "10000"))
BASE_URL = os.getenv("RENDER_EXTERNAL_URL", os.getenv("BASE_URL", ""))
WEBHOOK_PATH = "/webhook"
ALLOWED_UPDATES = ["message", "callback_query", "widget_query"]
_tasks = set()


def _bg(coro):
    t = asyncio.create_task(coro)
    _tasks.add(t)
    t.add_done_callback(_tasks.discard)


async def health(request):
    return web.Response(text="ok")


async def webhook(request):
    # widget_query'ni aiogram hali bilmaydi — o'zimiz ushlaymiz, qolganini aiogram'ga beramiz
    data = await request.json()
    if "widget_query" in data:
        _bg(on_widget_query(data["widget_query"]))
    else:
        _bg(dp.feed_raw_update(bot, data))
    return web.Response(text="ok")


async def on_startup(app):
    global HTTP
    HTTP = aiohttp.ClientSession()
    init_db()
    for r in all_active_reminders():
        schedule_reminder(r)
    scheduler.start()
    if BASE_URL:
        await bot.set_webhook(BASE_URL.rstrip("/") + WEBHOOK_PATH,
                              drop_pending_updates=True, allowed_updates=ALLOWED_UPDATES)
        print("Webhook o'rnatildi:", BASE_URL.rstrip("/") + WEBHOOK_PATH)
    else:
        print("DIQQAT: BASE_URL/RENDER_EXTERNAL_URL yo'q — webhook o'rnatilmadi.")
    print("Bot ishga tushdi.")


async def on_cleanup(app):
    if HTTP:
        await HTTP.close()


def build_app():
    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_get("/health", health)
    app.router.add_post(WEBHOOK_PATH, webhook)
    setup_application(app, dp, bot=bot)
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    return app


if __name__ == "__main__":
    web.run_app(build_app(), host="0.0.0.0", port=PORT)
