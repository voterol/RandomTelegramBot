# main.py — честный рандом-бот розыгрышей с привязкой каналов
# Зависимости: aiogram~=3.4
# Установка: pip install aiogram~=3.4
#
# Команды:
# /link_channel https://t.me/имя_канала
# /my_channels
# /start_giveaway <ссылка_или_id_канала> <кол-во_победителей> <часы | dd.mm.yy HH:MM> <текст...>
# /participants <id>
# /result <id>
# /end_giveaway <id>
#
# Примеры:
# /start_giveaway https://t.me/имяканала 2 1 Большой розыгрыш
# /start_giveaway https://t.me/имяканала 1 10.10.25 22:22 Большой розыгрыш

import logging, sqlite3, random, asyncio, json, html, re
from datetime import datetime, timedelta
from typing import Optional, List, Dict

from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command, CommandStart
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, CallbackQuery, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.client.default import DefaultBotProperties

# ======== CONFIG ========
BOT_TOKEN = "PUT_YOUR_TELEGRAM_BOT_TOKEN_HERE"   # <-- вставь токен
BOT_USERNAME = "YourBotUsername"                 # <-- username БЕЗ @ (для диплинка результатов)
DATABASE = "giveaways_linked_clean.db"
CHECK_INTERVAL_SEC = 15
# ========================

logging.basicConfig(level=logging.INFO)
bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher(storage=MemoryStorage())

# ======== Время (МСК) ========
def now_msk() -> datetime:
    return datetime.utcnow() + timedelta(hours=3)

RU_MONTHS_GEN = {
    1:"января",2:"февраля",3:"марта",4:"апреля",5:"мая",6:"июня",
    7:"июля",8:"августа",9:"сентября",10:"октября",11:"ноября",12:"декабря"
}
def format_msk_dmy_hm(dt: datetime) -> str:
    return f"{dt.day} {RU_MONTHS_GEN[dt.month]} в {dt:%H:%M} по МСК"

def render_time_hint(end_time: datetime) -> str:
    hours_left = (end_time - now_msk()).total_seconds()/3600
    if hours_left >= 24:
        return f"🕒 До {format_msk_dmy_hm(end_time)}"
    hrs = max(1, int(hours_left))
    return f"🕒 До окончания: {hrs} ч."

# ======== Парсинг времени ========
def parse_time_arg(arg1: str, arg2_optional: Optional[str]) -> Optional[datetime]:
    """
    Возвращает время окончания (МСК-наивное).
      - 'N' (часы)
      - 'dd.mm.yy HH:MM'
    """
    now = now_msk()
    if re.fullmatch(r"\d{1,3}", arg1):
        return now + timedelta(hours=int(arg1))
    if arg2_optional and re.fullmatch(r"\d{2}\.\d{2}\.\d{2}", arg1) and re.fullmatch(r"\d{2}:\d{2}", arg2_optional):
        try:
            return datetime.strptime(f"{arg1} {arg2_optional}", "%d.%m.%y %H:%M")
        except ValueError:
            return None
    return None

# ======== БД ========
def init_db():
    conn = sqlite3.connect(DATABASE)
    c = conn.cursor()
    c.execute("""
    CREATE TABLE IF NOT EXISTS giveaways(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        chat_id INTEGER,
        message_id INTEGER,
        title TEXT,
        owner_id INTEGER,
        active INTEGER DEFAULT 1,
        end_time TEXT,               -- МСК (наивное)
        winners_count INTEGER DEFAULT 1,
        media_file_id TEXT,
        media_type TEXT,
        final_winner_ids TEXT,       -- JSON list[int], фиксируются при публикации
        finalized_at TEXT
    )""")
    c.execute("""
    CREATE TABLE IF NOT EXISTS participants(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        giveaway_id INTEGER,
        user_id INTEGER,
        username TEXT,
        first_name TEXT,
        UNIQUE(giveaway_id, user_id)
    )""")
    c.execute("""
    CREATE TABLE IF NOT EXISTS channel_auth(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        owner_id INTEGER,
        channel_id INTEGER,
        UNIQUE(owner_id, channel_id)
    )""")
    conn.commit(); conn.close()

def create_giveaway_stub(chat_id:int, title:str, end_time:datetime,
                         owner_id:Optional[int], winners_count:int,
                         media_file_id:Optional[str], media_type:Optional[str]) -> int:
    conn = sqlite3.connect(DATABASE); c = conn.cursor()
    c.execute("""INSERT INTO giveaways(chat_id,message_id,title,owner_id,active,end_time,winners_count,media_file_id,media_type,final_winner_ids,finalized_at)
                 VALUES(?,NULL,?,?,1,?,?,?, ?, NULL, NULL)""",
              (chat_id, title, owner_id, end_time.isoformat(), winners_count, media_file_id, media_type))
    gid = c.lastrowid
    conn.commit(); conn.close()
    return gid

def update_giveaway_message_id(gid:int, message_id:int):
    conn=sqlite3.connect(DATABASE); c=conn.cursor()
    c.execute("UPDATE giveaways SET message_id=? WHERE id=?", (message_id, gid))
    conn.commit(); conn.close()

def get_giveaway(gid:int)->Optional[Dict]:
    conn=sqlite3.connect(DATABASE); c=conn.cursor()
    c.execute("""SELECT chat_id,message_id,title,owner_id,active,end_time,winners_count,media_file_id,media_type,final_winner_ids,finalized_at
                 FROM giveaways WHERE id=?""",(gid,))
    r=c.fetchone(); conn.close()
    if not r: return None
    return {
        "chat_id": r[0], "message_id": r[1], "title": r[2],
        "owner_id": r[3], "active": bool(r[4]),
        "end_time": datetime.fromisoformat(r[5]),
        "winners_count": int(r[6]),
        "media_file_id": r[7], "media_type": r[8],
        "final_ids": json.loads(r[9]) if r[9] else None,
        "final_at": r[10]
    }

def set_inactive(gid:int):
    conn=sqlite3.connect(DATABASE); c=conn.cursor()
    c.execute("UPDATE giveaways SET active=0 WHERE id=?", (gid,))
    conn.commit(); conn.close()

def set_final_winners(gid:int, user_ids:List[int]):
    conn=sqlite3.connect(DATABASE); c=conn.cursor()
    c.execute("UPDATE giveaways SET final_winner_ids=?, finalized_at=? WHERE id=?",
              (json.dumps(user_ids), now_msk().isoformat(), gid))
    conn.commit(); conn.close()

def participants(gid:int)->List[Dict]:
    conn=sqlite3.connect(DATABASE); c=conn.cursor()
    c.execute("SELECT user_id,username,first_name FROM participants WHERE giveaway_id=?", (gid,))
    rows=c.fetchall(); conn.close()
    return [{"user_id":r[0], "username":r[1], "first_name":r[2]} for r in rows]

def count_participants(gid:int)->int:
    conn=sqlite3.connect(DATABASE); c=conn.cursor()
    c.execute("SELECT COUNT(*) FROM participants WHERE giveaway_id=?", (gid,))
    n=c.fetchone()[0] or 0; conn.close(); return n

def add_participant(gid:int, user:types.User):
    conn=sqlite3.connect(DATABASE); c=conn.cursor()
    try:
        c.execute("INSERT INTO participants(giveaway_id,user_id,username,first_name) VALUES(?,?,?,?)",
                  (gid, user.id, user.username or "", user.first_name or ""))
        conn.commit()
    except sqlite3.IntegrityError:
        pass
    conn.close()

def remove_participant(gid:int, uid:int):
    conn=sqlite3.connect(DATABASE); c=conn.cursor()
    c.execute("DELETE FROM participants WHERE giveaway_id=? AND user_id=?", (gid, uid))
    conn.commit(); conn.close()

def add_channel_auth(owner_id:int, channel_id:int):
    conn=sqlite3.connect(DATABASE); c=conn.cursor()
    c.execute("INSERT OR IGNORE INTO channel_auth(owner_id,channel_id) VALUES(?,?)", (owner_id, channel_id))
    conn.commit(); conn.close()

def is_channel_allowed_for_user(owner_id:int, channel_id:int)->bool:
    conn=sqlite3.connect(DATABASE); c=conn.cursor()
    c.execute("SELECT 1 FROM channel_auth WHERE owner_id=? AND channel_id=?", (owner_id, channel_id))
    ok = c.fetchone() is not None
    conn.close()
    return ok

def list_user_channels(owner_id:int)->List[int]:
    conn=sqlite3.connect(DATABASE); c=conn.cursor()
    c.execute("SELECT channel_id FROM channel_auth WHERE owner_id=?", (owner_id,))
    rows=c.fetchall(); conn.close()
    return [r[0] for r in rows]

# ======== UI ========
def build_keyboard(gid:int)->InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text=f"Участвую! ({count_participants(gid)})", callback_data=f"join|{gid}"))
    kb.row(InlineKeyboardButton(text="Статус", callback_data=f"status|{gid}"))
    return kb.as_markup()

def build_result_button(gid:int)->InlineKeyboardMarkup:
    url = f"https://t.me/{BOT_USERNAME}?start=result_{gid}"
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔍 Проверить результат", url=url)]])

def winners_text(users: List[Dict]) -> str:
    if not users:
        return "Победителей нет (не было участников)."
    out=[]
    for i,u in enumerate(users, start=1):
        fname = html.escape(u["first_name"] or "")
        if u["username"]:
            out.append(f"{i}) {fname} (@{html.escape(u['username'])})")
        else:
            out.append(f"{i}) {fname} (id:{u['user_id']})")
    return "\n".join(out)

def announce_text(title:str, total:int, winners: List[Dict]) -> str:
    return (
        f"🎉 Итоги «{html.escape(title)}»\n\n"
        f"Всего участников: {total}\n"
        f"Победители:\n{winners_text(winners)}"
    )

# ======== Проверка подписки ========
async def is_subscribed(chat_id:int, user_id:int) -> bool:
    try:
        member = await bot.get_chat_member(chat_id, user_id)
        return member.status not in ("left", "kicked")
    except Exception:
        return False

# ======== Автотаймер ========
async def auto_timer():
    while True:
        await asyncio.sleep(CHECK_INTERVAL_SEC)
        conn=sqlite3.connect(DATABASE); c=conn.cursor()
        c.execute("SELECT id FROM giveaways WHERE active=1")
        ids=[r[0] for r in c.fetchall()]; conn.close()
        for gid in ids:
            g=get_giveaway(gid)
            if g and now_msk()>=g["end_time"]:
                await publish_results(gid)

# ======== Публикация итогов ========
async def publish_results(gid:int):
    g=get_giveaway(gid)
    if not g or not g["active"]: return

    ps=participants(gid); total=len(ps)
    k=max(0, min(g["winners_count"], total))
    winners=random.sample(ps, k) if k else []

    set_final_winners(gid, [u["user_id"] for u in winners])
    set_inactive(gid)

    text = announce_text(g["title"], total, winners)
    kb = build_result_button(gid)

    try:
        if g["media_file_id"] and g["media_type"]=="photo":
            await bot.send_photo(g["chat_id"], g["media_file_id"], caption=text, reply_markup=kb)
        else:
            await bot.send_message(g["chat_id"], text, reply_markup=kb)
    except Exception as e:
        logging.warning(f"send results failed: {e}")

# ======== /start ========
@dp.message(CommandStart())
async def cmd_start(m: Message):
    if m.chat.type != "private": return
    await m.answer(
            "Меню\n\n"
            "Команды:\n"
            "/start_giveaway ссылка_или_id канала КОЛИЧЕСТВО_ПОБЕДИТЕЛЕЙ время_оканчания_розыгрыша Текст розыгрыша\n\n"
            "  ВРЕМЯ: число часов или dd.mm.yy HH:MM (по МСК)\n\n"
            "Примеры:\n"
            "  /start_giveaway https://t.me/юзернейм_канала  2 1 Пример ---↓ \nЧерез час в канале юзернем_канал будет завершен розыгрыш на 2 победителя с текстом розыгрыша: Пример и опубликован результат\n"
            "  /start_giveaway https://t.me/юзернейм_канала  1 10.10.25 22:22 Большой розыгрыш ---↓ \nВ 10.10.25 22:22 в канале юзернем_канал будет завершен розыгрыш на 2 победителя с текстом розыгрыша: Большой розыгрыш и опубликован результат\n\n"
            "/end_giveaway ID — завершить свой розыгрыш\n\n"
            "/set_winners ID N — установить количество победителей в своём розыгрыше\n"
            "/participants ID — показать участников\n\n"
            "/result ID — посмотреть результаты\n\n"
            "/link_channel https://t.me/юзернейм_канала — привязать свой канал\n\n"
            "/my_channels — список привязанных каналов"
    )

    # deep-link: /start result_<id>
    if m.text and " " in m.text:
        _, payload = m.text.split(" ", 1)
        if payload.startswith("result_"):
            try:
                gid = int(payload.split("_", 1)[1])
                await show_result_to_user(m.from_user.id, gid)
            except Exception:
                pass

# ======== Привязка канала ========
@dp.message(Command("link_channel"))
async def link_channel(m: Message):
    if m.chat.type != "private": return
    args = m.text.split(maxsplit=1)
    if len(args) < 2:
        return await m.reply("Использование: /link_channel https://t.me/имя_канала")

    link = args[1].strip()
    if not link.startswith("https://t.me/") or "/joinchat/" in link or "+" in link:
        return await m.reply("Нужна ссылка вида https://t.me/имя_канала (публичный канал).")

    slug = link.split("/")[-1]
    try:
        chat = await bot.get_chat(f"@{slug}")
        channel_id = chat.id
    except Exception:
        return await m.reply("Не удалось найти канал по ссылке. Проверьте, что канал публичный и видимый боту.")

    uid = m.from_user.id
    # Проверка: и бот, и пользователь — админы канала
    try:
        me = await bot.get_me()
        bm = await bot.get_chat_member(channel_id, me.id)
        if bm.status not in ("administrator", "creator"):
            return await m.reply("Бот должен быть админом в канале.")
        um = await bot.get_chat_member(channel_id, uid)
        if um.status not in ("administrator", "creator"):
            return await m.reply("Вы должны быть админом в этом канале.")
    except Exception:
        return await m.reply("Не удалось проверить права в канале.")

    add_channel_auth(uid, channel_id)
    await m.reply(f"Канал «{chat.title}» привязан.")

@dp.message(Command("my_channels"))
async def my_channels(m: Message):
    if m.chat.type != "private": return
    uid = m.from_user.id
    chs = list_user_channels(uid)
    if not chs:
        return await m.reply("Нет привязанных каналов. Добавьте: /link_channel https://t.me/ваш_канал")
    lines=[]
    for cid in chs:
        try:
            chat = await bot.get_chat(cid)
            lines.append(f"{chat.title} ({cid})")
        except Exception:
            lines.append(str(cid))
    await m.reply("Ваши каналы:\n" + "\n".join(lines))

# ======== Создание розыгрыша ========
@dp.message(Command("start_giveaway"))
async def start_giveaway(m: Message):
    if m.chat.type != "private":
        return await m.reply("Эту команду нужно отправлять в личку боту.")

    # /start_giveaway <канал> <winners> <time | dd.mm.yy HH:MM> <title...>
    tokens = m.text.strip().split()
    if len(tokens) < 5:
        return await m.reply(
            "Использование:\n"
            "/start_giveaway ССЫЛКА_ИЛИ_ID КОЛВО_ПОБЕДИТЕЛЕЙ ВРЕМЯ НАЗВАНИЕ\n"
            "ВРЕМЯ: часы или dd.mm.yy HH:MM\n"
            "Примеры:\n"
            "/start_giveaway https://t.me/mychan 2 1 Nitro\n"
            "/start_giveaway https://t.me/mychan 1 10.10.25 22:22 Большой розыгрыш"
        )

    ch_arg = tokens[1]
    winners_str = tokens[2]
    time_token_1 = tokens[3]
    time_token_2 = tokens[4] if len(tokens) >= 6 and re.fullmatch(r"\d{2}:\d{2}", tokens[4]) and re.fullmatch(r"\d{2}\.\d{2}\.\d{2}", tokens[3]) else None
    title_start_index = 4 if time_token_2 is None else 5
    if title_start_index >= len(tokens):
        return await m.reply("Нужно указать название розыгрыша после времени.")
    title = " ".join(tokens[title_start_index:])

    # канал: ссылка или id
    if ch_arg.startswith("https://t.me/"):
        slug = ch_arg.split("/")[-1]
        try:
            chat = await bot.get_chat(f"@{slug}")
            channel_id = chat.id
        except Exception:
            return await m.reply("Не удалось найти канал по ссылке.")
    else:
        try:
            channel_id = int(ch_arg)
        except Exception:
            return await m.reply("Неверный идентификатор канала.")

    uid = m.from_user.id
    # канал должен быть привязан к пользователю
    if not is_channel_allowed_for_user(uid, channel_id):
        return await m.reply("Этот канал не привязан к вашему аккаунту. Используйте /link_channel.")

    # кол-во победителей
    try:
        winners_count = max(0, int(winners_str))
    except ValueError:
        return await m.reply("Количество победителей должно быть числом.")

    # время окончания
    end_time = parse_time_arg(time_token_1, time_token_2)
    if not end_time:
        return await m.reply("Неверный формат времени. Примеры: 2  или  10.10.25 22:22")
    if end_time <= now_msk():
        return await m.reply("Время окончания должно быть в будущем.")

    # медиа: фото в той же команде (или в реплае — если клиент позволяет)
    media_file_id = media_type = None
    if m.photo:
        media_file_id = m.photo[-1].file_id; media_type = "photo"
    elif m.reply_to_message and m.reply_to_message.photo:
        media_file_id = m.reply_to_message.photo[-1].file_id; media_type = "photo"

    gid = create_giveaway_stub(
        chat_id=channel_id,
        title=title,
        end_time=end_time,
        owner_id=uid,
        winners_count=winners_count,
        media_file_id=media_file_id,
        media_type=media_type
    )
    time_hint = render_time_hint(end_time)
    text = f"{html.escape(title)}\n\n{time_hint}\nНажми «Участвую!» чтобы принять участие."

    try:
        if media_file_id and media_type == "photo":
            sent = await bot.send_photo(channel_id, media_file_id, caption=text, reply_markup=build_keyboard(gid))
        else:
            sent = await bot.send_message(channel_id, text, reply_markup=build_keyboard(gid))
        update_giveaway_message_id(gid, sent.message_id)
    except Exception as e:
        # Если не можем постить — удалим черновик
        conn=sqlite3.connect(DATABASE); c=conn.cursor()
        c.execute("DELETE FROM giveaways WHERE id=?", (gid,)); conn.commit(); conn.close()
        return await m.reply("Не удалось опубликовать в канале. Проверьте, что бот — админ и может постить.")

    await m.reply(f"Розыгрыш #{gid} создан. Завершение: {format_msk_dmy_hm(end_time)}. Победителей: {winners_count}")

# ======== Завершение (только создатель) ========
@dp.message(Command("end_giveaway"))
async def end_giveaway(m: Message):
    if m.chat.type != "private": return
    args=m.text.split()
    if len(args)<2: return await m.reply("Использование: /end_giveaway ID")
    gid=int(args[1])
    g=get_giveaway(gid)
    if not g: return await m.reply("Розыгрыш не найден.")
    if not g["active"]: return await m.reply("Розыгрыш уже завершён.")
    if g["owner_id"] != m.from_user.id:
        return await m.reply("Завершить вручную может только создатель. Дождитесь авто-итогов.")
    await publish_results(gid)
    await m.reply("Итоги опубликованы в канале.")

# ======== Результаты (deep link и команда) ========
@dp.message(Command("result"))
async def cmd_result(m: Message):
    # можно вызывать в ЛС
    args=m.text.split()
    if len(args)<2: return await m.reply("Использование: /result ID")
    gid=int(args[1]); await show_result_to_user(m.from_user.id, gid)

async def show_result_to_user(uid:int, gid:int):
    g=get_giveaway(gid)
    if not g:
        return await bot.send_message(uid, "Розыгрыш не найден.")
    ps=participants(gid); total=len(ps)

    if g["active"]:
        text=(f"Розыгрыш #{gid} «{html.escape(g['title'])}» ещё идёт.\n"
              f"Завершение: {format_msk_dmy_hm(g['end_time'])}\n"
              f"Участников: {total}")
        return await bot.send_message(uid, text)

    winners=[]
    if g["final_ids"]:
        uidset=set(g["final_ids"])
        winners=[p for p in ps if p["user_id"] in uidset]

    text=announce_text(g["title"], total, winners)
    await bot.send_message(uid, f"Проверка результатов #{gid}:\n\n{text}")

# ======== Участники ========
@dp.message(Command("participants"))
async def participants_cmd(m: Message):
    args=m.text.split()
    if len(args)<2:
        return await m.reply("Использование: /participants ID")
    gid=int(args[1])
    ps=participants(gid)
    if not ps:
        return await m.reply("Нет участников.")
    txt = "Участники:\n" + "\n".join(
        f"{i+1}) {html.escape(p['first_name'] or '')} (@{html.escape(p['username'])})"
        if p["username"] else
        f"{i+1}) {html.escape(p['first_name'] or '')} (id:{p['user_id']})"
        for i,p in enumerate(ps)
    )
    await m.reply(txt[:4000])

# ======== Кнопки участия ========
@dp.callback_query(F.data.startswith("join|"))
async def join(cq: CallbackQuery):
    try:
        gid=int(cq.data.split("|",1)[1])
    except Exception:
        return await cq.answer("Кнопка устарела.")
    g=get_giveaway(gid)
    if not g or not g["active"]:
        return await cq.answer("Розыгрыш завершён.")

    if not await is_subscribed(g["chat_id"], cq.from_user.id):
        return await cq.answer("Чтобы участвовать, подпишитесь на канал.", show_alert=True)

    ids={p["user_id"] for p in participants(gid)}
    if cq.from_user.id in ids:
        remove_participant(gid, cq.from_user.id); msg="Вы вышли из розыгрыша."
    else:
        add_participant(gid, cq.from_user); msg="Вы участвуете!"

    try:
        await bot.edit_message_reply_markup(g["chat_id"], g["message_id"], reply_markup=build_keyboard(gid))
    except Exception:
        pass

    await cq.answer(msg)

@dp.callback_query(F.data.startswith("status|"))
async def status(cq: CallbackQuery):
    gid=int(cq.data.split("|",1)[1])
    await cq.answer(f"Всего участников: {count_participants(gid)}", show_alert=False)

# ======== Автозапуск ========
async def main():
    init_db()
    asyncio.create_task(auto_timer())
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())

