"""
consent_reply.py — авто-реплай в группе фиксированным текстом или картинкой,
но ТОЛЬКО после явного согласия самого пользователя.

Поток:
  1. Владелец бота пишет боту в ЛС: /setreply <user_id> <текст>
     либо присылает ФОТО с подписью /setreply <user_id> [подпись] —
     тогда реплай будет картинкой (Telegram передаёт команды и в caption).
  2. Бот сразу шлёт этот ТОЧНЫЙ текст или ТОЧНО ЭТУ картинку самому user_id
     в личные сообщения, затем — вопрос-подтверждение с кнопками
     «✅ Согласен» / «❌ Отказаться».
     Человек видит, что именно будет приходить, ДО включения.
  3. Если человек нажимает «✅ Согласен» — режим включается: в любой группе,
     где есть и бот, и этот человек, бот отвечает (reply) этим текстом на
     каждое его сообщение.
  4. Человек может отключить это для себя в любой момент командой /stopreply
     (в ЛС боту) — без необходимости спрашивать у владельца.

Почему это безопасно (в отличие от скрытого варианта):
  - человек видит точный текст ДО включения, а не после;
  - явное действие (нажатие кнопки) = осознанное согласие, а не молчаливое
    добавление в список;
  - отписаться может сам в одну команду, без участия владельца бота.

Подключение (в main.py):
    from consent_reply import init_consent_reply_db, consent_router
    await init_consent_reply_db()
    dp.include_router(consent_router)   # регистрировать ПОСЛЕДНИМ
"""

import logging

from aiogram import Router, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command

logger = logging.getLogger(__name__)

consent_router = Router()


def _get_db_type():
    try:
        from config import DB_TYPE
        return DB_TYPE
    except Exception:
        return "sqlite"


def _get_owner_id() -> int | None:
    try:
        from config import BOT_OWNER_ID
        return BOT_OWNER_ID
    except Exception:
        return None


def _is_owner(user_id: int) -> bool:
    owner_id = _get_owner_id()
    return owner_id is not None and user_id == owner_id


# ═══════════════════════════════════════════════════════════════════
# БАЗА ДАННЫХ
# ═══════════════════════════════════════════════════════════════════
# status: 'pending'  — текст отправлен человеку, ждём решения
#         'accepted' — человек согласился, реплай активен
#         'declined' — человек отказался

async def init_consent_reply_db():
    if _get_db_type() == "postgres":
        await _init_pg()
    else:
        await _init_sqlite()


async def _init_sqlite():
    import aiosqlite
    from database import DB_PATH

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS consent_replies (
                user_id        INTEGER PRIMARY KEY,
                reply_text     TEXT NOT NULL DEFAULT '',
                reply_type     TEXT NOT NULL DEFAULT 'text',
                reply_file_id  TEXT,
                status         TEXT NOT NULL DEFAULT 'pending',
                set_by         INTEGER,
                created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                decided_at     TIMESTAMP
            )
        """)

        # миграция для таблиц, созданных до появления типа реплая (фото)
        cur = await db.execute("PRAGMA table_info(consent_replies)")
        existing_cols = {row[1] for row in await cur.fetchall()}
        if "reply_type" not in existing_cols:
            await db.execute(
                "ALTER TABLE consent_replies ADD COLUMN reply_type TEXT NOT NULL DEFAULT 'text'"
            )
        if "reply_file_id" not in existing_cols:
            await db.execute("ALTER TABLE consent_replies ADD COLUMN reply_file_id TEXT")

        await db.commit()


async def _init_pg():
    from database_pg import get_pool
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS consent_replies (
                user_id        BIGINT PRIMARY KEY,
                reply_text     TEXT NOT NULL DEFAULT '',
                reply_type     TEXT NOT NULL DEFAULT 'text',
                reply_file_id  TEXT,
                status         TEXT NOT NULL DEFAULT 'pending',
                set_by         BIGINT,
                created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                decided_at     TIMESTAMP
            )
        """)
        # миграция для таблиц, созданных до появления типа реплая (фото)
        await conn.execute(
            "ALTER TABLE consent_replies ADD COLUMN IF NOT EXISTS reply_type TEXT NOT NULL DEFAULT 'text'"
        )
        await conn.execute(
            "ALTER TABLE consent_replies ADD COLUMN IF NOT EXISTS reply_file_id TEXT"
        )


async def upsert_pending(
    user_id: int,
    reply_text: str,
    set_by: int,
    reply_type: str = "text",
    file_id: str | None = None,
) -> None:
    if _get_db_type() == "postgres":
        from database_pg import get_pool
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO consent_replies
                    (user_id, reply_text, reply_type, reply_file_id, status, set_by, decided_at)
                VALUES ($1, $2, $3, $4, 'pending', $5, NULL)
                ON CONFLICT (user_id) DO UPDATE
                    SET reply_text = EXCLUDED.reply_text,
                        reply_type = EXCLUDED.reply_type,
                        reply_file_id = EXCLUDED.reply_file_id,
                        status = 'pending',
                        set_by = EXCLUDED.set_by,
                        decided_at = NULL
                """,
                user_id, reply_text, reply_type, file_id, set_by,
            )
        return

    import aiosqlite
    from database import DB_PATH
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            INSERT INTO consent_replies
                (user_id, reply_text, reply_type, reply_file_id, status, set_by, decided_at)
            VALUES (?, ?, ?, ?, 'pending', ?, NULL)
            ON CONFLICT(user_id) DO UPDATE SET
                reply_text = excluded.reply_text,
                reply_type = excluded.reply_type,
                reply_file_id = excluded.reply_file_id,
                status = 'pending',
                set_by = excluded.set_by,
                decided_at = NULL
            """,
            (user_id, reply_text, reply_type, file_id, set_by),
        )
        await db.commit()


async def set_status(user_id: int, status: str) -> None:
    if _get_db_type() == "postgres":
        from database_pg import get_pool
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE consent_replies SET status = $1, decided_at = NOW() WHERE user_id = $2",
                status, user_id,
            )
        return

    import aiosqlite
    from database import DB_PATH
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE consent_replies SET status = ?, decided_at = CURRENT_TIMESTAMP WHERE user_id = ?",
            (status, user_id),
        )
        await db.commit()


async def get_entry(user_id: int) -> dict | None:
    if _get_db_type() == "postgres":
        from database_pg import get_pool
        pool = await get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM consent_replies WHERE user_id = $1", user_id)
            return dict(row) if row else None

    import aiosqlite
    from database import DB_PATH
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM consent_replies WHERE user_id = ?", (user_id,))
        row = await cur.fetchone()
        return dict(row) if row else None


async def list_entries() -> list[dict]:
    if _get_db_type() == "postgres":
        from database_pg import get_pool
        pool = await get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM consent_replies ORDER BY created_at")
            return [dict(r) for r in rows]

    import aiosqlite
    from database import DB_PATH
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM consent_replies ORDER BY created_at")
        return [dict(r) for r in await cur.fetchall()]


async def delete_entry(user_id: int) -> None:
    if _get_db_type() == "postgres":
        from database_pg import get_pool
        pool = await get_pool()

        async with pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM consent_replies WHERE user_id = $1",
                user_id,
            )
        return

    import aiosqlite
    from database import DB_PATH

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "DELETE FROM consent_replies WHERE user_id = ?",
            (user_id,),
        )
        await db.commit()

# ═══════════════════════════════════════════════════════════════════
# ВЛАДЕЛЕЦ: задать текст для пользователя (только в ЛС боту)
# ═══════════════════════════════════════════════════════════════════

@consent_router.message(Command("setreply"), F.chat.type == "private")
async def cmd_setreply(message: Message):
    """
    /setreply <user_id> <текст>           — текстовый реплай
    Фото с подписью /setreply <user_id> [подпись] — реплай картинкой

    Отправляет ТОЧНО ЭТОТ контент пользователю в ЛС с запросом согласия.
    Реплай в группе включится только если пользователь подтвердит.
    """
    if not _is_owner(message.from_user.id):
        await message.reply("❌ Команда доступна только владельцу бота.")
        return

    raw = message.text or message.caption or ""
    parts = raw.split(maxsplit=2)
    if len(parts) < 2 or not parts[1].lstrip("-").isdigit():
        await message.reply(
            "Использование: <code>/setreply USER_ID текст</code>\n"
            "Например: <code>/setreply 123456789 Привет! Как дела?</code>\n\n"
            "Чтобы реплай был картинкой — пришлите фото с такой же подписью "
            "(<code>/setreply USER_ID [подпись]</code>).",
            parse_mode="HTML",
        )
        return

    target_id = int(parts[1])
    reply_text = parts[2].strip() if len(parts) > 2 else ""

    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Согласен", callback_data=f"consent_yes:{target_id}"),
        InlineKeyboardButton(text="❌ Отказаться", callback_data=f"consent_no:{target_id}"),
    ]])

    if message.photo:
        file_id = message.photo[-1].file_id
        await upsert_pending(
            target_id, reply_text, set_by=message.from_user.id,
            reply_type="photo", file_id=file_id,
        )

        try:
            # сначала показываем ТОЧНО ту же картинку, что будет уходить в группе
            await message.bot.send_photo(target_id, file_id, caption=reply_text or None)
            await message.bot.send_message(
                target_id,
                "👋 Пользователь, управляющий ботом, предложил, чтобы в группах, "
                "где вы оба состоите, бот отвечал вам показанным выше изображением "
                "на каждое ваше сообщение.\n\n"
                "Согласны ли вы получать именно это в ответ? "
                "В любой момент вы можете отключить это командой /stopreply здесь, в ЛС боту.",
                reply_markup=kb,
            )
        except Exception as e:
            logger.error(f"Could not DM user {target_id}: {e}")
            await message.reply(
                "⚠️ Не удалось отправить запрос пользователю в ЛС.\n"
                "Возможно, он не запускал бота (/start) — попросите его сначала написать боту."
            )
            return

        await message.reply(
            f"✅ Запрос на согласие (картинка) отправлен пользователю <code>{target_id}</code>.",
            parse_mode="HTML",
        )
        return

    if not reply_text:
        await message.reply("Текст не может быть пустым.")
        return

    await upsert_pending(target_id, reply_text, set_by=message.from_user.id, reply_type="text")

    try:
        await message.bot.send_message(
            target_id,
            "👋 Пользователь, управляющий ботом, предложил, чтобы в группах, "
            "где вы оба состоите, бот отвечал вам следующим сообщением "
            "на каждое ваше сообщение:\n\n"
            f"«{reply_text}»\n\n"
            "Согласны ли вы получать именно это в ответ? "
            "В любой момент вы можете отключить это командой /stopreply здесь, в ЛС боту.",
            reply_markup=kb,
        )
    except Exception as e:
        logger.error(f"Could not DM user {target_id}: {e}")
        await message.reply(
            "⚠️ Не удалось отправить запрос пользователю в ЛС.\n"
            "Возможно, он не запускал бота (/start) — попросите его сначала написать боту."
        )
        return

    await message.reply(f"✅ Запрос на согласие отправлен пользователю <code>{target_id}</code>.", parse_mode="HTML")


@consent_router.message(Command("replylist"), F.chat.type == "private")
async def cmd_replylist(message: Message):
    """/replylist — показывает все записи и их статус (владелец бота)."""
    if not _is_owner(message.from_user.id):
        await message.reply("❌ Команда доступна только владельцу бота.")
        return

    entries = await list_entries()
    if not entries:
        await message.reply("Список пуст.")
        return

    icons = {"pending": "⏳", "accepted": "✅", "declined": "❌"}
    lines = ["<b>Статусы согласий:</b>"]
    for e in entries:
        icon = icons.get(e["status"], "❓")
        kind = "📷" if e.get("reply_type") == "photo" else "💬"
        preview = e["reply_text"][:50] if e["reply_text"] else "(без подписи)"
        lines.append(f"{icon} {kind} <code>{e['user_id']}</code> — {e['status']}: «{preview}»")
    await message.reply("\n".join(lines), parse_mode="HTML")


@consent_router.message(Command("delreply"), F.chat.type == "private")
async def cmd_delreply(message: Message):
    """/delreply USER_ID Полностью удаляет подписку пользователя."""
    if not _is_owner(message.from_user.id):
        await message.reply("❌ Команда доступна только владельцу бота.")
        return

    parts = (message.text or "").split(maxsplit=1)

    if len(parts) != 2 or not parts[1].isdigit():
        await message.reply(
            "Использование:\n<code>/delreply USER_ID</code>",
            parse_mode="HTML",
        )
        return

    user_id = int(parts[1])

    entry = await get_entry(user_id)
    if not entry:
        await message.reply("Пользователь не найден.")
        return

    await delete_entry(user_id)

    await message.reply(
        f"🗑 Подписка пользователя <code>{user_id}</code> удалена.",
        parse_mode="HTML",
    )

    try:
        await message.bot.send_message(
            user_id,
            "🛑 Владелец бота отключил и удалил ваш авто-реплай."
        )
    except Exception:
        pass


# ═══════════════════════════════════════════════════════════════════
# ПОЛЬЗОВАТЕЛЬ: подтверждение / отказ / самостоятельное отключение
# ═══════════════════════════════════════════════════════════════════

@consent_router.callback_query(F.data.startswith("consent_yes:"))
async def cb_consent_yes(call: CallbackQuery):
    target_id = int(call.data.split(":")[1])
    if call.from_user.id != target_id:
        await call.answer("Это не для вас.", show_alert=True)
        return

    await set_status(target_id, "accepted")
    await call.message.edit_text(call.message.text + "\n\n✅ Вы согласились.")
    await call.answer("Готово, режим включён.")


@consent_router.callback_query(F.data.startswith("consent_no:"))
async def cb_consent_no(call: CallbackQuery):
    target_id = int(call.data.split(":")[1])
    if call.from_user.id != target_id:
        await call.answer("Это не для вас.", show_alert=True)
        return

    await set_status(target_id, "declined")
    await call.message.edit_text(call.message.text + "\n\n❌ Вы отказались. Ничего не включено.")
    await call.answer("Отказ зафиксирован.")


@consent_router.message(Command("stopreply"), F.chat.type == "private")
async def cmd_stopreply(message: Message):
    """Пользователь сам отключает реплай для себя — без участия владельца."""
    entry = await get_entry(message.from_user.id)
    if not entry or entry["status"] != "accepted":
        await message.reply("У вас и так не включён авто-реплай.")
        return

    await set_status(message.from_user.id, "declined")
    await message.reply("🛑 Авто-реплай для вас отключён.")


# ═══════════════════════════════════════════════════════════════════
# РЕПЛАЙ В ГРУППЕ — только для тех, кто статусом 'accepted'
# ═══════════════════════════════════════════════════════════════════

@consent_router.message(F.chat.type.in_({"group", "supergroup"}), F.text, ~F.text.startswith("/"))
async def on_consented_message(message: Message):
    """
    ВАЖНО: регистрировать consent_router ПОСЛЕДНИМ среди роутеров бота,
    иначе он перехватит сообщения, относящиеся к FSM-стейтам расписания
    и другим хэндлерам.
    """
    entry = await get_entry(message.from_user.id)
    if not entry or entry["status"] != "accepted":
        return

    if entry.get("reply_type") == "photo" and entry.get("reply_file_id"):
        await message.reply_photo(entry["reply_file_id"], caption=entry["reply_text"] or None)
    else:
        await message.reply(entry["reply_text"])