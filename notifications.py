import logging
from datetime import datetime, timedelta

from aiogram import Bot
from aiogram.exceptions import TelegramForbiddenError, TelegramBadRequest
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

import db
from config import NOTIFY_APPROACHING

logger = logging.getLogger(__name__)

def _as_bool(value) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)

# отправляем сообщение в личку, не падаем если бот заблокирован
async def safe_dm(bot: Bot, user_id: int, text: str, **kwargs) -> bool:
    try:
        await bot.send_message(user_id, text, **kwargs)
        return True
    except (TelegramForbiddenError, TelegramBadRequest) as e:
        logger.info(f"Cannot DM {user_id}: {e}")
        return False

def _ready_keyboard(queue_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Я прошёл, следующий!", callback_data=f"done_next:{queue_id}"),
        InlineKeyboardButton(text="🚪 Выйти", callback_data=f"pm_leave:{queue_id}"),
    ]])

# уведомляем нового первого в очереди
async def notify_became_first(bot: Bot, queue: dict, member: dict, chat_id: int):
    remind_min = queue.get("remind_timeout_min", 5)
    user_id = member["user_id"]

    dm_text = (
        f"🔔 <b>Твоя очередь!</b>\n\n"
        f"Ты первый в очереди <b>«{queue['name']}»</b>.\n"
        f"Нажми кнопку когда пройдёшь 👇"
    )
    dm_ok = await safe_dm(bot, user_id, dm_text,
                          reply_markup=_ready_keyboard(queue["id"]),
                          parse_mode="HTML")
    if not dm_ok:
        username = member.get("username")
        mention = f"@{username}" if username else member["display_name"]
        try:
            await bot.send_message(
                chat_id,
                f"🔔 {mention}, твоя очередь в <b>«{queue['name']}»</b>!",
                parse_mode="HTML"
            )
        except Exception as e:
            logger.warning(f"Cannot notify in group {chat_id}: {e}")

    fire_at = (datetime.utcnow() + timedelta(minutes=remind_min)).strftime("%Y-%m-%d %H:%M:%S")
    await db.create_reminder(queue["id"], user_id, fire_at)

# предупреждаем что скоро очередь — в личку, если нет — в группу
async def notify_approaching(bot: Bot, queue: dict, member: dict, position: int):
    if NOTIFY_APPROACHING <= 0:
        return
    if position > NOTIFY_APPROACHING:
        return
    left = position - 1
    text = (
        f"⚡ <b>Скоро твоя очередь!</b>\n\n"
        f"В очереди <b>«{queue['name']}»</b> до тебя осталось <b>{left}</b> чел.\n"
        f"Готовься!"
    )
    dm_ok = await safe_dm(bot, member["user_id"], text, parse_mode="HTML")
    if not dm_ok:
        username = member.get("username")
        mention = f"@{username}" if username else member["display_name"]
        try:
            await bot.send_message(
                queue["chat_id"],
                f"⚡ {mention}, до тебя в очереди <b>«{queue['name']}»</b> осталось <b>{left}</b> чел. Готовься!",
                parse_mode="HTML"
            )
        except Exception as e:
            logger.warning(f"Cannot notify approaching in group: {e}")

async def notify_kicked(bot: Bot, queue: dict, user_id: int, by_timeout: bool = False, position: int = None):
    reason = "за неактивность ⏱" if by_timeout else "администратором"
    await safe_dm(
        bot, user_id,
        f"⚠️ Тебя удалили из очереди <b>«{queue['name']}»</b> {reason}.",
        parse_mode="HTML"
    )

async def notify_leave_public(bot: Bot, queue: dict, member: dict, chat_id: int):
    if not queue:
        return
    if not _as_bool(queue.get("notify_leave_public", True)):
        return
    name = member["display_name"]
    username = member.get("username")
    mention = f"@{username}" if username else name
    try:
        await bot.send_message(
            chat_id,
            f"🚪 {mention} вышел(а) из очереди <b>«{queue['name']}»</b>.",
            parse_mode="HTML"
        )
    except Exception as e:
        logger.warning(f"Cannot post leave notice: {e}")

# говорим подписчикам что место освободилось
async def notify_slot_available(bot: Bot, queue: dict):
    subscribers = await db.get_queue_subscribers(queue["id"])
    for user_id in subscribers:
        count = await db.get_member_count(queue["id"])
        slots_left = queue["max_slots"] - count
        if slots_left > 0:
            kb = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(
                    text="✋ Занять место",
                    callback_data=f"pm_join:{queue['id']}"
                )
            ]])
            sent = await safe_dm(
                bot, user_id,
                f"🎉 <b>Место освободилось!</b>\n\n"
                f"В очереди <b>«{queue['name']}»</b> появилось {slots_left} свободных мест.",
                reply_markup=kb,
                parse_mode="HTML"
            )
            if sent:
                await db.unsubscribe_queue(queue["id"], user_id)

# крутится каждую минуту и обрабатывает просроченные напоминания
async def process_due_reminders(bot: Bot):
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    reminders = await db.get_due_reminders(now)

    for r in reminders:
        await db.mark_reminder_done(r["id"])
        queue = await db.get_queue(r["queue_id"])
        if not queue or not queue["is_active"]:
            continue

        members = await db.get_queue_members(r["queue_id"])
        if not members or members[0]["user_id"] != r["user_id"]:
            continue

        member = members[0]
        auto_kick = bool(queue.get("auto_kick", True))

        if auto_kick:
            await db.kick_member(r["queue_id"], r["user_id"])
            await notify_kicked(bot, queue, r["user_id"], by_timeout=True)
            await notify_leave_public(bot, queue, member, queue["chat_id"])
            new_members = await db.get_queue_members(r["queue_id"])
            if new_members:
                await notify_became_first(bot, queue, new_members[0], queue["chat_id"])
            if queue["max_slots"] > 0:
                await notify_slot_available(bot, queue)
        else:
            dm_ok = await safe_dm(
                bot, r["user_id"],
                f"⏰ <b>Напоминание!</b>\n\n"
                f"Ты всё ещё первый в очереди <b>«{queue['name']}»</b>.\n"
                f"Нажми кнопку когда пройдёшь 👇",
                reply_markup=_ready_keyboard(queue["id"]),
                parse_mode="HTML"
            )
            if not dm_ok:
                username = member.get("username")
                mention = f"@{username}" if username else member["display_name"]
                try:
                    await bot.send_message(
                        queue["chat_id"],
                        f"⏰ {mention}, ты всё ещё первый в <b>«{queue['name']}»</b>!",
                        parse_mode="HTML"
                    )
                except Exception:
                    pass
