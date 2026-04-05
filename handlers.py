import csv
import io
import logging

from aiogram import Router, F, Bot
from aiogram.types import Message, CallbackQuery, BufferedInputFile
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import ChatMemberUpdated
from aiogram.types import ErrorEvent

import database as db
from keyboards import (
    queue_list_keyboard, queue_actions_keyboard, queue_settings_keyboard,
    kick_members_keyboard, confirm_keyboard, cancel_keyboard,
    pm_chat_select_keyboard, pm_queue_select_keyboard, pm_queue_actions_keyboard,
    nick_group_select_keyboard, me_keyboard, pm_main_keyboard, pm_reply_keyboard,
    freeze_keyboard, swap_select_keyboard, swap_confirm_keyboard,
)
from helpers import format_queue_info, format_queue_list, format_pm_my_queues
from notifications import (
    notify_became_first, notify_kicked, notify_leave_public,
    notify_approaching, notify_slot_available,
)

logger = logging.getLogger(__name__)
router = Router()

# словарь chat_id -> название чата, живёт в памяти
_chat_names: dict[int, str] = {}

class CreateQueue(StatesGroup):
    name       = State()
    description = State()
    max_slots  = State()
    remind_min = State()

class SetNick(StatesGroup):
    choosing_group = State()
    entering_nick  = State()

class ResetNick(StatesGroup):
    choosing_group = State()

class SetRemind(StatesGroup):
    minutes = State()

# проверяем является ли юзер админом — Telegram-права, бот-права или владелец
async def is_admin(bot: Bot, chat_id: int, user_id: int) -> bool:
    from config import BOT_OWNER_ID
    if user_id == BOT_OWNER_ID:
        return True
    if await db.is_bot_admin(user_id, chat_id):
        return True
    try:
        m = await bot.get_chat_member(chat_id, user_id)
        return m.status in ("administrator", "creator")
    except Exception:
        return False

# общая логика после выхода из очереди — уведомления, следующий
async def after_leave(bot: Bot, queue_id: int, left_user_id: int,
                      left_member: dict, was_first: bool):
    queue = await db.get_queue(queue_id)
    members = await db.get_queue_members(queue_id)
    await notify_leave_public(bot, queue, left_member, queue["chat_id"])
    if was_first and members:
        await notify_became_first(bot, queue, members[0], queue["chat_id"])
    await db.cancel_reminders(queue_id, left_user_id)
    return queue, members

def all_known_queues() -> list[dict]:

    return []

# берём все очереди из известных чатов для меню в личке
async def get_all_queues_for_pm(user_id: int) -> list[dict]:
    known_chats = await db.get_known_chats()
    for c in known_chats:
        _chat_names.setdefault(c["chat_id"], c["title"])
    known_chat_ids = list(_chat_names.keys())
    return await db.get_all_active_queues_for_known_chats(known_chat_ids)

@router.errors()
async def error_handler(event: ErrorEvent):
    logger.error(f"ERROR: {event.exception}", exc_info=event.exception)

@router.my_chat_member()
async def on_bot_added(event: ChatMemberUpdated, bot: Bot):
    
    chat = event.chat
    if chat.type in ("group", "supergroup") and chat.title:
        _chat_names[chat.id] = chat.title
        await db.register_chat(chat.id, chat.title)

@router.message(CommandStart())
async def cmd_start(message: Message):
    if message.chat.title:
        _chat_names[message.chat.id] = message.chat.title
        await db.register_chat(message.chat.id, message.chat.title)
    u = message.from_user
    await db.upsert_user(u.id, u.full_name or u.username or str(u.id),
                         u.username, dm_available=True)
    if message.chat.type == "private":
        await _show_pm_home(message)
    else:
        _chat_names[message.chat.id] = message.chat.title or str(message.chat.id)
        await message.answer(
            "👋 Привет! Используй /queue для работы с очередями в этом чате.\n"
            "А в личке (/start) можно записываться не открывая группу."
        )

@router.message(Command("help"))
async def cmd_help(message: Message):
    if message.chat.title:
        _chat_names[message.chat.id] = message.chat.title
        await db.register_chat(message.chat.id, message.chat.title)
    if message.from_user:
        await db.upsert_user(message.from_user.id,
                             message.from_user.full_name or str(message.from_user.id),
                             message.from_user.username)
    await message.answer(
        "👋 <b>Queue Bot</b>\n\n"
        "<b>В группе:</b> /queue — список очередей\n\n"
        "<b>В личке:</b>\n"
        "  /start — выбрать группу и встать в очередь\n"
        "  /me — твои ники по группам\n"
        "  /myqueues — все твои текущие очереди\n\n"
        "🔔 Когда ты первый — бот пришлёт кнопки «Я готов» / «Выхожу».\n"
        "⚡ Не ответил за N мин — авто-кик (если включён администратором).\n"
        "✏️ Ник устанавливается отдельно для каждой группы через /me.",
        parse_mode="HTML"
    )

# главный экран в личке
async def _show_pm_home(message: Message):

    u = message.from_user
    name = u.first_name or u.full_name or "друг"
    queues = await get_all_queues_for_pm(u.id)
    memberships = await db.get_user_queue_memberships(u.id)
    has_queues = bool(queues)

    if memberships:
        lines = ["📋 <b>Твои активные очереди:</b>"]
        for e in memberships:
            e["chat_name"] = _chat_names.get(e["chat_id"], f"Чат {e['chat_id']}")
            lines.append(f"  • {e['chat_name']} → <b>{e['queue_name']}</b> #{e['position']}")
        text = f"👋 <b>{name}</b>, вот твоё состояние:\n\n" + "\n".join(lines)
    else:
        text = (
            f"👋 Привет, <b>{name}</b>!\n\n"
            f"Я помогаю управлять очередями в Telegram-группах.\n\n"
            f"Ты пока не стоишь ни в одной очереди."
        )

    await message.answer(
        "Используй кнопки внизу 👇",
        reply_markup=pm_reply_keyboard()
    )
    await message.answer(
        text,
        reply_markup=pm_main_keyboard(has_queues=has_queues),
        parse_mode="HTML"
    )

# показываем список групп с очередями
async def _show_pm_start(message: Message):
    u = message.from_user
    name = u.first_name or u.full_name or "друг"
    queues = await get_all_queues_for_pm(u.id)

    header = (
        f"👋 Привет, <b>{name}</b>!\n\n"
        f"Я помогаю управлять очередями в группах.\n"
        f"Всё можно делать прямо здесь — не открывая группу.\n\n"
    )

    if not queues:
        await message.answer(
            header +
            "😕 <b>Активных очередей не найдено.</b>\n\n"
            "Что нужно сделать:\n"
            "1. Добавь меня в группу\n"
            "2. Напиши в группе /queue\n"
            "3. Вернись сюда",
            reply_markup=pm_main_keyboard(has_queues=False),
            parse_mode="HTML"
        )
        return

    kb = pm_chat_select_keyboard(queues, _chat_names)
    await message.answer(
        header + "👇 <b>Выбери группу:</b>",
        reply_markup=kb, parse_mode="HTML"
    )

@router.callback_query(F.data == "pm_start")
async def cb_pm_start(call: CallbackQuery):
    queues = await get_all_queues_for_pm(call.from_user.id)
    if not queues:
        await call.message.edit_text(
            "😕 <b>Активных очередей не найдено.</b>\n\n"
            "Зайди в группу и напиши /queue чтобы создать очередь.",
            reply_markup=pm_main_keyboard(has_queues=False),
            parse_mode="HTML"
        )
    else:
        kb = pm_chat_select_keyboard(queues, _chat_names)
        await call.message.edit_text("👇 <b>Выбери группу:</b>", reply_markup=kb, parse_mode="HTML")
    await call.answer()

@router.callback_query(F.data.startswith("pm_chat:"))
async def cb_pm_chat(call: CallbackQuery):
    chat_id = int(call.data.split(":")[1])
    queues = await db.get_chat_queues(chat_id)
    chat_name = _chat_names.get(chat_id, f"Чат {chat_id}")
    if not queues:
        await call.answer("В этой группе нет активных очередей.", show_alert=True)
        return
    kb = pm_queue_select_keyboard(queues, chat_id)
    await call.message.edit_text(
        f"💬 <b>{chat_name}</b>\n\nВыбери очередь:",
        reply_markup=kb, parse_mode="HTML"
    )
    await call.answer()

@router.callback_query(F.data.startswith("pm_queue:"))
async def cb_pm_queue(call: CallbackQuery):
    queue_id = int(call.data.split(":")[1])
    queue = await db.get_queue(queue_id)
    if not queue:
        await call.answer("Очередь не найдена.", show_alert=True)
        return
    members = await db.get_queue_members(queue_id)
    text = format_queue_info(queue, members)
    user_in = any(m["user_id"] == call.from_user.id for m in members)
    user_is_first = bool(members) and members[0]["user_id"] == call.from_user.id
    is_full = queue["max_slots"] > 0 and len(members) >= queue["max_slots"]
    is_subscribed = await db.is_subscribed(queue_id, call.from_user.id)
    kb = pm_queue_actions_keyboard(queue_id, user_in, not queue["is_active"],
                                   queue["chat_id"], user_is_first, is_full, is_subscribed)
    await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await call.answer()

@router.callback_query(F.data.startswith("pm_join:"))
async def cb_pm_join(call: CallbackQuery):
    queue_id = int(call.data.split(":")[1])
    queue = await db.get_queue(queue_id)
    if not queue or not queue["is_active"]:
        await call.answer("Очередь закрыта.", show_alert=True)
        return
    if queue["max_slots"] > 0 and await db.get_member_count(queue_id) >= queue["max_slots"]:
        await call.answer("😔 Все места заняты!", show_alert=True)
        return

    u = call.from_user
    display = await db.resolve_display_name(u.id, queue["chat_id"],
                                            u.full_name or u.username or str(u.id))
    pos = await db.join_queue(queue_id, u.id, display, u.username or "")
    if pos == -1:
        await call.answer("Ты уже в этой очереди!", show_alert=True)
        return

    await call.answer(f"✅ Место #{pos} занято!", show_alert=True)
    if pos == 1:
        members = await db.get_queue_members(queue_id)
        await notify_became_first(call.bot, queue, members[0], queue["chat_id"])

    members = await db.get_queue_members(queue_id)
    text = format_queue_info(queue, members)
    user_is_first = bool(members) and members[0]["user_id"] == call.from_user.id
    kb = pm_queue_actions_keyboard(queue_id, True, False, queue["chat_id"], user_is_first)
    await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")

@router.callback_query(F.data.startswith("pm_leave:"))
async def cb_pm_leave(call: CallbackQuery):
    queue_id = int(call.data.split(":")[1])
    members_before = await db.get_queue_members(queue_id)
    left = next((m for m in members_before if m["user_id"] == call.from_user.id), None)
    if not left:
        await call.answer("Тебя нет в этой очереди.", show_alert=True)
        return
    was_first = left["position"] == 1
    if not await db.leave_queue(queue_id, call.from_user.id):
        await call.answer("Не удалось выйти.", show_alert=True)
        return
    await call.answer("🚪 Вышел.", show_alert=True)
    queue, members = await after_leave(call.bot, queue_id, call.from_user.id, left, was_first)
    text = format_queue_info(queue, members)
    kb = pm_queue_actions_keyboard(queue_id, False, not queue["is_active"], queue["chat_id"])
    await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")

@router.callback_query(F.data.startswith("confirm_ready:"))
async def cb_confirm_ready(call: CallbackQuery):
    queue_id = int(call.data.split(":")[1])
    queue = await db.get_queue(queue_id)
    members = await db.get_queue_members(queue_id)
    await db.cancel_reminders(queue_id, call.from_user.id)

    member = next((m for m in members if m["user_id"] == call.from_user.id), None)
    pos = member["position"] if member else "?"
    name = queue["name"] if queue else "очередь"

    await call.message.edit_text(
        f"✅ Отлично! Ты подтвердил готовность (место #{pos}) в очереди <b>«{name}»</b>.\n\n"
        f"Когда закончишь — нажми «Выйти» в боте или в группе.",
        parse_mode="HTML"
    )
    await call.answer("Готовность подтверждена!")

@router.callback_query(F.data.startswith("confirm_leave:"))
async def cb_confirm_leave(call: CallbackQuery):
    queue_id = int(call.data.split(":")[1])
    members_before = await db.get_queue_members(queue_id)
    left = next((m for m in members_before if m["user_id"] == call.from_user.id), None)
    if not left:
        await call.message.edit_text("Тебя уже нет в этой очереди.")
        await call.answer()
        return
    was_first = left["position"] == 1
    await db.leave_queue(queue_id, call.from_user.id)
    await call.answer("🚪 Ты вышел из очереди.", show_alert=True)
    queue = await db.get_queue(queue_id)
    members = await db.get_queue_members(queue_id)
    await notify_leave_public(call.bot, queue, left, queue["chat_id"])
    if was_first and members:
        await notify_became_first(call.bot, queue, members[0], queue["chat_id"])
    await db.cancel_reminders(queue_id, call.from_user.id)
    await call.message.edit_text(
        f"🚪 Ты вышел из очереди <b>«{queue['name']}»</b>. Спасибо!",
        parse_mode="HTML"
    )

@router.callback_query(F.data.startswith("done_next:"))
# кнопка "я прошёл" — выходим и уведомляем следующего
async def cb_done_next(call: CallbackQuery):

    queue_id = int(call.data.split(":")[1])
    members_before = await db.get_queue_members(queue_id)
    left = next((m for m in members_before if m["user_id"] == call.from_user.id), None)
    if not left:
        await call.answer("Тебя уже нет в очереди.", show_alert=True)
        return
    was_first = left["position"] == 1
    await db.leave_queue(queue_id, call.from_user.id)
    await db.cancel_reminders(queue_id, call.from_user.id)
    queue = await db.get_queue(queue_id)
    members = await db.get_queue_members(queue_id)
    await notify_leave_public(call.bot, queue, left, queue["chat_id"])
    if was_first and members:
        await notify_became_first(call.bot, queue, members[0], queue["chat_id"])
    await call.answer("✅ Отлично! Следующий уведомлён.", show_alert=True)
    await call.message.edit_text(
        f"✅ <b>Ты прошёл очередь «{queue['name']}»!</b>\n\nСпасибо, следующий уведомлён 🎉",
        parse_mode="HTML"
    )

@router.callback_query(F.data == "pm_home")
async def cb_pm_home(call: CallbackQuery):
    
    u = call.from_user
    name = u.first_name or u.full_name or "друг"
    queues = await get_all_queues_for_pm(u.id)
    memberships = await db.get_user_queue_memberships(u.id)
    has_queues = bool(queues)

    if memberships:
        lines = ["📋 <b>Твои активные очереди:</b>"]
        for e in memberships:
            e["chat_name"] = _chat_names.get(e["chat_id"], f"Чат {e['chat_id']}")
            lines.append(f"  • {e['chat_name']} → <b>{e['queue_name']}</b> #{e['position']}")
        text = f"👋 <b>{name}</b>, вот твоё состояние:\n\n" + "\n".join(lines)
    else:
        text = (
            f"👋 Привет, <b>{name}</b>!\n\n"
            f"Ты пока не стоишь ни в одной очереди.\n"
            f"Нажми «Найти очередь» чтобы записаться."
        )

    await call.message.edit_text(
        text,
        reply_markup=pm_main_keyboard(has_queues=has_queues),
        parse_mode="HTML"
    )
    await call.answer()

@router.callback_query(F.data == "pm_myqueues")
async def cb_pm_myqueues(call: CallbackQuery):
    
    entries = await db.get_user_queue_memberships(call.from_user.id)
    for e in entries:
        e["chat_name"] = _chat_names.get(e["chat_id"], f"Чат {e['chat_id']}")

    if not entries:
        await call.answer("Ты не стоишь ни в одной очереди.", show_alert=True)
        return

    lines = ["📋 <b>Твои активные очереди:</b>\n"]
    for e in entries:
        lines.append(f"💬 {e['chat_name']}\n   └ <b>{e['queue_name']}</b> — место <b>#{e['position']}</b>")

    from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
    buttons = []
    for e in entries:
        buttons.append([InlineKeyboardButton(
            text=f"📋 {e['queue_name']} (#{e['position']})",
            callback_data=f"pm_queue:{e['queue_id']}"
        )])
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data="pm_home")])
    kb = InlineKeyboardMarkup(inline_keyboard=buttons)

    await call.message.edit_text(
        "\n\n".join(lines),
        reply_markup=kb,
        parse_mode="HTML"
    )
    await call.answer()

@router.message(F.chat.type == "private", F.text == "📋 Мои очереди")
async def reply_myqueues(message: Message):
    entries = await db.get_user_queue_memberships(message.from_user.id)
    for e in entries:
        e["chat_name"] = _chat_names.get(e["chat_id"], f"Чат {e['chat_id']}")
    if not entries:
        await message.answer(
            "Ты пока не стоишь ни в одной очереди.\n\nНажми «🔍 Найти очередь» чтобы записаться."
        )
        return
    from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
    lines = ["📋 <b>Твои активные очереди:</b>\n"]
    buttons = []
    for e in entries:
        lines.append(f"💬 {e['chat_name']}\n   └ <b>{e['queue_name']}</b> — место <b>#{e['position']}</b>")
        buttons.append([InlineKeyboardButton(
            text=f"📋 {e['queue_name']} (#{e['position']})",
            callback_data=f"pm_queue:{e['queue_id']}"
        )])
    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    await message.answer("\n\n".join(lines), reply_markup=kb, parse_mode="HTML")

@router.message(F.chat.type == "private", F.text == "🔍 Найти очередь")
async def reply_find_queue(message: Message):
    queues = await get_all_queues_for_pm(message.from_user.id)
    if not queues:
        await message.answer(
            "😕 Не нашёл активных очередей.\n\n"
            "Зайди в группу и напиши /queue — бот запомнит её."
        )
        return
    kb = pm_chat_select_keyboard(queues, _chat_names)
    await message.answer("👇 <b>Выбери группу:</b>", reply_markup=kb, parse_mode="HTML")

@router.message(F.chat.type == "private", F.text == "👤 Профиль / Ник")
async def reply_profile(message: Message):
    await _show_me(message, message.from_user.id)

@router.message(F.chat.type == "private", F.text == "❓ Помощь")
async def reply_help(message: Message):
    await message.answer(
        "👋 <b>Queue Bot — помощь</b>\n\n"
        "<b>Кнопки внизу:</b>\n"
        "📋 Мои очереди — твои текущие позиции\n"
        "🔍 Найти очередь — записаться в очередь группы\n"
        "👤 Профиль / Ник — установить отображаемое имя\n\n"
        "<b>В группе:</b>\n"
        "/queue — список очередей\n"
        "/list — просмотр очередей со всеми участниками\n\n"
        "🔔 Когда ты станешь первым — я пришлю уведомление сюда.\n"
        "✅ Нажми «Я прошёл, следующий!» когда закончишь.",
        parse_mode="HTML"
    )

async def _show_me(message_or_call, user_id: int, edit: bool = False):
    nicks = await db.get_all_group_nicks(user_id)
    has_nick = bool(nicks)
    lines = ["👤 <b>Твои ники по группам</b>\n"]
    if nicks:
        for n in nicks:
            gname = _chat_names.get(n["chat_id"], f"Чат {n['chat_id']}")
            lines.append(f"💬 {gname}: <b>{n['nick']}</b>")
    else:
        lines.append("Кастомных ников нет — используется имя из Telegram.")
    text = "\n".join(lines)
    kb = me_keyboard(has_nick)
    if edit:
        await message_or_call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    else:
        await message_or_call.answer(text, reply_markup=kb, parse_mode="HTML")

@router.message(Command("me"))
async def cmd_me(message: Message):
    await _show_me(message, message.from_user.id)

@router.callback_query(F.data == "show_me")
async def cb_show_me(call: CallbackQuery):
    await _show_me(call, call.from_user.id, edit=True)
    await call.answer()

@router.callback_query(F.data == "set_nick_choose_group")
async def cb_set_nick_choose_group(call: CallbackQuery, state: FSMContext):
    queues = await get_all_queues_for_pm(call.from_user.id)
    if not queues:
        await call.answer("Нет доступных групп.", show_alert=True)
        return
    kb = nick_group_select_keyboard(queues, _chat_names, "set")
    await call.message.edit_text(
        "✏️ Для какой группы установить ник?", reply_markup=kb
    )
    await state.set_state(SetNick.choosing_group)
    await call.answer()

@router.callback_query(F.data.startswith("set_nick_group:"), SetNick.choosing_group)
async def cb_set_nick_group_chosen(call: CallbackQuery, state: FSMContext):
    chat_id = int(call.data.split(":")[1])
    gname = _chat_names.get(chat_id, f"Чат {chat_id}")
    await state.update_data(chat_id=chat_id, msg_id=call.message.message_id)
    await call.message.edit_text(
        f"✏️ Ник для <b>{gname}</b>\n\nВведи ник (до 32 символов):",
        reply_markup=cancel_keyboard(), parse_mode="HTML"
    )
    await state.set_state(SetNick.entering_nick)
    await call.answer()

@router.message(SetNick.entering_nick)
async def fsm_set_nick(message: Message, state: FSMContext):
    nick = message.text.strip()
    if len(nick) > 32:
        await message.answer("Слишком длинный (макс. 32 символа). Попробуй ещё:")
        return
    data = await state.get_data()
    await db.set_group_nick(message.from_user.id, data["chat_id"], nick)
    await state.clear()
    gname = _chat_names.get(data["chat_id"], f"Чат {data['chat_id']}")
    await message.bot.edit_message_text(
        f"✅ Ник <b>{nick}</b> установлен для группы <b>{gname}</b>.\n\n"
        f"Он будет использован при следующем входе в очередь этой группы.",
        chat_id=message.chat.id, message_id=data["msg_id"],
        parse_mode="HTML"
    )

@router.callback_query(F.data == "reset_nick_choose_group")
async def cb_reset_nick_choose_group(call: CallbackQuery, state: FSMContext):
    nicks = await db.get_all_group_nicks(call.from_user.id)
    if not nicks:
        await call.answer("Нет кастомных ников для сброса.", show_alert=True)
        return
    seen, buttons = set(), []
    for n in nicks:
        cid = n["chat_id"]
        if cid in seen:
            continue
        seen.add(cid)
        gname = _chat_names.get(cid, f"Чат {cid}")
        buttons.append([InlineKeyboardButton(
            text=f"💬 {gname} → {n['nick']}",
            callback_data=f"reset_nick_group:{cid}"
        )])
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data="show_me")])
    from aiogram.types import InlineKeyboardMarkup
    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    await call.message.edit_text("🗑 Для какой группы сбросить ник?", reply_markup=kb)
    await state.set_state(ResetNick.choosing_group)
    await call.answer()

@router.callback_query(F.data.startswith("reset_nick_group:"))
async def cb_reset_nick_group(call: CallbackQuery, state: FSMContext):
    chat_id = int(call.data.split(":")[1])
    await db.set_group_nick(call.from_user.id, chat_id, "")
    await state.clear()
    gname = _chat_names.get(chat_id, f"Чат {chat_id}")
    await call.answer(f"Ник для «{gname}» сброшен.", show_alert=True)
    await _show_me(call, call.from_user.id, edit=True)

@router.message(Command("myqueues"))
async def cmd_myqueues(message: Message):
    entries = await db.get_user_queue_memberships(message.from_user.id)
    for e in entries:
        e["chat_name"] = _chat_names.get(e["chat_id"], f"Чат {e['chat_id']}")
    await message.answer(format_pm_my_queues(entries), parse_mode="HTML")

@router.message(Command("list"))
# /list — все очереди с участниками одним сообщением
async def cmd_list(message: Message):

    if message.chat.type == "private":
        await message.answer("Эта команда работает только в группах.")
        return
    queues = await db.get_chat_queues(message.chat.id)
    if not queues:
        await message.answer("В этом чате нет активных очередей.")
        return
    parts = []
    for q in queues:
        members = await db.get_queue_members(q["id"])
        parts.append(format_queue_info(q, members))
    await message.answer("\n\n—————\n\n".join(parts), parse_mode="HTML")


@router.message(Command("myplace"))
async def cmd_myplace(message: Message):
    if message.chat.type == "private":
        await message.answer("Используй эту команду в группе.")
        return
    memberships = await db.get_user_queue_memberships(message.from_user.id)
    chat_memberships = [m for m in memberships if m["chat_id"] == message.chat.id]
    if not chat_memberships:
        await message.answer("Ты не стоишь ни в одной очереди этого чата.")
        return
    lines = ["\U0001f4cd <b>Твои места в очередях:</b>\n"]
    for m in chat_memberships:
        total = await db.get_member_count(m["queue_id"])
        lines.append(f"\U0001f4cb <b>{m['queue_name']}</b> — место <b>#{m['position']}</b> из {total}")
    await message.answer("\n".join(lines), parse_mode="HTML")


@router.message(Command("myplace"))
async def cmd_myplace(message: Message):
    if message.chat.type == "private":
        await message.answer("Используй эту команду в группе.")
        return
    memberships = await db.get_user_queue_memberships(message.from_user.id)
    chat_memberships = [m for m in memberships if m["chat_id"] == message.chat.id]
    if not chat_memberships:
        await message.answer("Ты не стоишь ни в одной очереди этого чата.")
        return
    lines = ["📍 <b>Твои места в очередях:</b>\n"]
    for m in chat_memberships:
        total = await db.get_member_count(m["queue_id"])
        lines.append(f"📋 <b>{m['queue_name']}</b> — место <b>#{m['position']}</b> из {total}")
    await message.answer("\n".join(lines), parse_mode="HTML")

@router.message(Command("queue"))
# /queue в группе — показываем список очередей
async def cmd_queue(message: Message):
    if message.chat.title:
        _chat_names[message.chat.id] = message.chat.title
        await db.register_chat(message.chat.id, message.chat.title)
    if message.from_user:
        await db.upsert_user(message.from_user.id, 
                             message.from_user.full_name or str(message.from_user.id),
                             message.from_user.username)

    if message.chat.type == "private":
        await message.answer("Используй /start в личке. 😊")
        return

    queues = await db.get_chat_queues(message.chat.id)
    admin = await is_admin(message.bot, message.chat.id, message.from_user.id)
    await message.answer(format_queue_list(queues),
                         reply_markup=queue_list_keyboard(queues, admin),
                         parse_mode="HTML")

@router.callback_query(F.data == "back_to_list")
async def cb_back_to_list(call: CallbackQuery):
    queues = await db.get_chat_queues(call.message.chat.id)
    admin = await is_admin(call.bot, call.message.chat.id, call.from_user.id)
    await call.message.edit_text(format_queue_list(queues),
                                 reply_markup=queue_list_keyboard(queues, admin),
                                 parse_mode="HTML")

@router.callback_query(F.data.startswith("view_queue:"))
async def cb_view_queue(call: CallbackQuery):
    queue_id = int(call.data.split(":")[1])
    queue = await db.get_queue(queue_id)
    if not queue:
        await call.answer("Очередь не найдена.", show_alert=True)
        return
    members = await db.get_queue_members(queue_id)
    user_in = any(m["user_id"] == call.from_user.id for m in members)
    admin = await is_admin(call.bot, call.message.chat.id, call.from_user.id)
    await call.message.edit_text(format_queue_info(queue, members),
                                 reply_markup=queue_actions_keyboard(queue_id, user_in, admin, not queue["is_active"]),
                                 parse_mode="HTML")
    await call.answer()

@router.callback_query(F.data.startswith("join:"))
# встаём в очередь прямо в группе
async def cb_join(call: CallbackQuery):
    queue_id = int(call.data.split(":")[1])
    queue = await db.get_queue(queue_id)
    if not queue or not queue["is_active"]:
        await call.answer("Очередь закрыта.", show_alert=True)
        return
    if queue["max_slots"] > 0 and await db.get_member_count(queue_id) >= queue["max_slots"]:
        await call.answer("😔 Все места заняты!", show_alert=True)
        return

    u = call.from_user
    await db.upsert_user(u.id, u.full_name or u.username or str(u.id), u.username)
    display = await db.resolve_display_name(u.id, call.message.chat.id,
                                            u.full_name or u.username or str(u.id))
    pos = await db.join_queue(queue_id, u.id, display, u.username or "")
    if pos == -1:
        await call.answer("Ты уже в этой очереди!", show_alert=True)
        return

    await call.answer(f"✅ Место #{pos} занято!", show_alert=True)
    members = await db.get_queue_members(queue_id)
    if pos == 1:
        await notify_became_first(call.bot, queue, members[0], call.message.chat.id)
    else:
        member = next((m for m in members if m["user_id"] == call.from_user.id), None)
        if member:
            await notify_approaching(call.bot, queue, member, pos)

    admin = await is_admin(call.bot, call.message.chat.id, call.from_user.id)
    user_is_first = bool(members) and members[0]["user_id"] == call.from_user.id
    await call.message.edit_text(format_queue_info(queue, members),
                                 reply_markup=queue_actions_keyboard(queue_id, True, admin, False, user_is_first),
                                 parse_mode="HTML")

@router.callback_query(F.data.startswith("leave:"))
# выходим из очереди через кнопку в группе
async def cb_leave(call: CallbackQuery):
    queue_id = int(call.data.split(":")[1])
    members_before = await db.get_queue_members(queue_id)
    left = next((m for m in members_before if m["user_id"] == call.from_user.id), None)
    if not left:
        await call.answer("Тебя нет в этой очереди.", show_alert=True)
        return
    was_first = left["position"] == 1
    if not await db.leave_queue(queue_id, call.from_user.id):
        await call.answer("Не удалось выйти.", show_alert=True)
        return
    await call.answer("🚪 Вышел.", show_alert=True)
    queue, members = await after_leave(call.bot, queue_id, call.from_user.id, left, was_first)
    if queue["max_slots"] > 0:
        await notify_slot_available(call.bot, queue)
    admin = await is_admin(call.bot, call.message.chat.id, call.from_user.id)
    user_is_first = bool(members) and members[0]["user_id"] == call.from_user.id
    await call.message.edit_text(format_queue_info(queue, members),
                                 reply_markup=queue_actions_keyboard(queue_id, False, admin, not queue["is_active"], user_is_first),
                                 parse_mode="HTML")

@router.callback_query(F.data == "create_queue")
async def cb_create_queue(call: CallbackQuery, state: FSMContext):
    if not await is_admin(call.bot, call.message.chat.id, call.from_user.id):
        await call.answer("❌ Только администраторы.", show_alert=True)
        return
    await state.update_data(chat_id=call.message.chat.id, msg_id=call.message.message_id)
    await call.message.edit_text("📝 <b>Новая очередь</b>\n\nНазвание:",
                                 reply_markup=cancel_keyboard(), parse_mode="HTML")
    await state.set_state(CreateQueue.name)
    await call.answer()

@router.message(CreateQueue.name)
async def fsm_name(message: Message, state: FSMContext):
    name = message.text.strip()
    if len(name) > 100:
        await message.answer("Макс. 100 символов:")
        return
    await state.update_data(name=name)
    d = await state.get_data()
    await message.bot.edit_message_text(
        f"📝 <b>{name}</b>\n\nОписание (или «-» пропустить):",
        chat_id=message.chat.id, message_id=d["msg_id"],
        reply_markup=cancel_keyboard(), parse_mode="HTML")
    await state.set_state(CreateQueue.description)

@router.message(CreateQueue.description)
async def fsm_desc(message: Message, state: FSMContext):
    desc = message.text.strip()
    await state.update_data(description=None if desc == "-" else desc)
    d = await state.get_data()
    await message.bot.edit_message_text(
        "Макс. мест (0 = без ограничений):",
        chat_id=message.chat.id, message_id=d["msg_id"],
        reply_markup=cancel_keyboard())
    await state.set_state(CreateQueue.max_slots)

@router.message(CreateQueue.max_slots)
async def fsm_slots(message: Message, state: FSMContext):
    try:
        slots = int(message.text.strip())
        if slots < 0: raise ValueError
    except ValueError:
        await message.answer("Введи число ≥ 0:")
        return
    await state.update_data(max_slots=slots)
    d = await state.get_data()
    await message.bot.edit_message_text(
        "⏱ Через сколько минут напоминать #1 / авто-кик? (1–60, рекомендую 5):",
        chat_id=message.chat.id, message_id=d["msg_id"],
        reply_markup=cancel_keyboard())
    await state.set_state(CreateQueue.remind_min)

@router.message(CreateQueue.remind_min)
async def fsm_remind(message: Message, state: FSMContext):
    try:
        mins = int(message.text.strip())
        if not (1 <= mins <= 60): raise ValueError
    except ValueError:
        await message.answer("Введи число от 1 до 60:")
        return
    d = await state.get_data()
    queue_id = await db.create_queue(
        chat_id=d["chat_id"], name=d["name"],
        description=d.get("description"), max_slots=d["max_slots"],
        created_by=message.from_user.id, remind_timeout_min=mins,
        notify_leave_public=True, auto_kick=True
    )
    await state.clear()
    queue = await db.get_queue(queue_id)
    await message.bot.edit_message_text(
        format_queue_info(queue, []),
        chat_id=d["chat_id"], message_id=d["msg_id"],
        reply_markup=queue_actions_keyboard(queue_id, False, True, False),
        parse_mode="HTML")

@router.callback_query(F.data == "cancel_fsm")
async def cb_cancel(call: CallbackQuery, state: FSMContext):
    await state.clear()
    if call.message.chat.type == "private":
        await call.message.edit_text("Отменено.")
        await call.answer()
        return
    queues = await db.get_chat_queues(call.message.chat.id)
    admin = await is_admin(call.bot, call.message.chat.id, call.from_user.id)
    await call.message.edit_text(format_queue_list(queues),
                                 reply_markup=queue_list_keyboard(queues, admin),
                                 parse_mode="HTML")
    await call.answer("Отменено")

@router.callback_query(F.data.startswith("queue_settings:"))
async def cb_queue_settings(call: CallbackQuery):
    queue_id = int(call.data.split(":")[1])
    if not await is_admin(call.bot, call.message.chat.id, call.from_user.id):
        await call.answer("❌ Только администраторы.", show_alert=True)
        return
    q = await db.get_queue(queue_id)
    await call.message.edit_text(
        f"⚙️ <b>Настройки «{q['name']}»</b>",
        reply_markup=queue_settings_keyboard(
            queue_id, bool(q["notify_leave_public"]),
            q["remind_timeout_min"], bool(q["auto_kick"])
        ), parse_mode="HTML")
    await call.answer()

@router.callback_query(F.data.startswith("toggle_leave_notif:"))
async def cb_toggle_leave(call: CallbackQuery):
    queue_id = int(call.data.split(":")[1])
    if not await is_admin(call.bot, call.message.chat.id, call.from_user.id):
        await call.answer("❌", show_alert=True)
        return
    q = await db.get_queue(queue_id)
    new = not bool(q["notify_leave_public"])
    await db.update_queue_settings(queue_id, q["remind_timeout_min"], new, bool(q["auto_kick"]))
    q = await db.get_queue(queue_id)
    await call.message.edit_reply_markup(
        reply_markup=queue_settings_keyboard(queue_id, bool(q["notify_leave_public"]),
                                             q["remind_timeout_min"], bool(q["auto_kick"])))
    await call.answer("Анонсы выхода " + ("включены." if new else "выключены."))

@router.callback_query(F.data.startswith("toggle_autokick:"))
async def cb_toggle_autokick(call: CallbackQuery):
    queue_id = int(call.data.split(":")[1])
    if not await is_admin(call.bot, call.message.chat.id, call.from_user.id):
        await call.answer("❌", show_alert=True)
        return
    q = await db.get_queue(queue_id)
    new = not bool(q["auto_kick"])
    await db.update_queue_settings(queue_id, q["remind_timeout_min"], bool(q["notify_leave_public"]), new)
    q = await db.get_queue(queue_id)
    await call.message.edit_reply_markup(
        reply_markup=queue_settings_keyboard(queue_id, bool(q["notify_leave_public"]),
                                             q["remind_timeout_min"], bool(q["auto_kick"])))
    await call.answer("Авто-кик " + ("включён." if new else "выключен."))

@router.callback_query(F.data.startswith("set_remind:"))
async def cb_set_remind(call: CallbackQuery, state: FSMContext):
    queue_id = int(call.data.split(":")[1])
    if not await is_admin(call.bot, call.message.chat.id, call.from_user.id):
        await call.answer("❌", show_alert=True)
        return
    await state.update_data(queue_id=queue_id, msg_id=call.message.message_id)
    await call.message.edit_text("⏱ Введи таймаут в минутах (1–60):",
                                 reply_markup=cancel_keyboard())
    await state.set_state(SetRemind.minutes)
    await call.answer()

@router.message(SetRemind.minutes)
async def fsm_remind_set(message: Message, state: FSMContext):
    try:
        mins = int(message.text.strip())
        if not (1 <= mins <= 60): raise ValueError
    except ValueError:
        await message.answer("Число от 1 до 60:")
        return
    d = await state.get_data()
    queue_id = d["queue_id"]
    q = await db.get_queue(queue_id)
    await db.update_queue_settings(queue_id, mins, bool(q["notify_leave_public"]), bool(q["auto_kick"]))
    await state.clear()
    q = await db.get_queue(queue_id)
    await message.bot.edit_message_text(
        f"⚙️ <b>Настройки «{q['name']}»</b>",
        chat_id=message.chat.id, message_id=d["msg_id"],
        reply_markup=queue_settings_keyboard(queue_id, bool(q["notify_leave_public"]),
                                             q["remind_timeout_min"], bool(q["auto_kick"])),
        parse_mode="HTML")

@router.callback_query(F.data.startswith("close_queue:"))
async def cb_close(call: CallbackQuery):
    queue_id = int(call.data.split(":")[1])
    if not await is_admin(call.bot, call.message.chat.id, call.from_user.id):
        await call.answer("❌", show_alert=True)
        return
    await call.message.edit_text("🔒 Закрыть очередь?",
                                 reply_markup=confirm_keyboard("close", queue_id))

@router.callback_query(F.data.startswith("confirm_close:"))
async def cb_confirm_close(call: CallbackQuery):
    queue_id = int(call.data.split(":")[1])
    await db.close_queue(queue_id)
    await call.answer("🔒 Закрыта.", show_alert=True)
    q = await db.get_queue(queue_id)
    members = await db.get_queue_members(queue_id)
    await call.message.edit_text(format_queue_info(q, members),
                                 reply_markup=queue_actions_keyboard(queue_id, False, True, True),
                                 parse_mode="HTML")

@router.callback_query(F.data.startswith("delete_queue:"))
async def cb_delete(call: CallbackQuery):
    queue_id = int(call.data.split(":")[1])
    if not await is_admin(call.bot, call.message.chat.id, call.from_user.id):
        await call.answer("❌", show_alert=True)
        return
    await call.message.edit_text("🗑 <b>Удалить очередь?</b> Это необратимо.",
                                 reply_markup=confirm_keyboard("delete", queue_id),
                                 parse_mode="HTML")

@router.callback_query(F.data.startswith("confirm_delete:"))
async def cb_confirm_delete(call: CallbackQuery):
    queue_id = int(call.data.split(":")[1])
    await db.delete_queue(queue_id)
    await call.answer("🗑 Удалена.", show_alert=True)
    queues = await db.get_chat_queues(call.message.chat.id)
    admin = await is_admin(call.bot, call.message.chat.id, call.from_user.id)
    await call.message.edit_text(format_queue_list(queues),
                                 reply_markup=queue_list_keyboard(queues, admin),
                                 parse_mode="HTML")

@router.callback_query(F.data.startswith("kick_menu:"))
async def cb_kick_menu(call: CallbackQuery):
    queue_id = int(call.data.split(":")[1])
    if not await is_admin(call.bot, call.message.chat.id, call.from_user.id):
        await call.answer("❌", show_alert=True)
        return
    members = await db.get_queue_members(queue_id)
    if not members:
        await call.answer("Очередь пуста.", show_alert=True)
        return
    await call.message.edit_text("👢 Выбери участника:",
                                 reply_markup=kick_members_keyboard(queue_id, members))
    await call.answer()

@router.callback_query(F.data.startswith("kick:"))
# кикаем участника из очереди
async def cb_kick(call: CallbackQuery):
    _, qid_s, uid_s = call.data.split(":")
    queue_id, user_id = int(qid_s), int(uid_s)
    if not await is_admin(call.bot, call.message.chat.id, call.from_user.id):
        await call.answer("❌", show_alert=True)
        return
    members_before = await db.get_queue_members(queue_id)
    left = next((m for m in members_before if m["user_id"] == user_id), None)
    if not left:
        await call.answer("Участник не найден.", show_alert=True)
        return
    was_first = left["position"] == 1
    await db.kick_member(queue_id, user_id)
    await call.answer("👢 Удалён.", show_alert=True)
    queue = await db.get_queue(queue_id)
    await notify_kicked(call.bot, queue, user_id, by_timeout=False)
    members = await db.get_queue_members(queue_id)
    if was_first and members:
        await notify_became_first(call.bot, queue, members[0], call.message.chat.id)
    await call.message.edit_text(format_queue_info(queue, members),
                                 reply_markup=queue_actions_keyboard(queue_id, False, True, not queue["is_active"]),
                                 parse_mode="HTML")

@router.callback_query(F.data.startswith("export:"))
# экспортируем список очереди в csv
async def cb_export(call: CallbackQuery):
    queue_id = int(call.data.split(":")[1])
    if not await is_admin(call.bot, call.message.chat.id, call.from_user.id):
        await call.answer("❌", show_alert=True)
        return
    queue = await db.get_queue(queue_id)
    members = await db.get_queue_members(queue_id)
    await call.answer("⏳ Генерирую...")
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["#", "Ник в очереди", "Telegram username", "User ID", "Вступил"])
    for m in members:
        w.writerow([m["position"], m["display_name"],
                    f"@{m['username']}" if m.get("username") else "",
                    m["user_id"], m.get("joined_at", "")])
    csv_bytes = out.getvalue().encode("utf-8-sig")
    fname = f"queue_{queue['name'].replace(' ', '_')}_{queue_id}.csv"
    await call.message.answer_document(
        BufferedInputFile(csv_bytes, filename=fname),
        caption=f"📥 <b>{queue['name']}</b> — {len(members)} участников",
        parse_mode="HTML"
    )

@router.callback_query(F.data.startswith("gen_invite:"))
# генерируем ссылку-приглашение в очередь
async def cb_gen_invite(call: CallbackQuery):
    queue_id = int(call.data.split(":")[1])
    queue = await db.get_queue(queue_id)
    if not queue:
        await call.answer("Очередь не найдена.", show_alert=True)
        return
    if not await is_admin(call.bot, queue["chat_id"], call.from_user.id):
        await call.answer("❌ Только администраторы.", show_alert=True)
        return

    from config import BOT_TOKEN
    bot_info = await call.bot.get_me()
    token = await db.create_invite(queue_id, call.from_user.id)
    link = f"https://t.me/{bot_info.username}?start=invite_{token}"

    await call.message.answer(
        f"🔗 <b>Ссылка-приглашение в очередь «{queue['name']}»:</b>\n\n"
        f"<code>{link}</code>\n\n"
        f"Человек нажмёт на ссылку и сразу встанет в очередь.",
        parse_mode="HTML"
    )
    await call.answer()

@router.message(CommandStart(deep_link=True))
# обрабатываем переход по ссылке-приглашению
async def cmd_start_invite(message: Message):
    
    u = message.from_user
    await db.upsert_user(u.id, u.full_name or u.username or str(u.id),
                         u.username, dm_available=True)

    args = message.text.split(maxsplit=1)
    param = args[1] if len(args) > 1 else ""

    if param.startswith("invite_"):
        token = param[7:]
        invite = await db.get_invite(token)
        if not invite:
            await message.answer("❌ Ссылка недействительна или уже использована.")
            return

        queue = await db.get_queue(invite["queue_id"])
        if not queue or not queue["is_active"]:
            await message.answer("❌ Очередь закрыта.")
            return

        if queue["max_slots"] > 0 and await db.get_member_count(queue["id"]) >= queue["max_slots"]:
            await message.answer(f"😔 В очереди «{queue['name']}» нет свободных мест.")
            return

        display = await db.resolve_display_name(u.id, queue["chat_id"],
                                                u.full_name or u.username or str(u.id))
        pos = await db.join_queue(queue["id"], u.id, display, u.username or "")
        if pos == -1:
            await message.answer(f"Ты уже стоишь в очереди «{queue['name']}»!")
            return

        await message.answer(
            f"✅ <b>Ты встал в очередь «{queue['name']}»!</b>\n\n"
            f"Твоё место: <b>#{pos}</b>\n"
            f"Я уведомлю тебя когда подойдёт очередь.",
            parse_mode="HTML"
        )
        members = await db.get_queue_members(queue["id"])
        if pos == 1:
            await notify_became_first(message.bot, queue, members[0], queue["chat_id"])
        return

    await _show_pm_home(message)

@router.callback_query(F.data.startswith("freeze_menu:"))
# меню заморозки места
async def cb_freeze_menu(call: CallbackQuery):
    queue_id = int(call.data.split(":")[1])
    member = await db.get_member(queue_id, call.from_user.id)
    if not member:
        await call.answer("Тебя нет в этой очереди.", show_alert=True)
        return
    frozen = await db.is_frozen(queue_id, call.from_user.id)
    if frozen:
        await call.answer("Ты уже заморожен. Сначала разморозься.", show_alert=True)
        return
    queue = await db.get_queue(queue_id)
    await call.message.edit_text(
        f"🧊 <b>Заморозка места в «{queue['name']}»</b>\n\n"
        f"Ты временно выйдешь из уведомлений, но сохранишь позицию #{member['position']}.\n"
        f"На сколько заморозить?",
        reply_markup=freeze_keyboard(queue_id),
        parse_mode="HTML"
    )
    await call.answer()

@router.callback_query(F.data.startswith("freeze:"))
# замораживаем место на выбранное время
async def cb_freeze(call: CallbackQuery):
    parts = call.data.split(":")
    queue_id, minutes = int(parts[1]), int(parts[2])
    member = await db.get_member(queue_id, call.from_user.id)
    if not member:
        await call.answer("Тебя нет в очереди.", show_alert=True)
        return
    await db.freeze_member(queue_id, call.from_user.id, minutes)
    queue = await db.get_queue(queue_id)
    await call.answer(f"🧊 Заморожен на {minutes} мин. Позиция #{member['position']} сохранена.", show_alert=True)
    members = await db.get_queue_members(queue_id)
    text = format_queue_info(queue, members)
    user_is_first = bool(members) and members[0]["user_id"] == call.from_user.id
    kb = pm_queue_actions_keyboard(queue_id, True, not queue["is_active"],
                                   queue["chat_id"], user_is_first)
    await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")

@router.callback_query(F.data.startswith("unfreeze:"))
async def cb_unfreeze(call: CallbackQuery):
    queue_id = int(call.data.split(":")[1])
    await db.unfreeze_member(queue_id, call.from_user.id)
    queue = await db.get_queue(queue_id)
    member = await db.get_member(queue_id, call.from_user.id)
    await call.answer(f"✅ Разморожен! Позиция #{member['position']} активна.", show_alert=True)
    members = await db.get_queue_members(queue_id)
    text = format_queue_info(queue, members)
    user_is_first = bool(members) and members[0]["user_id"] == call.from_user.id
    kb = pm_queue_actions_keyboard(queue_id, True, not queue["is_active"],
                                   queue["chat_id"], user_is_first)
    await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")

@router.callback_query(F.data.startswith("swap_menu:"))
# показываем список с кем можно поменяться
async def cb_swap_menu(call: CallbackQuery):
    queue_id = int(call.data.split(":")[1])
    my_member = await db.get_member(queue_id, call.from_user.id)
    if not my_member:
        await call.answer("Тебя нет в этой очереди.", show_alert=True)
        return
    members = await db.get_queue_members(queue_id)
    others = [m for m in members if m["user_id"] != call.from_user.id]
    if not others:
        await call.answer("Больше никого нет в очереди.", show_alert=True)
        return
    queue = await db.get_queue(queue_id)
    await call.message.edit_text(
        f"🔀 <b>Обмен позицией в «{queue['name']}»</b>\n\n"
        f"Твоя позиция: <b>#{my_member['position']}</b>\n\n"
        f"С кем хочешь поменяться?",
        reply_markup=swap_select_keyboard(queue_id, members, call.from_user.id),
        parse_mode="HTML"
    )
    await call.answer()

@router.callback_query(F.data.startswith("swap_request:"))
# отправляем запрос на обмен другому участнику
async def cb_swap_request(call: CallbackQuery):
    _, queue_id_s, to_user_s = call.data.split(":")
    queue_id, to_user_id = int(queue_id_s), int(to_user_s)

    my_member = await db.get_member(queue_id, call.from_user.id)
    target_member = await db.get_member(queue_id, to_user_id)
    if not my_member or not target_member:
        await call.answer("Кто-то уже вышел из очереди.", show_alert=True)
        return

    queue = await db.get_queue(queue_id)
    request_id = await db.create_swap_request(queue_id, call.from_user.id, to_user_id)

    from notifications import safe_dm
    my_name = my_member["display_name"]
    sent = await safe_dm(
        call.bot, to_user_id,
        f"🔀 <b>{my_name}</b> хочет поменяться с тобой местами в очереди <b>«{queue['name']}»</b>\n\n"
        f"Их место: <b>#{my_member['position']}</b> → твоё место: <b>#{target_member['position']}</b>",
        reply_markup=swap_confirm_keyboard(request_id),
        parse_mode="HTML"
    )

    if sent:
        await call.answer(f"✅ Запрос отправлен {target_member['display_name']}. Ждём ответа.", show_alert=True)
    else:
        await call.answer("😔 Не удалось отправить запрос — пользователь не писал боту в личку.", show_alert=True)

    members = await db.get_queue_members(queue_id)
    text = format_queue_info(queue, members)
    user_is_first = bool(members) and members[0]["user_id"] == call.from_user.id
    kb = pm_queue_actions_keyboard(queue_id, True, not queue["is_active"],
                                   queue["chat_id"], user_is_first)
    await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")

@router.callback_query(F.data.startswith("swap_accept:"))
# принимаем обмен позициями
async def cb_swap_accept(call: CallbackQuery):
    request_id = int(call.data.split(":")[1])
    req = await db.get_swap_request(request_id)
    if not req or req["status"] != "pending":
        await call.message.edit_text("❌ Запрос уже недействителен.")
        return

    success = await db.execute_swap(req["queue_id"], req["from_user"], req["to_user"])
    if not success:
        await call.message.edit_text("❌ Не удалось выполнить обмен — кто-то уже вышел из очереди.")
        return

    queue = await db.get_queue(req["queue_id"])
    my_pos = (await db.get_member(req["queue_id"], call.from_user.id))["position"]
    their_pos = (await db.get_member(req["queue_id"], req["from_user"]))["position"]

    await call.message.edit_text(
        f"✅ <b>Обмен выполнен!</b>\n\n"
        f"Твоя новая позиция: <b>#{my_pos}</b> в очереди «{queue['name']}».",
        parse_mode="HTML"
    )

    from notifications import safe_dm
    requester_member = await db.get_member(req["queue_id"], req["from_user"])
    if requester_member:
        target_name = (await db.get_member(req["queue_id"], call.from_user.id))
        await safe_dm(
            call.bot, req["from_user"],
            f"✅ <b>Обмен принят!</b>\n\n"
            f"Твоя новая позиция: <b>#{their_pos}</b> в очереди «{queue['name']}».",
            parse_mode="HTML"
        )
    await call.answer()

@router.callback_query(F.data.startswith("swap_decline:"))
async def cb_swap_decline(call: CallbackQuery):
    request_id = int(call.data.split(":")[1])
    req = await db.get_swap_request(request_id)
    if not req:
        await call.message.edit_text("Запрос не найден.")
        return
    await db.decline_swap(request_id)
    queue = await db.get_queue(req["queue_id"])
    await call.message.edit_text(
        f"❌ Ты отклонил запрос на обмен в очереди «{queue['name'] if queue else '?'}»."
    )
    from notifications import safe_dm
    await safe_dm(
        call.bot, req["from_user"],
        f"❌ Твой запрос на обмен в очереди «{queue['name'] if queue else '?'}» отклонён.",
    )
    await call.answer()

@router.callback_query(F.data.startswith("subscribe:"))
async def cb_subscribe(call: CallbackQuery):
    queue_id = int(call.data.split(":")[1])
    queue = await db.get_queue(queue_id)
    if not queue:
        await call.answer("Очередь не найдена.", show_alert=True)
        return
    added = await db.subscribe_queue(queue_id, call.from_user.id)
    if added:
        await call.answer(
            f"🔔 Подписался! Уведомлю когда появится место в «{queue['name']}».",
            show_alert=True
        )
    else:
        await call.answer("Ты уже подписан на эту очередь.", show_alert=True)

@router.callback_query(F.data.startswith("unsubscribe:"))
async def cb_unsubscribe(call: CallbackQuery):
    queue_id = int(call.data.split(":")[1])
    await db.unsubscribe_queue(queue_id, call.from_user.id)
    await call.answer("🔕 Подписка отменена.", show_alert=True)


# команды управления бот-админами — только для владельца бота
@router.message(Command("addadmin"))
async def cmd_addadmin(message: Message):
    from config import BOT_OWNER_ID
    if message.from_user.id != BOT_OWNER_ID:
        await message.answer("❌ Только владелец бота может это делать.")
        return

    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.answer(
            "Использование: /addadmin @username или /addadmin 123456789\n\n"
            "Пользователь должен хотя бы раз написать боту /start."
        )
        return

    target = args[1].strip().lstrip("@")

    # ищем по username или user_id
    if target.isdigit():
        row = await db.get_user_profile(int(target))
    else:
        row = await db.get_user_profile_by_username(target)

    if not row:
        await message.answer(
            "❌ Пользователь не найден в базе.\n"
            "Попроси его написать /start боту в личке."
        )
        return

    user_id = row["user_id"]
    name = row["full_name"] or row["username"] or str(user_id)
    added = await db.add_bot_admin(user_id)

    if added:
        await message.answer(f"✅ {name} теперь бот-администратор во всех группах.")
        from notifications import safe_dm
        await safe_dm(
            message.bot, user_id,
            "✅ Тебе выданы права администратора бота во всех группах."
        )
    else:
        await message.answer(f"ℹ️ {name} уже является бот-администратором.")


@router.message(Command("removeadmin"))
async def cmd_removeadmin(message: Message):
    from config import BOT_OWNER_ID
    if message.from_user.id != BOT_OWNER_ID:
        await message.answer("❌ Только владелец бота может это делать.")
        return

    if message.chat.type == "private":
        await message.answer("❌ Команду нужно использовать в группе.")
        return

    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.answer("Использование: /removeadmin @username или /removeadmin 123456789")
        return

    target = args[1].strip().lstrip("@")
    chat_id = message.chat.id

    if target.isdigit():
        row = await db.get_user_profile(int(target))
    else:
        row = await db.get_user_profile_by_username(target)

    if not row:
        await message.answer("❌ Пользователь не найден.")
        return

    user_id = row["user_id"]
    name = row["full_name"] or row["username"] or str(user_id)
    await db.remove_bot_admin(user_id, chat_id)
    await message.answer(f"✅ Права администратора в этой группе сняты с {name}.")


@router.message(Command("admins"))
async def cmd_admins(message: Message):
    from config import BOT_OWNER_ID
    if message.from_user.id != BOT_OWNER_ID:
        await message.answer("❌ Только владелец бота.")
        return

    if message.chat.type == "private":
        await message.answer("❌ Используй команду в группе.")
        return

    admins = await db.get_bot_admins(message.chat.id)
    if not admins:
        await message.answer("В этой группе нет бот-администраторов.")
        return

    lines = [f"👑 <b>Бот-администраторы в {message.chat.title}:</b>\n"]
    for a in admins:
        name = a.get("full_name") or a.get("username") or str(a["user_id"])
        username = f" (@{a['username']})" if a.get("username") else ""
        lines.append(f"• {name}{username} — <code>{a['user_id']}</code>")

    await message.answer("\n".join(lines), parse_mode="HTML")

@router.callback_query(F.data == "noop")
async def cb_noop(call: CallbackQuery):
    await call.answer()