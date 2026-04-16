"""
Хендлеры для управления расписанием учебных групп.
"""
import logging
from datetime import datetime

from aiogram import Router, F, Bot
from aiogram.types import Message, CallbackQuery
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

import database as db
import schedule_db as sdb
from schedule_ocr import (
    parse_schedule_image, parse_change_image,
    parse_change_text, format_schedule
)
from schedule_manager import get_today_schedule, get_week_schedule, WEEKDAY_NAMES

logger = logging.getLogger(__name__)
sched_router = Router()


class ScheduleSetup(StatesGroup):
    waiting_group_name = State()
    waiting_schedule_image = State()


async def is_admin(bot: Bot, chat_id: int, user_id: int) -> bool:
    try:
        m = await bot.get_chat_member(chat_id, user_id)
        return m.status in ("administrator", "creator")
    except Exception:
        return False


@sched_router.message(Command("schedule_setup"))
async def cmd_schedule_setup(message: Message, state: FSMContext):
    if message.chat.type == "private":
        await message.answer("Команду нужно использовать в группе.")
        return
    if not await is_admin(message.bot, message.chat.id, message.from_user.id):
        await message.answer("Только администраторы.")
        return

    group = await sdb.get_study_group(message.chat.id)
    if group:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📸 Загрузить новое расписание",
                                  callback_data="sched_upload_new")],
            [InlineKeyboardButton(text="📋 Вся неделя", callback_data="sched_show")],
            [InlineKeyboardButton(text="📅 На сегодня", callback_data="sched_today")],
        ])
        await message.answer(
            f"Группа: <b>{group['group_name']}</b>\n\nВыбери действие:",
            reply_markup=kb, parse_mode="HTML"
        )
    else:
        await message.answer("Введи название учебной группы (например: ИТ-21):")
        await state.update_data(chat_id=message.chat.id)
        await state.set_state(ScheduleSetup.waiting_group_name)


@sched_router.message(ScheduleSetup.waiting_group_name)
async def fsm_group_name(message: Message, state: FSMContext):
    name = message.text.strip()
    data = await state.get_data()
    chat_id = data.get("chat_id", message.chat.id)
    group_id = await sdb.create_study_group(chat_id, name)
    await state.update_data(group_id=group_id)
    await message.answer(
        f"Группа <b>{name}</b> создана!\n\nОтправь фото с расписанием — распознаю автоматически.",
        parse_mode="HTML"
    )
    await state.set_state(ScheduleSetup.waiting_schedule_image)


@sched_router.message(ScheduleSetup.waiting_schedule_image, F.photo)
async def fsm_schedule_image(message: Message, state: FSMContext):
    data = await state.get_data()
    group_id = data.get("group_id")
    if not group_id:
        group = await sdb.get_study_group(message.chat.id)
        if not group:
            await state.clear()
            return
        group_id = group["id"]

    msg = await message.answer("Распознаю расписание...")
    photo = message.photo[-1]
    file = await message.bot.get_file(photo.file_id)
    image_bytes = (await message.bot.download_file(file.file_path)).read()

    result = await parse_schedule_image(image_bytes)
    if not result or not result.get("lessons"):
        await msg.edit_text(
            "Не удалось распознать расписание. Убедись что фото чёткое и это расписание занятий."
        )
        await state.clear()
        return

    lessons = result["lessons"]
    await sdb.save_lessons(group_id, lessons)
    await sdb.save_raw_image(group_id, photo.file_id, result)
    schedule_text = format_schedule(lessons)
    await msg.edit_text(
        f"Загружено пар: <b>{len(lessons)}</b>\n{schedule_text}\n\n"
        f"Бот будет автоматически открывать очереди в начале каждой пары.",
        parse_mode="HTML"
    )
    await state.clear()


@sched_router.callback_query(F.data == "sched_upload_new")
async def cb_sched_upload(call: CallbackQuery, state: FSMContext):
    group = await sdb.get_study_group(call.message.chat.id)
    await state.update_data(group_id=group["id"], chat_id=call.message.chat.id)
    await call.message.edit_text("Отправь фото с новым расписанием:")
    await state.set_state(ScheduleSetup.waiting_schedule_image)
    await call.answer()


@sched_router.callback_query(F.data == "sched_show")
async def cb_sched_show(call: CallbackQuery):
    group = await sdb.get_study_group(call.message.chat.id)
    if not group:
        await call.answer("Расписание не настроено.", show_alert=True)
        return
    week = await get_week_schedule(group["id"])
    if not week:
        await call.message.edit_text("Расписание пусто.")
        await call.answer()
        return
    all_lessons = [l for lessons in week.values() for l in lessons]
    text = f"<b>Расписание — {group['group_name']}</b>\n" + format_schedule(all_lessons)
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="📅 На сегодня", callback_data="sched_today"),
    ]])
    try:
        await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    except Exception:
        pass
    await call.answer()


@sched_router.callback_query(F.data == "sched_today")
async def cb_sched_today(call: CallbackQuery):
    group = await sdb.get_study_group(call.message.chat.id)
    if not group:
        await call.answer("Расписание не настроено.", show_alert=True)
        return
    today_lessons = await get_today_schedule(group["id"])
    now = datetime.now()
    weekday_name = WEEKDAY_NAMES.get(now.weekday(), "")
    date_str = now.strftime("%d.%m.%Y")
    if not today_lessons:
        try:
            await call.message.edit_text(
                f"<b>{weekday_name}, {date_str}</b>\n\nСегодня занятий нет",
                parse_mode="HTML"
            )
        except Exception:
            pass
        await call.answer()
        return
    lines = [f"<b>{weekday_name}, {date_str} — {group['group_name']}</b>\n"]
    current_time = now.strftime("%H:%M")
    for l in today_lessons:
        is_current = l["time_start"] <= current_time <= l["time_end"]
        is_past = l["time_end"] < current_time
        icon = "▶️" if is_current else ("✅" if is_past else "⏳")
        teacher = f" — {l['teacher']}" if l.get("teacher") else ""
        room = f" [{l['room']}]" if l.get("room") else ""
        lines.append(f"{icon} <b>{l['lesson_num']}.</b> {l['time_start']}–{l['time_end']} <b>{l['subject']}</b>{teacher}{room}")
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="📋 Вся неделя", callback_data="sched_show"),
        InlineKeyboardButton(text="🔄", callback_data="sched_today"),
    ]])
    try:
        await call.message.edit_text("\n".join(lines), reply_markup=kb, parse_mode="HTML")
    except Exception:
        pass
    await call.answer()


@sched_router.message(Command("schedule_today"))
async def cmd_schedule_today(message: Message):
    group = await sdb.get_study_group(message.chat.id)
    if not group:
        await message.answer("Расписание не настроено. Используй /schedule_setup")
        return
    today_lessons = await get_today_schedule(group["id"])
    now = datetime.now()
    weekday_name = WEEKDAY_NAMES.get(now.weekday(), "")
    date_str = now.strftime("%d.%m.%Y")
    if not today_lessons:
        await message.answer(f"<b>{weekday_name}, {date_str}</b>\n\nСегодня занятий нет 🎉", parse_mode="HTML")
        return
    lines = [f"<b>{weekday_name}, {date_str} — {group['group_name']}</b>\n"]
    current_time = now.strftime("%H:%M")
    for l in today_lessons:
        is_current = l["time_start"] <= current_time <= l["time_end"]
        is_past = l["time_end"] < current_time
        icon = "▶️" if is_current else ("✅" if is_past else "⏳")
        teacher = f" — {l['teacher']}" if l.get("teacher") else ""
        room = f" [{l['room']}]" if l.get("room") else ""
        lines.append(f"{icon} <b>{l['lesson_num']}.</b> {l['time_start']}–{l['time_end']} <b>{l['subject']}</b>{teacher}{room}")
    await message.answer("\n".join(lines), parse_mode="HTML")


@sched_router.message(Command("schedule_week"))
async def cmd_schedule_week(message: Message):
    group = await sdb.get_study_group(message.chat.id)
    if not group:
        await message.answer("Расписание не настроено. Используй /schedule_setup")
        return
    week = await get_week_schedule(group["id"])
    if not week:
        await message.answer("Расписание пусто.")
        return
    all_lessons = [l for lessons in week.values() for l in lessons]
    text = f"<b>Расписание — {group['group_name']}</b>\n" + format_schedule(all_lessons)
    await message.answer(text, parse_mode="HTML")


@sched_router.message(F.photo, F.chat.type.in_({"group", "supergroup"}))
async def auto_detect_change_photo(message: Message):
    group = await sdb.get_study_group(message.chat.id)
    if not group:
        return
    caption = (message.caption or "").lower()
    keywords = ["расписан", "замен", "отмен", "перенос", "пар"]
    if not any(kw in caption for kw in keywords):
        return
    photo = message.photo[-1]
    file = await message.bot.get_file(photo.file_id)
    image_bytes = (await message.bot.download_file(file.file_path)).read()
    result = await parse_change_image(image_bytes)
    if result and result.get("changes"):
        await _apply_changes(message, group, result["changes"])


@sched_router.message(F.text, F.chat.type.in_({"group", "supergroup"}))
async def auto_detect_change_text(message: Message):
    group = await sdb.get_study_group(message.chat.id)
    if not group:
        return
    text = message.text or ""
    if len(text) < 15:
        return
    result = await parse_change_text(text)
    if result and result.get("changes"):
        await _apply_changes(message, group, result["changes"])


async def _apply_changes(message: Message, group: dict, changes: list[dict]):
    applied = []
    for change in changes:
        if not change.get("action") or not change.get("date"):
            continue
        await sdb.add_override(
            group_id=group["id"],
            date=change["date"],
            action=change["action"],
            lesson_num=change.get("lesson_num"),
            subject=change.get("subject"),
            time_start=change.get("time_start"),
            time_end=change.get("time_end"),
            note=change.get("note"),
            source="auto"
        )
        applied.append(change)
    if not applied:
        return
    action_names = {
        "cancel": "Отмена", "reschedule": "Перенос",
        "add": "Добавлена", "room_change": "Смена ауд.", "teacher_change": "Замена преп."
    }
    lines = ["📢 <b>Расписание обновлено:</b>"]
    for c in applied:
        label = action_names.get(c["action"], c["action"])
        subj = c.get("subject") or f"пара {c.get('lesson_num', '?')}"
        lines.append(f"• {label}: {subj} ({c.get('date', '?')})")
    try:
        await message.reply("\n".join(lines), parse_mode="HTML")
    except Exception:
        pass


# ─── Skip queue management ────────────────────────────────────────────────────

@schedule_router.callback_query(F.data.startswith("schedule_skip:"))
async def cb_schedule_skip(call: CallbackQuery):
    chat_id = int(call.data.split(":")[1])
    groups = await sdb.get_chat_groups(chat_id)
    if not groups:
        await call.answer("Расписание не загружено.", show_alert=True)
        return

    lines = ["🔕 <b>Пары без очереди</b>\n\nВыбери пары которые НЕ должны создавать очередь:\n"]
    buttons = []
    for group in groups:
        lines.append(f"\n👥 <b>{group['group_name']}</b>")
        for wd in range(1, 8):
            lessons = await sdb.get_lessons_for_day_full(group["id"], wd)
        for lesson in lessons:
            skip = bool(lesson.get("skip_queue", 0))
            icon = "🔕" if skip else "🔔"

            label = f"{icon} {WEEKDAY_NAMES.get(wd, '')} {lesson['time_start']} — {lesson['subject']}"

            buttons.append([InlineKeyboardButton(
                text=label,
                callback_data=f"toggle_skip:{lesson['id']}:{chat_id}"
            )]) 

    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data=f"schedule_back:{chat_id}")])
    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    await call.message.edit_text("\n".join(lines), reply_markup=kb, parse_mode="HTML")
    await call.answer()


@schedule_router.callback_query(F.data.startswith("toggle_skip:"))
async def cb_toggle_skip(call: CallbackQuery):
    parts = call.data.split(":")
    lesson_id, chat_id = int(parts[1]), int(parts[2])

    from database import DB_PATH
    import aiosqlite
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT skip_queue FROM schedule_lessons WHERE id=?", (lesson_id,))
        row = await cur.fetchone()

    if row is None:
        await call.answer("Пара не найдена.", show_alert=True)
        return

    new_val = not bool(row[0])
    await sdb.set_lesson_skip_queue(lesson_id, new_val)
    status = "🔕 без очереди" if new_val else "🔔 с очередью"
    await call.answer(f"Обновлено: {status}", show_alert=True)

    # Refresh the list
    await cb_schedule_skip.__wrapped__(call) if hasattr(cb_schedule_skip, '__wrapped__') else None
    # Just re-trigger
    call.data = f"schedule_skip:{chat_id}"
    await cb_schedule_skip(call)


# ─── Source management ────────────────────────────────────────────────────────

class SourceState(StatesGroup):
    waiting_source = State()


@schedule_router.callback_query(F.data.startswith("schedule_sources:"))
async def cb_schedule_sources(call: CallbackQuery):
    chat_id = int(call.data.split(":")[1])
    sources = await sdb.get_sources(chat_id)

    lines = ["📡 <b>Источники изменений расписания</b>\n"]
    if sources:
        for s in sources:
            icon = "💬" if s["source_type"] == "telegram" else "🔵"
            checked = s.get("last_checked") or "никогда"
            lines.append(f"{icon} {s['source_id']} — проверен: {checked}")
    else:
        lines.append("Источники не добавлены.")

    buttons = []
    for s in sources:
        buttons.append([InlineKeyboardButton(
            text=f"❌ Удалить {s['source_id']}",
            callback_data=f"del_source:{s['id']}:{chat_id}"
        )])

    buttons.append([InlineKeyboardButton(
        text="➕ Добавить Telegram канал",
        callback_data=f"add_source:telegram:{chat_id}"
    )])
    buttons.append([InlineKeyboardButton(
        text="➕ Добавить ВКонтакте группу",
        callback_data=f"add_source:vk:{chat_id}"
    )])
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data=f"schedule_back:{chat_id}")])

    await call.message.edit_text(
        "\n".join(lines),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode="HTML"
    )
    await call.answer()


@sched_router.callback_query(F.data.startswith("add_source:"))
async def cb_add_source(call: CallbackQuery, state: FSMContext):
    parts = call.data.split(":")
    source_type, chat_id = parts[1], int(parts[2])
    await state.update_data(source_type=source_type, chat_id=chat_id)

    if source_type == "telegram":
        prompt = (
            "💬 <b>Telegram источник</b>\n\n"
            "Отправь username канала или группы, например:\n"
            "<code>@mychannel</code> или <code>mychannel</code>\n\n"
            "⚠️ Канал должен быть публичным."
        )
    else:
        prompt = (
            "🔵 <b>ВКонтакте источник</b>\n\n"
            "Отправь короткое имя группы, например:\n"
            "<code>mygroup</code> (из vk.com/<b>mygroup</b>)\n\n"
            "⚠️ Добавь VK_TOKEN в переменные окружения."
        )

    await call.message.edit_text(prompt, parse_mode="HTML")
    await state.set_state(SourceState.waiting_source)
    await call.answer()


@schedule_router.message(SourceState.waiting_source)
async def fsm_add_source(message: Message, state: FSMContext):
    data = await state.get_data()
    source_type = data["source_type"]
    chat_id = data["chat_id"]
    source_id = message.text.strip().lstrip("@")

    await sdb.add_source(chat_id, source_type, source_id)
    await state.clear()

    icon = "💬" if source_type == "telegram" else "🔵"
    await message.answer(
        f"✅ {icon} Источник <b>{source_id}</b> добавлен!\n\n"
        f"Бот будет проверять его каждые 15 минут.",
        reply_markup=schedule_main_keyboard(chat_id),
        parse_mode="HTML"
    )


@schedule_router.callback_query(F.data.startswith("del_source:"))
async def cb_del_source(call: CallbackQuery):
    parts = call.data.split(":")
    source_db_id, chat_id = int(parts[1]), int(parts[2])
    await sdb.remove_source(source_db_id)
    await call.answer("✅ Источник удалён.", show_alert=True)
    call.data = f"schedule_sources:{chat_id}"
    await cb_schedule_sources(call)


@schedule_router.callback_query(F.data.startswith("schedule_back:"))
async def cb_schedule_back(call: CallbackQuery):
    chat_id = int(call.data.split(":")[1])
    groups = await sdb.get_chat_groups(chat_id)
    if groups:
        names = ", ".join(g["group_name"] for g in groups)
        text = f"📅 <b>Расписание</b>\n\nЗагруженные группы: <b>{names}</b>\n\nЧто сделать?"
    else:
        text = "📅 <b>Расписание</b>\n\nРасписание ещё не загружено."
    await call.message.edit_text(
        text, reply_markup=schedule_main_keyboard(chat_id), parse_mode="HTML"
    )
    await call.answer()
