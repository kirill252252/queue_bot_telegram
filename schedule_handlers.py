"""
Хэндлеры расписания — команды и колбэки для управления расписанием в группе.
Регистрирует команды: /schedule
Обрабатывает: загрузку фото расписания, OCR, сохранение в БД,
              показ расписания, настройку источников и очередей,
              авто-распознавание изменений из фото в группе.
"""
import logging
from datetime import datetime

from aiogram import Router, F, Bot
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton,
)
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

import db
import schedule_db as sdb
from schedule_ocr import parse_schedule_image, parse_change_image, format_schedule
from schedule_manager import get_today_schedule, get_week_schedule
from schedule_ui import schedule_main_keyboard

sched_router = Router()
logger = logging.getLogger(__name__)

DAYS_FULL = {
    1: "Понедельник", 2: "Вторник", 3: "Среда",
    4: "Четверг", 5: "Пятница", 6: "Суббота", 7: "Воскресенье",
}


# ─────────────────────────────────────────────
# FSM
# ─────────────────────────────────────────────

class ScheduleStates(StatesGroup):
    waiting_photo  = State()   # ждём фото расписания
    waiting_source = State()   # ждём ввод источника (TG/VK username)


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

async def _is_admin(bot: Bot, chat_id: int, user_id: int) -> bool:
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


async def _download_photo(message: Message) -> bytes:
    photo = message.photo[-1]
    file = await message.bot.get_file(photo.file_id)
    data = await message.bot.download_file(file.file_path)
    return data.read()


# ─────────────────────────────────────────────
# /schedule — главное меню
# ─────────────────────────────────────────────

@sched_router.message(Command("schedule"))
async def cmd_schedule(message: Message):
    if message.chat.type == "private":
        await message.answer("Команда /schedule работает только в группах.")
        return

    if message.chat.title:
        await db.register_chat(message.chat.id, message.chat.title)

    await message.answer(
        "📅 <b>Расписание</b>\n\nВыберите действие:",
        reply_markup=schedule_main_keyboard(message.chat.id),
        parse_mode="HTML",
    )


# ─────────────────────────────────────────────
# UPLOAD — загрузка нового расписания
# ─────────────────────────────────────────────

@sched_router.callback_query(F.data == "sched_upload_new")
async def cb_upload_new(call: CallbackQuery, state: FSMContext):
    if not await _is_admin(call.bot, call.message.chat.id, call.from_user.id):
        await call.answer("❌ Только администраторы могут загружать расписание.", show_alert=True)
        return

    await state.update_data(chat_id=call.message.chat.id)
    await state.set_state(ScheduleStates.waiting_photo)

    await call.message.answer(
        "📸 <b>Отправьте фотографию расписания.</b>\n\n"
        "Я автоматически распознаю занятия, группу, время и аудитории.\n\n"
        "Чтобы отменить — отправьте /schedule",
        parse_mode="HTML",
    )
    await call.answer()


@sched_router.message(ScheduleStates.waiting_photo, F.photo)
async def fsm_receive_schedule_photo(message: Message, state: FSMContext):
    data = await state.get_data()
    chat_id = data.get("chat_id", message.chat.id)
    await state.clear()

    status_msg = await message.answer("⏳ Распознаю расписание, подождите...")

    try:
        image_bytes = await _download_photo(message)
    except Exception as e:
        logger.error(f"Photo download error: {e}")
        await status_msg.edit_text("❌ Не удалось скачать фото. Попробуйте ещё раз.")
        return

    result = await parse_schedule_image(image_bytes)

    # parse_schedule_image всегда возвращает {"groups": [...]} или None/"error"
    if not result or "error" in result:
        await status_msg.edit_text(
            "❌ <b>Не удалось распознать расписание.</b>\n\n"
            "Советы:\n"
            "• Сфотографируйте так, чтобы весь текст был виден\n"
            "• Избегайте бликов и размытия\n"
            "• Отправьте снова или попробуйте другое фото",
            parse_mode="HTML",
        )
        return

    groups_data = result.get("groups") or []
    if not groups_data:
        await status_msg.edit_text(
            "❌ <b>Расписание не распознано — занятия не найдены.</b>\n\n"
            "Попробуйте снять чётче или с другого угла.",
            parse_mode="HTML",
        )
        return

    # Сохраняем все группы из фото
    total_lessons = 0
    saved_groups = []
    for g in groups_data:
        group_name = g.get("group_name") or "Группа"
        lessons = g.get("lessons") or []
        if not lessons:
            continue
        group_id = await sdb.upsert_group(chat_id, group_name)
        await sdb.save_lessons(group_id, lessons)
        total_lessons += len(lessons)
        saved_groups.append((group_name, len(lessons)))

    if not saved_groups:
        await status_msg.edit_text("❌ Не удалось сохранить расписание — занятия не распознаны.")
        return

    groups_text = "\n".join(f"  👥 <b>{name}</b> — {cnt} занятий" for name, cnt in saved_groups)
    # Форматируем первую группу для предпросмотра
    preview = format_schedule(groups_data[0].get("lessons", []))

    await status_msg.edit_text(
        f"✅ <b>Расписание сохранено!</b>\n\n"
        f"Распознано групп: <b>{len(saved_groups)}</b>\n"
        f"{groups_text}\n"
        f"Всего занятий: <b>{total_lessons}</b>\n"
        f"\n{preview}\n\n"
        f"Очереди будут открываться автоматически в начале каждой пары.",
        parse_mode="HTML",
    )


@sched_router.message(ScheduleStates.waiting_photo)
async def fsm_no_photo(message: Message):
    """Пользователь прислал не фото — напоминаем."""
    if message.text and message.text.startswith("/"):
        return  # команды обработают другие хэндлеры
    await message.answer("Пожалуйста, отправьте <b>фотографию</b> расписания.", parse_mode="HTML")


# ─────────────────────────────────────────────
# SHOW WEEK — расписание на всю неделю
# ─────────────────────────────────────────────

@sched_router.callback_query(F.data == "sched_show")
async def cb_show_week(call: CallbackQuery):
    chat_id = call.message.chat.id
    groups = await sdb.get_chat_groups(chat_id)

    if not groups:
        await call.answer(
            "Расписание не загружено.\nИспользуйте «📸 Загрузить расписание».",
            show_alert=True,
        )
        return

    parts = []
    for group in groups:
        week = await get_week_schedule(group["id"])
        if not week:
            continue

        lines = [f"📅 <b>Расписание — {group['group_name']}</b>"]
        bells_cache = {b["lesson_num"]: b for b in await sdb.get_bells(chat_id)}
        for wd in sorted(week):
            lines.append(f"\n<b>{DAYS_FULL.get(wd, wd)}</b>")
            for l in sorted(week[wd], key=lambda x: x.get("lesson_num", 0)):
                teacher = f" — {l['teacher']}" if l.get("teacher") else ""
                room = f" [{l['room']}]" if l.get("room") else ""
                ts = l.get("time_start") or ""
                te = l.get("time_end") or ""
                if not ts or not te:
                    bell = bells_cache.get(l["lesson_num"])
                    if bell:
                        ts, te = bell["time_start"], bell["time_end"]
                time_str = f" {ts}–{te}" if ts and te else ""
                lines.append(
                    f"  {l['lesson_num']}.{time_str} "
                    f"<b>{l['subject']}</b>{teacher}{room}"
                )
        parts.append("\n".join(lines))

    if not parts:
        await call.answer("Расписание пусто.", show_alert=True)
        return

    for chunk in parts:
        await call.message.answer(chunk, parse_mode="HTML")

    await call.answer()


# ─────────────────────────────────────────────
# SHOW TODAY — расписание на сегодня
# ─────────────────────────────────────────────

@sched_router.callback_query(F.data == "sched_today")
async def cb_show_today(call: CallbackQuery):
    chat_id = call.message.chat.id
    groups = await sdb.get_chat_groups(chat_id)

    if not groups:
        await call.answer("Расписание не загружено.", show_alert=True)
        return

    wd = datetime.now().isoweekday()
    day_name = DAYS_FULL.get(wd, str(wd))

    parts = []
    for group in groups:
        lessons = await get_today_schedule(group["id"])

        if not lessons:
            parts.append(f"😴 <b>{group['group_name']}</b> — {day_name}: пар нет")
            continue

        bells_cache = {b["lesson_num"]: b for b in await sdb.get_bells(chat_id)}
        lines = [f"📅 <b>{group['group_name']} — {day_name}</b>\n"]
        for l in lessons:
            teacher = f" — {l.get('teacher')}" if l.get("teacher") else ""
            room = f" [{l.get('room')}]" if l.get("room") else ""
            ts = l.get("time_start") or ""
            te = l.get("time_end") or ""
            if not ts or not te:
                bell = bells_cache.get(l["lesson_num"])
                if bell:
                    ts, te = bell["time_start"], bell["time_end"]
            time_str = f" {ts}–{te}" if ts and te else ""
            lines.append(
                f"{l['lesson_num']}.{time_str} "
                f"<b>{l['subject']}</b>{teacher}{room}"
            )
        parts.append("\n".join(lines))

    if parts:
        await call.message.answer("\n\n".join(parts), parse_mode="HTML")
    else:
        await call.message.answer("Сегодня пар нет 🎉")

    await call.answer()


# ─────────────────────────────────────────────
# SOURCES — источники изменений расписания
# ─────────────────────────────────────────────

async def _build_sources_keyboard(chat_id: int, sources: list[dict]) -> InlineKeyboardMarkup:
    buttons = []
    for s in sources:
        icon = "📢" if s["source_type"] == "telegram" else "📣"
        buttons.append([InlineKeyboardButton(
            text=f"{icon} {s['source_id']}  [удалить]",
            callback_data=f"sched_del_source:{s['id']}",
        )])
    buttons += [
        [InlineKeyboardButton(
            text="➕ Добавить Telegram-канал",
            callback_data=f"sched_add_source:{chat_id}:telegram",
        )],
        [InlineKeyboardButton(
            text="➕ Добавить ВКонтакте группу",
            callback_data=f"sched_add_source:{chat_id}:vk",
        )],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="sched_show")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)


@sched_router.callback_query(F.data.startswith("schedule_sources:"))
async def cb_sources(call: CallbackQuery):
    chat_id = int(call.data.split(":")[1])

    if not await _is_admin(call.bot, chat_id, call.from_user.id):
        await call.answer("❌ Только администраторы.", show_alert=True)
        return

    sources = await sdb.get_chat_sources(chat_id)
    kb = await _build_sources_keyboard(chat_id, sources)

    if sources:
        text = (
            "📡 <b>Источники изменений расписания</b>\n\n"
            "Бот мониторит эти каналы и автоматически применяет изменения:\n\n"
            + "\n".join(
                f"• {'TG' if s['source_type'] == 'telegram' else 'VK'}: {s['source_id']}"
                for s in sources
            )
        )
    else:
        text = (
            "📡 <b>Источники изменений расписания</b>\n\n"
            "Источники ещё не добавлены.\n\n"
            "Добавьте Telegram-канал или VK-группу — бот будет проверять их "
            "каждые 15 минут и автоматически вносить изменения в расписание."
        )

    await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await call.answer()


@sched_router.callback_query(F.data.startswith("sched_add_source:"))
async def cb_add_source(call: CallbackQuery, state: FSMContext):
    parts = call.data.split(":")
    chat_id = int(parts[1])
    source_type = parts[2]

    if not await _is_admin(call.bot, chat_id, call.from_user.id):
        await call.answer("❌ Только администраторы.", show_alert=True)
        return

    await state.update_data(chat_id=chat_id, source_type=source_type)
    await state.set_state(ScheduleStates.waiting_source)

    if source_type == "telegram":
        prompt = "📢 Введите @username Telegram-канала (например @myuniversity):"
    else:
        prompt = "📣 Введите короткое имя VK-группы (например myuniversity):"

    await call.message.answer(prompt)
    await call.answer()


@sched_router.message(ScheduleStates.waiting_source)
async def fsm_receive_source(message: Message, state: FSMContext):
    data = await state.get_data()
    chat_id = data["chat_id"]
    source_type = data["source_type"]
    source_id = message.text.strip().lstrip("@")

    await state.clear()

    if not source_id:
        await message.answer("❌ Пустое значение. Попробуйте снова через /schedule.")
        return

    # Добавляем @ обратно для Telegram
    if source_type == "telegram" and not source_id.startswith("@"):
        source_id = "@" + source_id

    await sdb.add_source(chat_id, source_type, source_id)

    await message.answer(
        f"✅ Источник добавлен!\n\n"
        f"Тип: <b>{'Telegram' if source_type == 'telegram' else 'ВКонтакте'}</b>\n"
        f"Канал/группа: <b>{source_id}</b>\n\n"
        f"Бот будет проверять его каждые 15 минут.",
        parse_mode="HTML",
    )


@sched_router.callback_query(F.data.startswith("sched_del_source:"))
async def cb_del_source(call: CallbackQuery):
    source_id_int = int(call.data.split(":")[1])
    await sdb.delete_source(source_id_int)
    await call.answer("✅ Источник удалён.")

    # Перестраиваем клавиатуру
    chat_id = call.message.chat.id
    sources = await sdb.get_chat_sources(chat_id)
    kb = await _build_sources_keyboard(chat_id, sources)
    try:
        await call.message.edit_reply_markup(reply_markup=kb)
    except Exception:
        pass


# ─────────────────────────────────────────────
# SKIP QUEUE — настройка: для каких пар не создавать очереди
# ─────────────────────────────────────────────

@sched_router.callback_query(F.data.startswith("schedule_skip:"))
async def cb_schedule_skip(call: CallbackQuery):
    chat_id = int(call.data.split(":")[1])

    if not await _is_admin(call.bot, chat_id, call.from_user.id):
        await call.answer("❌ Только администраторы.", show_alert=True)
        return

    groups = await sdb.get_chat_groups(chat_id)
    if not groups:
        await call.answer("Расписание не загружено.", show_alert=True)
        return

    buttons = []
    for group in groups:
        for wd in range(1, 8):
            lessons = await sdb.get_lessons_for_day(group["id"], wd)
            for l in lessons:
                skip = bool(l.get("skip_queue", 0))
                icon = "🔕" if skip else "🔔"
                day_abbr = ["", "Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"][wd]
                buttons.append([InlineKeyboardButton(
                    text=f"{icon} {day_abbr} {l['lesson_num']}. {l['subject'][:30]}",
                    callback_data=f"sched_toggle_skip:{l['id']}:{chat_id}",
                )])

    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data="sched_show")])

    await call.message.edit_text(
        "🔔 <b>Настройка автоматических очередей</b>\n\n"
        "Нажмите на занятие чтобы включить или выключить создание очереди для него:\n\n"
        "🔔 — очередь создаётся\n"
        "🔕 — очередь не создаётся",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode="HTML",
    )
    await call.answer()


@sched_router.callback_query(F.data.startswith("sched_toggle_skip:"))
async def cb_toggle_skip(call: CallbackQuery):
    parts = call.data.split(":")
    lesson_id = int(parts[1])
    chat_id = int(parts[2])

    if not await _is_admin(call.bot, chat_id, call.from_user.id):
        await call.answer("❌ Только администраторы.", show_alert=True)
        return

    await sdb.toggle_lesson_skip_queue(lesson_id)
    await call.answer("✅ Обновлено.")

    # Обновляем клавиатуру (повторно вызываем cb_schedule_skip)
    call.data = f"schedule_skip:{chat_id}"
    await cb_schedule_skip(call)


# ─────────────────────────────────────────────
# AUTO-DETECT CHANGES — авто-распознавание изменений из фото в группе
# ─────────────────────────────────────────────

CHANGE_KEYWORDS = ["расписани", "изменени", "отмен", "перенос", "замен", "пара", "лекц", "семинар"]


@sched_router.message(
    F.chat.type.in_({"group", "supergroup"}),
    F.photo,
)
async def on_group_photo(message: Message, state: FSMContext):
    """
    Если в группе есть расписание и фото подписано ключевыми словами —
    пробуем распознать изменения и применить их.
    Не срабатывает если FSM активна (человек грузит расписание).
    """
    current_state = await state.get_state()
    if current_state is not None:
        return  # FSM уже активна, другой хэндлер разберётся

    groups = await sdb.get_chat_groups(message.chat.id)
    if not groups:
        return

    caption = (message.caption or "").lower()
    if not any(k in caption for k in CHANGE_KEYWORDS):
        return

    try:
        image_bytes = await _download_photo(message)
    except Exception:
        return

    result = await parse_change_image(image_bytes)
    if not result:
        return

    changes = result.get("changes") or []
    if not changes:
        return

    fallback_date = result.get("date")  # дата из заголовка листа изменений

    applied = []
    for change in changes:
        for group in groups:
            await sdb.save_override(group["id"], change, fallback_date=fallback_date)
        action = change.get("action") or change.get("type") or "изменение"
        subject = change.get("subject") or "?"
        group_label = change.get("group") or "все группы"
        applied.append(f"<b>{group_label}</b> лента {change.get('lesson_num','?')}: {action} — {subject}")

    if applied:
        date_line = f"📅 Дата: {fallback_date}\n" if fallback_date else ""
        await message.reply(
            f"📢 <b>Изменения расписания применены:</b>\n{date_line}\n"
            + "\n".join(f"• {a}" for a in applied),
            parse_mode="HTML",
        )


# ═══════════════════════════════════════════════════════════════════
# РАСПИСАНИЕ ЗВОНКОВ — просмотр и редактирование
# ═══════════════════════════════════════════════════════════════════

def _bells_text(bells: list[dict]) -> str:
    lines = ["🔔 <b>Расписание звонков</b>\n"]
    for b in bells:
        lines.append(f"  Пара <b>{b['lesson_num']}</b>: {b['time_start']} – {b['time_end']}")
    return "\n".join(lines)


def _bells_keyboard(chat_id: int, bells: list[dict]) -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton(
            text=f"✏️ Пара {b['lesson_num']}: {b['time_start']}–{b['time_end']}",
            callback_data=f"bells_edit:{chat_id}:{b['lesson_num']}",
        )]
        for b in bells
    ]
    buttons += [
        [InlineKeyboardButton(
            text="➕ Добавить пару",
            callback_data=f"bells_add:{chat_id}",
        )],
        [InlineKeyboardButton(
            text="🔄 Сбросить к дефолту",
            callback_data=f"bells_reset:{chat_id}",
        )],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="sched_back_main")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)


@sched_router.callback_query(F.data.startswith("sched_bells:"))
async def cb_bells_menu(call: CallbackQuery):
    chat_id = int(call.data.split(":")[1])

    if not await _is_admin(call.bot, chat_id, call.from_user.id):
        await call.answer("❌ Только администраторы.", show_alert=True)
        return

    bells = await sdb.get_bells(chat_id)
    await call.message.edit_text(
        _bells_text(bells) + "\n\nНажмите на пару чтобы изменить время:",
        reply_markup=_bells_keyboard(chat_id, bells),
        parse_mode="HTML",
    )
    await call.answer()


# ─── Редактировать время пары ───

class BellStates(StatesGroup):
    waiting_time     = State()   # ждём HH:MM-HH:MM для существующей пары
    waiting_add_num  = State()   # ждём номер пары при добавлении
    waiting_add_time = State()   # ждём время при добавлении


@sched_router.callback_query(F.data.startswith("bells_edit:"))
async def cb_bells_edit(call: CallbackQuery, state: FSMContext):
    _, chat_id_s, num_s = call.data.split(":")
    chat_id, lesson_num = int(chat_id_s), int(num_s)

    if not await _is_admin(call.bot, chat_id, call.from_user.id):
        await call.answer("❌ Только администраторы.", show_alert=True)
        return

    await state.update_data(chat_id=chat_id, lesson_num=lesson_num)
    await state.set_state(BellStates.waiting_time)

    bell = next(
        (b for b in await sdb.get_bells(chat_id) if b["lesson_num"] == lesson_num),
        None
    )
    current = f"{bell['time_start']}–{bell['time_end']}" if bell else "не задано"

    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(
            text="🗑 Удалить пару из звонков",
            callback_data=f"bells_del:{chat_id}:{lesson_num}"
        ),
        InlineKeyboardButton(text="❌ Отмена", callback_data=f"sched_bells:{chat_id}"),
    ]])
    await call.message.answer(
        f"⏰ <b>Пара {lesson_num}</b> — сейчас: <code>{current}</code>\n\n"
        f"Введите новое время в формате <b>HH:MM-HH:MM</b>\n"
        f"Например: <code>09:45-11:20</code>",
        reply_markup=kb,
        parse_mode="HTML",
    )
    await call.answer()


@sched_router.message(BellStates.waiting_time)
async def fsm_bells_receive_time(message: Message, state: FSMContext):
    import re
    data = await state.get_data()
    chat_id    = data["chat_id"]
    lesson_num = data["lesson_num"]

    m = re.match(r"(\d{1,2}:\d{2})\s*[-–]\s*(\d{1,2}:\d{2})", message.text.strip())
    if not m:
        await message.answer(
            "❌ Неверный формат. Введите как <b>09:45-11:20</b>",
            parse_mode="HTML"
        )
        return

    await state.clear()
    ts, te = m.group(1), m.group(2)
    await sdb.set_bell(chat_id, lesson_num, ts, te)

    bells = await sdb.get_bells(chat_id)
    await message.answer(
        f"✅ Пара <b>{lesson_num}</b>: {ts} – {te} сохранено.\n\n"
        + _bells_text(bells),
        reply_markup=_bells_keyboard(chat_id, bells),
        parse_mode="HTML",
    )


# ─── Добавить новую пару ───

@sched_router.callback_query(F.data.startswith("bells_add:"))
async def cb_bells_add(call: CallbackQuery, state: FSMContext):
    chat_id = int(call.data.split(":")[1])

    if not await _is_admin(call.bot, chat_id, call.from_user.id):
        await call.answer("❌ Только администраторы.", show_alert=True)
        return

    bells = await sdb.get_bells(chat_id)
    next_num = max((b["lesson_num"] for b in bells), default=0) + 1

    await state.update_data(chat_id=chat_id, lesson_num=next_num)
    await state.set_state(BellStates.waiting_add_time)

    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="❌ Отмена", callback_data=f"sched_bells:{chat_id}")
    ]])
    await call.message.answer(
        f"➕ <b>Новая пара {next_num}</b>\n\n"
        f"Введите время в формате <b>HH:MM-HH:MM</b>\n"
        f"Например: <code>18:50-20:25</code>",
        reply_markup=kb,
        parse_mode="HTML",
    )
    await call.answer()


@sched_router.message(BellStates.waiting_add_time)
async def fsm_bells_add_time(message: Message, state: FSMContext):
    import re
    data = await state.get_data()
    chat_id    = data["chat_id"]
    lesson_num = data["lesson_num"]

    m = re.match(r"(\d{1,2}:\d{2})\s*[-–]\s*(\d{1,2}:\d{2})", message.text.strip())
    if not m:
        await message.answer(
            "❌ Неверный формат. Введите как <b>18:50-20:25</b>",
            parse_mode="HTML"
        )
        return

    await state.clear()
    ts, te = m.group(1), m.group(2)
    await sdb.set_bell(chat_id, lesson_num, ts, te)

    bells = await sdb.get_bells(chat_id)
    await message.answer(
        f"✅ Пара <b>{lesson_num}</b>: {ts} – {te} добавлена.\n\n"
        + _bells_text(bells),
        reply_markup=_bells_keyboard(chat_id, bells),
        parse_mode="HTML",
    )


# ─── Удалить звонок пары ───

@sched_router.callback_query(F.data.startswith("bells_del:"))
async def cb_bells_del(call: CallbackQuery, state: FSMContext):
    _, chat_id_s, num_s = call.data.split(":")
    chat_id, lesson_num = int(chat_id_s), int(num_s)

    if not await _is_admin(call.bot, chat_id, call.from_user.id):
        await call.answer("❌ Только администраторы.", show_alert=True)
        return

    await state.clear()

    # Удаляем только кастомный звонок, дефолт останется
    if _get_db_type() == "postgres":
        from database_pg import get_pool
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM schedule_bells WHERE chat_id = $1 AND lesson_num = $2",
                chat_id, lesson_num
            )
    else:
        import schedule_db as _sdb_local
        await _sdb_local._execute(
            "DELETE FROM schedule_bells WHERE chat_id = ? AND lesson_num = ?",
            (chat_id, lesson_num)
        )

    bells = await sdb.get_bells(chat_id)
    await call.answer(f"✅ Пара {lesson_num} удалена из звонков (восстановлен дефолт).")
    await call.message.edit_text(
        _bells_text(bells) + "\n\nНажмите на пару чтобы изменить время:",
        reply_markup=_bells_keyboard(chat_id, bells),
        parse_mode="HTML",
    )


# ─── Сброс к дефолту ───

@sched_router.callback_query(F.data.startswith("bells_reset:"))
async def cb_bells_reset(call: CallbackQuery):
    chat_id = int(call.data.split(":")[1])

    if not await _is_admin(call.bot, chat_id, call.from_user.id):
        await call.answer("❌ Только администраторы.", show_alert=True)
        return

    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Да, сбросить", callback_data=f"bells_reset_confirm:{chat_id}"),
        InlineKeyboardButton(text="❌ Отмена",        callback_data=f"sched_bells:{chat_id}"),
    ]])
    await call.message.edit_text(
        "⚠️ <b>Сбросить расписание звонков к дефолтному?</b>\n\n"
        "Дефолт:\n"
        + "\n".join(f"  Пара {n}: {ts}–{te}" for n, ts, te in sdb.DEFAULT_BELLS),
        reply_markup=kb,
        parse_mode="HTML",
    )
    await call.answer()


@sched_router.callback_query(F.data.startswith("bells_reset_confirm:"))
async def cb_bells_reset_confirm(call: CallbackQuery):
    chat_id = int(call.data.split(":")[1])

    if not await _is_admin(call.bot, chat_id, call.from_user.id):
        await call.answer("❌ Только администраторы.", show_alert=True)
        return

    await sdb.reset_bells(chat_id)
    bells = await sdb.get_bells(chat_id)
    await call.message.edit_text(
        "✅ Расписание звонков сброшено к дефолтному.\n\n"
        + _bells_text(bells),
        reply_markup=_bells_keyboard(chat_id, bells),
        parse_mode="HTML",
    )
    await call.answer()


def _get_db_type():
    from config import DB_TYPE
    return DB_TYPE
