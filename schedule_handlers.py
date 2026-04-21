"""
schedule_handlers.py — все хэндлеры для управления расписанием.

Регистрирует команду /schedule и обрабатывает:
  - загрузку фото расписания и OCR через Groq AI
  - показ расписания на день/неделю
  - ручное редактирование занятий (предмет, преподаватель, аудитория, время)
  - добавление и удаление занятий из базового расписания
  - отмену пар на конкретную дату
  - расписание звонков (редактирование времени начала/конца каждой пары)
  - настройку источников автомониторинга (TG-канал, VK-группа)
  - автораспознавание изменений из фото в группе
"""

import logging
import re
from datetime import datetime, timedelta, date as dt_date

from aiogram import Router, F, Bot
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton,
)
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from config import SOURCE_MONITOR_INTERVAL_MIN
import db
import schedule_db as sdb
from schedule_group_match import build_group_lookup, resolve_target_groups
from schedule_ocr import (
    parse_schedule_image,
    parse_schedule_change,
    format_schedule,
    split_by_week,
)
from schedule_manager import get_today_schedule, get_week_schedule
from schedule_ui import schedule_main_keyboard

sched_router = Router()
logger = logging.getLogger(__name__)

# Словари для перевода номера дня недели в название
DAYS_FULL = {
    1: "Понедельник", 2: "Вторник", 3: "Среда",
    4: "Четверг", 5: "Пятница", 6: "Суббота", 7: "Воскресенье",
}
DAYS_SHORT = ["", "Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]

# Ключевые слова для авто-распознавания изменений из фото
CHANGE_KEYWORDS = ["расписани", "изменени", "отмен", "перенос", "замен", "пара", "лекц", "семинар"]

# Метки типов недели для отображения
WEEK_TYPE_LABELS = {0: "каждую неделю", 1: "нечётные недели", 2: "чётные недели"}
WEEK_TYPE_ICONS  = {0: "📆", 1: "1️⃣", 2: "2️⃣"}


# ═══════════════════════════════════════════════════════════════════
# FSM — состояния для всех диалогов с пользователем
# ═══════════════════════════════════════════════════════════════════

class ScheduleStates(StatesGroup):
    # Загрузка расписания
    waiting_photo       = State()  # ждём фото расписания
    waiting_source      = State()  # ждём @username для источника мониторинга

    # Редактирование поля занятия (предмет / препод / аудитория / время)
    edit_lesson_field   = State()  # ждём новое значение поля

    # Добавление нового занятия — пошаговый диалог
    add_lesson_subject  = State()  # шаг 1: название предмета
    add_lesson_teacher  = State()  # шаг 2: преподаватель
    add_lesson_room     = State()  # шаг 3: аудитория
    add_lesson_time     = State()  # шаг 4: время HH:MM-HH:MM


class BellStates(StatesGroup):
    # Редактирование расписания звонков
    waiting_time        = State()  # ждём время для существующей пары
    waiting_add_time    = State()  # ждём время для новой пары


# ═══════════════════════════════════════════════════════════════════
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ═══════════════════════════════════════════════════════════════════

async def _is_admin(bot: Bot, chat_id: int, user_id: int) -> bool:
    """Проверяем: является ли пользователь администратором чата или владельцем бота."""
    from config import BOT_OWNER_ID
    if user_id == BOT_OWNER_ID:
        return True  # Владелец бота — всегда админ
    if await db.is_bot_admin(user_id, chat_id):
        return True  # Бот-администратор (назначен через команду)
    try:
        m = await bot.get_chat_member(chat_id, user_id)
        return m.status in ("administrator", "creator")  # Telegram-администратор
    except Exception:
        return False


async def _download_photo(message: Message) -> bytes:
    """Скачиваем фото из сообщения в байты. Берём самое большое превью."""
    photo = message.photo[-1]  # последний элемент — самое высокое качество
    file = await message.bot.get_file(photo.file_id)
    data = await message.bot.download_file(file.file_path)
    return data.read()


def _lesson_time_str(lesson: dict, bells_cache: dict) -> str:
    """
    Возвращает строку времени для пары.
    Сначала ищем явное время в занятии, потом — в кэше звонков.
    """
    ts = lesson.get("time_start") or ""
    te = lesson.get("time_end") or ""
    if not ts or not te:
        bell = bells_cache.get(lesson.get("lesson_num"))
        if bell:
            ts, te = bell["time_start"], bell["time_end"]
    return f" {ts}–{te}" if ts and te else ""


def _lesson_week_icon(lesson: dict) -> str:
    """Иконка типа недели: 1️⃣ нечётная, 2️⃣ чётная, пусто = каждую."""
    wt = lesson.get("week_type", 0)
    if lesson.get("is_event"):
        return " 🎓"  # мероприятие
    return {1: " 1️⃣", 2: " 2️⃣"}.get(wt, "")


# ═══════════════════════════════════════════════════════════════════
# /schedule — ГЛАВНОЕ МЕНЮ
# ═══════════════════════════════════════════════════════════════════

@sched_router.message(Command("schedule"))
async def cmd_schedule(message: Message):
    """Команда /schedule — открывает главное меню расписания."""
    if message.chat.type == "private":
        await message.answer("Команда /schedule работает только в группах.")
        return

    # Регистрируем чат чтобы он появился в веб-панели
    if message.chat.title:
        await db.register_chat(message.chat.id, message.chat.title)

    await message.answer(
        "📅 <b>Расписание</b>\n\nВыберите действие:",
        reply_markup=schedule_main_keyboard(message.chat.id),
        parse_mode="HTML",
    )


@sched_router.callback_query(F.data == "sched_back_main")
async def cb_back_main(call: CallbackQuery):
    """Кнопка «◀️ Назад» — возврат в главное меню расписания."""
    chat_id = call.message.chat.id
    await call.message.edit_text(
        "📅 <b>Расписание</b>\n\nВыберите действие:",
        reply_markup=schedule_main_keyboard(chat_id),
        parse_mode="HTML",
    )
    await call.answer()


# ═══════════════════════════════════════════════════════════════════
# ЗАГРУЗКА РАСПИСАНИЯ — фото → OCR → сохранение в БД
# ═══════════════════════════════════════════════════════════════════

@sched_router.callback_query(F.data == "sched_upload_new")
async def cb_upload_new(call: CallbackQuery, state: FSMContext):
    """Кнопка «📸 Загрузить расписание» — переводим в режим ожидания фото."""
    if not await _is_admin(call.bot, call.message.chat.id, call.from_user.id):
        await call.answer("❌ Только администраторы могут загружать расписание.", show_alert=True)
        return

    # Сохраняем chat_id в состоянии — пригодится когда фото придёт в личку
    await state.update_data(chat_id=call.message.chat.id)
    await state.set_state(ScheduleStates.waiting_photo)

    await call.message.answer(
        "📸 <b>Отправьте фотографию расписания.</b>\n\n"
        "Бот автоматически распознает занятия, группы, время и аудитории.\n\n"
        "Чтобы отменить — отправьте /schedule",
        parse_mode="HTML",
    )
    await call.answer()


@sched_router.message(ScheduleStates.waiting_photo, F.photo)
async def fsm_receive_schedule_photo(message: Message, state: FSMContext):
    """Получаем фото расписания — отправляем в Groq AI и сохраняем результат."""
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

    # Отправляем в Groq Vision — получаем структурированное расписание
    result = await parse_schedule_image(image_bytes)

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

    # parse_schedule_image всегда возвращает {"groups": [...]}
    groups_data = result.get("groups") or []
    if not groups_data:
        await status_msg.edit_text(
            "❌ <b>Расписание не распознано — занятия не найдены.</b>\n\n"
            "Попробуйте снять чётче или с другого угла.",
            parse_mode="HTML",
        )
        return

    # Сохраняем все группы из одного фото (таблица может содержать несколько групп)
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

    preview_lessons = groups_data[0].get("lessons", [])
    even_lessons, odd_lessons = split_by_week(preview_lessons)
    even_text = format_schedule(even_lessons)
    odd_text = format_schedule(odd_lessons)
    preview = (
        "📅 <b>Чётная неделя</b>\n"
        f"{even_text}\n\n"
        "📅 <b>Нечётная неделя</b>\n"
        f"{odd_text}"
    )

    await status_msg.edit_text(
        f"✅ <b>Расписание сохранено!</b>\n\n"
        f"Распознано групп: <b>{len(saved_groups)}</b>\n"
        f"{groups_text}\n"
        f"Всего занятий: <b>{total_lessons}</b>\n\n"
        f"{preview}\n\n"
        f"⏰ Звонки: настройте время через <b>🔔 Звонки</b> в меню /schedule",
        parse_mode="HTML",
    )


@sched_router.message(ScheduleStates.waiting_photo)
async def fsm_no_photo(message: Message):
    """Пользователь в режиме ожидания фото прислал что-то другое."""
    if message.text and message.text.startswith("/"):
        return  # команды пропускаем — другие хэндлеры разберутся
    await message.answer("Пожалуйста, отправьте <b>фотографию</b> расписания.", parse_mode="HTML")


# ═══════════════════════════════════════════════════════════════════
# ПОКАЗ РАСПИСАНИЯ
# ═══════════════════════════════════════════════════════════════════

@sched_router.callback_query(F.data == "sched_show")
async def cb_show_week(call: CallbackQuery):
    """Кнопка «📋 Вся неделя» — показываем полное расписание со звонками."""
    chat_id = call.message.chat.id
    groups = await sdb.get_chat_groups(chat_id)

    if not groups:
        await call.answer("Расписание не загружено. Используйте «📸 Загрузить расписание».", show_alert=True)
        return

    # Загружаем звонки один раз — используем для всех групп
    bells_cache = {b["lesson_num"]: b for b in await sdb.get_bells(chat_id)}

    parts = []
    for group in groups:
        week = await get_week_schedule(group["id"])
        if not week:
            continue

        lines = [f"📅 <b>Расписание — {group['group_name']}</b>"]
        for wd in sorted(week):
            lines.append(f"\n<b>{DAYS_FULL.get(wd, wd)}</b>")
            for l in sorted(week[wd], key=lambda x: x.get("lesson_num", 0)):
                teacher = f" — {l['teacher']}" if l.get("teacher") else ""
                room    = f" [{l['room']}]" if l.get("room") else ""
                time_s  = _lesson_time_str(l, bells_cache)
                skip_s  = " 🔕" if l.get("skip_queue") else ""
                week_s  = _lesson_week_icon(l)
                lines.append(f"  {l['lesson_num']}.{time_s}{week_s} <b>{l['subject']}</b>{teacher}{room}{skip_s}")
        parts.append("\n".join(lines))

    if not parts:
        await call.answer("Расписание пусто.", show_alert=True)
        return

    for chunk in parts:
        await call.message.answer(chunk, parse_mode="HTML")
    await call.answer()


@sched_router.callback_query(F.data == "sched_today")
async def cb_show_today(call: CallbackQuery):
    """Кнопка «📅 Сегодня» — расписание на текущий день с учётом переопределений."""
    chat_id = call.message.chat.id
    groups  = await sdb.get_chat_groups(chat_id)

    if not groups:
        await call.answer("Расписание не загружено.", show_alert=True)
        return

    wd       = sdb.get_local_now().isoweekday()
    day_name = DAYS_FULL.get(wd, str(wd))
    bells_cache = {b["lesson_num"]: b for b in await sdb.get_bells(chat_id)}

    parts = []
    for group in groups:
        lessons = await get_today_schedule(group["id"])  # учитывает overrides

        if not lessons:
            parts.append(f"😴 <b>{group['group_name']}</b> — {day_name}: пар нет")
            continue

        lines = [f"📅 <b>{group['group_name']} — {day_name}</b>\n"]
        for l in lessons:
            teacher = f" — {l.get('teacher')}" if l.get("teacher") else ""
            room    = f" [{l.get('room')}]" if l.get("room") else ""
            time_s  = _lesson_time_str(l, bells_cache)
            week_s  = _lesson_week_icon(l)
            lines.append(f"{l['lesson_num']}.{time_s}{week_s} <b>{l['subject']}</b>{teacher}{room}")
        parts.append("\n".join(lines))

    await call.message.answer("\n\n".join(parts) if parts else "Сегодня пар нет 🎉", parse_mode="HTML")
    await call.answer()


# ═══════════════════════════════════════════════════════════════════
# ИСТОЧНИКИ МОНИТОРИНГА — TG-каналы и VK-группы
# ═══════════════════════════════════════════════════════════════════

async def _build_sources_keyboard(chat_id: int, sources: list[dict]) -> InlineKeyboardMarkup:
    """Строим клавиатуру со списком источников и кнопками управления."""
    buttons = []
    for s in sources:
        icon = "📢" if s["source_type"] == "telegram" else "📣"
        buttons.append([InlineKeyboardButton(
            text=f"{icon} {s['source_id']}  [удалить]",
            callback_data=f"sched_del_source:{s['id']}",
        )])
    buttons += [
        [InlineKeyboardButton(text="➕ Добавить Telegram-канал", callback_data=f"sched_add_source:{chat_id}:telegram")],
        [InlineKeyboardButton(text="➕ Добавить ВКонтакте группу", callback_data=f"sched_add_source:{chat_id}:vk")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="sched_back_main")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)


@sched_router.callback_query(F.data.startswith("schedule_sources:"))
async def cb_sources(call: CallbackQuery):
    """Sources menu for schedule change monitoring."""
    chat_id = int(call.data.split(":")[1])

    if not await _is_admin(call.bot, chat_id, call.from_user.id):
        await call.answer("Admins only.", show_alert=True)
        return

    sources = await sdb.get_chat_sources(chat_id)
    kb = await _build_sources_keyboard(chat_id, sources)

    if sources:
        lines = "\n".join(
            f"- {'TG' if s['source_type'] == 'telegram' else 'VK'}: {s['source_id']}"
            for s in sources
        )
        text = (
            "<b>Schedule Sources</b>\n\n"
            "The bot watches these channels and applies changes automatically:\n\n"
            f"{lines}"
        )
    else:
        text = (
            "<b>Schedule Sources</b>\n\n"
            "No sources added yet.\n\n"
            "Add a Telegram channel or VK group and the bot will check it every "
            f"{SOURCE_MONITOR_INTERVAL_MIN} minutes."
        )

    await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await call.answer()

@sched_router.callback_query(F.data.startswith("sched_add_source:"))
async def cb_add_source(call: CallbackQuery, state: FSMContext):
    """Начинаем добавление источника — просим ввести username."""
    parts       = call.data.split(":")
    chat_id     = int(parts[1])
    source_type = parts[2]

    if not await _is_admin(call.bot, chat_id, call.from_user.id):
        await call.answer("❌ Только администраторы.", show_alert=True)
        return

    await state.update_data(chat_id=chat_id, source_type=source_type)
    await state.set_state(ScheduleStates.waiting_source)

    prompt = ("📢 Введите @username Telegram-канала (например @myuniversity):"
              if source_type == "telegram" else
              "📣 Введите короткое имя VK-группы (например myuniversity):")
    await call.message.answer(prompt)
    await call.answer()


@sched_router.message(ScheduleStates.waiting_source)
async def fsm_receive_source(message: Message, state: FSMContext):
    """Store one monitoring source."""
    data        = await state.get_data()
    chat_id     = data["chat_id"]
    source_type = data["source_type"]
    source_id   = message.text.strip().lstrip("@")
    await state.clear()

    if not source_id:
        await message.answer("Empty value. Try again via /schedule.")
        return

    if source_type == "telegram":
        source_id = "@" + source_id

    await sdb.add_source(chat_id, source_type, source_id)
    await message.answer(
        f"Source added.\n\n"
        f"Type: <b>{'Telegram' if source_type == 'telegram' else 'VK'}</b>\n"
        f"Channel/Group: <b>{source_id}</b>\n\n"
        f"Check interval: every {SOURCE_MONITOR_INTERVAL_MIN} minutes.",
        parse_mode="HTML",
    )

@sched_router.callback_query(F.data.startswith("sched_del_source:"))
async def cb_del_source(call: CallbackQuery):
    """Удаляем источник мониторинга и перестраиваем клавиатуру."""
    source_id_int = int(call.data.split(":")[1])
    await sdb.delete_source(source_id_int)
    await call.answer("✅ Источник удалён.")

    chat_id = call.message.chat.id
    sources = await sdb.get_chat_sources(chat_id)
    kb = await _build_sources_keyboard(chat_id, sources)
    try:
        await call.message.edit_reply_markup(reply_markup=kb)
    except Exception:
        pass


# ═══════════════════════════════════════════════════════════════════
# НАСТРОЙКА ОЧЕРЕДЕЙ — для каких пар создавать/не создавать очередь
# ═══════════════════════════════════════════════════════════════════

@sched_router.callback_query(F.data.startswith("schedule_skip:"))
async def cb_schedule_skip(call: CallbackQuery):
    """Показываем список занятий с переключателями skip_queue."""
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
                skip    = bool(l.get("skip_queue", 0))
                icon    = "🔕" if skip else "🔔"
                day_abbr = DAYS_SHORT[wd]
                buttons.append([InlineKeyboardButton(
                    text=f"{icon} {day_abbr} {l['lesson_num']}. {l['subject'][:30]}",
                    callback_data=f"sched_toggle_skip:{l['id']}:{chat_id}",
                )])

    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data="sched_back_main")])

    await call.message.edit_text(
        "🔔 <b>Настройка автоматических очередей</b>\n\n"
        "Нажмите на занятие чтобы включить/выключить создание очереди:\n\n"
        "🔔 — очередь создаётся автоматически\n"
        "🔕 — очередь не создаётся",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode="HTML",
    )
    await call.answer()


@sched_router.callback_query(F.data.startswith("sched_toggle_skip:"))
async def cb_toggle_skip(call: CallbackQuery):
    """Переключаем skip_queue для занятия и обновляем список."""
    parts     = call.data.split(":")
    lesson_id = int(parts[1])
    chat_id   = int(parts[2])

    if not await _is_admin(call.bot, chat_id, call.from_user.id):
        await call.answer("❌ Только администраторы.", show_alert=True)
        return

    await sdb.toggle_lesson_skip_queue(lesson_id)
    await call.answer("✅ Обновлено.")

    # Перезагружаем страницу настроек
    call.data = f"schedule_skip:{chat_id}"
    await cb_schedule_skip(call)


# ═══════════════════════════════════════════════════════════════════
# РЕДАКТОР БАЗОВОГО РАСПИСАНИЯ — изменение занятий вручную
# ═══════════════════════════════════════════════════════════════════

@sched_router.callback_query(F.data.startswith("sched_edit:"))
async def cb_edit_entry(call: CallbackQuery):
    """Точка входа в редактор — выбор группы."""
    chat_id = int(call.data.split(":")[1])

    if not await _is_admin(call.bot, chat_id, call.from_user.id):
        await call.answer("❌ Только администраторы.", show_alert=True)
        return

    groups = await sdb.get_chat_groups(chat_id)
    if not groups:
        await call.answer("Расписание не загружено.", show_alert=True)
        return

    buttons = [
        [InlineKeyboardButton(text=f"👥 {g['group_name']}", callback_data=f"sched_edit_group:{chat_id}:{g['id']}")]
        for g in groups
    ]
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data="sched_back_main")])

    await call.message.edit_text(
        "✏️ <b>Редактирование расписания</b>\n\nВыберите группу:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode="HTML",
    )
    await call.answer()


@sched_router.callback_query(F.data.startswith("sched_edit_group:"))
async def cb_edit_group(call: CallbackQuery):
    """Выбор дня недели для редактирования."""
    _, chat_id_s, group_id_s = call.data.split(":")
    chat_id, group_id = int(chat_id_s), int(group_id_s)

    if not await _is_admin(call.bot, chat_id, call.from_user.id):
        await call.answer("❌ Только администраторы.", show_alert=True)
        return

    buttons = []
    for wd in range(1, 8):
        lessons = await sdb.get_lessons_for_day(group_id, wd)
        if lessons:
            buttons.append([InlineKeyboardButton(
                text=f"📅 {DAYS_FULL[wd]} ({len(lessons)} пар)",
                callback_data=f"sched_edit_day:{chat_id}:{group_id}:{wd}",
            )])

    buttons.append([InlineKeyboardButton(
        text="➕ Добавить занятие",
        callback_data=f"sched_add_day_select:{chat_id}:{group_id}",
    )])
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data=f"sched_edit:{chat_id}")])

    await call.message.edit_text(
        "✏️ <b>Выберите день для редактирования:</b>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode="HTML",
    )
    await call.answer()


@sched_router.callback_query(F.data.startswith("sched_edit_day:"))
async def cb_edit_day(call: CallbackQuery):
    """Список занятий выбранного дня."""
    _, chat_id_s, group_id_s, wd_s = call.data.split(":")
    chat_id, group_id, wd = int(chat_id_s), int(group_id_s), int(wd_s)

    if not await _is_admin(call.bot, chat_id, call.from_user.id):
        await call.answer("❌ Только администраторы.", show_alert=True)
        return

    lessons = await sdb.get_lessons_for_day(group_id, wd)
    bells_cache = {b["lesson_num"]: b for b in await sdb.get_bells(chat_id)}

    buttons = []
    for l in sorted(lessons, key=lambda x: x.get("lesson_num", 0)):
        time_s    = _lesson_time_str(l, bells_cache)
        skip_icon = " 🔕" if l.get("skip_queue") else ""
        buttons.append([InlineKeyboardButton(
            text=f"{l['lesson_num']}.{time_s} {l['subject'][:28]}{skip_icon}",
            callback_data=f"sched_edit_lesson:{chat_id}:{group_id}:{l['id']}:{wd}",
        )])

    buttons.append([InlineKeyboardButton(
        text="➕ Добавить занятие в этот день",
        callback_data=f"sched_add_lesson:{chat_id}:{group_id}:{wd}",
    )])
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data=f"sched_edit_group:{chat_id}:{group_id}")])

    await call.message.edit_text(
        f"✏️ <b>{DAYS_FULL.get(wd)}</b> — выберите занятие:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode="HTML",
    )
    await call.answer()


def _lesson_actions_keyboard(chat_id: int, group_id: int, lesson_id: int, wd: int) -> InlineKeyboardMarkup:
    """Кнопки действий с конкретным занятием: изменить поле / удалить / переключить очередь."""
    base = f"{chat_id}:{group_id}:{lesson_id}:{wd}"
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📚 Предмет",       callback_data=f"sched_ef:{base}:subject"),
            InlineKeyboardButton(text="👤 Преподаватель", callback_data=f"sched_ef:{base}:teacher"),
        ],
        [
            InlineKeyboardButton(text="🏫 Аудитория",     callback_data=f"sched_ef:{base}:room"),
            InlineKeyboardButton(text="⏰ Время",          callback_data=f"sched_ef:{base}:time"),
        ],
        [
            InlineKeyboardButton(text="🔔/🔕 Очередь",   callback_data=f"sched_toggle_skip2:{chat_id}:{lesson_id}:{group_id}:{wd}"),
            InlineKeyboardButton(text="📆 Неделя",         callback_data=f"sched_toggle_week:{chat_id}:{lesson_id}:{group_id}:{wd}"),
        ],
        [
            InlineKeyboardButton(text="🎓 Мероприятие",   callback_data=f"sched_toggle_event:{chat_id}:{lesson_id}:{group_id}:{wd}"),
            InlineKeyboardButton(text="🗑 Удалить",        callback_data=f"sched_del_lesson:{chat_id}:{group_id}:{lesson_id}:{wd}"),
        ],
        [InlineKeyboardButton(text="◀️ Назад", callback_data=f"sched_edit_day:{chat_id}:{group_id}:{wd}")],
    ])


@sched_router.callback_query(F.data.startswith("sched_edit_lesson:"))
async def cb_edit_lesson(call: CallbackQuery):
    """Карточка занятия с кнопками для изменения каждого поля."""
    _, chat_id_s, group_id_s, lesson_id_s, wd_s = call.data.split(":")
    chat_id, group_id, lesson_id, wd = int(chat_id_s), int(group_id_s), int(lesson_id_s), int(wd_s)

    if not await _is_admin(call.bot, chat_id, call.from_user.id):
        await call.answer("❌ Только администраторы.", show_alert=True)
        return

    lesson = await sdb.get_lesson_by_id(lesson_id)
    if not lesson:
        await call.answer("Занятие не найдено.", show_alert=True)
        return

    bells_cache = {b["lesson_num"]: b for b in await sdb.get_bells(chat_id)}
    time_s = _lesson_time_str(lesson, bells_cache)

    wt   = lesson.get("week_type", 0)
    ev   = lesson.get("is_event", 0)
    wt_s = WEEK_TYPE_LABELS.get(wt, "?")
    ev_s = " | 🎓 мероприятие" if ev else ""
    text = (
        f"✏️ <b>Редактирование занятия</b>\n\n"
        f"📚 Предмет: <b>{lesson['subject']}</b>\n"
        f"👤 Преподаватель: {lesson.get('teacher') or '—'}\n"
        f"🏫 Аудитория: {lesson.get('room') or '—'}\n"
        f"⏰ Время:{time_s or ' —'}\n"
        f"🔔 Очередь: {'выкл 🔕' if lesson.get('skip_queue') else 'вкл 🔔'}\n"
        f"📅 {DAYS_FULL.get(wd)}, пара {lesson['lesson_num']}\n"
        f"📆 Неделя: <b>{wt_s}</b>{ev_s}"
    )
    await call.message.edit_text(
        text,
        reply_markup=_lesson_actions_keyboard(chat_id, group_id, lesson_id, wd),
        parse_mode="HTML",
    )
    await call.answer()


# Метки полей для FSM редактирования
FIELD_PROMPTS = {
    "subject": "📚 Введите новое <b>название предмета</b>:",
    "teacher": "👤 Введите <b>ФИО преподавателя</b> (или «-» чтобы убрать):",
    "room":    "🏫 Введите <b>номер аудитории</b> (или «-» чтобы убрать):",
    "time":    "⏰ Введите <b>время</b> в формате <code>HH:MM-HH:MM</code>\nНапример: <code>08:00-09:35</code>",
}


@sched_router.callback_query(F.data.startswith("sched_ef:"))
async def cb_edit_field_start(call: CallbackQuery, state: FSMContext):
    """Начало редактирования поля занятия — запрашиваем новое значение."""
    # Формат: sched_ef:{chat_id}:{group_id}:{lesson_id}:{wd}:{field}
    parts = call.data.split(":")
    chat_id, group_id, lesson_id, wd, field = (
        int(parts[1]), int(parts[2]), int(parts[3]), int(parts[4]), parts[5]
    )

    if not await _is_admin(call.bot, chat_id, call.from_user.id):
        await call.answer("❌ Только администраторы.", show_alert=True)
        return

    await state.update_data(chat_id=chat_id, group_id=group_id, lesson_id=lesson_id, wd=wd, field=field)
    await state.set_state(ScheduleStates.edit_lesson_field)

    cancel_kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="❌ Отмена", callback_data=f"sched_edit_lesson:{chat_id}:{group_id}:{lesson_id}:{wd}")
    ]])
    await call.message.answer(
        FIELD_PROMPTS.get(field, "Введите значение:"),
        reply_markup=cancel_kb, parse_mode="HTML"
    )
    await call.answer()


@sched_router.message(ScheduleStates.edit_lesson_field)
async def fsm_edit_field_receive(message: Message, state: FSMContext):
    """Получаем новое значение поля и сохраняем в БД."""
    data      = await state.get_data()
    lesson_id = data["lesson_id"]
    wd        = data["wd"]
    field     = data["field"]
    value     = message.text.strip()
    await state.clear()

    if value == "-":
        value = ""  # прочерк означает «очистить поле»

    if field == "time":
        # Парсим время в формате HH:MM-HH:MM
        m = re.match(r"(\d{1,2}:\d{2})\s*[-–]\s*(\d{1,2}:\d{2})", value)
        if not m:
            await message.answer("❌ Неверный формат. Введите как <b>08:00-09:35</b>", parse_mode="HTML")
            return
        await sdb.update_lesson_field(lesson_id, "time_start", m.group(1))
        await sdb.update_lesson_field(lesson_id, "time_end", m.group(2))
        await message.answer(f"✅ Время обновлено: <b>{m.group(1)}–{m.group(2)}</b>", parse_mode="HTML")
    else:
        await sdb.update_lesson_field(lesson_id, field, value)
        labels = {"subject": "Предмет", "teacher": "Преподаватель", "room": "Аудитория"}
        val_text = f"\nНовое значение: <b>{value}</b>" if value else "\nЗначение очищено."
        await message.answer(f"✅ {labels.get(field, field)} обновлён.{val_text}", parse_mode="HTML")


@sched_router.callback_query(F.data.startswith("sched_toggle_skip2:"))
async def cb_toggle_skip2(call: CallbackQuery):
    """Переключатель очереди прямо из карточки занятия."""
    # Формат: sched_toggle_skip2:{chat_id}:{lesson_id}:{group_id}:{wd}
    _, chat_id_s, lesson_id_s, group_id_s, wd_s = call.data.split(":")
    chat_id, lesson_id, group_id, wd = int(chat_id_s), int(lesson_id_s), int(group_id_s), int(wd_s)

    if not await _is_admin(call.bot, chat_id, call.from_user.id):
        await call.answer("❌ Только администраторы.", show_alert=True)
        return

    await sdb.toggle_lesson_skip_queue(lesson_id)
    await call.answer("✅ Обновлено.")

    # Перерисовываем карточку занятия
    call.data = f"sched_edit_lesson:{chat_id}:{group_id}:{lesson_id}:{wd}"
    await cb_edit_lesson(call)


@sched_router.callback_query(F.data.startswith("sched_del_lesson:"))
async def cb_del_lesson(call: CallbackQuery):
    """Запрос подтверждения удаления занятия из базового расписания."""
    _, chat_id_s, group_id_s, lesson_id_s, wd_s = call.data.split(":")
    chat_id, group_id, lesson_id, wd = int(chat_id_s), int(group_id_s), int(lesson_id_s), int(wd_s)

    if not await _is_admin(call.bot, chat_id, call.from_user.id):
        await call.answer("❌ Только администраторы.", show_alert=True)
        return

    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Да, удалить", callback_data=f"sched_del_lesson_confirm:{chat_id}:{group_id}:{lesson_id}:{wd}"),
        InlineKeyboardButton(text="❌ Отмена",      callback_data=f"sched_edit_lesson:{chat_id}:{group_id}:{lesson_id}:{wd}"),
    ]])
    await call.message.edit_text(
        "⚠️ <b>Удалить это занятие из базового расписания?</b>\n\n"
        "Это действие нельзя отменить.\n"
        "Если нужно убрать пару только на один день — используйте <b>📋 Изменить на дату</b>.",
        reply_markup=kb, parse_mode="HTML",
    )
    await call.answer()


@sched_router.callback_query(F.data.startswith("sched_del_lesson_confirm:"))
async def cb_del_lesson_confirm(call: CallbackQuery):
    """Подтверждаем и удаляем занятие из базового расписания."""
    _, chat_id_s, group_id_s, lesson_id_s, wd_s = call.data.split(":")
    chat_id, group_id, lesson_id, wd = int(chat_id_s), int(group_id_s), int(lesson_id_s), int(wd_s)

    if not await _is_admin(call.bot, chat_id, call.from_user.id):
        await call.answer("❌ Только администраторы.", show_alert=True)
        return

    await sdb.delete_lesson(lesson_id)
    await call.answer("✅ Занятие удалено.")

    # Возвращаемся к списку занятий дня
    call.data = f"sched_edit_day:{chat_id}:{group_id}:{wd}"
    await cb_edit_day(call)


# ─── Добавление нового занятия — пошаговый диалог ───

@sched_router.callback_query(F.data.startswith("sched_add_day_select:"))
async def cb_add_day_select(call: CallbackQuery):
    """Выбор дня недели для добавления нового занятия."""
    _, chat_id_s, group_id_s = call.data.split(":")
    chat_id, group_id = int(chat_id_s), int(group_id_s)

    if not await _is_admin(call.bot, chat_id, call.from_user.id):
        await call.answer("❌ Только администраторы.", show_alert=True)
        return

    buttons = [
        [InlineKeyboardButton(text=DAYS_FULL[wd], callback_data=f"sched_add_lesson:{chat_id}:{group_id}:{wd}")]
        for wd in range(1, 7)
    ]
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data=f"sched_edit_group:{chat_id}:{group_id}")])
    await call.message.edit_text(
        "➕ Выберите день для нового занятия:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode="HTML",
    )
    await call.answer()


@sched_router.callback_query(F.data.startswith("sched_add_lesson:"))
async def cb_add_lesson_start(call: CallbackQuery, state: FSMContext):
    """Шаг 1 — запрашиваем название предмета."""
    _, chat_id_s, group_id_s, wd_s = call.data.split(":")
    chat_id, group_id, wd = int(chat_id_s), int(group_id_s), int(wd_s)

    if not await _is_admin(call.bot, chat_id, call.from_user.id):
        await call.answer("❌ Только администраторы.", show_alert=True)
        return

    # Определяем следующий номер пары автоматически
    existing = await sdb.get_lessons_for_day(group_id, wd)
    next_num = max((l["lesson_num"] for l in existing), default=0) + 1

    await state.update_data(
        chat_id=chat_id, group_id=group_id, wd=wd, lesson_num=next_num,
        new_teacher="", new_room="", new_time_start="", new_time_end=""
    )
    await state.set_state(ScheduleStates.add_lesson_subject)

    cancel_kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="❌ Отмена", callback_data=f"sched_edit_day:{chat_id}:{group_id}:{wd}")
    ]])
    await call.message.answer(
        f"➕ <b>Новое занятие — {DAYS_FULL.get(wd)}, пара {next_num}</b>\n\n"
        f"Шаг 1/4 — Введите <b>название предмета</b>:",
        reply_markup=cancel_kb, parse_mode="HTML",
    )
    await call.answer()


@sched_router.message(ScheduleStates.add_lesson_subject)
async def fsm_add_subject(message: Message, state: FSMContext):
    """Шаг 1 получен — переходим к преподавателю."""
    await state.update_data(new_subject=message.text.strip())
    await state.set_state(ScheduleStates.add_lesson_teacher)
    data = await state.get_data()
    cancel_kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="⏭ Пропустить", callback_data="add_skip_teacher"),
        InlineKeyboardButton(text="❌ Отмена", callback_data=f"sched_edit_day:{data['chat_id']}:{data['group_id']}:{data['wd']}")
    ]])
    await message.answer("Шаг 2/4 — Введите <b>ФИО преподавателя</b> (или нажмите Пропустить):",
                         reply_markup=cancel_kb, parse_mode="HTML")


@sched_router.callback_query(F.data == "add_skip_teacher")
async def cb_skip_teacher(call: CallbackQuery, state: FSMContext):
    """Пропустить ввод преподавателя."""
    await state.update_data(new_teacher="")
    await state.set_state(ScheduleStates.add_lesson_room)
    data = await state.get_data()
    cancel_kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="⏭ Пропустить", callback_data="add_skip_room"),
        InlineKeyboardButton(text="❌ Отмена", callback_data=f"sched_edit_day:{data['chat_id']}:{data['group_id']}:{data['wd']}")
    ]])
    await call.message.answer("Шаг 3/4 — Введите <b>аудиторию</b> (или нажмите Пропустить):",
                               reply_markup=cancel_kb, parse_mode="HTML")
    await call.answer()


@sched_router.message(ScheduleStates.add_lesson_teacher)
async def fsm_add_teacher(message: Message, state: FSMContext):
    """Шаг 2 получен — переходим к аудитории."""
    await state.update_data(new_teacher=message.text.strip())
    await state.set_state(ScheduleStates.add_lesson_room)
    data = await state.get_data()
    cancel_kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="⏭ Пропустить", callback_data="add_skip_room"),
        InlineKeyboardButton(text="❌ Отмена", callback_data=f"sched_edit_day:{data['chat_id']}:{data['group_id']}:{data['wd']}")
    ]])
    await message.answer("Шаг 3/4 — Введите <b>аудиторию</b> (или нажмите Пропустить):",
                         reply_markup=cancel_kb, parse_mode="HTML")


@sched_router.callback_query(F.data == "add_skip_room")
async def cb_skip_room(call: CallbackQuery, state: FSMContext):
    """Пропустить ввод аудитории."""
    await state.update_data(new_room="")
    await state.set_state(ScheduleStates.add_lesson_time)
    data = await state.get_data()
    cancel_kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="⏭ Пропустить", callback_data="add_skip_time"),
        InlineKeyboardButton(text="❌ Отмена", callback_data=f"sched_edit_day:{data['chat_id']}:{data['group_id']}:{data['wd']}")
    ]])
    await call.message.answer(
        "Шаг 4/4 — Введите <b>время</b> в формате <code>HH:MM-HH:MM</code>\n"
        "Например: <code>08:00-09:35</code>\n(или нажмите Пропустить, время возьмётся из звонков)",
        reply_markup=cancel_kb, parse_mode="HTML"
    )
    await call.answer()


@sched_router.message(ScheduleStates.add_lesson_room)
async def fsm_add_room(message: Message, state: FSMContext):
    """Шаг 3 получен — переходим к времени."""
    await state.update_data(new_room=message.text.strip())
    await state.set_state(ScheduleStates.add_lesson_time)
    data = await state.get_data()
    cancel_kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="⏭ Пропустить", callback_data="add_skip_time"),
        InlineKeyboardButton(text="❌ Отмена", callback_data=f"sched_edit_day:{data['chat_id']}:{data['group_id']}:{data['wd']}")
    ]])
    await message.answer(
        "Шаг 4/4 — Введите <b>время</b> в формате <code>HH:MM-HH:MM</code>\n"
        "Например: <code>08:00-09:35</code>\n(или нажмите Пропустить, время возьмётся из звонков)",
        reply_markup=cancel_kb, parse_mode="HTML"
    )


async def _finish_add_lesson(target, state: FSMContext, ts: str = "", te: str = ""):
    """Финальный шаг — сохраняем новое занятие в БД."""
    data = await state.get_data()
    await state.clear()

    lesson = {
        "weekday":    data["wd"],
        "lesson_num": data["lesson_num"],
        "subject":    data.get("new_subject", "Занятие"),
        "teacher":    data.get("new_teacher") or None,
        "room":       data.get("new_room") or None,
        "time_start": ts,
        "time_end":   te,
        "skip_queue": 0,
    }
    # Используем отдельную функцию чтобы не удалить все остальные занятия!
    await sdb.add_single_lesson(data["group_id"], lesson)

    text = (
        f"✅ <b>Занятие добавлено!</b>\n\n"
        f"📚 {lesson['subject']}\n"
        f"📅 {DAYS_FULL.get(data['wd'])}, пара {lesson['lesson_num']}\n"
        + (f"👤 {lesson['teacher']}\n" if lesson['teacher'] else "")
        + (f"🏫 {lesson['room']}\n" if lesson['room'] else "")
        + (f"⏰ {ts}–{te}" if ts else "⏰ Время возьмётся из расписания звонков")
    )

    if hasattr(target, "answer"):
        await target.answer(text, parse_mode="HTML")
    else:
        await target.message.answer(text, parse_mode="HTML")
        await target.answer()


@sched_router.callback_query(F.data == "add_skip_time")
async def cb_skip_time(call: CallbackQuery, state: FSMContext):
    """Пропустить ввод времени — сохраняем занятие без явного времени."""
    await _finish_add_lesson(call, state)


@sched_router.message(ScheduleStates.add_lesson_time)
async def fsm_add_time(message: Message, state: FSMContext):
    """Шаг 4 получен — сохраняем занятие с временем."""
    m = re.match(r"(\d{1,2}:\d{2})\s*[-–]\s*(\d{1,2}:\d{2})", message.text.strip())
    if not m:
        await message.answer("❌ Неверный формат. Введите как <b>08:00-09:35</b> или нажмите Пропустить.", parse_mode="HTML")
        return
    await _finish_add_lesson(message, state, m.group(1), m.group(2))


# ═══════════════════════════════════════════════════════════════════
# ИЗМЕНЕНИЯ НА ДАТУ — отмена/добавление пар на конкретный день
# ═══════════════════════════════════════════════════════════════════

@sched_router.callback_query(F.data.startswith("sched_override:"))
async def cb_override_entry(call: CallbackQuery):
    """Точка входа в меню ручных изменений на дату — выбор группы."""
    chat_id = int(call.data.split(":")[1])

    if not await _is_admin(call.bot, chat_id, call.from_user.id):
        await call.answer("❌ Только администраторы.", show_alert=True)
        return

    groups = await sdb.get_chat_groups(chat_id)
    if not groups:
        await call.answer("Расписание не загружено.", show_alert=True)
        return

    buttons = [
        [InlineKeyboardButton(text=f"👥 {g['group_name']}", callback_data=f"sched_override_group:{chat_id}:{g['id']}")]
        for g in groups
    ]
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data="sched_back_main")])
    await call.message.edit_text(
        "📋 <b>Изменение расписания на дату</b>\n\nВыберите группу:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode="HTML",
    )
    await call.answer()


@sched_router.callback_query(F.data.startswith("sched_override_group:"))
async def cb_override_group(call: CallbackQuery):
    """Показываем ближайшие 7 дней с занятиями для выбора дня."""
    _, chat_id_s, group_id_s = call.data.split(":")
    chat_id, group_id = int(chat_id_s), int(group_id_s)

    if not await _is_admin(call.bot, chat_id, call.from_user.id):
        await call.answer("❌ Только администраторы.", show_alert=True)
        return

    today   = sdb.get_local_now()
    buttons = []
    for delta in range(7):
        day = today + timedelta(days=delta)
        wd  = day.isoweekday()
        lessons = await sdb.get_lessons_for_day(group_id, wd)
        date_str = day.strftime("%d.%m")
        label    = f"{'Сегодня' if delta == 0 else 'Завтра' if delta == 1 else DAYS_SHORT[wd]} {date_str}"
        if lessons:
            label += f" ({len(lessons)} пар)"
        buttons.append([InlineKeyboardButton(
            text=f"📅 {label}",
            callback_data=f"sched_override_day:{chat_id}:{group_id}:{day.strftime('%Y-%m-%d')}",
        )])

    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data=f"sched_override:{chat_id}")])
    await call.message.edit_text(
        "📋 Выберите день для изменений:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode="HTML",
    )
    await call.answer()


@sched_router.callback_query(F.data.startswith("sched_override_day:"))
async def cb_override_day(call: CallbackQuery):
    """Список пар на выбранный день — можно отменить каждую или добавить новую."""
    parts = call.data.split(":")
    chat_id, group_id, date_str = int(parts[1]), int(parts[2]), parts[3]

    if not await _is_admin(call.bot, chat_id, call.from_user.id):
        await call.answer("❌ Только администраторы.", show_alert=True)
        return

    d = dt_date.fromisoformat(date_str)
    lessons = await sdb.get_lessons_for_day(group_id, d.isoweekday())

    # Проверяем какие пары уже отменены на эту дату
    overrides = await sdb.get_overrides_for_date(group_id, date_str)
    cancelled = {o["lesson_num"] for o in overrides if o.get("action") == "cancel"}

    buttons = []
    for l in sorted(lessons, key=lambda x: x.get("lesson_num", 0)):
        num = l.get("lesson_num", 0)
        if num in cancelled:
            # Пара уже отменена — показываем с пометкой и кнопкой восстановления
            buttons.append([InlineKeyboardButton(
                text=f"↩️ {num}. {l['subject'][:25]} [отменена]",
                callback_data=f"sched_or_restore:{chat_id}:{group_id}:{num}:{date_str}",
            )])
        else:
            buttons.append([InlineKeyboardButton(
                text=f"❌ {num}. {l['subject'][:25]}",
                callback_data=f"sched_or_cancel:{chat_id}:{group_id}:{num}:{date_str}",
            )])

    buttons.append([InlineKeyboardButton(
        text="➕ Добавить занятие на эту дату",
        callback_data=f"sched_or_add:{chat_id}:{group_id}:{date_str}",
    )])
    buttons.append([InlineKeyboardButton(
        text="◀️ Назад",
        callback_data=f"sched_override_group:{chat_id}:{group_id}",
    )])

    day_fmt = d.strftime("%d.%m.%Y")
    await call.message.edit_text(
        f"📋 <b>{day_fmt}</b>\n\n"
        f"❌ Нажмите чтобы <b>отменить пару</b> на этот день\n"
        f"↩️ Нажмите чтобы <b>восстановить</b> отменённую пару",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode="HTML",
    )
    await call.answer()


@sched_router.callback_query(F.data.startswith("sched_or_cancel:"))
async def cb_or_cancel(call: CallbackQuery):
    """Отменяем конкретную пару на дату — записываем override с action=cancel."""
    parts = call.data.split(":")
    chat_id, group_id, lesson_num, date_str = int(parts[1]), int(parts[2]), int(parts[3]), parts[4]

    if not await _is_admin(call.bot, chat_id, call.from_user.id):
        await call.answer("❌ Только администраторы.", show_alert=True)
        return

    await sdb.save_override(group_id, {"action": "cancel", "lesson_num": lesson_num}, fallback_date=date_str)

    day_fmt = dt_date.fromisoformat(date_str).strftime("%d.%m.%Y")
    await call.answer(f"✅ Пара {lesson_num} на {day_fmt} отменена.", show_alert=False)

    # Перерисовываем страницу с обновлёнными статусами
    call.data = f"sched_override_day:{chat_id}:{group_id}:{date_str}"
    await cb_override_day(call)


@sched_router.callback_query(F.data.startswith("sched_or_restore:"))
async def cb_or_restore(call: CallbackQuery):
    """Восстанавливаем ранее отменённую пару — удаляем override из БД."""
    parts = call.data.split(":")
    chat_id, group_id, lesson_num, date_str = int(parts[1]), int(parts[2]), int(parts[3]), parts[4]

    if not await _is_admin(call.bot, chat_id, call.from_user.id):
        await call.answer("❌ Только администраторы.", show_alert=True)
        return

    await sdb.delete_override(group_id, lesson_num, date_str)

    day_fmt = dt_date.fromisoformat(date_str).strftime("%d.%m.%Y")
    await call.answer(f"↩️ Пара {lesson_num} на {day_fmt} восстановлена.", show_alert=False)

    call.data = f"sched_override_day:{chat_id}:{group_id}:{date_str}"
    await cb_override_day(call)


@sched_router.callback_query(F.data.startswith("sched_or_add:"))
async def cb_or_add(call: CallbackQuery, state: FSMContext):
    """Начинаем добавление внепланового занятия на конкретную дату."""
    parts = call.data.split(":")
    chat_id, group_id, date_str = int(parts[1]), int(parts[2]), parts[3]

    if not await _is_admin(call.bot, chat_id, call.from_user.id):
        await call.answer("❌ Только администраторы.", show_alert=True)
        return

    await state.update_data(
        chat_id=chat_id, group_id=group_id,
        wd=dt_date.fromisoformat(date_str).isoweekday(),
        lesson_num=99,  # спецномер для внепланового
        new_teacher="", new_room="", new_time_start="", new_time_end="",
        override_date=date_str,  # запоминаем дату — нужно для сохранения override
    )
    await state.set_state(ScheduleStates.add_lesson_subject)

    day_fmt = dt_date.fromisoformat(date_str).strftime("%d.%m.%Y")
    cancel_kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="❌ Отмена", callback_data=f"sched_override_day:{chat_id}:{group_id}:{date_str}")
    ]])
    await call.message.answer(
        f"➕ <b>Внеплановое занятие на {day_fmt}</b>\n\nШаг 1/4 — Введите название предмета:",
        reply_markup=cancel_kb, parse_mode="HTML"
    )
    await call.answer()


# ═══════════════════════════════════════════════════════════════════
# РАСПИСАНИЕ ЗВОНКОВ — редактирование времени начала/конца пар
# ═══════════════════════════════════════════════════════════════════

def _bells_text(bells: list[dict]) -> str:
    """Форматируем расписание звонков для отображения."""
    lines = ["🔔 <b>Расписание звонков</b>\n"]
    for b in bells:
        lines.append(f"  Пара <b>{b['lesson_num']}</b>: {b['time_start']} – {b['time_end']}")
    return "\n".join(lines)


def _bells_keyboard(chat_id: int, bells: list[dict]) -> InlineKeyboardMarkup:
    """Клавиатура для редактора звонков — каждая пара кликабельна."""
    buttons = [
        [InlineKeyboardButton(
            text=f"✏️ Пара {b['lesson_num']}: {b['time_start']}–{b['time_end']}",
            callback_data=f"bells_edit:{chat_id}:{b['lesson_num']}",
        )]
        for b in bells
    ]
    buttons += [
        [InlineKeyboardButton(text="➕ Добавить пару", callback_data=f"bells_add:{chat_id}")],
        [InlineKeyboardButton(text="🔄 Сбросить к дефолту", callback_data=f"bells_reset:{chat_id}")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="sched_back_main")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)


@sched_router.callback_query(F.data.startswith("sched_bells:"))
async def cb_bells_menu(call: CallbackQuery):
    """Меню редактора расписания звонков."""
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


@sched_router.callback_query(F.data.startswith("bells_edit:"))
async def cb_bells_edit(call: CallbackQuery, state: FSMContext):
    """Редактирование времени конкретной пары."""
    _, chat_id_s, num_s = call.data.split(":")
    chat_id, lesson_num = int(chat_id_s), int(num_s)

    if not await _is_admin(call.bot, chat_id, call.from_user.id):
        await call.answer("❌ Только администраторы.", show_alert=True)
        return

    await state.update_data(chat_id=chat_id, lesson_num=lesson_num)
    await state.set_state(BellStates.waiting_time)

    # Показываем текущее время
    bells = await sdb.get_bells(chat_id)
    bell  = next((b for b in bells if b["lesson_num"] == lesson_num), None)
    current = f"{bell['time_start']}–{bell['time_end']}" if bell else "дефолт"

    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="🗑 Удалить (вернуть дефолт)", callback_data=f"bells_del:{chat_id}:{lesson_num}"),
        InlineKeyboardButton(text="❌ Отмена", callback_data=f"sched_bells:{chat_id}"),
    ]])
    await call.message.answer(
        f"⏰ <b>Пара {lesson_num}</b> — сейчас: <code>{current}</code>\n\n"
        f"Введите новое время в формате <b>HH:MM-HH:MM</b>\n"
        f"Например: <code>09:45-11:20</code>",
        reply_markup=kb, parse_mode="HTML",
    )
    await call.answer()


@sched_router.message(BellStates.waiting_time)
async def fsm_bells_receive_time(message: Message, state: FSMContext):
    """Получаем новое время для звонка и сохраняем."""
    data = await state.get_data()
    m    = re.match(r"(\d{1,2}:\d{2})\s*[-–]\s*(\d{1,2}:\d{2})", message.text.strip())
    if not m:
        await message.answer("❌ Неверный формат. Введите как <b>09:45-11:20</b>", parse_mode="HTML")
        return

    await state.clear()
    ts, te = m.group(1), m.group(2)
    await sdb.set_bell(data["chat_id"], data["lesson_num"], ts, te)

    bells = await sdb.get_bells(data["chat_id"])
    await message.answer(
        f"✅ Пара <b>{data['lesson_num']}</b>: {ts} – {te} сохранено.\n\n" + _bells_text(bells),
        reply_markup=_bells_keyboard(data["chat_id"], bells),
        parse_mode="HTML",
    )


@sched_router.callback_query(F.data.startswith("bells_add:"))
async def cb_bells_add(call: CallbackQuery, state: FSMContext):
    """Добавление новой пары в расписание звонков."""
    chat_id  = int(call.data.split(":")[1])

    if not await _is_admin(call.bot, chat_id, call.from_user.id):
        await call.answer("❌ Только администраторы.", show_alert=True)
        return

    bells    = await sdb.get_bells(chat_id)
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
        reply_markup=kb, parse_mode="HTML",
    )
    await call.answer()


@sched_router.message(BellStates.waiting_add_time)
async def fsm_bells_add_time(message: Message, state: FSMContext):
    """Сохраняем время новой пары."""
    data = await state.get_data()
    m    = re.match(r"(\d{1,2}:\d{2})\s*[-–]\s*(\d{1,2}:\d{2})", message.text.strip())
    if not m:
        await message.answer("❌ Неверный формат. Введите как <b>18:50-20:25</b>", parse_mode="HTML")
        return

    await state.clear()
    ts, te = m.group(1), m.group(2)
    await sdb.set_bell(data["chat_id"], data["lesson_num"], ts, te)

    bells = await sdb.get_bells(data["chat_id"])
    await message.answer(
        f"✅ Пара <b>{data['lesson_num']}</b>: {ts} – {te} добавлена.\n\n" + _bells_text(bells),
        reply_markup=_bells_keyboard(data["chat_id"], bells),
        parse_mode="HTML",
    )


@sched_router.callback_query(F.data.startswith("bells_del:"))
async def cb_bells_del(call: CallbackQuery, state: FSMContext):
    """Удаляем кастомный звонок — пара вернётся к дефолтному времени."""
    _, chat_id_s, num_s = call.data.split(":")
    chat_id, lesson_num = int(chat_id_s), int(num_s)

    if not await _is_admin(call.bot, chat_id, call.from_user.id):
        await call.answer("❌ Только администраторы.", show_alert=True)
        return

    await state.clear()
    # Удаляем запись — _get_db_type() определён в schedule_db
    await sdb.delete_bell(chat_id, lesson_num)

    bells = await sdb.get_bells(chat_id)
    await call.answer(f"✅ Пара {lesson_num} сброшена к дефолту.")
    await call.message.edit_text(
        _bells_text(bells) + "\n\nНажмите на пару чтобы изменить время:",
        reply_markup=_bells_keyboard(chat_id, bells),
        parse_mode="HTML",
    )


@sched_router.callback_query(F.data.startswith("bells_reset:"))
async def cb_bells_reset(call: CallbackQuery):
    """Запрос подтверждения сброса ВСЕХ звонков к дефолту."""
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
        reply_markup=kb, parse_mode="HTML",
    )
    await call.answer()


@sched_router.callback_query(F.data.startswith("bells_reset_confirm:"))
async def cb_bells_reset_confirm(call: CallbackQuery):
    """Сбрасываем все кастомные звонки к дефолту."""
    chat_id = int(call.data.split(":")[1])

    if not await _is_admin(call.bot, chat_id, call.from_user.id):
        await call.answer("❌ Только администраторы.", show_alert=True)
        return

    await sdb.reset_bells(chat_id)
    bells = await sdb.get_bells(chat_id)
    await call.message.edit_text(
        "✅ Расписание звонков сброшено к дефолтному.\n\n" + _bells_text(bells),
        reply_markup=_bells_keyboard(chat_id, bells),
        parse_mode="HTML",
    )
    await call.answer()


# ═══════════════════════════════════════════════════════════════════
# АВТО-РАСПОЗНАВАНИЕ ИЗМЕНЕНИЙ — из фото прямо в группе
# ═══════════════════════════════════════════════════════════════════

@sched_router.message(
    F.chat.type.in_({"group", "supergroup"}),
    F.photo,
)
async def on_group_photo(message: Message, state: FSMContext):
    """Try to parse schedule changes from a photo posted in the group."""
    if await state.get_state() is not None:
        return

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

    result = await parse_schedule_change(message.caption or "", image_bytes)
    if not result:
        return

    changes = result.get("changes") or []
    fallback_date = result.get("date")
    if not changes:
        return

    group_lookup = build_group_lookup(groups)
    applied = []
    for change in changes:
        targets = resolve_target_groups(change, groups, group_lookup)
        if not targets:
            if change.get("group"):
                logger.warning("Schedule change skipped in group chat: unknown group %r", change.get("group"))
            continue

        for group in targets:
            await sdb.save_override(group["id"], change, fallback_date=fallback_date)

        action = change.get("action") or change.get("type") or "change"
        subject = change.get("subject") or "?"
        grp_lbl = change.get("group") or "all groups"
        applied.append(f"<b>{grp_lbl}</b> lesson {change.get('lesson_num', '?')}: {action} - {subject}")

    if applied:
        date_line = f"Date: {fallback_date}\n" if fallback_date else ""
        await message.reply(
            f"<b>Schedule changes applied</b>\n{date_line}\n"
            + "\n".join(f"- {a}" for a in applied),
            parse_mode="HTML",
        )

@sched_router.callback_query(F.data.startswith("sched_toggle_week:"))
async def cb_toggle_week_type(call: CallbackQuery):
    """
    Переключает тип недели для занятия по циклу:
    0 (каждую) → 1 (нечётные) → 2 (чётные) → 0
    Это нужно для пар через дробь — когда на одной неделе один предмет, на другой другой.
    """
    _, chat_id_s, lesson_id_s, group_id_s, wd_s = call.data.split(":")
    chat_id, lesson_id, group_id, wd = int(chat_id_s), int(lesson_id_s), int(group_id_s), int(wd_s)

    if not await _is_admin(call.bot, chat_id, call.from_user.id):
        await call.answer("❌ Только администраторы.", show_alert=True)
        return

    lesson = await sdb.get_lesson_by_id(lesson_id)
    if not lesson:
        await call.answer("Занятие не найдено.", show_alert=True)
        return

    # Цикл: 0 → 1 → 2 → 0
    current_wt = lesson.get("week_type", 0)
    new_wt = (current_wt + 1) % 3

    await sdb.update_lesson_field(lesson_id, "week_type", str(new_wt))
    label = WEEK_TYPE_LABELS.get(new_wt, "?")
    await call.answer(f"📆 Неделя: {label}")

    # Перерисовываем карточку
    call.data = f"sched_edit_lesson:{chat_id}:{group_id}:{lesson_id}:{wd}"
    await cb_edit_lesson(call)


@sched_router.callback_query(F.data.startswith("sched_toggle_event:"))
async def cb_toggle_event(call: CallbackQuery):
    """
    Переключает флаг is_event (мероприятие/обычное занятие).
    Мероприятия (Разговоры о важном и т.п.) не создают очередь и не открывают её.
    """
    _, chat_id_s, lesson_id_s, group_id_s, wd_s = call.data.split(":")
    chat_id, lesson_id, group_id, wd = int(chat_id_s), int(lesson_id_s), int(group_id_s), int(wd_s)

    if not await _is_admin(call.bot, chat_id, call.from_user.id):
        await call.answer("❌ Только администраторы.", show_alert=True)
        return

    lesson = await sdb.get_lesson_by_id(lesson_id)
    if not lesson:
        await call.answer("Занятие не найдено.", show_alert=True)
        return

    new_val = 0 if lesson.get("is_event", 0) else 1
    await sdb.update_lesson_field(lesson_id, "is_event", str(new_val))

    status = "🎓 Помечено как мероприятие (очереди не будет)" if new_val else "📚 Обычное занятие (очередь создаётся)"
    await call.answer(status, show_alert=True)

    call.data = f"sched_edit_lesson:{chat_id}:{group_id}:{lesson_id}:{wd}"
    await cb_edit_lesson(call)


# ═══════════════════════════════════════════════════════════════════
# ПОКАЗ РАСПИСАНИЯ ПО НЕДЕЛЯМ — отдельные команды для чётной/нечётной
# ═══════════════════════════════════════════════════════════════════

@sched_router.callback_query(F.data == "sched_show_odd")
async def cb_show_odd_week(call: CallbackQuery):
    """Показывает расписание только нечётной недели."""
    await _show_filtered_week(call, week_type=1)


@sched_router.callback_query(F.data == "sched_show_even")
async def cb_show_even_week(call: CallbackQuery):
    """Показывает расписание только чётной недели."""
    await _show_filtered_week(call, week_type=2)


async def _show_filtered_week(call: CallbackQuery, week_type: int):
    """Показывает расписание недели с фильтром по типу (нечётная/чётная)."""
    chat_id = call.message.chat.id
    groups  = await sdb.get_chat_groups(chat_id)
    if not groups:
        await call.answer("Расписание не загружено.", show_alert=True)
        return

    bells_cache = {b["lesson_num"]: b for b in await sdb.get_bells(chat_id)}
    week_name   = "Нечётная неделя 1️⃣" if week_type == 1 else "Чётная неделя 2️⃣"

    parts = []
    for group in groups:
        week = await get_week_schedule(group["id"])
        if not week:
            continue

        lines = [f"📅 <b>{group['group_name']} — {week_name}</b>"]
        for wd in sorted(week):
            # Фильтруем только нужный тип + общие занятия
            day_lessons = [
                l for l in week[wd]
                if l.get("week_type", 0) in (0, week_type)
                and not l.get("is_event")
            ]
            if not day_lessons:
                continue
            lines.append(f"\n<b>{DAYS_FULL.get(wd, wd)}</b>")
            for l in sorted(day_lessons, key=lambda x: x.get("lesson_num", 0)):
                teacher = f" — {l['teacher']}" if l.get("teacher") else ""
                room    = f" [{l['room']}]" if l.get("room") else ""
                time_s  = _lesson_time_str(l, bells_cache)
                lines.append(f"  {l['lesson_num']}.{time_s} <b>{l['subject']}</b>{teacher}{room}")
        parts.append("\n".join(lines))

    if not parts:
        await call.answer("Нет занятий.", show_alert=True)
        return

    for chunk in parts:
        await call.message.answer(chunk, parse_mode="HTML")
    await call.answer()


# ═══════════════════════════════════════════════════════════════════
# НАСТРОЙКИ УВЕДОМЛЕНИЙ РАСПИСАНИЯ
# ═══════════════════════════════════════════════════════════════════

class NotifyBeforeState(StatesGroup):
    waiting_minutes = State()  # ждём количество минут для предупреждения заранее


def _notify_settings_text(settings: dict) -> str:
    """Форматируем текст с текущими настройками уведомлений расписания."""
    from config import TZ_OFFSET
    on_open  = "✅ вкл" if settings.get("notify_on_open",  1) else "❌ выкл"
    on_close = "✅ вкл" if settings.get("notify_on_close", 1) else "❌ выкл"
    before   = settings.get("notify_before_min", 0)
    before_s = f"{before} мин" if before > 0 else "выкл"
    tz_s     = f"UTC+{TZ_OFFSET}" if TZ_OFFSET >= 0 else f"UTC{TZ_OFFSET}"
    return (
        f"📣 <b>Настройки уведомлений расписания</b>\n\n"
        f"🟢 При начале пары:       <b>{on_open}</b>\n"
        f"🔴 При конце пары:        <b>{on_close}</b>\n"
        f"⏰ Предупреждать заранее: <b>{before_s}</b>\n\n"
        f"🕐 Часовой пояс: <b>{tz_s}</b>\n"
        f"<i>Изменить пояс: добавьте TZ_OFFSET=N в .env</i>"
    )


def _notify_settings_keyboard(chat_id: int, settings: dict) -> InlineKeyboardMarkup:
    """Кнопки управления уведомлениями расписания."""
    on_open  = settings.get("notify_on_open",  1)
    on_close = settings.get("notify_on_close", 1)
    before   = settings.get("notify_before_min", 0)
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text=f"{'✅' if on_open  else '❌'} Начало пары",
            callback_data=f"sched_ntoggle:{chat_id}:notify_on_open",
        )],
        [InlineKeyboardButton(
            text=f"{'✅' if on_close else '❌'} Конец пары",
            callback_data=f"sched_ntoggle:{chat_id}:notify_on_close",
        )],
        [InlineKeyboardButton(
            text=f"⏰ За {before} мин" if before > 0 else "⏰ Заранее: выкл",
            callback_data=f"sched_nbefore:{chat_id}",
        )],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="sched_back_main")],
    ])


@sched_router.callback_query(F.data.startswith("sched_notify:"))
async def cb_notify_settings(call: CallbackQuery):
    """Меню настроек уведомлений расписания."""
    chat_id = int(call.data.split(":")[1])
    if not await _is_admin(call.bot, chat_id, call.from_user.id):
        await call.answer("❌ Только администраторы.", show_alert=True)
        return
    settings = await sdb.get_chat_schedule_settings(chat_id)
    await call.message.edit_text(
        _notify_settings_text(settings),
        reply_markup=_notify_settings_keyboard(chat_id, settings),
        parse_mode="HTML",
    )
    await call.answer()


@sched_router.callback_query(F.data.startswith("sched_ntoggle:"))
async def cb_notify_toggle(call: CallbackQuery):
    """Переключает одну настройку уведомлений (вкл/выкл)."""
    parts   = call.data.split(":")
    chat_id = int(parts[1])
    field   = parts[2]
    if not await _is_admin(call.bot, chat_id, call.from_user.id):
        await call.answer("❌ Только администраторы.", show_alert=True)
        return
    new_val = await sdb.toggle_chat_schedule_setting(chat_id, field)
    label   = "Начало пары" if field == "notify_on_open" else "Конец пары"
    await call.answer(f"{'✅ Вкл' if new_val else '❌ Выкл'}: {label}")
    settings = await sdb.get_chat_schedule_settings(chat_id)
    await call.message.edit_text(
        _notify_settings_text(settings),
        reply_markup=_notify_settings_keyboard(chat_id, settings),
        parse_mode="HTML",
    )


@sched_router.callback_query(F.data.startswith("sched_nbefore:"))
async def cb_notify_before(call: CallbackQuery, state: FSMContext):
    """Настройка: предупреждать о паре за N минут до начала."""
    chat_id = int(call.data.split(":")[1])
    if not await _is_admin(call.bot, chat_id, call.from_user.id):
        await call.answer("❌ Только администраторы.", show_alert=True)
        return
    await state.update_data(chat_id=chat_id)
    await state.set_state(NotifyBeforeState.waiting_minutes)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="5 мин",   callback_data=f"sched_nbefore_set:{chat_id}:5"),
            InlineKeyboardButton(text="10 мин",  callback_data=f"sched_nbefore_set:{chat_id}:10"),
            InlineKeyboardButton(text="15 мин",  callback_data=f"sched_nbefore_set:{chat_id}:15"),
            InlineKeyboardButton(text="30 мин",  callback_data=f"sched_nbefore_set:{chat_id}:30"),
        ],
        [InlineKeyboardButton(text="Выкл (0)", callback_data=f"sched_nbefore_set:{chat_id}:0")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data=f"sched_notify:{chat_id}")],
    ])
    await call.message.answer(
        "⏰ <b>Предупреждать о начале пары заранее</b>\n\n"
        "Выберите время или введите число минут (0 = выключить):",
        reply_markup=kb, parse_mode="HTML",
    )
    await call.answer()


@sched_router.callback_query(F.data.startswith("sched_nbefore_set:"))
async def cb_notify_before_set(call: CallbackQuery, state: FSMContext):
    """Сохраняем значение «заранее» через кнопку."""
    parts   = call.data.split(":")
    chat_id = int(parts[1])
    minutes = int(parts[2])
    await state.clear()
    await sdb.update_chat_schedule_settings(chat_id, notify_before_min=minutes)
    await call.answer(f"✅ {'Выключено' if minutes == 0 else f'За {minutes} мин'}")
    settings = await sdb.get_chat_schedule_settings(chat_id)
    await call.message.edit_text(
        _notify_settings_text(settings),
        reply_markup=_notify_settings_keyboard(chat_id, settings),
        parse_mode="HTML",
    )


@sched_router.message(NotifyBeforeState.waiting_minutes)
async def fsm_notify_before_minutes(message: Message, state: FSMContext):
    """Сохраняем значение «заранее» из текстового ввода."""
    data    = await state.get_data()
    chat_id = data["chat_id"]
    if not message.text or not message.text.strip().isdigit():
        await message.answer("❌ Введите число минут (например: 10) или 0 чтобы выключить.")
        return
    minutes = int(message.text.strip())
    if not (0 <= minutes <= 120):
        await message.answer("❌ Допустимый диапазон: 0–120 минут.")
        return
    await state.clear()
    await sdb.update_chat_schedule_settings(chat_id, notify_before_min=minutes)
    settings = await sdb.get_chat_schedule_settings(chat_id)
    await message.answer(
        f"✅ Сохранено: {'выключено' if minutes == 0 else f'за {minutes} мин'}.\n\n"
        + _notify_settings_text(settings),
        reply_markup=_notify_settings_keyboard(chat_id, settings),
        parse_mode="HTML",
    )
