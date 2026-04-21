"""
Планировщик событий на основе расписания.
Каждую минуту проверяет текущее время (с учётом TZ_OFFSET) и:
  - Фильтрует занятия по типу недели (чётная/нечётная)
  - Пропускает мероприятия (Разговоры о важном и т.п.)
  - Создаёт очередь в начале пары (если skip_queue=0)
  - Закрывает очередь в конце пары
  - Отправляет уведомления согласно настройкам чата
"""
import logging
from datetime import datetime, timezone, timedelta

import db
import schedule_db as sdb

logger = logging.getLogger(__name__)


def _now() -> datetime:
    """
    Текущее время с учётом TZ_OFFSET из .env.
    Railway/VPS работают на UTC — без смещения очереди не откроются в нужное время.
    Добавьте в .env: TZ_OFFSET=4 (для UTC+4)
    """
    try:
        from config import TZ_OFFSET
        offset = timedelta(hours=TZ_OFFSET)
    except Exception:
        offset = timedelta(0)
    return datetime.now(timezone.utc) + offset


# ─────────────────────────────────────────────
# APPLY OVERRIDES
# ─────────────────────────────────────────────

def get_effective_lessons(lessons: list[dict], overrides: list[dict],
                          date_str: str) -> list[dict]:
    """
    Применяем изменения расписания на конкретную дату.
    При cancel убираем ВСЕ записи с данным lesson_num (в т.ч. обе половины дроби).
    """
    result = [dict(l) for l in lessons]

    for override in overrides:
        action     = override.get("action") or override.get("type")
        lesson_num = override.get("lesson_num")
        if not action:
            continue

        if action == "cancel" and lesson_num:
            result = [l for l in result if l["lesson_num"] != lesson_num]

        elif action == "reschedule" and lesson_num:
            for l in result:
                if l["lesson_num"] == lesson_num:
                    if override.get("time_start"): l["time_start"] = override["time_start"]
                    if override.get("time_end"):   l["time_end"]   = override["time_end"]
                    if override.get("subject"):    l["subject"]    = override["subject"]

        elif action == "add":
            next_num = max((l["lesson_num"] for l in result), default=0) + 1
            result.append({
                "lesson_num": lesson_num or next_num,
                "subject":    override.get("subject", "Дополнительное занятие"),
                "time_start": override.get("time_start", "08:00"),
                "time_end":   override.get("time_end",   "09:35"),
                "teacher":    override.get("teacher"),
                "room":       override.get("room"),
                "skip_queue": 0, "week_type": 0, "is_event": 0,
            })

        elif action == "room_change" and lesson_num:
            for l in result:
                if l["lesson_num"] == lesson_num and override.get("room"):
                    l["room"] = override["room"]

        elif action == "teacher_change" and lesson_num:
            for l in result:
                if l["lesson_num"] == lesson_num and override.get("teacher"):
                    l["teacher"] = override["teacher"]

    return sorted(result, key=lambda x: x.get("time_start", ""))


# ─────────────────────────────────────────────
# MAIN TICK
# ─────────────────────────────────────────────

async def process_schedule_tick(bot):
    """
    Запускается каждую минуту из background_loop.
    Алгоритм:
      1. Берём локальное время с учётом TZ_OFFSET
      2. Для каждой группы: занятия дня → overrides → фильтр недели → открыть/закрыть очереди
    """
    now          = _now()
    today        = now.strftime("%Y-%m-%d")
    current_time = now.strftime("%H:%M")
    weekday      = now.isoweekday()

    groups = await sdb.get_all_study_groups()
    if not groups:
        return

    for group in groups:
        group_id = group["id"]
        chat_id  = group["chat_id"]

        lessons = await sdb.get_lessons_for_day(group_id, weekday)
        if not lessons:
            continue

        overrides = await sdb.get_overrides_for_date(group_id, today)
        effective = get_effective_lessons(lessons, overrides, today)

        # Фильтр по типу недели и мероприятиям
        effective = sdb.filter_by_week_type(effective)

        for lesson in effective:
            if lesson.get("skip_queue"):
                continue

            lesson_num = lesson.get("lesson_num")
            time_start = lesson.get("time_start") or ""
            time_end   = lesson.get("time_end") or ""

            # Если времени нет в занятии — берём из расписания звонков
            if not time_start or not time_end:
                time_start, time_end = await sdb.get_bell_time(chat_id, lesson_num)
                lesson = dict(lesson)
                lesson["time_start"] = time_start
                lesson["time_end"]   = time_end

            if not time_start or not time_end:
                continue

            if current_time == time_start:
                await _open_lesson_queue(bot, group, lesson, today, chat_id)

            if current_time == time_end:
                await _close_lesson_queue(bot, group, lesson, today, chat_id)

            # Предупреждение заранее
            settings   = await sdb.get_chat_schedule_settings(chat_id)
            before_min = int(settings.get("notify_before_min") or 0)
            if before_min > 0 and settings.get("notify_on_open", 1) and time_start:
                try:
                    start_dt  = datetime.strptime(f"{today} {time_start}", "%Y-%m-%d %H:%M")
                    warn_time = (start_dt - timedelta(minutes=before_min)).strftime("%H:%M")
                    if current_time == warn_time:
                        wt    = sdb.get_current_week_type()
                        wlbl  = {1: " (нечётная)", 2: " (чётная)"}.get(wt, "")
                        await bot.send_message(
                            chat_id,
                            f"⏰ <b>Через {before_min} мин — пара {lesson_num}</b>{wlbl}\n"
                            f"📚 {lesson['subject']}"
                            + (f"\n🏫 Ауд. {lesson.get('room')}" if lesson.get("room") else ""),
                            parse_mode="HTML",
                        )
                except Exception as e:
                    logger.debug(f"Before-notify error: {e}")


# ─────────────────────────────────────────────
# OPEN QUEUE
# ─────────────────────────────────────────────

async def _open_lesson_queue(bot, group: dict, lesson: dict,
                             date_str: str, chat_id: int):
    """Создаём очередь для пары если её ещё нет."""
    subject    = lesson["subject"]
    lesson_num = lesson["lesson_num"]
    time_start = lesson["time_start"]
    time_end   = lesson["time_end"]
    teacher    = lesson.get("teacher") or ""
    room       = lesson.get("room") or ""

    # Защита от дублей
    pending = await sdb.get_pending_events(date_str)
    for e in pending:
        if e["group_id"] == group["id"] and e["lesson_num"] == lesson_num:
            return

    desc_parts = []
    if teacher: desc_parts.append(f"👤 {teacher}")
    if room:    desc_parts.append(f"🏫 Ауд. {room}")
    desc_parts.append(f"⏰ {time_start}–{time_end}")

    wt         = sdb.get_current_week_type()
    week_label = {1: " (нечётная)", 2: " (чётная)"}.get(wt, "")

    queue_id = await db.create_queue(
        chat_id=chat_id, name=f"Пара {lesson_num}: {subject}",
        description=" | ".join(desc_parts), max_slots=0,
        created_by=0, remind_timeout_min=10,
        notify_leave_public=False, auto_kick=False,
    )

    event_id = await sdb.create_schedule_event(
        group_id=group["id"], chat_id=chat_id, date=date_str,
        lesson_num=lesson_num, subject=subject,
        time_start=time_start, time_end=time_end,
    )
    await sdb.update_event_queue(event_id, queue_id)
    await sdb.update_event_status(event_id, "active")

    settings = await sdb.get_chat_schedule_settings(chat_id)
    if settings.get("notify_on_open", 1):
        try:
            await bot.send_message(
                chat_id,
                f"🔔 <b>Начинается пара {lesson_num}!</b>{week_label}\n\n"
                f"📚 <b>{subject}</b>\n⏰ {time_start}–{time_end}"
                + (f"\n👤 {teacher}" if teacher else "")
                + (f"\n🏫 Ауд. {room}" if room else "")
                + "\n\nОчередь открыта — запишитесь через /queue",
                parse_mode="HTML",
            )
        except Exception as e:
            logger.error(f"Cannot notify group {chat_id}: {e}")


# ─────────────────────────────────────────────
# CLOSE QUEUE
# ─────────────────────────────────────────────

async def _close_lesson_queue(bot, group: dict, lesson: dict,
                              date_str: str, chat_id: int):
    """Закрываем очередь по окончании пары."""
    lesson_num = lesson["lesson_num"]
    subject    = lesson["subject"]

    active = await sdb.get_active_events(date_str)
    for event in active:
        if event["group_id"] == group["id"] and event["lesson_num"] == lesson_num:
            if event.get("queue_id"):
                await db.close_queue(event["queue_id"])
            await sdb.update_event_status(event["id"], "closed")

            settings = await sdb.get_chat_schedule_settings(chat_id)
            if settings.get("notify_on_close", 1):
                try:
                    await bot.send_message(
                        chat_id,
                        f"✅ <b>Пара {lesson_num} завершена.</b>\n"
                        f"📚 {subject} — очередь закрыта.",
                        parse_mode="HTML",
                    )
                except Exception as e:
                    logger.error(f"Cannot notify group {chat_id}: {e}")


# ─────────────────────────────────────────────
# HELPERS — для schedule_handlers.py
# ─────────────────────────────────────────────

async def get_today_schedule(group_id: int) -> list[dict]:
    """
    Расписание на сегодня с учётом TZ_OFFSET, переопределений и типа недели.
    Возвращает занятия которые реально идут сегодня (включая мероприятия для показа).
    """
    now     = _now()
    weekday = now.isoweekday()
    today   = now.strftime("%Y-%m-%d")

    lessons   = await sdb.get_lessons_for_day(group_id, weekday)
    overrides = await sdb.get_overrides_for_date(group_id, today)
    effective = get_effective_lessons(lessons, overrides, today)

    # Показываем занятия текущей недели включая мероприятия (для информации)
    current = sdb.get_current_week_type()
    return [l for l in effective if int(l.get("week_type") or 0) in (0, current)]


async def get_week_schedule(group_id: int) -> dict[int, list[dict]]:
    """Базовое расписание на всю неделю без фильтрации по неделям."""
    result = {}
    for wd in range(1, 8):
        lessons = await sdb.get_lessons_for_day(group_id, wd)
        if lessons:
            result[wd] = lessons
    return result
