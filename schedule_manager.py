"""
Планировщик событий на основе расписания.
Каждую минуту проверяет текущее время и:
- Создаёт очередь в начале пары (если skip_queue=0)
- Закрывает очередь в конце пары
- Отправляет уведомления в чат
"""
import logging
from datetime import datetime, timezone, timedelta

import db
import schedule_db as sdb


def _now() -> datetime:
    """Текущее время с учётом TZ_OFFSET — чтобы совпадало с реальным временем пользователя."""
    try:
        from config import TZ_OFFSET
        offset = timedelta(hours=TZ_OFFSET)
    except Exception:
        offset = timedelta(0)
    return datetime.now(timezone.utc) + offset

logger = logging.getLogger(__name__)

WEEKDAY_NAMES = {
    1: "Понедельник", 2: "Вторник", 3: "Среда",
    4: "Четверг", 5: "Пятница", 6: "Суббота", 7: "Воскресенье",
}


# ─────────────────────────────────────────────
# APPLY OVERRIDES
# ─────────────────────────────────────────────

def get_effective_lessons(lessons: list[dict], overrides: list[dict],
                           date_str: str) -> list[dict]:
    """Применяем изменения к базовому расписанию на конкретную дату."""
    result = [dict(l) for l in lessons]

    for override in overrides:
        action = override.get("action") or override.get("type")
        lesson_num = override.get("lesson_num")

        if not action:
            continue

        if action == "cancel" and lesson_num:
            result = [l for l in result if l["lesson_num"] != lesson_num]

        elif action == "reschedule" and lesson_num:
            for l in result:
                if l["lesson_num"] == lesson_num:
                    if override.get("time_start"):
                        l["time_start"] = override["time_start"]
                    if override.get("time_end"):
                        l["time_end"] = override["time_end"]
                    if override.get("subject"):
                        l["subject"] = override["subject"]

        elif action == "add":
            next_num = max((l["lesson_num"] for l in result), default=0) + 1
            result.append({
                "lesson_num": lesson_num or next_num,
                "subject": override.get("subject", "Дополнительное занятие"),
                "time_start": override.get("time_start", "08:00"),
                "time_end": override.get("time_end", "09:35"),
                "teacher": override.get("teacher"),
                "room": override.get("room"),
                "skip_queue": 0,
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
    """Запускается каждую минуту. Создаёт и закрывает очереди по расписанию."""
    now = _now()  # учитываем TZ_OFFSET
    today = now.strftime("%Y-%m-%d")
    current_time = now.strftime("%H:%M")
    weekday = now.isoweekday()

    groups = await sdb.get_all_study_groups()
    if not groups:
        return

    for group in groups:
        group_id = group["id"]
        chat_id = group["chat_id"]

        lessons = await sdb.get_lessons_for_day(group_id, weekday)
        if not lessons:
            continue

        overrides = await sdb.get_overrides_for_date(group_id, today)
        effective = get_effective_lessons(lessons, overrides, today)

        # Фильтруем по типу недели (чётная/нечётная) и убираем мероприятия
        effective = sdb.filter_by_week_type(effective)

        for lesson in effective:
            # Пропускаем занятия с флагом skip_queue
            if lesson.get("skip_queue"):
                continue

            lesson_num = lesson.get("lesson_num")

            # Если у занятия нет явного времени — берём из расписания звонков
            time_start = lesson.get("time_start") or ""
            time_end   = lesson.get("time_end") or ""
            if not time_start or not time_end:
                time_start, time_end = await sdb.get_bell_time(chat_id, lesson_num)
                lesson = dict(lesson)  # не мутируем оригинал
                lesson["time_start"] = time_start
                lesson["time_end"]   = time_end

            if not time_start or not time_end:
                continue  # время неизвестно — пропускаем

            if current_time == time_start:
                await _open_lesson_queue(bot, group, lesson, today, chat_id)

            if current_time == time_end:
                await _close_lesson_queue(bot, group, lesson, today, chat_id)


# ─────────────────────────────────────────────
# OPEN QUEUE
# ─────────────────────────────────────────────

async def _open_lesson_queue(bot, group: dict, lesson: dict,
                              date_str: str, chat_id: int):
    subject = lesson["subject"]
    lesson_num = lesson["lesson_num"]
    time_start = lesson["time_start"]
    time_end = lesson["time_end"]
    teacher = lesson.get("teacher") or ""
    room = lesson.get("room") or ""

    # Проверяем: уже создана очередь для этой пары сегодня?
    pending = await sdb.get_pending_events(date_str)
    for e in pending:
        if e["group_id"] == group["id"] and e["lesson_num"] == lesson_num:
            return  # уже есть

    desc_parts = []
    if teacher:
        desc_parts.append(f"👤 {teacher}")
    if room:
        desc_parts.append(f"🏫 Ауд. {room}")
    desc_parts.append(f"⏰ {time_start}–{time_end}")
    description = " | ".join(desc_parts)

    # Создаём очередь
    queue_id = await db.create_queue(
        chat_id=chat_id,
        name=f"Пара {lesson_num}: {subject}",
        description=description,
        max_slots=0,
        created_by=0,
        remind_timeout_min=10,
        notify_leave_public=False,
        auto_kick=False,
    )

    # Фиксируем событие в БД
    event_id = await sdb.create_schedule_event(
        group_id=group["id"],
        chat_id=chat_id,
        date=date_str,
        lesson_num=lesson_num,
        subject=subject,
        time_start=time_start,
        time_end=time_end,
    )
    await sdb.update_event_queue(event_id, queue_id)
    await sdb.update_event_status(event_id, "active")

    # Отправляем уведомление о начале пары, если это разрешено в настройках
    settings = await sdb.get_chat_schedule_settings(chat_id)
    if settings.get("notify_on_open", 1):
        try:
            week_type = sdb.get_current_week_type()
            week_label = " (нечётная неделя)" if week_type == 1 else " (чётная неделя)" if week_type == 2 else ""
            await bot.send_message(
                chat_id,
                f"🔔 <b>Начинается пара {lesson_num}!</b>{week_label}\n\n"
                f"📚 <b>{subject}</b>\n"
                f"⏰ {time_start}–{time_end}"
                + (f"\n👤 {teacher}" if teacher else "")
                + (f"\n🏫 Ауд. {room}" if room else "")
                + f"\n\nОчередь открыта — запишитесь через /queue",
                parse_mode="HTML",
            )
        except Exception as e:
            logger.error(f"Cannot notify group {chat_id}: {e}")

    # Предупреждение заранее о следующей паре (notify_before_min)
    before_min = settings.get("notify_before_min", 0)
    if before_min > 0:
        try:
            from datetime import datetime as _dt, timedelta as _td
            start_dt = _dt.strptime(f"{date_str} {time_start}", "%Y-%m-%d %H:%M")
            warn_dt = start_dt - _td(minutes=before_min)
            if _now().strftime("%H:%M") == warn_dt.strftime("%H:%M"):
                await bot.send_message(
                    chat_id,
                    f"⏰ <b>Через {before_min} мин — пара {lesson_num}</b>\n"
                    f"📚 {subject}"
                    + (f"\n🏫 Ауд. {room}" if room else ""),
                    parse_mode="HTML",
                )
        except Exception as e:
            logger.debug(f"Before-notify error: {e}")


# ─────────────────────────────────────────────
# CLOSE QUEUE
# ─────────────────────────────────────────────

async def _close_lesson_queue(bot, group: dict, lesson: dict,
                               date_str: str, chat_id: int):
    lesson_num = lesson["lesson_num"]
    subject = lesson["subject"]

    active = await sdb.get_active_events(date_str)

    for event in active:
        if event["group_id"] == group["id"] and event["lesson_num"] == lesson_num:
            if event.get("queue_id"):
                await db.close_queue(event["queue_id"])

            await sdb.update_event_status(event["id"], "closed")

            # Проверяем настройки перед отправкой уведомления о конце пары
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
    """Расписание на сегодня с учётом переопределений."""
    weekday = _now().isoweekday()
    today = _now().strftime("%Y-%m-%d")

    lessons = await sdb.get_lessons_for_day(group_id, weekday)
    overrides = await sdb.get_overrides_for_date(group_id, today)

    return get_effective_lessons(lessons, overrides, today)


async def get_week_schedule(group_id: int) -> dict[int, list[dict]]:
    """Базовое расписание на неделю (без переопределений)."""
    result = {}
    for wd in range(1, 8):
        lessons = await sdb.get_lessons_for_day(group_id, wd)
        if lessons:
            result[wd] = lessons
    return result
