"""
Планировщик событий на основе расписания.
Каждую минуту проверяет текущее время и:
- Создаёт очередь в начале пары
- Закрывает очередь в конце пары
"""
import logging
from datetime import datetime, date, timedelta
from typing import Optional

import database as db
import schedule_db as sdb

logger = logging.getLogger(__name__)

WEEKDAY_NAMES = {
    0: "Понедельник", 1: "Вторник", 2: "Среда",
    3: "Четверг", 4: "Пятница", 5: "Суббота", 6: "Воскресенье"
}


def get_effective_lessons(lessons: list[dict], overrides: list[dict],
                           date_str: str) -> list[dict]:
    """Применяем изменения к базовому расписанию на конкретную дату."""
    result = list(lessons)

    for override in overrides:
        action = override["action"]
        lesson_num = override.get("lesson_num")

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
            result.append({
                "lesson_num": lesson_num or (max(l["lesson_num"] for l in result) + 1 if result else 1),
                "subject": override.get("subject", "Дополнительное занятие"),
                "time_start": override.get("time_start", "08:00"),
                "time_end": override.get("time_end", "09:35"),
                "teacher": override.get("teacher"),
                "room": override.get("room"),
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


async def process_schedule_tick(bot):
    """Запускается каждую минуту. Создаёт и закрывает очереди по расписанию."""
    now = datetime.now()
    today = now.strftime("%Y-%m-%d")
    current_time = now.strftime("%H:%M")
    weekday = now.isoweekday()  # 1=Пн ... 7=Вс

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

        for lesson in effective:
            time_start = lesson["time_start"]
            time_end = lesson["time_end"]
            subject = lesson["subject"]
            lesson_num = lesson["lesson_num"]

            # начало пары — создаём очередь
            if current_time == time_start:
                await _open_lesson_queue(bot, group, lesson, today, chat_id)

            # конец пары — закрываем очередь
            if current_time == time_end:
                await _close_lesson_queue(bot, group, lesson, today, chat_id)


async def _open_lesson_queue(bot, group: dict, lesson: dict,
                              date_str: str, chat_id: int):
    """Создаём очередь для пары."""
    subject = lesson["subject"]
    lesson_num = lesson["lesson_num"]
    time_start = lesson["time_start"]
    time_end = lesson["time_end"]
    teacher = lesson.get("teacher") or ""
    room = lesson.get("room") or ""

    # проверяем не создали ли уже событие
    existing = await sdb.get_pending_events(date_str)
    for e in existing:
        if (e["group_id"] == group["id"] and
                e["lesson_num"] == lesson_num and
                e["status"] in ("pending", "active")):
            return

    desc_parts = []
    if teacher:
        desc_parts.append(f"👤 {teacher}")
    if room:
        desc_parts.append(f"🏫 Ауд. {room}")
    desc_parts.append(f"⏰ {time_start}–{time_end}")
    description = " | ".join(desc_parts)

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

    room_text = f" | Ауд. {room}" if room else ""
    teacher_text = f"\n👤 {teacher}" if teacher else ""

    try:
        await bot.send_message(
            chat_id,
            f"🔔 <b>Начинается пара {lesson_num}!</b>\n\n"
            f"📚 <b>{subject}</b>{room_text}{teacher_text}\n"
            f"⏰ {time_start}–{time_end}\n\n"
            f"Очередь открыта — нажми /queue чтобы занять место.",
            parse_mode="HTML"
        )
        logger.info(f"Queue opened for {subject} in chat {chat_id}")
    except Exception as e:
        logger.error(f"Cannot notify group {chat_id}: {e}")


async def _close_lesson_queue(bot, group: dict, lesson: dict,
                               date_str: str, chat_id: int):
    """Закрываем очередь по окончании пары."""
    lesson_num = lesson["lesson_num"]
    subject = lesson["subject"]

    active = await sdb.get_active_events(date_str)
    for event in active:
        if event["group_id"] == group["id"] and event["lesson_num"] == lesson_num:
            if event.get("queue_id"):
                await db.close_queue(event["queue_id"])
            await sdb.update_event_status(event["id"], "closed")

            try:
                await bot.send_message(
                    chat_id,
                    f"✅ <b>Пара {lesson_num} завершена.</b>\n"
                    f"📚 {subject} — очередь закрыта.",
                    parse_mode="HTML"
                )
            except Exception as e:
                logger.error(f"Cannot notify group {chat_id}: {e}")

            logger.info(f"Queue closed for {subject} in chat {chat_id}")


async def get_today_schedule(group_id: int) -> list[dict]:
    """Получаем эффективное расписание на сегодня с учётом изменений."""
    weekday = datetime.now().isoweekday()
    today = datetime.now().strftime("%Y-%m-%d")
    lessons = await sdb.get_lessons_for_day(group_id, weekday)
    overrides = await sdb.get_overrides_for_date(group_id, today)
    return get_effective_lessons(lessons, overrides, today)


async def get_week_schedule(group_id: int) -> dict[int, list[dict]]:
    """Расписание на всю неделю."""
    result = {}
    for wd in range(1, 8):
        lessons = await sdb.get_lessons_for_day(group_id, wd)
        if lessons:
            result[wd] = lessons
    return result
