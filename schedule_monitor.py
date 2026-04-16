"""
Фоновый планировщик расписания.
Каждую минуту проверяет текущее время и:
- Открывает очередь в начале пары
- Закрывает очередь в конце пары
- Отправляет уведомления в чат
"""
import asyncio
import logging
from datetime import datetime

from aiogram import Bot

import database as db
import schedule_db as sdb

logger = logging.getLogger(__name__)


def apply_overrides(lessons: list[dict], overrides: list[dict]) -> list[dict]:
    """
    Применяет изменения расписания (cancel / reschedule / add / room_change / teacher_change)
    """
    result = [dict(l) for l in lessons]

    for o in overrides:
        otype = o.get("type")

        # ─── CANCEL ─────────────────────
        if otype == "cancel":
            subject = (o.get("subject") or "").lower()
            result = [
                l for l in result
                if l["subject"].lower() != subject
            ]

        # ─── RESCHEDULE ────────────────
        elif otype == "reschedule":
            subject = (o.get("subject") or "").lower()

            for l in result:
                if l["subject"].lower() == subject:
                    if o.get("time_start"):
                        l["time_start"] = o["time_start"]
                    if o.get("time_end"):
                        l["time_end"] = o["time_end"]

        # ─── ADD ───────────────────────
        elif otype == "add":
            result.append({
                "subject": o.get("subject", "Доп. занятие"),
                "time_start": o.get("time_start", "08:00"),
                "time_end": o.get("time_end", "09:30"),
                "room": o.get("room"),
                "teacher": o.get("teacher"),
            })

    return sorted(result, key=lambda x: x["time_start"])


async def schedule_tick(bot: Bot):
    """Вызывается каждую минуту."""
    now = datetime.now()
    current_time = now.strftime("%H:%M")
    date_str = now.strftime("%d.%m.%Y")
    weekday = now.isoweekday()

    groups = await sdb.get_all_groups()
    if not groups:
        return

    for group in groups:
        group_id = group["id"]
        chat_id = group["chat_id"]

        lessons = await sdb.get_lessons_for_day(group_id, weekday)
        if not lessons:
            continue

        overrides = await sdb.get_overrides_for_date(group_id, date_str) \
            if hasattr(sdb, "get_overrides_for_date") else []

        effective = apply_overrides(lessons, overrides)

        for lesson in effective:
            subject = lesson["subject"]
            time_start = lesson["time_start"]
            time_end = lesson["time_end"]
            room = lesson.get("room") or ""
            teacher = lesson.get("teacher") or ""

            # ─── OPEN QUEUE ─────────────────────────
            if current_time == time_start:
                existing = await sdb.get_open_schedule_queue(group_id, date_str)

                if not existing:
                    queue_id = await db.create_queue(
                        chat_id=chat_id,
                        name=f"{subject} ({group['group_name']})",
                        description=f"⏰ {time_start}–{time_end}" +
                                    (f" | 🏫 {room}" if room else ""),
                        max_slots=0,
                        created_by=0,
                        remind_timeout_min=5,
                        notify_leave_public=False,
                        auto_kick=False
                    )

                    await sdb.mark_queue_opened(group_id, queue_id, date_str)

                    try:
                        await bot.send_message(
                            chat_id,
                            f"📚 <b>Начинается пара!</b>\n\n"
                            f"👥 Группа: <b>{group['group_name']}</b>\n"
                            f"📖 {subject}\n"
                            f"⏰ {time_start}–{time_end}"
                            + (f"\n🏫 {room}" if room else ""),
                            parse_mode="HTML"
                        )
                    except Exception as e:
                        logger.warning(f"Notify error {chat_id}: {e}")

            # ─── CLOSE QUEUE ────────────────────────
            if current_time == time_end:
                existing = await sdb.get_open_schedule_queue(group_id, date_str)

                if existing:
                    queue_id = existing["queue_id"]

                    await db.close_queue(queue_id)
                    await sdb.mark_queue_closed(group_id, date_str)

                    try:
                        await bot.send_message(
                            chat_id,
                            f"🔔 <b>Пара завершена</b>\n\n"
                            f"📖 {subject} ({group['group_name']})\n"
                            f"Очередь закрыта.",
                            parse_mode="HTML"
                        )
                    except Exception as e:
                        logger.warning(f"Notify error {chat_id}: {e}")


async def schedule_loop(bot: Bot):
    """Бесконечный цикл планировщика."""
    while True:
        try:
            await schedule_tick(bot)
        except Exception as e:
            logger.error(f"Schedule tick error: {e}")

        now = datetime.now()
        await asyncio.sleep(60 - now.second)