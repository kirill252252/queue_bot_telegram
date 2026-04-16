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


async def schedule_tick(bot: Bot):
    """Вызывается каждую минуту."""
    now = datetime.now()
    current_time = now.strftime("%H:%M")
    date_str = now.strftime("%d.%m.%Y")
    weekday = now.isoweekday()

    lessons = await sdb.get_all_lessons_today()

    for lesson in lessons:
        # пропускаем отменённые пары
        if lesson.get("override_type") == "cancel":
            logger.info(f"Skipping cancelled lesson: {lesson['subject']}")
            continue

        lesson_id = lesson["id"]
        chat_id = lesson["chat_id"]
        group_name = lesson["group_name"]
        subject = lesson["subject"]
        time_start = lesson["time_start"]
        time_end = lesson["time_end"]
        room = lesson.get("room") or ""

        # Открываем очередь в начале пары
        # пропускаем пары для которых отключено создание очереди
        if lesson.get("skip_queue"):
            continue

        if current_time == time_start:
            existing = await sdb.get_open_schedule_queue(lesson_id, date_str)
            if not existing:
                queue_name = f"{subject} ({group_name})"
                queue_id = await db.create_queue(
                    chat_id=chat_id,
                    name=queue_name,
                    description=f"📅 {date_str} | ⏰ {time_start}–{time_end}" + (f" | 🏫 {room}" if room else ""),
                    max_slots=0,
                    created_by=0,
                    remind_timeout_min=5,
                    notify_leave_public=False,
                    auto_kick=False
                )
                await sdb.mark_queue_opened(lesson_id, queue_id, date_str)
                logger.info(f"Opened queue for lesson: {queue_name}")

                try:
                    await bot.send_message(
                        chat_id,
                        f"📚 <b>Начинается пара!</b>\n\n"
                        f"👥 Группа: <b>{group_name}</b>\n"
                        f"📖 Предмет: <b>{subject}</b>\n"
                        f"⏰ Время: {time_start}–{time_end}"
                        + (f"\n🏫 Аудитория: {room}" if room else "") +
                        f"\n\nОчередь открыта — используй /queue для записи.",
                        parse_mode="HTML"
                    )
                except Exception as e:
                    logger.warning(f"Cannot notify chat {chat_id}: {e}")

        # Закрываем очередь в конце пары
        if current_time == time_end:
            existing = await sdb.get_open_schedule_queue(lesson_id, date_str)
            if existing:
                queue_id = existing["queue_id"]
                await db.close_queue(queue_id)
                await sdb.mark_queue_closed(lesson_id, date_str)
                logger.info(f"Closed queue for lesson: {subject}")

                try:
                    await bot.send_message(
                        chat_id,
                        f"🔔 <b>Пара завершена!</b>\n\n"
                        f"📖 <b>{subject}</b> ({group_name})\n"
                        f"Очередь закрыта.",
                        parse_mode="HTML"
                    )
                except Exception as e:
                    logger.warning(f"Cannot notify chat {chat_id}: {e}")


async def schedule_loop(bot: Bot):
    """Бесконечный цикл — каждую минуту проверяем расписание."""
    while True:
        try:
            await schedule_tick(bot)
        except Exception as e:
            logger.error(f"Schedule tick error: {e}")

        # ждём до следующей минуты
        now = datetime.now()
        seconds_to_next = 60 - now.second
        await asyncio.sleep(seconds_to_next)
