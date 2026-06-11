"""
Планировщик событий на основе расписания.
Каждую минуту проверяет текущее время (с учётом TZ_OFFSET) и:
  - Фильтрует занятия по типу недели (чётная/нечётная)
  - Пропускает мероприятия (Разговоры о важном и т.п.)
  - Создаёт очередь в начале пары (если skip_queue=0)
  - Закрывает очередь в конце пары
  - Отправляет уведомления согласно настройкам чата

ИСПРАВЛЕНИЯ v2:
  - _open_lesson_queue: защита от дублей проверяет pending+active (уже было в sdb,
    но теперь логируем дубль явно)
  - Настройки уведомлений инициализируются в БД при первом тике, чтобы
    дефолт «1» не возникал заново после отключения
  - Настройки загружаются ОДИН РАЗ на группу за тик, а не для каждой пары

ИСПРАВЛЕНИЯ v3:
  - BUG: точное сравнение current_time == time_start пропускало первую пару
    если бот перезапускался после её начала. Теперь используется окно
    STARTUP_CATCH_UP_MINUTES: при старте бота догоняем пары начавшиеся
    в пределах этого окна и у которых ещё не создана очередь.
  - BUG: get_pending_events не фильтровал по group_id — событие другой
    группы с тем же lesson_num блокировало создание очереди для первой пары.
    Фильтр по group_id добавлен прямо в _open_lesson_queue.
"""
import logging
from datetime import datetime, timezone, timedelta

import db
import schedule_db as sdb

logger = logging.getLogger(__name__)


def _now() -> datetime:
    """Глобальное локальное время (fallback на TZ_OFFSET из .env)."""
    return sdb.get_local_now()


async def _now_for_chat(chat_id: int) -> datetime:
    """Локальное время с учётом таймзоны конкретного чата."""
    return await sdb.get_local_now_for_chat(chat_id)


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
# ENSURE SETTINGS INITIALIZED
# ─────────────────────────────────────────────

async def _ensure_settings_initialized(chat_id: int) -> dict:
    """
    ИСПРАВЛЕНИЕ: Гарантируем, что в БД есть строка настроек для чата.
    Если строки нет — создаём с дефолтными значениями (все уведомления вкл).
    Это предотвращает ситуацию когда пользователь отключил уведомления,
    но при перезапуске бота get_chat_schedule_settings возвращает дефолт «1».
    """
    settings = await sdb.get_chat_schedule_settings(chat_id)
    # Если в БД нет строки (chat_id не совпадает), создаём её
    if settings.get("chat_id") != chat_id:
        await sdb.update_chat_schedule_settings(
            chat_id,
            notify_on_open=1,
            notify_on_close=1,
            notify_before_min=0,
        )
        settings = await sdb.get_chat_schedule_settings(chat_id)
    return settings


# ─────────────────────────────────────────────
# MAIN TICK
# ─────────────────────────────────────────────

async def process_schedule_tick(bot):
    """
    Запускается каждую минуту из background_loop.
    Алгоритм:
      1. Берём локальное время с учётом таймзоны чата (или TZ_OFFSET из .env)
      2. Для каждой группы: занятия дня → overrides → фильтр недели → открыть/закрыть очереди

    ИСПРАВЛЕНИЕ v2: Настройки загружаются ОДИН РАЗ на группу (не на каждую пару),
    и сразу гарантируем наличие строки в БД.

    ИСПРАВЛЕНИЕ v3: При первом тике после перезапуска догоняем пары начавшиеся
    в пределах STARTUP_CATCH_UP_MINUTES и у которых ещё нет созданной очереди.
    """
    groups = await sdb.get_all_study_groups()
    if not groups:
        return

    for group in groups:
        group_id = group["id"]
        chat_id  = group["chat_id"]

        # Время берём с учётом таймзоны конкретного чата
        now          = await _now_for_chat(chat_id)
        today        = now.strftime("%Y-%m-%d")
        current_time = now.strftime("%H:%M")
        weekday      = now.isoweekday()

        lessons = await sdb.get_lessons_for_day(group_id, weekday)
        if not lessons:
            continue

        overrides = await sdb.get_overrides_for_date(group_id, today)
        effective = get_effective_lessons(lessons, overrides, today)

        # Фильтр по типу недели и мероприятиям
        effective = sdb.filter_by_week_type(effective)

        # ИСПРАВЛЕНИЕ: загружаем настройки ОДИН РАЗ для группы
        settings = await _ensure_settings_initialized(chat_id)
        notify_open  = bool(settings.get("notify_on_open",  1))
        notify_close = bool(settings.get("notify_on_close", 1))
        before_min   = int(settings.get("notify_before_min") or 0)

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

            # BUG FIX v4: заменяем точное сравнение HH:MM на диапазон.
            # Тик мог опоздать на 1-2 минуты (asyncio.sleep неточен + время
            # на выполнение тика) и пропустить первую пару навсегда.
            # Теперь очередь открывается если пара идёт прямо сейчас
            # и ещё не была создана (защита от дублей — в _open_lesson_queue).
            should_open = False
            is_catch_up = False

            try:
                start_dt  = datetime.strptime(f"{today} {time_start}", "%Y-%m-%d %H:%M")
                end_dt    = datetime.strptime(f"{today} {time_end}",   "%Y-%m-%d %H:%M")
                now_naive = now.replace(tzinfo=None)

                if start_dt <= now_naive < end_dt:
                    should_open = True
                    # Если опоздали хотя бы на минуту — без уведомления
                    is_catch_up = (now_naive - start_dt).total_seconds() >= 60
            except Exception:
                pass

            if should_open:
                await _open_lesson_queue(
                    bot, group, lesson, today, chat_id,
                    notify=notify_open and not is_catch_up,
                )

            if current_time == time_end:
                await _close_lesson_queue(bot, group, lesson, today, chat_id, notify_close)

            # Предупреждение заранее
            if before_min > 0 and notify_open and time_start:
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
                             date_str: str, chat_id: int,
                             notify: bool = True):
    """Создаём очередь для пары если её ещё нет."""
    subject    = lesson["subject"]
    lesson_num = lesson["lesson_num"]
    time_start = lesson["time_start"]
    time_end   = lesson["time_end"]
    teacher    = lesson.get("teacher") or ""
    room       = lesson.get("room") or ""

    # BUG FIX v3: get_pending_events возвращает события ВСЕХ групп на дату.
    # Если у другой группы есть lesson_num=1, проверка ниже давала ложный дубль
    # и блокировала создание очереди для первой пары нашей группы.
    # Фильтруем по group_id прямо здесь.
    pending = await sdb.get_pending_events(date_str)
    for e in pending:
        if e["group_id"] == group["id"] and e["lesson_num"] == lesson_num:
            logger.debug(
                "Skipping duplicate queue for group %s lesson %s on %s (status=%s)",
                group["id"], lesson_num, date_str, e.get("status")
            )
            return

    # BUG FIX v4: если очередь уже была создана и закрыта (вручную или по времени)
    # — не пересоздаём, иначе диапазонная проверка будет открывать её снова.
    closed = await sdb.get_closed_events_today(date_str)
    for e in closed:
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

    # ИСПРАВЛЕНИЕ: notify передаётся извне — настройки уже загружены единожды
    if notify:
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
                              date_str: str, chat_id: int,
                              notify: bool = True):
    """Закрываем очередь по окончании пары."""
    lesson_num = lesson["lesson_num"]
    subject    = lesson["subject"]

    active = await sdb.get_active_events(date_str)
    for event in active:
        if event["group_id"] == group["id"] and event["lesson_num"] == lesson_num:
            if event.get("queue_id"):
                await db.close_queue(event["queue_id"])
            await sdb.update_event_status(event["id"], "closed")

            # ИСПРАВЛЕНИЕ: notify передаётся извне
            if notify:
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