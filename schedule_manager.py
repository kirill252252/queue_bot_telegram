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
import asyncio
import logging
from datetime import datetime, timezone, timedelta

import db
import schedule_db as sdb

logger = logging.getLogger(__name__)

# Фоновая задача мониторинга источников (TG/VK) — храним ссылку, чтобы не
# создать дубль при повторном вызове start_background_jobs.
_source_monitor_task: asyncio.Task | None = None


def start_background_jobs(bot):
    """
    Запускает все фоновые циклы бота: тик расписания (очереди) и мониторинг
    источников изменений (Telegram/VK).

    БАГ: source_monitor_loop (парсинг изменений из ТГ/ВК-каналов) был написан,
    но никогда не запускался — ни здесь, ни в main.py. Из-за этого источники
    можно было добавить через /sched_add_source, но они никогда не опрашивались,
    и изменения расписания из Telegram не подхватывались.
    Вызови start_background_jobs(bot) один раз при старте бота (on_startup).
    """
    global _source_monitor_task

    from source_monitor import source_monitor_loop

    if _source_monitor_task is None or _source_monitor_task.done():
        _source_monitor_task = asyncio.create_task(source_monitor_loop(bot))
        logger.info("source_monitor_loop started")
    else:
        logger.debug("source_monitor_loop already running")


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


# ─────────────────────────────────────────────
# MERGE CONSECUTIVE IDENTICAL LESSONS
# ─────────────────────────────────────────────

def _normalize_subject(s: str) -> str:
    """Нормализуем название предмета для сравнения (без учёта регистра и пробелов)."""
    return (s or "").strip().lower()


def _merge_consecutive_lessons(lessons: list[dict]) -> list[dict]:
    """
    Объединяем подряд идущие пары с одинаковым предметом в одну.

    Критерий слияния: два занятия считаются «одинаковыми подряд идущими» если:
      - одинаковый subject (без учёта регистра)
      - time_start следующей == time_end предыдущей (пары вплотную)
        ИЛИ разница ≤ 10 минут (короткий перерыв между «сдвоенными» парами)

    Результирующая запись:
      - lesson_num берётся от ПЕРВОЙ пары (для привязки событий в БД)
      - merged_lesson_nums — список всех объединённых номеров (для дубль-проверки)
      - time_start от первой, time_end от последней
      - teacher/room от первой (если отличаются — игнорируем, редкий кейс)
    """
    if not lessons:
        return []

    merged: list[dict] = []
    i = 0
    while i < len(lessons):
        current = dict(lessons[i])
        current.setdefault("merged_lesson_nums", [current["lesson_num"]])
        j = i + 1
        while j < len(lessons):
            nxt = lessons[j]
            same_subject = (
                _normalize_subject(current["subject"]) ==
                _normalize_subject(nxt["subject"])
            )
            if not same_subject:
                break
            # Проверяем что пары идут вплотную (≤ 10 минут между концом и началом)
            try:
                end_dt   = datetime.strptime(current["time_end"],  "%H:%M")
                start_dt = datetime.strptime(nxt["time_start"],    "%H:%M")
                gap_min  = (start_dt - end_dt).total_seconds() / 60
            except Exception:
                break
            if gap_min < 0 or gap_min > 10:
                break
            # Сливаем: расширяем конец, добавляем номер
            current["time_end"] = nxt["time_end"]
            current["merged_lesson_nums"].append(nxt["lesson_num"])
            j += 1
        merged.append(current)
        i = j
    return merged


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

        # Объединяем подряд идущие одинаковые пары в одну очередь
        effective = _merge_consecutive_lessons(effective)

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

            # Закрываем по time_end последней пары в блоке
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
    """Создаём очередь для пары если её ещё нет.

    Поддерживает объединённые пары: если lesson содержит merged_lesson_nums,
    проверяем дубли по ВСЕМ номерам и создаём одну очередь на весь блок.
    """
    subject    = lesson["subject"]
    lesson_num = lesson["lesson_num"]  # номер первой (главной) пары в блоке
    time_start = lesson["time_start"]
    time_end   = lesson["time_end"]
    teacher    = lesson.get("teacher") or ""
    room       = lesson.get("room") or ""
    merged_nums: list = lesson.get("merged_lesson_nums", [lesson_num])

    # Дубль-проверка: если хотя бы для одного из объединённых номеров уже
    # есть pending/active событие этой группы — пропускаем весь блок.
    pending = await sdb.get_pending_events(date_str)
    for e in pending:
        if e["group_id"] == group["id"] and e["lesson_num"] in merged_nums:
            logger.debug(
                "Skipping duplicate queue for group %s lessons %s on %s (status=%s)",
                group["id"], merged_nums, date_str, e.get("status")
            )
            return

    # BUG FIX v4: аналогично для закрытых событий.
    closed = await sdb.get_closed_events_today(date_str)
    for e in closed:
        if e["group_id"] == group["id"] and e["lesson_num"] in merged_nums:
            return

    desc_parts = []
    if teacher: desc_parts.append(f"👤 {teacher}")
    if room:    desc_parts.append(f"🏫 Ауд. {room}")
    desc_parts.append(f"⏰ {time_start}–{time_end}")

    wt         = sdb.get_current_week_type()
    week_label = {1: " (нечётная)", 2: " (чётная)"}.get(wt, "")

    # Название очереди: для сдвоенных показываем диапазон номеров
    if len(merged_nums) > 1:
        num_label = f"{merged_nums[0]}–{merged_nums[-1]}"
    else:
        num_label = str(lesson_num)

    queue_id = await db.create_queue(
        chat_id=chat_id, name=f"Пара {num_label}: {subject}",
        description=" | ".join(desc_parts), max_slots=0,
        created_by=0, remind_timeout_min=10,
        notify_leave_public=False, auto_kick=False,
    )

    # Создаём event-записи для каждого номера пары в блоке.
    # Первый event — главный (к нему привязан queue_id), остальные — ссылочные.
    for idx, num in enumerate(merged_nums):
        event_id = await sdb.create_schedule_event(
            group_id=group["id"], chat_id=chat_id, date=date_str,
            lesson_num=num, subject=subject,
            time_start=time_start, time_end=time_end,
        )
        if idx == 0:
            await sdb.update_event_queue(event_id, queue_id)
        await sdb.update_event_status(event_id, "active")

    if notify:
        try:
            await bot.send_message(
                chat_id,
                f"🔔 <b>Начинается пара {num_label}!</b>{week_label}\n\n"
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
    """Закрываем очередь по окончании пары (или блока сдвоенных пар)."""
    merged_nums: list = lesson.get("merged_lesson_nums", [lesson["lesson_num"]])
    lesson_num = lesson["lesson_num"]
    subject    = lesson["subject"]

    if len(merged_nums) > 1:
        num_label = f"{merged_nums[0]}–{merged_nums[-1]}"
    else:
        num_label = str(lesson_num)

    active = await sdb.get_active_events(date_str)
    closed_queue_ids: set = set()
    for event in active:
        if event["group_id"] == group["id"] and event["lesson_num"] in merged_nums:
            qid = event.get("queue_id")
            if qid and qid not in closed_queue_ids:
                await db.close_queue(qid)
                closed_queue_ids.add(qid)
            await sdb.update_event_status(event["id"], "closed")

    if closed_queue_ids and notify:
        try:
            await bot.send_message(
                chat_id,
                f"✅ <b>Пара {num_label} завершена.</b>\n"
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


async def get_tomorrow_schedule(group_id: int, chat_id: int) -> list[dict]:
    """
    Расписание на завтра с учётом таймзоны чата, переопределений (overrides)
    и типа недели на дату завтра (а не на сегодня — важно у границы недель).
    """
    now      = await _now_for_chat(chat_id)
    tomorrow = now.date() + timedelta(days=1)
    weekday  = tomorrow.isoweekday()
    date_str = tomorrow.strftime("%Y-%m-%d")

    lessons   = await sdb.get_lessons_for_day(group_id, weekday)
    overrides = await sdb.get_overrides_for_date(group_id, date_str)
    effective = get_effective_lessons(lessons, overrides, date_str)

    week_type = sdb.get_week_type_for_date(tomorrow)
    return [l for l in effective if int(l.get("week_type") or 0) in (0, week_type)]


async def get_week_schedule(group_id: int, chat_id: int | None = None) -> dict[int, list[dict]]:
    """
    Расписание на ближайшие 7 дней (начиная с сегодня), С УЧЁТОМ переопределений
    (overrides) на каждую конкретную дату — изменения из листа «Изменения в
    расписании» (cancel/reschedule/room_change/...) теперь видны и в недельном
    расписании, а не только в /sched_today.

    Если chat_id не передан — работает по старому (без per-chat таймзоны и без
    overrides), для обратной совместимости.
    """
    if chat_id is None:
        result = {}
        for wd in range(1, 8):
            lessons = await sdb.get_lessons_for_day(group_id, wd)
            if lessons:
                result[wd] = lessons
        return result

    now = await _now_for_chat(chat_id)
    result: dict[int, list[dict]] = {}

    for offset in range(7):
        day      = now.date() + timedelta(days=offset)
        weekday  = day.isoweekday()
        date_str = day.strftime("%Y-%m-%d")

        lessons = await sdb.get_lessons_for_day(group_id, weekday)
        if not lessons:
            continue

        overrides = await sdb.get_overrides_for_date(group_id, date_str)
        effective = get_effective_lessons(lessons, overrides, date_str) if overrides else lessons

        # week_type фильтруется при отображении (handlers группируют по дробям),
        # поэтому здесь оставляем все варианты — как и раньше.
        result[weekday] = effective

    return result