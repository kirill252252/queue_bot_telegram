"""
База данных для расписания — полная версия со всеми функциями.
Формат дат: YYYY-MM-DD (стандарт ISO, единый для всех модулей).
"""
import aiosqlite
import logging
from datetime import date, datetime, timedelta, timezone
from typing import Optional

logger = logging.getLogger(__name__)

_ALLOWED_LESSON_UPDATE_FIELDS = {
    "subject",
    "teacher",
    "room",
    "time_start",
    "time_end",
    "lesson_num",
    "weekday",
    "skip_queue",
    "week_type",
    "is_event",
}


def _get_db_type():
    try:
        from config import DB_TYPE
        return DB_TYPE
    except Exception:
        return "sqlite"


def _get_local_offset() -> timedelta:
    try:
        from config import TZ_OFFSET
        return timedelta(hours=TZ_OFFSET)
    except Exception:
        return timedelta(0)


def get_local_now() -> datetime:
    return datetime.now(timezone.utc) + _get_local_offset()


def get_week_type_for_date(
    current_date: date,
    reference_date: Optional[date] = None,
    reference_week_type: int = 1,
) -> int:
    if reference_date is None:
        iso_week = current_date.isocalendar()[1]
        return 1 if iso_week % 2 else 2

    base_type = 1 if reference_week_type not in (1, 2) else reference_week_type
    current_monday = current_date - timedelta(days=current_date.isoweekday() - 1)
    reference_monday = reference_date - timedelta(days=reference_date.isoweekday() - 1)
    week_shift = (current_monday - reference_monday).days // 7

    if week_shift % 2 == 0:
        return base_type
    return 2 if base_type == 1 else 1


# ─────────────────────────────────────────────
# INIT
# ─────────────────────────────────────────────

async def init_schedule_db():
    if _get_db_type() == "postgres":
        await _init_pg()
    else:
        await _init_sqlite()


async def _init_sqlite():
    from database import DB_PATH

    async with aiosqlite.connect(DB_PATH) as db:

        # Учебные группы
        await db.execute("""
            CREATE TABLE IF NOT EXISTS schedule_groups (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                group_name TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(chat_id, group_name)
            )
        """)

        # Занятия (базовое расписание)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS schedule_lessons (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                group_id INTEGER NOT NULL,
                weekday INTEGER NOT NULL,
                lesson_num INTEGER NOT NULL,
                subject TEXT NOT NULL,
                time_start TEXT NOT NULL,
                time_end TEXT NOT NULL,
                room TEXT,
                teacher TEXT,
                is_active INTEGER DEFAULT 1,
                skip_queue INTEGER DEFAULT 0,
                week_type  INTEGER DEFAULT 0,
                is_event   INTEGER DEFAULT 0
            )
        """)

        # Переопределения (изменения расписания на конкретную дату)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS schedule_overrides (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                group_id INTEGER NOT NULL,
                override_date TEXT NOT NULL,
                lesson_num INTEGER,
                action TEXT NOT NULL,
                subject TEXT,
                time_start TEXT,
                time_end TEXT,
                room TEXT,
                teacher TEXT,
                comment TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Записи об открытии/закрытии очередей по расписанию
        await db.execute("""
            CREATE TABLE IF NOT EXISTS schedule_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                group_id INTEGER NOT NULL,
                chat_id INTEGER NOT NULL,
                date TEXT NOT NULL,
                lesson_num INTEGER NOT NULL,
                subject TEXT NOT NULL,
                time_start TEXT NOT NULL,
                time_end TEXT NOT NULL,
                queue_id INTEGER,
                status TEXT DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Источники мониторинга изменений расписания
        await db.execute("""
            CREATE TABLE IF NOT EXISTS schedule_sources (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                source_type TEXT NOT NULL,
                source_id TEXT NOT NULL,
                last_post_id TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(chat_id, source_type, source_id)
            )
        """)


        # Расписание звонков (время начала/конца каждой пары для чата)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS schedule_bells (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                lesson_num INTEGER NOT NULL,
                time_start TEXT NOT NULL,
                time_end TEXT NOT NULL,
                UNIQUE(chat_id, lesson_num)
            )
        """)

        # Настройки уведомлений расписания
        await db.execute("""
            CREATE TABLE IF NOT EXISTS schedule_chat_settings (
                chat_id INTEGER PRIMARY KEY,
                notify_on_open INTEGER DEFAULT 1,
                notify_on_close INTEGER DEFAULT 1,
                notify_before_min INTEGER DEFAULT 0
            )
        """)
        await db.commit()


async def _init_pg():
    from database_pg import get_pool
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS schedule_groups (
                id SERIAL PRIMARY KEY,
                chat_id BIGINT NOT NULL,
                group_name TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(chat_id, group_name)
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS schedule_lessons (
                id SERIAL PRIMARY KEY,
                group_id INTEGER NOT NULL,
                weekday INTEGER NOT NULL,
                lesson_num INTEGER NOT NULL,
                subject TEXT NOT NULL,
                time_start TEXT NOT NULL,
                time_end TEXT NOT NULL,
                room TEXT,
                teacher TEXT,
                is_active INTEGER DEFAULT 1,
                skip_queue INTEGER DEFAULT 0,
                week_type  INTEGER DEFAULT 0,
                is_event   INTEGER DEFAULT 0
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS schedule_overrides (
                id SERIAL PRIMARY KEY,
                group_id INTEGER NOT NULL,
                override_date TEXT NOT NULL,
                lesson_num INTEGER,
                action TEXT NOT NULL,
                subject TEXT,
                time_start TEXT,
                time_end TEXT,
                room TEXT,
                teacher TEXT,
                comment TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS schedule_events (
                id SERIAL PRIMARY KEY,
                group_id INTEGER NOT NULL,
                chat_id BIGINT NOT NULL,
                date TEXT NOT NULL,
                lesson_num INTEGER NOT NULL,
                subject TEXT NOT NULL,
                time_start TEXT NOT NULL,
                time_end TEXT NOT NULL,
                queue_id INTEGER,
                status TEXT DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS schedule_sources (
                id SERIAL PRIMARY KEY,
                chat_id BIGINT NOT NULL,
                source_type TEXT NOT NULL,
                source_id TEXT NOT NULL,
                last_post_id TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(chat_id, source_type, source_id)
            )
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS schedule_bells (
                id SERIAL PRIMARY KEY,
                chat_id BIGINT NOT NULL,
                lesson_num INTEGER NOT NULL,
                time_start TEXT NOT NULL,
                time_end TEXT NOT NULL,
                UNIQUE(chat_id, lesson_num)
            )
        """)

        # Настройки уведомлений расписания (вкл/выкл, заранее)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS schedule_chat_settings (
                chat_id BIGINT PRIMARY KEY,
                notify_on_open INTEGER DEFAULT 1,
                notify_on_close INTEGER DEFAULT 1,
                notify_before_min INTEGER DEFAULT 0
            )
        """)

        # ── Миграции: добавляем колонки которых может не быть в старой БД ──
        # CREATE TABLE IF NOT EXISTS не трогает уже существующие таблицы,
        # поэтому добавляем недостающие колонки через ALTER TABLE.
        migrations = [
            # schedule_lessons
            "ALTER TABLE schedule_lessons ADD COLUMN IF NOT EXISTS lesson_num  INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE schedule_lessons ADD COLUMN IF NOT EXISTS weekday     INTEGER NOT NULL DEFAULT 1",
            "ALTER TABLE schedule_lessons ADD COLUMN IF NOT EXISTS time_start  TEXT NOT NULL DEFAULT '00:00'",
            "ALTER TABLE schedule_lessons ADD COLUMN IF NOT EXISTS time_end    TEXT NOT NULL DEFAULT '00:00'",
            "ALTER TABLE schedule_lessons ADD COLUMN IF NOT EXISTS room        TEXT",
            "ALTER TABLE schedule_lessons ADD COLUMN IF NOT EXISTS teacher     TEXT",
            "ALTER TABLE schedule_lessons ADD COLUMN IF NOT EXISTS is_active   INTEGER DEFAULT 1",
            "ALTER TABLE schedule_lessons ADD COLUMN IF NOT EXISTS skip_queue  INTEGER DEFAULT 0",
            "ALTER TABLE schedule_lessons ADD COLUMN IF NOT EXISTS week_type   INTEGER DEFAULT 0",
            "ALTER TABLE schedule_lessons ADD COLUMN IF NOT EXISTS is_event    INTEGER DEFAULT 0",
            # schedule_overrides
            "ALTER TABLE schedule_overrides ADD COLUMN IF NOT EXISTS lesson_num    INTEGER",
            "ALTER TABLE schedule_overrides ADD COLUMN IF NOT EXISTS action        TEXT",
            "ALTER TABLE schedule_overrides ADD COLUMN IF NOT EXISTS override_date TEXT",
            "ALTER TABLE schedule_overrides ADD COLUMN IF NOT EXISTS subject       TEXT",
            "ALTER TABLE schedule_overrides ADD COLUMN IF NOT EXISTS time_start    TEXT",
            "ALTER TABLE schedule_overrides ADD COLUMN IF NOT EXISTS time_end      TEXT",
            "ALTER TABLE schedule_overrides ADD COLUMN IF NOT EXISTS room          TEXT",
            "ALTER TABLE schedule_overrides ADD COLUMN IF NOT EXISTS teacher       TEXT",
            "ALTER TABLE schedule_overrides ADD COLUMN IF NOT EXISTS comment       TEXT",
            # schedule_events
            "ALTER TABLE schedule_events ADD COLUMN IF NOT EXISTS lesson_num  INTEGER",
            "ALTER TABLE schedule_events ADD COLUMN IF NOT EXISTS subject     TEXT",
            "ALTER TABLE schedule_events ADD COLUMN IF NOT EXISTS time_start  TEXT",
            "ALTER TABLE schedule_events ADD COLUMN IF NOT EXISTS time_end    TEXT",
            "ALTER TABLE schedule_events ADD COLUMN IF NOT EXISTS queue_id    INTEGER",
            "ALTER TABLE schedule_events ADD COLUMN IF NOT EXISTS status      TEXT DEFAULT 'pending'",
            # schedule_sources
            "ALTER TABLE schedule_sources ADD COLUMN IF NOT EXISTS last_post_id TEXT",
            "ALTER TABLE schedule_sources ADD COLUMN IF NOT EXISTS source_type  TEXT",
            "ALTER TABLE schedule_sources ADD COLUMN IF NOT EXISTS source_id    TEXT",
        ]
        for sql in migrations:
            try:
                await conn.execute(sql)
            except Exception as e:
                logger.debug(f"Migration skipped ({e}): {sql[:60]}")


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

async def _fetchall(query: str, params=()):
    from database import DB_PATH
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(query, params)
        return [dict(r) for r in await cur.fetchall()]


async def _fetchone(query: str, params=()):
    from database import DB_PATH
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(query, params)
        row = await cur.fetchone()
        return dict(row) if row else None


async def _execute(query: str, params=()) -> int:
    """Execute write query, return lastrowid."""
    from database import DB_PATH
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(query, params)
        await db.commit()
        return cur.lastrowid


# ─────────────────────────────────────────────
# GROUPS
# ─────────────────────────────────────────────

async def get_all_study_groups() -> list[dict]:
    if _get_db_type() == "postgres":
        from database_pg import get_pool
        pool = await get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM schedule_groups")
            return [dict(r) for r in rows]
    return await _fetchall("SELECT * FROM schedule_groups")


# Алиас — используется в schedule_monitor.py
async def get_all_groups() -> list[dict]:
    return await get_all_study_groups()


async def get_chat_groups(chat_id: int) -> list[dict]:
    """Учебные группы для конкретного чата."""
    if _get_db_type() == "postgres":
        from database_pg import get_pool
        pool = await get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM schedule_groups WHERE chat_id=$1", chat_id
            )
            return [dict(r) for r in rows]
    return await _fetchall(
        "SELECT * FROM schedule_groups WHERE chat_id=?", (chat_id,)
    )


async def upsert_group(chat_id: int, group_name: str) -> int:
    """Создать или найти группу, вернуть её id."""
    if _get_db_type() == "postgres":
        from database_pg import get_pool
        pool = await get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow("""
                INSERT INTO schedule_groups (chat_id, group_name)
                VALUES ($1, $2)
                ON CONFLICT (chat_id, group_name) DO UPDATE SET group_name=EXCLUDED.group_name
                RETURNING id
            """, chat_id, group_name)
            return row["id"]

    from database import DB_PATH
    async with aiosqlite.connect(DB_PATH) as db:
        # Пробуем найти существующую
        cur = await db.execute(
            "SELECT id FROM schedule_groups WHERE chat_id=? AND group_name=?",
            (chat_id, group_name)
        )
        row = await cur.fetchone()
        if row:
            return row[0]
        cur = await db.execute(
            "INSERT INTO schedule_groups (chat_id, group_name) VALUES (?,?)",
            (chat_id, group_name)
        )
        await db.commit()
        return cur.lastrowid


# ─────────────────────────────────────────────
# LESSONS
# ─────────────────────────────────────────────

async def get_lessons_for_day(group_id: int, weekday: int) -> list[dict]:
    if _get_db_type() == "postgres":
        from database_pg import get_pool
        pool = await get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT * FROM schedule_lessons
                WHERE group_id=$1 AND weekday=$2 AND is_active=1
                ORDER BY time_start
            """, group_id, weekday)
            return [dict(r) for r in rows]
    return await _fetchall("""
        SELECT * FROM schedule_lessons
        WHERE group_id=? AND weekday=? AND is_active=1
        ORDER BY time_start
    """, (group_id, weekday))


async def save_lessons(group_id: int, lessons: list[dict]):
    """Заменить все занятия группы новыми (из OCR)."""
    if _get_db_type() == "postgres":
        from database_pg import get_pool
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM schedule_lessons WHERE group_id=$1", group_id
            )
            for l in lessons:
                await conn.execute("""
                    INSERT INTO schedule_lessons
                        (group_id, weekday, lesson_num, subject,
                         time_start, time_end, room, teacher,
                         week_type, is_event)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
                """, group_id, l["weekday"], l["lesson_num"],
                    l["subject"],
                    l.get("time_start") or "",
                    l.get("time_end") or "",
                    l.get("room"), l.get("teacher"),
                    int(l.get("week_type") or 0),
                    int(l.get("is_event") or 0))
        return

    from database import DB_PATH
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "DELETE FROM schedule_lessons WHERE group_id=?", (group_id,)
        )
        for l in lessons:
            await db.execute("""
                INSERT INTO schedule_lessons
                    (group_id, weekday, lesson_num, subject,
                     time_start, time_end, room, teacher,
                     week_type, is_event)
                VALUES (?,?,?,?,?,?,?,?,?,?)
            """, (group_id, l["weekday"], l["lesson_num"],
                  l["subject"],
                  l.get("time_start") or "",
                  l.get("time_end") or "",
                  l.get("room"), l.get("teacher"),
                  int(l.get("week_type") or 0),
                  int(l.get("is_event") or 0)))
        await db.commit()


async def toggle_lesson_skip_queue(lesson_id: int):
    """Переключить флаг skip_queue для занятия."""
    if _get_db_type() == "postgres":
        from database_pg import get_pool
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute("""
                UPDATE schedule_lessons
                SET skip_queue = CASE WHEN skip_queue=1 THEN 0 ELSE 1 END
                WHERE id=$1
            """, lesson_id)
        return
    await _execute("""
        UPDATE schedule_lessons
        SET skip_queue = CASE WHEN skip_queue=1 THEN 0 ELSE 1 END
        WHERE id=?
    """, (lesson_id,))


# ─────────────────────────────────────────────
# OVERRIDES
# ─────────────────────────────────────────────

async def get_overrides_for_date(group_id: int, date_str: str) -> list[dict]:
    """date_str формат: YYYY-MM-DD"""
    if _get_db_type() == "postgres":
        from database_pg import get_pool
        pool = await get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT * FROM schedule_overrides
                WHERE group_id=$1 AND override_date=$2
            """, group_id, date_str)
            return [dict(r) for r in rows]
    return await _fetchall("""
        SELECT * FROM schedule_overrides
        WHERE group_id=? AND override_date=?
    """, (group_id, date_str))


async def save_override(group_id: int, override: dict, fallback_date: str = None):
    """
    Сохранить изменение расписания из парсера.
    override может содержать: action/type, lesson_num, subject,
    time_start, time_end, room, teacher, date, comment.
    fallback_date — дата из заголовка листа изменений (если в строке нет своей даты).
    """
    action = override.get("action") or override.get("type") or "cancel"

    # Определяем дату: из строки → из заголовка → сегодня
    date_raw = override.get("date") or override.get("new_date") or fallback_date
    if date_raw:
        for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d.%m.%Y."):
            try:
                date_str = datetime.strptime(date_raw.rstrip("."), fmt).strftime("%Y-%m-%d")
                break
            except ValueError:
                continue
        else:
            date_str = get_local_now().strftime("%Y-%m-%d")
    else:
        date_str = get_local_now().strftime("%Y-%m-%d")

    if _get_db_type() == "postgres":
        from database_pg import get_pool
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO schedule_overrides
                    (group_id, override_date, lesson_num, action,
                     subject, time_start, time_end, room, teacher, comment)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
            """, group_id, date_str,
                override.get("lesson_num"),
                action,
                override.get("subject"),
                override.get("time_start") or override.get("new_time_start"),
                override.get("time_end") or override.get("new_time_end"),
                override.get("room"),
                override.get("teacher"),
                override.get("comment"))
        return

    await _execute("""
        INSERT INTO schedule_overrides
            (group_id, override_date, lesson_num, action,
             subject, time_start, time_end, room, teacher, comment)
        VALUES (?,?,?,?,?,?,?,?,?,?)
    """, (group_id, date_str,
          override.get("lesson_num"),
          action,
          override.get("subject"),
          override.get("time_start") or override.get("new_time_start"),
          override.get("time_end") or override.get("new_time_end"),
          override.get("room"),
          override.get("teacher"),
          override.get("comment")))


# ─────────────────────────────────────────────
# SCHEDULE EVENTS (очереди по расписанию)
# ─────────────────────────────────────────────

async def get_pending_events(date_str: str) -> list[dict]:
    """События (pending/active) на дату."""
    if _get_db_type() == "postgres":
        from database_pg import get_pool
        pool = await get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT * FROM schedule_events
                WHERE date=$1 AND status IN ('pending','active')
            """, date_str)
            return [dict(r) for r in rows]
    return await _fetchall("""
        SELECT * FROM schedule_events
        WHERE date=? AND status IN ('pending','active')
    """, (date_str,))


async def get_active_events(date_str: str) -> list[dict]:
    """Активные события на дату."""
    if _get_db_type() == "postgres":
        from database_pg import get_pool
        pool = await get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT * FROM schedule_events
                WHERE date=$1 AND status='active'
            """, date_str)
            return [dict(r) for r in rows]
    return await _fetchall("""
        SELECT * FROM schedule_events
        WHERE date=? AND status='active'
    """, (date_str,))


async def create_schedule_event(group_id: int, chat_id: int, date: str,
                                 lesson_num: int, subject: str,
                                 time_start: str, time_end: str) -> int:
    if _get_db_type() == "postgres":
        from database_pg import get_pool
        pool = await get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow("""
                INSERT INTO schedule_events
                    (group_id, chat_id, date, lesson_num, subject, time_start, time_end)
                VALUES ($1,$2,$3,$4,$5,$6,$7)
                RETURNING id
            """, group_id, chat_id, date, lesson_num, subject, time_start, time_end)
            return row["id"]

    return await _execute("""
        INSERT INTO schedule_events
            (group_id, chat_id, date, lesson_num, subject, time_start, time_end)
        VALUES (?,?,?,?,?,?,?)
    """, (group_id, chat_id, date, lesson_num, subject, time_start, time_end))


async def update_event_queue(event_id: int, queue_id: int):
    if _get_db_type() == "postgres":
        from database_pg import get_pool
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE schedule_events SET queue_id=$1 WHERE id=$2",
                queue_id, event_id
            )
        return
    await _execute(
        "UPDATE schedule_events SET queue_id=? WHERE id=?",
        (queue_id, event_id)
    )


async def update_event_status(event_id: int, status: str):
    if _get_db_type() == "postgres":
        from database_pg import get_pool
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE schedule_events SET status=$1 WHERE id=$2",
                status, event_id
            )
        return
    await _execute(
        "UPDATE schedule_events SET status=? WHERE id=?",
        (status, event_id)
    )


# ─────────────────────────────────────────────
# SOURCES (мониторинг VK/Telegram)
# ─────────────────────────────────────────────

async def get_all_sources() -> list[dict]:
    if _get_db_type() == "postgres":
        from database_pg import get_pool
        pool = await get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM schedule_sources")
            return [dict(r) for r in rows]
    return await _fetchall("SELECT * FROM schedule_sources")


async def get_chat_sources(chat_id: int) -> list[dict]:
    if _get_db_type() == "postgres":
        from database_pg import get_pool
        pool = await get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM schedule_sources WHERE chat_id=$1", chat_id
            )
            return [dict(r) for r in rows]
    return await _fetchall(
        "SELECT * FROM schedule_sources WHERE chat_id=?", (chat_id,)
    )


async def add_source(chat_id: int, source_type: str, source_id: str):
    if _get_db_type() == "postgres":
        from database_pg import get_pool
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO schedule_sources (chat_id, source_type, source_id)
                VALUES ($1,$2,$3) ON CONFLICT DO NOTHING
            """, chat_id, source_type, source_id)
        return
    await _execute("""
        INSERT OR IGNORE INTO schedule_sources (chat_id, source_type, source_id)
        VALUES (?,?,?)
    """, (chat_id, source_type, source_id))


async def delete_source(source_id: int):
    if _get_db_type() == "postgres":
        from database_pg import get_pool
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM schedule_sources WHERE id=$1", source_id
            )
        return
    await _execute("DELETE FROM schedule_sources WHERE id=?", (source_id,))


async def update_source_checkpoint(source_id: int, checkpoint: str):
    if _get_db_type() == "postgres":
        from database_pg import get_pool
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE schedule_sources SET last_post_id=$1 WHERE id=$2",
                checkpoint, source_id
            )
        return
    await _execute(
        "UPDATE schedule_sources SET last_post_id=? WHERE id=?",
        (checkpoint, source_id)
    )


# ─────────────────────────────────────────────
# LEGACY (schedule_monitor.py совместимость)
# ─────────────────────────────────────────────

async def get_open_schedule_queue(group_id: int, date_str: str) -> Optional[dict]:
    """Открытая очередь для группы на дату."""
    if _get_db_type() == "postgres":
        from database_pg import get_pool
        pool = await get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT * FROM schedule_events
                WHERE group_id=$1 AND date=$2 AND status='active'
                ORDER BY id DESC LIMIT 1
            """, group_id, date_str)
            return dict(row) if row else None
    return await _fetchone("""
        SELECT * FROM schedule_events
        WHERE group_id=? AND date=? AND status='active'
        ORDER BY id DESC LIMIT 1
    """, (group_id, date_str))


async def mark_queue_opened(group_id: int, queue_id: int, date_str: str):
    """Совместимость со старым schedule_monitor.py."""
    pass  # schedule_manager.py теперь использует create_schedule_event


async def mark_queue_closed(group_id: int, date_str: str):
    """Совместимость со старым schedule_monitor.py."""
    pass  # schedule_manager.py теперь использует update_event_status


# ─────────────────────────────────────────────
# TODAY FULL
# ─────────────────────────────────────────────

async def get_all_lessons_today() -> list[dict]:
    today = get_local_now()
    weekday = today.isoweekday()
    date_str = today.strftime("%Y-%m-%d")

    if _get_db_type() == "postgres":
        from database_pg import get_pool
        pool = await get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT l.*, g.chat_id, g.group_name,
                       o.action as override_type
                FROM schedule_lessons l
                JOIN schedule_groups g ON g.id = l.group_id
                LEFT JOIN schedule_overrides o
                    ON o.group_id = l.group_id
                    AND o.lesson_num = l.lesson_num
                    AND o.override_date = $2
                WHERE l.weekday = $1 AND l.is_active = 1
                ORDER BY l.time_start
            """, weekday, date_str)
            return [dict(r) for r in rows]

    return await _fetchall("""
        SELECT l.*, g.chat_id, g.group_name,
               o.action as override_type
        FROM schedule_lessons l
        JOIN schedule_groups g ON g.id = l.group_id
        LEFT JOIN schedule_overrides o
            ON o.group_id = l.group_id
            AND o.lesson_num = l.lesson_num
            AND o.override_date = ?
        WHERE l.weekday = ? AND l.is_active = 1
        ORDER BY l.time_start
    """, (date_str, weekday))


# ═══════════════════════════════════════════════════════════════════
# РАСПИСАНИЕ ЗВОНКОВ (schedule_bells)
# ═══════════════════════════════════════════════════════════════════

# Звонки по умолчанию (типовое расписание колледжа)
DEFAULT_BELLS = [
    (1, "08:00", "09:35"),
    (2, "09:45", "11:20"),
    (3, "11:30", "13:05"),
    (4, "13:35", "15:10"),
    (5, "15:20", "16:55"),
    (6, "17:05", "18:40"),
    (7, "18:50", "20:25"),
]


async def get_bells(chat_id: int) -> list[dict]:
    """
    Возвращает расписание звонков для чата.
    Если не настроено — возвращает DEFAULT_BELLS.
    """
    if _get_db_type() == "postgres":
        from database_pg import get_pool
        pool = await get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM schedule_bells WHERE chat_id = $1 ORDER BY lesson_num",
                chat_id
            )
            result = [dict(r) for r in rows]
    else:
        result = await _fetchall(
            "SELECT * FROM schedule_bells WHERE chat_id = ? ORDER BY lesson_num",
            (chat_id,)
        )

    if not result:
        # Возвращаем дефолт в том же формате
        return [
            {"chat_id": chat_id, "lesson_num": n, "time_start": ts, "time_end": te}
            for n, ts, te in DEFAULT_BELLS
        ]
    return result


async def get_bell(chat_id: int, lesson_num: int) -> dict | None:
    """Получить звонок для конкретной пары."""
    if _get_db_type() == "postgres":
        from database_pg import get_pool
        pool = await get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM schedule_bells WHERE chat_id = $1 AND lesson_num = $2",
                chat_id, lesson_num
            )
            return dict(row) if row else None
    rows = await _fetchall(
        "SELECT * FROM schedule_bells WHERE chat_id = ? AND lesson_num = ?",
        (chat_id, lesson_num)
    )
    return rows[0] if rows else None


async def set_bell(chat_id: int, lesson_num: int, time_start: str, time_end: str):
    """Установить / обновить звонок для пары."""
    if _get_db_type() == "postgres":
        from database_pg import get_pool
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO schedule_bells (chat_id, lesson_num, time_start, time_end)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (chat_id, lesson_num)
                DO UPDATE SET time_start = EXCLUDED.time_start,
                              time_end   = EXCLUDED.time_end
            """, chat_id, lesson_num, time_start, time_end)
        return
    await _execute("""
        INSERT INTO schedule_bells (chat_id, lesson_num, time_start, time_end)
        VALUES (?, ?, ?, ?)
        ON CONFLICT (chat_id, lesson_num)
        DO UPDATE SET time_start = excluded.time_start,
                      time_end   = excluded.time_end
    """, (chat_id, lesson_num, time_start, time_end))


async def reset_bells(chat_id: int):
    """Сбросить расписание звонков чата к дефолту (удалить кастомные)."""
    if _get_db_type() == "postgres":
        from database_pg import get_pool
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM schedule_bells WHERE chat_id = $1", chat_id
            )
        return
    await _execute("DELETE FROM schedule_bells WHERE chat_id = ?", (chat_id,))


async def get_bell_time(chat_id: int, lesson_num: int) -> tuple[str, str]:
    """
    Вернуть (time_start, time_end) для пары.
    Ищет сначала кастомный звонок, потом дефолт.
    """
    bell = await get_bell(chat_id, lesson_num)
    if bell:
        return bell["time_start"], bell["time_end"]
    # Поиск в дефолтных
    for num, ts, te in DEFAULT_BELLS:
        if num == lesson_num:
            return ts, te
    return "", ""


# ═══════════════════════════════════════════════════════════════════
# CRUD ДЛЯ РЕДАКТОРА ЗАНЯТИЙ
# ═══════════════════════════════════════════════════════════════════

async def get_lesson_by_id(lesson_id: int) -> dict | None:
    """Получить занятие по ID — нужно чтобы показать карточку для редактирования."""
    if _get_db_type() == "postgres":
        from database_pg import get_pool
        pool = await get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM schedule_lessons WHERE id = $1", lesson_id)
            return dict(row) if row else None
    rows = await _fetchall("SELECT * FROM schedule_lessons WHERE id = ?", (lesson_id,))
    return rows[0] if rows else None


async def update_lesson_field(lesson_id: int, field: str, value: str):
    """
    Обновить одно поле занятия.
    Белый список полей — защита от SQL-инъекций.
    """
    if field not in _ALLOWED_LESSON_UPDATE_FIELDS:
        raise ValueError(f"Field {field!r} not allowed for update")

    if _get_db_type() == "postgres":
        from database_pg import get_pool
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                f'UPDATE schedule_lessons SET "{field}" = $1 WHERE id = $2',
                value or None, lesson_id
            )
        return
    await _execute(
        f'UPDATE schedule_lessons SET "{field}" = ? WHERE id = ?',
        (value or None, lesson_id)
    )


async def delete_lesson(lesson_id: int):
    """Удалить занятие из базового расписания навсегда."""
    if _get_db_type() == "postgres":
        from database_pg import get_pool
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM schedule_lessons WHERE id = $1", lesson_id)
        return
    await _execute("DELETE FROM schedule_lessons WHERE id = ?", (lesson_id,))


async def add_single_lesson(group_id: int, lesson: dict):
    """
    Добавить одно занятие к группе — НЕ удаляет остальные.
    Используется в редакторе при ручном добавлении пары.
    (В отличие от save_lessons, которая удаляет всё и заново вставляет.)
    """
    if _get_db_type() == "postgres":
        from database_pg import get_pool
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO schedule_lessons
                    (group_id, weekday, lesson_num, subject,
                     time_start, time_end, room, teacher, skip_queue,
                     week_type, is_event)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
            """, group_id,
                lesson["weekday"], lesson["lesson_num"],
                lesson["subject"],
                lesson.get("time_start") or "",
                lesson.get("time_end") or "",
                lesson.get("room"),
                lesson.get("teacher"),
                int(lesson.get("skip_queue", 0)),
                int(lesson.get("week_type", 0)),
                int(lesson.get("is_event", 0)))
        return

    from database import DB_PATH
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO schedule_lessons
                (group_id, weekday, lesson_num, subject,
                 time_start, time_end, room, teacher, skip_queue,
                 week_type, is_event)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """, (group_id,
              lesson["weekday"], lesson["lesson_num"],
              lesson["subject"],
              lesson.get("time_start") or "",
              lesson.get("time_end") or "",
              lesson.get("room"),
              lesson.get("teacher"),
              int(lesson.get("skip_queue", 0)),
              int(lesson.get("week_type", 0)),
              int(lesson.get("is_event", 0))))
        await db.commit()


async def delete_override(group_id: int, lesson_num: int, date_str: str):
    """
    Удалить конкретный override на дату.
    Используется для восстановления отменённой пары.
    """
    if _get_db_type() == "postgres":
        from database_pg import get_pool
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute("""
                DELETE FROM schedule_overrides
                WHERE group_id = $1 AND lesson_num = $2
                  AND override_date = $3 AND action = 'cancel'
            """, group_id, lesson_num, date_str)
        return
    await _execute("""
        DELETE FROM schedule_overrides
        WHERE group_id = ? AND lesson_num = ?
          AND override_date = ? AND action = 'cancel'
    """, (group_id, lesson_num, date_str))


async def delete_bell(chat_id: int, lesson_num: int):
    """
    Удалить один кастомный звонок.
    После удаления пара вернётся к дефолтному времени.
    """
    if _get_db_type() == "postgres":
        from database_pg import get_pool
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM schedule_bells WHERE chat_id = $1 AND lesson_num = $2",
                chat_id, lesson_num
            )
        return
    await _execute(
        "DELETE FROM schedule_bells WHERE chat_id = ? AND lesson_num = ?",
        (chat_id, lesson_num)
    )


# ═══════════════════════════════════════════════════════════════════
# ЧЁТНЫЕ / НЕЧЁТНЫЕ НЕДЕЛИ
# ═══════════════════════════════════════════════════════════════════

def get_current_week_type() -> int:
    """
    Тип ТЕКУЩЕЙ учебной недели по ISO-номеру:
      1 = нечётная (1, 3, 5, 7... ISO week)
      2 = чётная   (2, 4, 6, 8... ISO week)
    Учитывает TZ_OFFSET из .env чтобы использовать локальное время.
    """
    current_date = get_local_now().date()
    reference_date = None
    reference_week_type = 1

    try:
        from config import (
            ACADEMIC_WEEK_REFERENCE_DATE,
            ACADEMIC_WEEK_REFERENCE_TYPE,
        )

        raw_reference = (ACADEMIC_WEEK_REFERENCE_DATE or "").strip()
        if raw_reference:
            reference_date = date.fromisoformat(raw_reference)

        if ACADEMIC_WEEK_REFERENCE_TYPE in (1, 2):
            reference_week_type = ACADEMIC_WEEK_REFERENCE_TYPE
    except Exception:
        reference_date = None

    return get_week_type_for_date(
        current_date,
        reference_date=reference_date,
        reference_week_type=reference_week_type,
    )


def filter_by_week_type(lessons: list[dict]) -> list[dict]:
    """
    Фильтрует занятия под ТЕКУЩУЮ неделю.

    Правила для каждого занятия:
      week_type = 0 → идёт каждую неделю (включаем всегда)
      week_type = 1 → только нечётная неделя
      week_type = 2 → только чётная неделя
      is_event  = 1 → мероприятие (Разговоры о важном и т.п.) — пропускаем

    Примеры из расписания П-5-24:
      Пн лента 2 нечётная: Инструментальные средства (week_type=1)
      Пн лента 2 чётная:   Программирование web-приложений (week_type=2)
      Ср лента 1: только на чётной (week_type=2), на нечётной — прочерк
      Ср лента 5: только на нечётной (week_type=1), на чётной — прочерк
    """
    current = get_current_week_type()
    result = []
    for lesson in lessons:
        if int(lesson.get("is_event") or 0):
            continue  # мероприятие — без очереди
        wt = int(lesson.get("week_type") or 0)
        if wt == 0 or wt == current:
            result.append(lesson)
    return result


# Ключевые слова для автодетекта мероприятий в названии предмета
_EVENT_KEYWORDS = [
    "разговор о важном", "разговоры о важном",
    "внеклассное", "классный час", "воспитательное", "мероприятие",
]


def is_event_lesson(subject: str) -> bool:
    """True если занятие — внеклассное мероприятие (не создаём очередь)."""
    s = subject.lower()
    return any(kw in s for kw in _EVENT_KEYWORDS)


# ═══════════════════════════════════════════════════════════════════
# НАСТРОЙКИ УВЕДОМЛЕНИЙ РАСПИСАНИЯ
# ═══════════════════════════════════════════════════════════════════

async def get_chat_schedule_settings(chat_id: int) -> dict:
    """
    Настройки уведомлений расписания для чата.
    Дефолт если не настроено: всё включено.
    """
    if _get_db_type() == "postgres":
        from database_pg import get_pool
        pool = await get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM schedule_chat_settings WHERE chat_id=$1", chat_id
            )
            if row:
                return dict(row)
    else:
        rows = await _fetchall(
            "SELECT * FROM schedule_chat_settings WHERE chat_id=?", (chat_id,)
        )
        if rows:
            return rows[0]
    return {"chat_id": chat_id, "notify_on_open": 1, "notify_on_close": 1, "notify_before_min": 0}


async def update_chat_schedule_settings(chat_id: int, **kwargs):
    """UPSERT настроек уведомлений расписания."""
    allowed = {"notify_on_open", "notify_on_close", "notify_before_min"}
    updates = {k: v for k, v in kwargs.items() if k in allowed}
    if not updates:
        return
    if _get_db_type() == "postgres":
        from database_pg import get_pool
        pool = await get_pool()
        async with pool.acquire() as conn:
            set_c = ", ".join(f"{k} = ${i+2}" for i, k in enumerate(updates))
            cols  = ", ".join(updates.keys())
            ph    = ", ".join(f"${i+2}" for i in range(len(updates)))
            await conn.execute(
                f"INSERT INTO schedule_chat_settings (chat_id, {cols}) "
                f"VALUES ($1, {ph}) ON CONFLICT (chat_id) DO UPDATE SET {set_c}",
                chat_id, *list(updates.values())
            )
        return
    from database import DB_PATH
    async with aiosqlite.connect(DB_PATH) as db:
        set_c = ", ".join(f"{k} = ?" for k in updates)
        cols  = ", ".join(updates.keys())
        ph    = ", ".join("?" for _ in updates)
        vals  = list(updates.values())
        await db.execute(
            f"INSERT INTO schedule_chat_settings (chat_id, {cols}) "
            f"VALUES (?, {ph}) ON CONFLICT(chat_id) DO UPDATE SET {set_c}",
            [chat_id] + vals + vals
        )
        await db.commit()


async def toggle_chat_schedule_setting(chat_id: int, field: str) -> bool:
    """Переключает bool-поле (0→1, 1→0). Возвращает новое значение."""
    if field not in {"notify_on_open", "notify_on_close"}:
        raise ValueError(f"Field {field!r} not toggleable")
    settings = await get_chat_schedule_settings(chat_id)
    new_val  = 0 if settings.get(field, 1) else 1
    await update_chat_schedule_settings(chat_id, **{field: new_val})
    return bool(new_val)
