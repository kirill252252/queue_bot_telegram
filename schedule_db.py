"""
База данных для расписания — полная версия со всеми функциями.
Формат дат: YYYY-MM-DD (стандарт ISO, единый для всех модулей).
"""
import aiosqlite
import logging
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)


def _get_db_type():
    try:
        from config import DB_TYPE
        return DB_TYPE
    except Exception:
        return "sqlite"


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
                skip_queue INTEGER DEFAULT 0
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
                skip_queue INTEGER DEFAULT 0
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
                         time_start, time_end, room, teacher)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                """, group_id, l["weekday"], l["lesson_num"],
                    l["subject"], l["time_start"], l["time_end"],
                    l.get("room"), l.get("teacher"))
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
                     time_start, time_end, room, teacher)
                VALUES (?,?,?,?,?,?,?,?)
            """, (group_id, l["weekday"], l["lesson_num"],
                  l["subject"], l["time_start"], l["time_end"],
                  l.get("room"), l.get("teacher")))
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


async def save_override(group_id: int, override: dict):
    """
    Сохранить изменение расписания из парсера.
    override может содержать: action/type, lesson_num, subject,
    time_start, time_end, room, teacher, date, comment
    """
    action = override.get("action") or override.get("type") or "cancel"

    # Определяем дату: явная или сегодня
    date_raw = override.get("date") or override.get("new_date")
    if date_raw:
        # Пробуем нормализовать формат
        for fmt in ("%Y-%m-%d", "%d.%m.%Y"):
            try:
                date_str = datetime.strptime(date_raw, fmt).strftime("%Y-%m-%d")
                break
            except ValueError:
                continue
        else:
            date_str = datetime.now().strftime("%Y-%m-%d")
    else:
        date_str = datetime.now().strftime("%Y-%m-%d")

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
    today = datetime.now()
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
