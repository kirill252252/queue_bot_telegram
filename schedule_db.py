"""
База данных для расписания (исправленная версия под scheduler).
"""
import aiosqlite
import asyncpg
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


# ─────────────────────────────
# INIT
# ─────────────────────────────

async def init_schedule_db():
    if _get_db_type() == "postgres":
        await _init_pg()
    else:
        await _init_sqlite()


async def _init_sqlite():
    from database import DB_PATH

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS schedule_groups (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                group_name TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(chat_id, group_name)
            )
        """)

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

        await db.execute("""
            CREATE TABLE IF NOT EXISTS schedule_queues (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lesson_id INTEGER NOT NULL,
                queue_id INTEGER NOT NULL,
                date TEXT NOT NULL,
                opened_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                closed_at TIMESTAMP
            )
        """)

        await db.commit()


# ─────────────────────────────
# GROUPS
# ─────────────────────────────

async def get_all_study_groups():
    db_type = _get_db_type()

    if db_type == "postgres":
        from database_pg import get_pool
        pool = await get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM schedule_groups")
            return [dict(r) for r in rows]

    from database import DB_PATH
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM schedule_groups")
        return [dict(r) for r in await cur.fetchall()]


# ─────────────────────────────
# LESSONS
# ─────────────────────────────

async def get_lessons_for_day(group_id: int, weekday: int):
    db_type = _get_db_type()

    if db_type == "postgres":
        from database_pg import get_pool
        pool = await get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT * FROM schedule_lessons
                WHERE group_id=$1 AND weekday=$2 AND is_active=1
                ORDER BY time_start
            """, group_id, weekday)
            return [dict(r) for r in rows]

    from database import DB_PATH
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("""
            SELECT * FROM schedule_lessons
            WHERE group_id=? AND weekday=? AND is_active=1
            ORDER BY time_start
        """, (group_id, weekday))
        return [dict(r) for r in await cur.fetchall()]


# ─────────────────────────────
# OVERRIDES
# ─────────────────────────────

async def get_overrides_for_date(group_id: int, date_str: str):
    db_type = _get_db_type()

    if db_type == "postgres":
        from database_pg import get_pool
        pool = await get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT * FROM schedule_overrides
                WHERE group_id=$1 AND override_date=$2
            """, group_id, date_str)
            return [dict(r) for r in rows]

    from database import DB_PATH
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("""
            SELECT * FROM schedule_overrides
            WHERE group_id=? AND override_date=?
        """, (group_id, date_str))
        return [dict(r) for r in await cur.fetchall()]


# ─────────────────────────────
# TODAY FULL (FIXED)
# ─────────────────────────────

async def get_all_lessons_today():
    today = datetime.now()
    weekday = today.isoweekday()
    date_str = today.strftime("%d.%m.%Y")

    db_type = _get_db_type()

    if db_type == "postgres":
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

    from database import DB_PATH
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("""
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

        return [dict(r) for r in await cur.fetchall()]


# ─────────────────────────────
# QUEUES
# ─────────────────────────────

async def get_open_schedule_queue(lesson_id: int, date_str: str):
    db_type = _get_db_type()

    if db_type == "postgres":
        from database_pg import get_pool
        pool = await get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT * FROM schedule_queues
                WHERE lesson_id=$1 AND date=$2 AND closed_at IS NULL
            """, lesson_id, date_str)
            return dict(row) if row else None

    from database import DB_PATH
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("""
            SELECT * FROM schedule_queues
            WHERE lesson_id=? AND date=? AND closed_at IS NULL
        """, (lesson_id, date_str))
        row = await cur.fetchone()
        return dict(row) if row else None


async def mark_queue_opened(lesson_id: int, queue_id: int, date_str: str):
    from datetime import datetime

    db_type = _get_db_type()

    if db_type == "postgres":
        from database_pg import get_pool
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO schedule_queues (lesson_id, queue_id, date)
                VALUES ($1,$2,$3)
                ON CONFLICT DO NOTHING
            """, lesson_id, queue_id, date_str)
        return

    from database import DB_PATH
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO schedule_queues (lesson_id, queue_id, date)
            VALUES (?,?,?)
        """, (lesson_id, queue_id, date_str))
        await db.commit()


async def mark_queue_closed(lesson_id: int, date_str: str):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    db_type = _get_db_type()

    if db_type == "postgres":
        from database_pg import get_pool
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute("""
                UPDATE schedule_queues
                SET closed_at=$1
                WHERE lesson_id=$2 AND date=$3 AND closed_at IS NULL
            """, now, lesson_id, date_str)
        return

    from database import DB_PATH
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            UPDATE schedule_queues
            SET closed_at=?
            WHERE lesson_id=? AND date=? AND closed_at IS NULL
        """, (now, lesson_id, date_str))
        await db.commit()