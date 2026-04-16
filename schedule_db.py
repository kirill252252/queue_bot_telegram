"""
База данных для расписания.
Отдельный модуль чтобы не мешать с основным database.py.
"""
import aiosqlite
import asyncpg
import json
import logging
from datetime import datetime, date
from typing import Optional

logger = logging.getLogger(__name__)

# импортируем нужный модуль в зависимости от конфига
def _get_db_type():
    try:
        from config import DB_TYPE
        return DB_TYPE
    except Exception:
        return "sqlite"


async def init_schedule_db():
    """Создаём таблицы расписания."""
    db_type = _get_db_type()
    if db_type == "postgres":
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
                group_id INTEGER NOT NULL REFERENCES schedule_groups(id) ON DELETE CASCADE,
                weekday INTEGER NOT NULL,
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
            CREATE TABLE IF NOT EXISTS schedule_sources (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                source_type TEXT NOT NULL,
                source_id TEXT NOT NULL,
                last_checked TEXT,
                last_post_id TEXT,
                UNIQUE(chat_id, source_type, source_id)
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS schedule_overrides (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                group_id INTEGER NOT NULL REFERENCES schedule_groups(id) ON DELETE CASCADE,
                override_date TEXT NOT NULL,
                lesson_id INTEGER,
                type TEXT NOT NULL,
                subject TEXT,
                time_start TEXT,
                time_end TEXT,
                comment TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS schedule_queues (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lesson_id INTEGER NOT NULL,
                queue_id INTEGER NOT NULL,
                opened_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                closed_at TIMESTAMP,
                date TEXT NOT NULL
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
                created_at TIMESTAMP DEFAULT NOW(),
                UNIQUE(chat_id, group_name)
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS schedule_lessons (
                id SERIAL PRIMARY KEY,
                group_id INTEGER NOT NULL REFERENCES schedule_groups(id) ON DELETE CASCADE,
                weekday INTEGER NOT NULL,
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
            CREATE TABLE IF NOT EXISTS schedule_sources (
                id SERIAL PRIMARY KEY,
                chat_id BIGINT NOT NULL,
                source_type TEXT NOT NULL,
                source_id TEXT NOT NULL,
                last_checked TEXT,
                last_post_id TEXT,
                UNIQUE(chat_id, source_type, source_id)
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS schedule_overrides (
                id SERIAL PRIMARY KEY,
                group_id INTEGER NOT NULL REFERENCES schedule_groups(id) ON DELETE CASCADE,
                override_date TEXT NOT NULL,
                lesson_id INTEGER,
                type TEXT NOT NULL,
                subject TEXT,
                time_start TEXT,
                time_end TEXT,
                comment TEXT,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS schedule_queues (
                id SERIAL PRIMARY KEY,
                lesson_id INTEGER NOT NULL,
                queue_id INTEGER NOT NULL,
                opened_at TIMESTAMP DEFAULT NOW(),
                closed_at TIMESTAMP,
                date TEXT NOT NULL
            )
        """)


# ─── Groups ──────────────────────────────────────────────────────────────────

async def upsert_group(chat_id: int, group_name: str) -> int:
    db_type = _get_db_type()
    if db_type == "postgres":
        from database_pg import get_pool
        pool = await get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow("""
                INSERT INTO schedule_groups (chat_id, group_name)
                VALUES ($1, $2)
                ON CONFLICT(chat_id, group_name) DO UPDATE SET group_name=EXCLUDED.group_name
                RETURNING id
            """, chat_id, group_name)
            return row['id']
    else:
        from database import DB_PATH
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("""
                INSERT INTO schedule_groups (chat_id, group_name)
                VALUES (?, ?)
                ON CONFLICT(chat_id, group_name) DO UPDATE SET group_name=excluded.group_name
            """, (chat_id, group_name))
            await db.commit()
            cur = await db.execute(
                "SELECT id FROM schedule_groups WHERE chat_id=? AND group_name=?",
                (chat_id, group_name))
            row = await cur.fetchone()
            return row[0]


async def get_chat_groups(chat_id: int) -> list[dict]:
    db_type = _get_db_type()
    if db_type == "postgres":
        from database_pg import get_pool
        pool = await get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM schedule_groups WHERE chat_id=$1", chat_id)
            return [dict(r) for r in rows]
    else:
        from database import DB_PATH
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(
                "SELECT * FROM schedule_groups WHERE chat_id=?", (chat_id,))
            return [dict(r) for r in await cur.fetchall()]


async def get_all_groups() -> list[dict]:
    db_type = _get_db_type()
    if db_type == "postgres":
        from database_pg import get_pool
        pool = await get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM schedule_groups")
            return [dict(r) for r in rows]
    else:
        from database import DB_PATH
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute("SELECT * FROM schedule_groups")
            return [dict(r) for r in await cur.fetchall()]


# ─── Lessons ─────────────────────────────────────────────────────────────────

async def save_lessons(group_id: int, lessons: list[dict]):
    """Сохраняем расписание группы — сначала удаляем старое."""
    db_type = _get_db_type()
    if db_type == "postgres":
        from database_pg import get_pool
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM schedule_lessons WHERE group_id=$1", group_id)
            for l in lessons:
                await conn.execute("""
                    INSERT INTO schedule_lessons
                        (group_id, weekday, subject, time_start, time_end, room, teacher)
                    VALUES ($1,$2,$3,$4,$5,$6,$7)
                """, group_id, l["weekday"], l["subject"],
                    l["time_start"], l["time_end"],
                    l.get("room"), l.get("teacher"))
    else:
        from database import DB_PATH
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("DELETE FROM schedule_lessons WHERE group_id=?", (group_id,))
            for l in lessons:
                await db.execute("""
                    INSERT INTO schedule_lessons
                        (group_id, weekday, subject, time_start, time_end, room, teacher)
                    VALUES (?,?,?,?,?,?,?)
                """, (group_id, l["weekday"], l["subject"],
                      l["time_start"], l["time_end"],
                      l.get("room"), l.get("teacher")))
            await db.commit()


async def get_lessons_for_day(group_id: int, weekday: int) -> list[dict]:
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
    else:
        from database import DB_PATH
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute("""
                SELECT * FROM schedule_lessons
                WHERE group_id=? AND weekday=? AND is_active=1
                ORDER BY time_start
            """, (group_id, weekday))
            return [dict(r) for r in await cur.fetchall()]


async def get_all_lessons_today() -> list[dict]:
    """Все занятия на сегодня по всем группам."""
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
                       o.type as override_type, o.comment as override_comment
                FROM schedule_lessons l
                JOIN schedule_groups g ON g.id = l.group_id
                LEFT JOIN schedule_overrides o ON o.lesson_id = l.id AND o.override_date = $2
                WHERE l.weekday = $1 AND l.is_active = 1
                ORDER BY l.time_start
            """, weekday, date_str)
            return [dict(r) for r in rows]
    else:
        from database import DB_PATH
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute("""
                SELECT l.*, g.chat_id, g.group_name,
                       o.type as override_type, o.comment as override_comment
                FROM schedule_lessons l
                JOIN schedule_groups g ON g.id = l.group_id
                LEFT JOIN schedule_overrides o ON o.lesson_id = l.id AND o.override_date = ?
                WHERE l.weekday = ? AND l.is_active = 1
                ORDER BY l.time_start
            """, (date_str, weekday))
            return [dict(r) for r in await cur.fetchall()]


# ─── Overrides ───────────────────────────────────────────────────────────────

async def save_override(group_id: int, override: dict):
    date_str = override.get("date") or datetime.now().strftime("%d.%m.%Y")
    db_type = _get_db_type()
    if db_type == "postgres":
        from database_pg import get_pool
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO schedule_overrides
                    (group_id, override_date, lesson_id, type, subject, time_start, time_end, comment)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
            """, group_id, date_str, override.get("lesson_id"),
                override["type"], override.get("subject"),
                override.get("time_start"), override.get("time_end"),
                override.get("comment"))
    else:
        from database import DB_PATH
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("""
                INSERT INTO schedule_overrides
                    (group_id, override_date, lesson_id, type, subject, time_start, time_end, comment)
                VALUES (?,?,?,?,?,?,?,?)
            """, (group_id, date_str, override.get("lesson_id"),
                  override["type"], override.get("subject"),
                  override.get("time_start"), override.get("time_end"),
                  override.get("comment")))
            await db.commit()


# ─── Schedule queues tracking ─────────────────────────────────────────────────

async def mark_queue_opened(lesson_id: int, queue_id: int, date_str: str):
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
    else:
        from database import DB_PATH
        async with aiosqlite.connect(DB_PATH) as db:
            try:
                await db.execute(
                    "INSERT INTO schedule_queues (lesson_id, queue_id, date) VALUES (?,?,?)",
                    (lesson_id, queue_id, date_str))
                await db.commit()
            except Exception:
                pass


async def get_open_schedule_queue(lesson_id: int, date_str: str) -> Optional[dict]:
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
    else:
        from database import DB_PATH
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute("""
                SELECT * FROM schedule_queues
                WHERE lesson_id=? AND date=? AND closed_at IS NULL
            """, (lesson_id, date_str))
            row = await cur.fetchone()
            return dict(row) if row else None


async def mark_queue_closed(lesson_id: int, date_str: str):
    db_type = _get_db_type()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if db_type == "postgres":
        from database_pg import get_pool
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute("""
                UPDATE schedule_queues SET closed_at=$1
                WHERE lesson_id=$2 AND date=$3 AND closed_at IS NULL
            """, now, lesson_id, date_str)
    else:
        from database import DB_PATH
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("""
                UPDATE schedule_queues SET closed_at=?
                WHERE lesson_id=? AND date=? AND closed_at IS NULL
            """, (now, lesson_id, date_str))
            await db.commit()


# ─── Skip queue management ────────────────────────────────────────────────────

async def set_lesson_skip_queue(lesson_id: int, skip: bool):
    db_type = _get_db_type()
    if db_type == "postgres":
        from database_pg import get_pool
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE schedule_lessons SET skip_queue=$1 WHERE id=$2",
                int(skip), lesson_id)
    else:
        from database import DB_PATH
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "UPDATE schedule_lessons SET skip_queue=? WHERE id=?",
                (int(skip), lesson_id))
            await db.commit()


async def get_lessons_for_day_full(group_id: int, weekday: int) -> list[dict]:
    """Все пары включая skip_queue флаг."""
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
    else:
        from database import DB_PATH
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute("""
                SELECT * FROM schedule_lessons
                WHERE group_id=? AND weekday=? AND is_active=1
                ORDER BY time_start
            """, (group_id, weekday))
            return [dict(r) for r in await cur.fetchall()]


# ─── Source monitoring ────────────────────────────────────────────────────────

async def add_source(chat_id: int, source_type: str, source_id: str):
    db_type = _get_db_type()
    if db_type == "postgres":
        from database_pg import get_pool
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO schedule_sources (chat_id, source_type, source_id)
                VALUES ($1,$2,$3) ON CONFLICT DO NOTHING
            """, chat_id, source_type, source_id)
    else:
        from database import DB_PATH
        async with aiosqlite.connect(DB_PATH) as db:
            try:
                await db.execute("""
                    INSERT INTO schedule_sources (chat_id, source_type, source_id)
                    VALUES (?,?,?)
                """, (chat_id, source_type, source_id))
                await db.commit()
            except Exception:
                pass


async def get_sources(chat_id: int) -> list[dict]:
    db_type = _get_db_type()
    if db_type == "postgres":
        from database_pg import get_pool
        pool = await get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM schedule_sources WHERE chat_id=$1", chat_id)
            return [dict(r) for r in rows]
    else:
        from database import DB_PATH
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(
                "SELECT * FROM schedule_sources WHERE chat_id=?", (chat_id,))
            return [dict(r) for r in await cur.fetchall()]


async def get_all_sources() -> list[dict]:
    db_type = _get_db_type()
    if db_type == "postgres":
        from database_pg import get_pool
        pool = await get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM schedule_sources")
            return [dict(r) for r in rows]
    else:
        from database import DB_PATH
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute("SELECT * FROM schedule_sources")
            return [dict(r) for r in await cur.fetchall()]


async def update_source_checkpoint(source_id_db: int, last_post_id: str):
    from datetime import datetime
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    db_type = _get_db_type()
    if db_type == "postgres":
        from database_pg import get_pool
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE schedule_sources SET last_checked=$1, last_post_id=$2 WHERE id=$3",
                now, last_post_id, source_id_db)
    else:
        from database import DB_PATH
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "UPDATE schedule_sources SET last_checked=?, last_post_id=? WHERE id=?",
                (now, last_post_id, source_id_db))
            await db.commit()


async def remove_source(source_db_id: int):
    db_type = _get_db_type()
    if db_type == "postgres":
        from database_pg import get_pool
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM schedule_sources WHERE id=$1", source_db_id)
    else:
        from database import DB_PATH
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("DELETE FROM schedule_sources WHERE id=?", (source_db_id,))
            await db.commit()
