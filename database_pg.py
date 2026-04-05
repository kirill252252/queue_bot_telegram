"""
PostgreSQL backend — drop-in replacement for database.py
Uses asyncpg directly (faster than SQLAlchemy for this use case).
Switch by setting DB_TYPE=postgres in .env
"""
import asyncpg
from typing import Optional
from config import POSTGRES_DSN

_pool: asyncpg.Pool = None


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(POSTGRES_DSN, min_size=2, max_size=10)
    return _pool


async def init_db():
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS queues (
                id SERIAL PRIMARY KEY,
                chat_id BIGINT NOT NULL,
                name TEXT NOT NULL,
                description TEXT,
                max_slots INTEGER DEFAULT 0,
                created_by BIGINT NOT NULL,
                is_active INTEGER DEFAULT 1,
                remind_timeout_min INTEGER DEFAULT 5,
                notify_leave_public INTEGER DEFAULT 1,
                auto_kick INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS queue_members (
                id SERIAL PRIMARY KEY,
                queue_id INTEGER NOT NULL REFERENCES queues(id) ON DELETE CASCADE,
                user_id BIGINT NOT NULL,
                display_name TEXT NOT NULL,
                username TEXT DEFAULT '',
                position INTEGER NOT NULL,
                joined_at TIMESTAMP DEFAULT NOW(),
                UNIQUE(queue_id, user_id)
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS user_profiles (
                user_id BIGINT PRIMARY KEY,
                full_name TEXT DEFAULT '',
                username TEXT DEFAULT '',
                dm_available INTEGER DEFAULT 0
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS group_nicks (
                user_id BIGINT NOT NULL,
                chat_id BIGINT NOT NULL,
                nick TEXT NOT NULL,
                PRIMARY KEY (user_id, chat_id)
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS known_chats (
                chat_id BIGINT PRIMARY KEY,
                title TEXT DEFAULT ''
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS reminder_tasks (
                id SERIAL PRIMARY KEY,
                queue_id INTEGER NOT NULL,
                user_id BIGINT NOT NULL,
                fire_at TIMESTAMP NOT NULL,
                done INTEGER DEFAULT 0
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS queue_subscriptions (
                id SERIAL PRIMARY KEY,
                queue_id INTEGER NOT NULL REFERENCES queues(id) ON DELETE CASCADE,
                user_id BIGINT NOT NULL,
                UNIQUE(queue_id, user_id)
            )
        """)


# ─── All functions mirror database.py exactly ────────────────────────────────

async def upsert_user(user_id: int, full_name: str, username: Optional[str], dm_available: bool = False):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO user_profiles (user_id, full_name, username, dm_available)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT(user_id) DO UPDATE SET
                full_name = EXCLUDED.full_name,
                username = EXCLUDED.username,
                dm_available = CASE WHEN EXCLUDED.dm_available = 1 THEN 1 ELSE user_profiles.dm_available END
        """, user_id, full_name, username or '', int(dm_available))


async def get_user_profile(user_id: int) -> Optional[dict]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM user_profiles WHERE user_id = $1", user_id)
        return dict(row) if row else None


async def set_group_nick(user_id: int, chat_id: int, nick: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        if nick:
            await conn.execute("""
                INSERT INTO group_nicks (user_id, chat_id, nick) VALUES ($1, $2, $3)
                ON CONFLICT(user_id, chat_id) DO UPDATE SET nick = EXCLUDED.nick
            """, user_id, chat_id, nick)
        else:
            await conn.execute("DELETE FROM group_nicks WHERE user_id=$1 AND chat_id=$2", user_id, chat_id)


async def get_all_group_nicks(user_id: int) -> list[dict]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM group_nicks WHERE user_id=$1", user_id)
        return [dict(r) for r in rows]


async def resolve_display_name(user_id: int, chat_id: int, fallback: str) -> str:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT nick FROM group_nicks WHERE user_id=$1 AND chat_id=$2", user_id, chat_id)
        if row and row['nick']:
            return row['nick']
        row2 = await conn.fetchrow("SELECT full_name FROM user_profiles WHERE user_id=$1", user_id)
        if row2 and row2['full_name']:
            return row2['full_name']
        return fallback


async def register_chat(chat_id: int, title: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO known_chats (chat_id, title) VALUES ($1, $2)
            ON CONFLICT(chat_id) DO UPDATE SET title = EXCLUDED.title
        """, chat_id, title)


async def get_known_chats() -> list[dict]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM known_chats")
        return [dict(r) for r in rows]


async def create_queue(chat_id, name, description, max_slots, created_by,
                       remind_timeout_min=5, notify_leave_public=True, auto_kick=True) -> int:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO queues (chat_id, name, description, max_slots, created_by,
                                remind_timeout_min, notify_leave_public, auto_kick)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING id
        """, chat_id, name, description, max_slots, created_by,
             remind_timeout_min, int(notify_leave_public), int(auto_kick))
        return row['id']


async def get_chat_queues(chat_id: int) -> list[dict]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM queues WHERE chat_id=$1 AND is_active=1 ORDER BY created_at DESC", chat_id)
        return [dict(r) for r in rows]


async def get_all_active_queues_for_known_chats(chat_ids: list[int]) -> list[dict]:
    if not chat_ids:
        return []
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM queues WHERE chat_id = ANY($1) AND is_active=1 ORDER BY created_at DESC",
            chat_ids)
        return [dict(r) for r in rows]


async def get_queue(queue_id: int) -> Optional[dict]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM queues WHERE id=$1", queue_id)
        return dict(row) if row else None


async def update_queue_settings(queue_id, remind_timeout_min, notify_leave_public, auto_kick):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE queues SET remind_timeout_min=$1, notify_leave_public=$2, auto_kick=$3
            WHERE id=$4
        """, remind_timeout_min, int(notify_leave_public), int(auto_kick), queue_id)


async def close_queue(queue_id: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("UPDATE queues SET is_active=0 WHERE id=$1", queue_id)


async def delete_queue(queue_id: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM queues WHERE id=$1", queue_id)


async def get_queue_members(queue_id: int) -> list[dict]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM queue_members WHERE queue_id=$1 ORDER BY position ASC", queue_id)
        return [dict(r) for r in rows]


async def get_member(queue_id: int, user_id: int) -> Optional[dict]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM queue_members WHERE queue_id=$1 AND user_id=$2", queue_id, user_id)
        return dict(row) if row else None


async def join_queue(queue_id: int, user_id: int, display_name: str, username: str) -> int:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT MAX(position) FROM queue_members WHERE queue_id=$1", queue_id)
        next_pos = (row['max'] or 0) + 1
        try:
            await conn.execute("""
                INSERT INTO queue_members (queue_id, user_id, display_name, username, position)
                VALUES ($1,$2,$3,$4,$5)
            """, queue_id, user_id, display_name, username, next_pos)
            return next_pos
        except asyncpg.UniqueViolationError:
            return -1


async def leave_queue(queue_id: int, user_id: int) -> bool:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT position FROM queue_members WHERE queue_id=$1 AND user_id=$2", queue_id, user_id)
        if not row:
            return False
        pos = row['position']
        await conn.execute(
            "DELETE FROM queue_members WHERE queue_id=$1 AND user_id=$2", queue_id, user_id)
        await conn.execute(
            "UPDATE queue_members SET position=position-1 WHERE queue_id=$1 AND position>$2",
            queue_id, pos)
        return True


async def kick_member(queue_id: int, user_id: int) -> bool:
    return await leave_queue(queue_id, user_id)


async def get_member_count(queue_id: int) -> int:
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetchval(
            "SELECT COUNT(*) FROM queue_members WHERE queue_id=$1", queue_id)


async def get_user_queue_memberships(user_id: int) -> list[dict]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT qm.queue_id, qm.position, q.name as queue_name, q.chat_id
            FROM queue_members qm
            JOIN queues q ON q.id = qm.queue_id
            WHERE qm.user_id=$1 AND q.is_active=1
            ORDER BY qm.position
        """, user_id)
        return [dict(r) for r in rows]


async def create_reminder(queue_id: int, user_id: int, fire_at: str, kind: str = 'remind'):
    from datetime import datetime
    fire_dt = datetime.strptime(fire_at, "%Y-%m-%d %H:%M:%S")
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE reminder_tasks SET done=1 WHERE queue_id=$1 AND user_id=$2 AND done=0",
            queue_id, user_id)
        await conn.execute(
            "INSERT INTO reminder_tasks (queue_id, user_id, fire_at) VALUES ($1,$2,$3)",
            queue_id, user_id, fire_dt)


async def get_due_reminders(now: str) -> list[dict]:
    from datetime import datetime
    pool = await get_pool()
    now_dt = datetime.strptime(now, "%Y-%m-%d %H:%M:%S")
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM reminder_tasks WHERE fire_at <= $1 AND done=0", now_dt)
        return [dict(r) for r in rows]


async def mark_reminder_done(reminder_id: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("UPDATE reminder_tasks SET done=1 WHERE id=$1", reminder_id)


async def cancel_reminders(queue_id: int, user_id: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE reminder_tasks SET done=1 WHERE queue_id=$1 AND user_id=$2 AND done=0",
            queue_id, user_id)


# ─── Subscriptions ────────────────────────────────────────────────────────────

async def subscribe_queue(queue_id: int, user_id: int) -> bool:
    pool = await get_pool()
    async with pool.acquire() as conn:
        try:
            await conn.execute(
                "INSERT INTO queue_subscriptions (queue_id, user_id) VALUES ($1,$2)",
                queue_id, user_id)
            return True
        except asyncpg.UniqueViolationError:
            return False


async def unsubscribe_queue(queue_id: int, user_id: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM queue_subscriptions WHERE queue_id=$1 AND user_id=$2",
            queue_id, user_id)


async def get_queue_subscribers(queue_id: int) -> list[int]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT user_id FROM queue_subscriptions WHERE queue_id=$1", queue_id)
        return [r['user_id'] for r in rows]


# ─── Stats ────────────────────────────────────────────────────────────────────

async def get_stats(chat_id: int) -> dict:
    pool = await get_pool()
    async with pool.acquire() as conn:
        total_queues = await conn.fetchval(
            "SELECT COUNT(*) FROM queues WHERE chat_id=$1", chat_id)
        active_queues = await conn.fetchval(
            "SELECT COUNT(*) FROM queues WHERE chat_id=$1 AND is_active=1", chat_id)
        total_members = await conn.fetchval("""
            SELECT COUNT(*) FROM queue_members qm
            JOIN queues q ON q.id=qm.queue_id WHERE q.chat_id=$1
        """, chat_id)
        return {
            "total_queues": total_queues,
            "active_queues": active_queues,
            "total_members": total_members,
        }