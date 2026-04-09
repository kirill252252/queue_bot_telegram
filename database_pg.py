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
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS queue_invites (
                token TEXT PRIMARY KEY,
                queue_id INTEGER NOT NULL,
                created_by BIGINT NOT NULL,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS swap_requests (
                id SERIAL PRIMARY KEY,
                queue_id INTEGER NOT NULL,
                from_user BIGINT NOT NULL,
                to_user BIGINT NOT NULL,
                status TEXT DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS bot_admins (
                user_id BIGINT NOT NULL,
                chat_id BIGINT NOT NULL,
                added_at TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY (user_id, chat_id)
            )
        """)
        await conn.execute("""
            ALTER TABLE queue_members ADD COLUMN IF NOT EXISTS frozen_until TIMESTAMP
        """)
        await conn.execute("""
            ALTER TABLE reminder_tasks ADD COLUMN IF NOT EXISTS kind TEXT DEFAULT 'remind'
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS user_chats (
                user_id BIGINT NOT NULL,
                chat_id BIGINT NOT NULL,
                PRIMARY KEY (user_id, chat_id)
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


async def create_reminder(queue_id: int, user_id: int, fire_at: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE reminder_tasks SET done=1 WHERE queue_id=$1 AND user_id=$2 AND done=0",
            queue_id, user_id)
        await conn.execute(
            "INSERT INTO reminder_tasks (queue_id, user_id, fire_at) VALUES ($1,$2,$3::timestamp)",
            queue_id, user_id, fire_at)


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


import secrets
from datetime import datetime, timedelta


async def create_invite(queue_id: int, created_by: int) -> str:
    token = secrets.token_urlsafe(8)
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO queue_invites (token, queue_id, created_by) VALUES ($1,$2,$3)",
            token, queue_id, created_by)
    return token


async def get_invite(token: str) -> Optional[dict]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM queue_invites WHERE token=$1", token)
        return dict(row) if row else None


async def freeze_member(queue_id: int, user_id: int, minutes: int) -> bool:
    until = datetime.utcnow() + timedelta(minutes=minutes)
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE queue_members SET frozen_until=$1 WHERE queue_id=$2 AND user_id=$3",
            until, queue_id, user_id)
    return True


async def unfreeze_member(queue_id: int, user_id: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE queue_members SET frozen_until=NULL WHERE queue_id=$1 AND user_id=$2",
            queue_id, user_id)


async def is_frozen(queue_id: int, user_id: int) -> bool:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT frozen_until FROM queue_members WHERE queue_id=$1 AND user_id=$2",
            queue_id, user_id)
        if not row or not row['frozen_until']:
            return False
        return row['frozen_until'] > datetime.utcnow()


async def get_queue_members_active(queue_id: int) -> list[dict]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT * FROM queue_members
            WHERE queue_id=$1 AND (frozen_until IS NULL OR frozen_until <= NOW())
            ORDER BY position ASC
        """, queue_id)
        return [dict(r) for r in rows]


async def create_swap_request(queue_id: int, from_user: int, to_user: int) -> int:
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE swap_requests SET status='cancelled'
            WHERE queue_id=$1 AND from_user=$2 AND to_user=$3 AND status='pending'
        """, queue_id, from_user, to_user)
        row = await conn.fetchrow(
            "INSERT INTO swap_requests (queue_id, from_user, to_user) VALUES ($1,$2,$3) RETURNING id",
            queue_id, from_user, to_user)
        return row['id']


async def get_swap_request(request_id: int) -> Optional[dict]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM swap_requests WHERE id=$1", request_id)
        return dict(row) if row else None


async def execute_swap(queue_id: int, user_a: int, user_b: int) -> bool:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row_a = await conn.fetchrow(
            "SELECT position FROM queue_members WHERE queue_id=$1 AND user_id=$2", queue_id, user_a)
        row_b = await conn.fetchrow(
            "SELECT position FROM queue_members WHERE queue_id=$1 AND user_id=$2", queue_id, user_b)
        if not row_a or not row_b:
            return False
        pos_a, pos_b = row_a['position'], row_b['position']
        await conn.execute(
            "UPDATE queue_members SET position=9999 WHERE queue_id=$1 AND user_id=$2", queue_id, user_a)
        await conn.execute(
            "UPDATE queue_members SET position=$1 WHERE queue_id=$2 AND user_id=$3", pos_a, queue_id, user_b)
        await conn.execute(
            "UPDATE queue_members SET position=$1 WHERE queue_id=$2 AND user_id=$3", pos_b, queue_id, user_a)
        await conn.execute("""
            UPDATE swap_requests SET status='done'
            WHERE queue_id=$1 AND from_user=$2 AND to_user=$3 AND status='pending'
        """, queue_id, user_a, user_b)
    return True


async def decline_swap(request_id: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("UPDATE swap_requests SET status='declined' WHERE id=$1", request_id)


async def add_bot_admin(user_id: int, chat_id: int) -> bool:
    pool = await get_pool()
    async with pool.acquire() as conn:
        try:
            await conn.execute(
                "INSERT INTO bot_admins (user_id, chat_id) VALUES ($1,$2)", user_id, chat_id)
            return True
        except asyncpg.UniqueViolationError:
            return False


async def remove_bot_admin(user_id: int, chat_id: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM bot_admins WHERE user_id=$1 AND chat_id=$2", user_id, chat_id)


async def is_bot_admin(user_id: int, chat_id: int) -> bool:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT 1 FROM bot_admins WHERE user_id=$1 AND chat_id=$2", user_id, chat_id)
        return bool(row)


async def get_bot_admins(chat_id: int) -> list[dict]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT b.user_id, b.chat_id, b.added_at, p.full_name, p.username
            FROM bot_admins b
            LEFT JOIN user_profiles p ON p.user_id = b.user_id
            WHERE b.chat_id=$1
        """, chat_id)
        return [dict(r) for r in rows]


async def is_subscribed(queue_id: int, user_id: int) -> bool:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT 1 FROM queue_subscriptions WHERE queue_id=$1 AND user_id=$2",
            queue_id, user_id)
        return bool(row)


async def get_global_stats() -> dict:
    pool = await get_pool()
    async with pool.acquire() as conn:
        total_queues = await conn.fetchval("SELECT COUNT(*) FROM queues")
        active_queues = await conn.fetchval("SELECT COUNT(*) FROM queues WHERE is_active=1")
        total_members = await conn.fetchval("SELECT COUNT(*) FROM queue_members")
        total_users = await conn.fetchval("SELECT COUNT(*) FROM user_profiles")
        total_chats = await conn.fetchval("SELECT COUNT(*) FROM known_chats")
        return {
            "total_queues": total_queues,
            "active_queues": active_queues,
            "total_members": total_members,
            "total_users": total_users,
            "total_chats": total_chats,
        }


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


async def get_user_profile_by_username(username: str) -> Optional[dict]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM user_profiles WHERE username = $1", username)
        return dict(row) if row else None


async def get_all_users() -> list[int]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT user_id FROM user_profiles WHERE dm_available = 1")
        return [r['user_id'] for r in rows]


async def get_user_known_chats(user_id: int) -> list[int]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT DISTINCT q.chat_id FROM queue_members qm
            JOIN queues q ON q.id = qm.queue_id
            WHERE qm.user_id = $1
        """, user_id)
        return [r['chat_id'] for r in rows]
    
async def register_user_chat(user_id: int, chat_id: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        try:
            await conn.execute(
                "INSERT INTO user_chats (user_id, chat_id) VALUES ($1,$2) ON CONFLICT DO NOTHING",
                user_id, chat_id)
        except Exception:
            pass

async def get_user_known_chats(user_id: int) -> list[int]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT chat_id FROM user_chats WHERE user_id = $1", user_id)
        return [r['chat_id'] for r in rows]