import aiosqlite
import secrets
from datetime import datetime, timedelta
from typing import Optional

DB_PATH = "queue_bot.db"


# создаём все таблицы при старте
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS queues (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                description TEXT,
                max_slots INTEGER DEFAULT 0,
                created_by INTEGER NOT NULL,
                is_active INTEGER DEFAULT 1,
                remind_timeout_min INTEGER DEFAULT 5,
                notify_leave_public INTEGER DEFAULT 1,
                auto_kick INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS queue_members (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                queue_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                display_name TEXT NOT NULL,
                username TEXT,
                position INTEGER NOT NULL,
                frozen_until TIMESTAMP,
                joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (queue_id) REFERENCES queues(id),
                UNIQUE(queue_id, user_id)
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS user_profiles (
                user_id INTEGER PRIMARY KEY,
                full_name TEXT,
                username TEXT,
                dm_available INTEGER DEFAULT 0
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS user_group_nicks (
                user_id INTEGER NOT NULL,
                chat_id INTEGER NOT NULL,
                nick TEXT NOT NULL,
                PRIMARY KEY (user_id, chat_id)
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS reminder_tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                queue_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                fire_at TIMESTAMP NOT NULL,
                kind TEXT NOT NULL DEFAULT 'remind',
                done INTEGER DEFAULT 0
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS known_chats (
                chat_id INTEGER PRIMARY KEY,
                title TEXT
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS queue_subscriptions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                queue_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                FOREIGN KEY (queue_id) REFERENCES queues(id) ON DELETE CASCADE,
                UNIQUE(queue_id, user_id)
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS queue_invites (
                token TEXT PRIMARY KEY,
                queue_id INTEGER NOT NULL,
                created_by INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS swap_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                queue_id INTEGER NOT NULL,
                from_user INTEGER NOT NULL,
                to_user INTEGER NOT NULL,
                status TEXT DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        for col, defn in [
            ("remind_timeout_min", "INTEGER DEFAULT 5"),
            ("notify_leave_public", "INTEGER DEFAULT 1"),
            ("auto_kick", "INTEGER DEFAULT 1"),
        ]:
            try:
                await db.execute(f"ALTER TABLE queues ADD COLUMN {col} {defn}")
            except Exception:
                pass
        try:
            await db.execute("ALTER TABLE queue_members ADD COLUMN frozen_until TIMESTAMP")
        except Exception:
            pass
        try:
            await db.execute("ALTER TABLE reminder_tasks ADD COLUMN kind TEXT NOT NULL DEFAULT 'remind'")
        except Exception:
            pass
        await db.commit()


# сохраняем чат чтобы потом показывать его в личке
async def register_chat(chat_id: int, title: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO known_chats (chat_id, title) VALUES (?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET title = excluded.title
        """, (chat_id, title))
        await db.commit()


async def get_known_chats() -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM known_chats")
        return [dict(r) for r in await cur.fetchall()]


async def upsert_user(user_id: int, full_name: str, username: Optional[str], dm_available: bool = False):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO user_profiles (user_id, full_name, username, dm_available)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                full_name = excluded.full_name,
                username  = excluded.username,
                dm_available = CASE WHEN excluded.dm_available = 1 THEN 1 ELSE dm_available END
        """, (user_id, full_name, username, int(dm_available)))
        await db.commit()


async def get_user_profile(user_id: int) -> Optional[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM user_profiles WHERE user_id = ?", (user_id,))
        row = await cur.fetchone()
        return dict(row) if row else None


async def get_user_profile_by_username(username: str) -> Optional[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT * FROM user_profiles WHERE username = ?", (username,))
        row = await cur.fetchone()
        return dict(row) if row else None


async def set_group_nick(user_id: int, chat_id: int, nick: str):
    async with aiosqlite.connect(DB_PATH) as db:
        if nick:
            await db.execute("""
                INSERT INTO user_group_nicks (user_id, chat_id, nick)
                VALUES (?, ?, ?)
                ON CONFLICT(user_id, chat_id) DO UPDATE SET nick = excluded.nick
            """, (user_id, chat_id, nick))
        else:
            await db.execute(
                "DELETE FROM user_group_nicks WHERE user_id = ? AND chat_id = ?",
                (user_id, chat_id))
        await db.commit()


async def get_group_nick(user_id: int, chat_id: int) -> Optional[str]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT nick FROM user_group_nicks WHERE user_id = ? AND chat_id = ?",
            (user_id, chat_id))
        row = await cur.fetchone()
        return row[0] if row else None


async def get_all_group_nicks(user_id: int) -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT chat_id, nick FROM user_group_nicks WHERE user_id = ?", (user_id,))
        return [dict(r) for r in await cur.fetchall()]


async def resolve_display_name(user_id: int, chat_id: int, fallback: str) -> str:
    nick = await get_group_nick(user_id, chat_id)
    return nick if nick else fallback


async def create_queue(chat_id: int, name: str, description: Optional[str],
                       max_slots: int, created_by: int,
                       remind_timeout_min: int = 5,
                       notify_leave_public: bool = True,
                       auto_kick: bool = True) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("""
            INSERT INTO queues
              (chat_id, name, description, max_slots, created_by,
               remind_timeout_min, notify_leave_public, auto_kick)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (chat_id, name, description, max_slots, created_by,
              remind_timeout_min, int(notify_leave_public), int(auto_kick)))
        await db.commit()
        return cur.lastrowid


async def get_chat_queues(chat_id: int) -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT * FROM queues WHERE chat_id = ? AND is_active = 1 ORDER BY created_at DESC",
            (chat_id,))
        return [dict(r) for r in await cur.fetchall()]


async def get_all_active_queues_for_known_chats(chat_ids: list[int]) -> list[dict]:
    if not chat_ids:
        return []
    placeholders = ",".join("?" * len(chat_ids))
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            f"SELECT * FROM queues WHERE chat_id IN ({placeholders}) AND is_active = 1",
            chat_ids)
        return [dict(r) for r in await cur.fetchall()]


async def get_user_queue_memberships(user_id: int) -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("""
            SELECT q.id as queue_id, q.name as queue_name, q.chat_id,
                   qm.position
            FROM queue_members qm
            JOIN queues q ON q.id = qm.queue_id
            WHERE qm.user_id = ? AND q.is_active = 1
            ORDER BY q.chat_id, qm.position
        """, (user_id,))
        return [dict(r) for r in await cur.fetchall()]


async def get_queue(queue_id: int) -> Optional[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM queues WHERE id = ?", (queue_id,))
        row = await cur.fetchone()
        return dict(row) if row else None


async def update_queue_settings(queue_id: int, remind_timeout_min: int,
                                notify_leave_public: bool, auto_kick: bool):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            UPDATE queues SET remind_timeout_min=?, notify_leave_public=?, auto_kick=?
            WHERE id=?
        """, (remind_timeout_min, int(notify_leave_public), int(auto_kick), queue_id))
        await db.commit()


async def close_queue(queue_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE queues SET is_active = 0 WHERE id = ?", (queue_id,))
        await db.commit()


async def delete_queue(queue_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM queue_members WHERE queue_id = ?", (queue_id,))
        await db.execute("DELETE FROM queues WHERE id = ?", (queue_id,))
        await db.execute("DELETE FROM reminder_tasks WHERE queue_id = ?", (queue_id,))
        await db.commit()


async def get_queue_members(queue_id: int) -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT * FROM queue_members WHERE queue_id = ? ORDER BY position ASC",
            (queue_id,))
        return [dict(r) for r in await cur.fetchall()]


async def get_queue_members_active(queue_id: int) -> list[dict]:
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("""
            SELECT * FROM queue_members
            WHERE queue_id = ?
              AND (frozen_until IS NULL OR frozen_until <= ?)
            ORDER BY position ASC
        """, (queue_id, now))
        return [dict(r) for r in await cur.fetchall()]


async def get_member(queue_id: int, user_id: int) -> Optional[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT * FROM queue_members WHERE queue_id = ? AND user_id = ?",
            (queue_id, user_id))
        row = await cur.fetchone()
        return dict(row) if row else None


async def join_queue(queue_id: int, user_id: int, display_name: str, username: str) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT MAX(position) FROM queue_members WHERE queue_id = ?", (queue_id,))
        row = await cur.fetchone()
        next_pos = (row[0] or 0) + 1
        try:
            await db.execute("""
                INSERT INTO queue_members (queue_id, user_id, display_name, username, position)
                VALUES (?, ?, ?, ?, ?)
            """, (queue_id, user_id, display_name, username, next_pos))
            await db.commit()
            return next_pos
        except aiosqlite.IntegrityError:
            return -1


# выходим из очереди и сдвигаем всех за нами
async def leave_queue(queue_id: int, user_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT position FROM queue_members WHERE queue_id = ? AND user_id = ?",
            (queue_id, user_id))
        row = await cur.fetchone()
        if not row:
            return False
        pos = row[0]
        await db.execute(
            "DELETE FROM queue_members WHERE queue_id = ? AND user_id = ?",
            (queue_id, user_id))
        await db.execute(
            "UPDATE queue_members SET position = position - 1 WHERE queue_id = ? AND position > ?",
            (queue_id, pos))
        await db.commit()
        return True


async def kick_member(queue_id: int, user_id: int) -> bool:
    return await leave_queue(queue_id, user_id)


async def get_member_count(queue_id: int) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT COUNT(*) FROM queue_members WHERE queue_id = ?", (queue_id,))
        row = await cur.fetchone()
        return row[0]


# планируем напоминание на конкретное время
async def create_reminder(queue_id: int, user_id: int, fire_at: str, kind: str = 'remind'):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE reminder_tasks SET done=1 WHERE queue_id=? AND user_id=? AND done=0",
            (queue_id, user_id))
        await db.execute(
            "INSERT INTO reminder_tasks (queue_id, user_id, fire_at, kind) VALUES (?,?,?,?)",
            (queue_id, user_id, fire_at, kind))
        await db.commit()


async def get_due_reminders(now: str) -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT * FROM reminder_tasks WHERE fire_at <= ? AND done = 0", (now,))
        return [dict(r) for r in await cur.fetchall()]


async def mark_reminder_done(reminder_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE reminder_tasks SET done = 1 WHERE id = ?", (reminder_id,))
        await db.commit()


async def cancel_reminders(queue_id: int, user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE reminder_tasks SET done=1 WHERE queue_id=? AND user_id=? AND done=0",
            (queue_id, user_id))
        await db.commit()


# подписка — хочет узнать когда освободится место
async def subscribe_queue(queue_id: int, user_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        try:
            await db.execute(
                "INSERT INTO queue_subscriptions (queue_id, user_id) VALUES (?, ?)",
                (queue_id, user_id))
            await db.commit()
            return True
        except aiosqlite.IntegrityError:
            return False


async def unsubscribe_queue(queue_id: int, user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "DELETE FROM queue_subscriptions WHERE queue_id=? AND user_id=?",
            (queue_id, user_id))
        await db.commit()


async def get_queue_subscribers(queue_id: int) -> list[int]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT user_id FROM queue_subscriptions WHERE queue_id=?", (queue_id,))
        return [r[0] for r in await cur.fetchall()]


async def is_subscribed(queue_id: int, user_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT 1 FROM queue_subscriptions WHERE queue_id=? AND user_id=?",
            (queue_id, user_id))
        return bool(await cur.fetchone())


async def get_stats(chat_id: int) -> dict:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT COUNT(*) FROM queues WHERE chat_id=?", (chat_id,))
        total_queues = (await cur.fetchone())[0]
        cur = await db.execute(
            "SELECT COUNT(*) FROM queues WHERE chat_id=? AND is_active=1", (chat_id,))
        active_queues = (await cur.fetchone())[0]
        cur = await db.execute("""
            SELECT COUNT(*) FROM queue_members qm
            JOIN queues q ON q.id=qm.queue_id WHERE q.chat_id=?
        """, (chat_id,))
        total_members = (await cur.fetchone())[0]
        cur = await db.execute("""
            SELECT COUNT(DISTINCT qm.user_id) FROM queue_members qm
            JOIN queues q ON q.id=qm.queue_id WHERE q.chat_id=?
        """, (chat_id,))
        unique_users = (await cur.fetchone())[0]
        return {
            "total_queues": total_queues,
            "active_queues": active_queues,
            "total_members": total_members,
            "unique_users": unique_users,
        }


async def get_global_stats() -> dict:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT COUNT(*) FROM queues")
        total_queues = (await cur.fetchone())[0]
        cur = await db.execute("SELECT COUNT(*) FROM queues WHERE is_active=1")
        active_queues = (await cur.fetchone())[0]
        cur = await db.execute("SELECT COUNT(*) FROM queue_members")
        total_members = (await cur.fetchone())[0]
        cur = await db.execute("SELECT COUNT(*) FROM user_profiles")
        total_users = (await cur.fetchone())[0]
        cur = await db.execute("SELECT COUNT(*) FROM known_chats")
        total_chats = (await cur.fetchone())[0]
        return {
            "total_queues": total_queues,
            "active_queues": active_queues,
            "total_members": total_members,
            "total_users": total_users,
            "total_chats": total_chats,
        }


# генерируем уникальный токен для ссылки-приглашения
async def create_invite(queue_id: int, created_by: int) -> str:
    token = secrets.token_urlsafe(8)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO queue_invites (token, queue_id, created_by) VALUES (?, ?, ?)",
            (token, queue_id, created_by))
        await db.commit()
    return token


async def get_invite(token: str) -> Optional[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT * FROM queue_invites WHERE token = ?", (token,))
        row = await cur.fetchone()
        return dict(row) if row else None


# замораживаем место — позиция сохраняется, уведомления не идут
async def freeze_member(queue_id: int, user_id: int, minutes: int) -> bool:
    until = (datetime.utcnow() + timedelta(minutes=minutes)).strftime("%Y-%m-%d %H:%M:%S")
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE queue_members SET frozen_until = ? WHERE queue_id = ? AND user_id = ?",
            (until, queue_id, user_id))
        await db.commit()
    return True


async def unfreeze_member(queue_id: int, user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE queue_members SET frozen_until = NULL WHERE queue_id = ? AND user_id = ?",
            (queue_id, user_id))
        await db.commit()


async def is_frozen(queue_id: int, user_id: int) -> bool:
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT frozen_until FROM queue_members WHERE queue_id=? AND user_id=?",
            (queue_id, user_id))
        row = await cur.fetchone()
        if not row or not row[0]:
            return False
        return row[0] > now


# создаём запрос на обмен позициями
async def create_swap_request(queue_id: int, from_user: int, to_user: int) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            UPDATE swap_requests SET status='cancelled'
            WHERE queue_id=? AND from_user=? AND to_user=? AND status='pending'
        """, (queue_id, from_user, to_user))
        cur = await db.execute(
            "INSERT INTO swap_requests (queue_id, from_user, to_user) VALUES (?,?,?)",
            (queue_id, from_user, to_user))
        await db.commit()
        return cur.lastrowid


async def get_swap_request(request_id: int) -> Optional[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM swap_requests WHERE id=?", (request_id,))
        row = await cur.fetchone()
        return dict(row) if row else None


# меняем позиции двух участников местами
async def execute_swap(queue_id: int, user_a: int, user_b: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT position FROM queue_members WHERE queue_id=? AND user_id=?",
            (queue_id, user_a))
        row_a = await cur.fetchone()
        cur = await db.execute(
            "SELECT position FROM queue_members WHERE queue_id=? AND user_id=?",
            (queue_id, user_b))
        row_b = await cur.fetchone()
        if not row_a or not row_b:
            return False
        pos_a, pos_b = row_a[0], row_b[0]
        # через 9999 чтобы не словить ошибку уникальности
        await db.execute(
            "UPDATE queue_members SET position=9999 WHERE queue_id=? AND user_id=?",
            (queue_id, user_a))
        await db.execute(
            "UPDATE queue_members SET position=? WHERE queue_id=? AND user_id=?",
            (pos_a, queue_id, user_b))
        await db.execute(
            "UPDATE queue_members SET position=? WHERE queue_id=? AND user_id=?",
            (pos_b, queue_id, user_a))
        await db.execute(
            "UPDATE swap_requests SET status='done' WHERE queue_id=? AND from_user=? AND to_user=? AND status='pending'",
            (queue_id, user_a, user_b))
        await db.commit()
        return True


async def decline_swap(request_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE swap_requests SET status='declined' WHERE id=?", (request_id,))
        await db.commit()


# бот-админы — назначаются владельцем бота для конкретной группы
async def add_bot_admin(user_id: int, chat_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS bot_admins (
                user_id INTEGER NOT NULL,
                chat_id INTEGER NOT NULL,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (user_id, chat_id)
            )
        """)
        try:
            await db.execute(
                "INSERT INTO bot_admins (user_id, chat_id) VALUES (?, ?)",
                (user_id, chat_id))
            await db.commit()
            return True
        except aiosqlite.IntegrityError:
            return False


async def remove_bot_admin(user_id: int, chat_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "DELETE FROM bot_admins WHERE user_id = ? AND chat_id = ?",
            (user_id, chat_id))
        await db.commit()


async def is_bot_admin(user_id: int, chat_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS bot_admins (
                user_id INTEGER NOT NULL,
                chat_id INTEGER NOT NULL,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (user_id, chat_id)
            )
        """)
        cur = await db.execute(
            "SELECT 1 FROM bot_admins WHERE user_id = ? AND chat_id = ?",
            (user_id, chat_id))
        return bool(await cur.fetchone())


async def get_bot_admins(chat_id: int) -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        await db.execute("""
            CREATE TABLE IF NOT EXISTS bot_admins (
                user_id INTEGER NOT NULL,
                chat_id INTEGER NOT NULL,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (user_id, chat_id)
            )
        """)
        cur = await db.execute("""
            SELECT b.user_id, b.chat_id, b.added_at, p.full_name, p.username
            FROM bot_admins b
            LEFT JOIN user_profiles p ON p.user_id = b.user_id
            WHERE b.chat_id = ?
        """, (chat_id,))
        return [dict(r) for r in await cur.fetchall()]


async def get_all_users() -> list[int]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT user_id FROM user_profiles WHERE dm_available = 1")
        return [r[0] for r in await cur.fetchall()]


async def get_user_known_chats(user_id: int) -> list[int]:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS user_chats (
                user_id INTEGER NOT NULL,
                chat_id INTEGER NOT NULL,
                PRIMARY KEY (user_id, chat_id)
            )
        """)
        cur = await db.execute(
            "SELECT chat_id FROM user_chats WHERE user_id = ?", (user_id,))
        return [r[0] for r in await cur.fetchall()]
    
async def register_user_chat(user_id: int, chat_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS user_chats (
                user_id INTEGER NOT NULL,
                chat_id INTEGER NOT NULL,
                PRIMARY KEY (user_id, chat_id)
            )
        """)
        try:
            await db.execute(
                "INSERT INTO user_chats (user_id, chat_id) VALUES (?, ?)",
                (user_id, chat_id))
            await db.commit()
        except Exception:
            pass    
