"""
Migration script: SQLite -> PostgreSQL.
Run once: python migrate_to_pg.py
"""
import asyncio
from datetime import datetime
import os

import aiosqlite
import asyncpg

import database_pg
import schedule_db
from config import POSTGRES_DSN

SQLITE_PATH = os.getenv("SQLITE_PATH", "queue_bot.db")
TIMESTAMP_COLUMNS = {"created_at", "joined_at", "fire_at", "added_at", "frozen_until"}
TABLE_MAPPINGS = [
    ("known_chats", "known_chats", False),
    ("user_profiles", "user_profiles", False),
    ("user_group_nicks", "group_nicks", False),
    ("queues", "queues", True),
    ("queue_members", "queue_members", True),
    ("reminder_tasks", "reminder_tasks", True),
    ("queue_subscriptions", "queue_subscriptions", True),
    ("queue_invites", "queue_invites", False),
    ("swap_requests", "swap_requests", True),
    ("bot_admins", "bot_admins", False),
    ("user_chats", "user_chats", False),
    ("schedule_groups", "schedule_groups", True),
    ("schedule_lessons", "schedule_lessons", True),
    ("schedule_overrides", "schedule_overrides", True),
    ("schedule_events", "schedule_events", True),
    ("schedule_sources", "schedule_sources", True),
]


def q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def normalize_value(column: str, value):
    if value in (None, ""):
        return value
    if column in TIMESTAMP_COLUMNS and isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            try:
                return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                return value
    return value


async def sqlite_table_exists(conn: aiosqlite.Connection, table_name: str) -> bool:
    cur = await conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    )
    return await cur.fetchone() is not None


async def fetch_rows(conn: aiosqlite.Connection, table_name: str):
    cur = await conn.execute(f"SELECT * FROM {q(table_name)}")
    rows = await cur.fetchall()
    cols = [item[0] for item in cur.description]
    return rows, cols


async def reset_sequence(conn: asyncpg.Connection, table_name: str):
    max_id = await conn.fetchval(f"SELECT COALESCE(MAX(id), 1) FROM {q(table_name)}")
    await conn.execute(
        "SELECT setval(pg_get_serial_sequence($1, 'id'), $2, true)",
        table_name,
        max_id,
    )


async def migrate_table(sq: aiosqlite.Connection, pg: asyncpg.Connection,
                        source_table: str, target_table: str,
                        reset_serial: bool):
    if not await sqlite_table_exists(sq, source_table):
        print(f"- skip {source_table}: table does not exist")
        return

    rows, cols = await fetch_rows(sq, source_table)
    if not rows:
        print(f"- skip {source_table}: empty")
        return

    quoted_cols = ", ".join(q(col) for col in cols)
    placeholders = ", ".join(f"${i}" for i in range(1, len(cols) + 1))
    sql = (
        f"INSERT INTO {q(target_table)} ({quoted_cols}) "
        f"VALUES ({placeholders}) ON CONFLICT DO NOTHING"
    )

    migrated = 0
    for row in rows:
        payload = [normalize_value(col, row[col]) for col in cols]
        try:
            await pg.execute(sql, *payload)
            migrated += 1
        except Exception as exc:
            print(f"! row error in {source_table}: {exc}")

    if reset_serial and "id" in cols:
        await reset_sequence(pg, target_table)

    print(f"+ {source_table} -> {target_table}: {migrated}/{len(rows)} rows")


async def migrate():
    if not POSTGRES_DSN:
        raise RuntimeError("POSTGRES_DSN is not configured")

    print("Preparing PostgreSQL schema...")
    await database_pg.init_db()
    await schedule_db._init_pg()

    print(f"Reading SQLite database: {SQLITE_PATH}")
    pg = await asyncpg.connect(POSTGRES_DSN)

    try:
        async with aiosqlite.connect(SQLITE_PATH) as sq:
            sq.row_factory = aiosqlite.Row

            for source_table, target_table, reset_serial in TABLE_MAPPINGS:
                await migrate_table(sq, pg, source_table, target_table, reset_serial)
    finally:
        await pg.close()

    print("Migration complete")


if __name__ == "__main__":
    asyncio.run(migrate())
