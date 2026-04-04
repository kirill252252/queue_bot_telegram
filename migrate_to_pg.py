"""
Migration script: SQLite → PostgreSQL
Run once: python migrate_to_pg.py
"""
import asyncio
import aiosqlite
import asyncpg
from config import POSTGRES_DSN

SQLITE_PATH = "queue_bot.db"


async def migrate():
    print("🔄 Connecting to PostgreSQL...")
    pg = await asyncpg.connect(POSTGRES_DSN)
    print("✅ Connected")

    async with aiosqlite.connect(SQLITE_PATH) as sq:
        sq.row_factory = aiosqlite.Row

        tables = ["known_chats", "user_profiles", "group_nicks",
                  "queues", "queue_members", "reminder_tasks", "queue_subscriptions"]

        for table in tables:
            cur = await sq.execute(f"SELECT * FROM {table}")
            rows = await cur.fetchall()
            if not rows:
                print(f"  ⏭ {table}: empty")
                continue

            cols = [d[0] for d in cur.description]
            placeholders = ", ".join(f"${i+1}" for i in range(len(cols)))
            col_names = ", ".join(cols)

            count = 0
            for row in rows:
                try:
                    await pg.execute(
                        f"INSERT INTO {table} ({col_names}) VALUES ({placeholders}) ON CONFLICT DO NOTHING",
                        *[row[c] for c in cols]
                    )
                    count += 1
                except Exception as e:
                    print(f"    ⚠️ Row error in {table}: {e}")
            print(f"  ✅ {table}: {count}/{len(rows)} rows migrated")

    await pg.close()
    print("\n✅ Migration complete!")


if __name__ == "__main__":
    asyncio.run(migrate())
