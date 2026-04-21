import asyncio
from contextlib import suppress
import logging
import sys

from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage

from config import (
    BOT_TOKEN,
    BOT_MODE,
    WEBHOOK_HOST,
    WEBHOOK_PATH,
    WEBHOOK_PORT,
    WEB_PANEL_ENABLED,
    WEB_PANEL_PORT,
    DB_TYPE,
    SOURCE_MONITOR_INTERVAL_MIN,
)

import db
import schedule_db as sdb
from handlers import router
from notifications import process_due_reminders
from schedule_handlers import sched_router
from schedule_manager import process_schedule_tick
from source_monitor import close_session as close_source_monitor_session
from source_monitor import source_monitor_loop

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

logger = logging.getLogger(__name__)


async def background_loop(bot: Bot):
    while True:
        try:
            await process_due_reminders(bot)
            await process_schedule_tick(bot)
        except Exception as e:
            logger.error(f"Background loop error: {e}")

        await asyncio.sleep(60)


async def start_web_panel():
    if not WEB_PANEL_ENABLED:
        return

    try:
        import uvicorn
        from webpanel import app as panel_app

        config = uvicorn.Config(
            panel_app,
            host="0.0.0.0",
            port=WEB_PANEL_PORT,
            log_level="warning",
        )

        server = uvicorn.Server(config)
        asyncio.create_task(server.serve())

        logger.info(f"Web panel started on port {WEB_PANEL_PORT}")

    except ImportError:
        logger.warning("Web panel disabled (uvicorn/fastapi missing)")


async def main():
    if not BOT_TOKEN or BOT_TOKEN == "YOUR_BOT_TOKEN_HERE":
        logger.error("BOT_TOKEN not set")
        sys.exit(1)

    logger.info("Using %s database", "PostgreSQL" if DB_TYPE == "postgres" else "SQLite")

    await db.init_db()
    await sdb.init_schedule_db()

    logger.info(f"Bot started (mode={BOT_MODE}, db={DB_TYPE})")

    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher(storage=MemoryStorage())
    dp["bot"] = bot

    dp.include_router(router)
    dp.include_router(sched_router)

    tasks = [
        asyncio.create_task(background_loop(bot), name="background_loop"),
        asyncio.create_task(
            source_monitor_loop(bot, interval_minutes=SOURCE_MONITOR_INTERVAL_MIN),
            name="source_monitor_loop",
        ),
    ]

    try:
        await start_web_panel()

        if BOT_MODE == "webhook" and WEBHOOK_HOST:
            from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
            from aiohttp import web

            webhook_url = f"{WEBHOOK_HOST}{WEBHOOK_PATH}"
            await bot.set_webhook(webhook_url)

            logger.info(f"Webhook: {webhook_url}")

            app = web.Application()

            SimpleRequestHandler(dispatcher=dp, bot=bot).register(app, path=WEBHOOK_PATH)
            setup_application(app, dp, bot=bot)

            runner = web.AppRunner(app)
            await runner.setup()

            site = web.TCPSite(runner, "0.0.0.0", WEBHOOK_PORT)
            await site.start()

            logger.info(f"Webhook server on :{WEBHOOK_PORT}")
            await asyncio.Event().wait()
        else:
            await bot.delete_webhook(drop_pending_updates=True)
            await dp.start_polling(
                bot,
                allowed_updates=[
                    "message",
                    "callback_query",
                    "my_chat_member",
                    "chat_member",
                ],
            )
    finally:
        for task in tasks:
            task.cancel()
        for task in tasks:
            with suppress(asyncio.CancelledError):
                await task
        await close_source_monitor_session()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
