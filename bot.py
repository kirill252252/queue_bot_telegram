import asyncio
import logging
import sys

from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage

from config import (BOT_TOKEN, BOT_MODE, WEBHOOK_HOST, WEBHOOK_PATH, WEBHOOK_PORT,
                    WEB_PANEL_ENABLED, WEB_PANEL_PORT, DB_TYPE)
from handlers import router
from notifications import process_due_reminders
from schedule_handlers import sched_router
from schedule_manager import process_schedule_tick
import schedule_db as sdb
from schedule_monitor import schedule_loop
from source_monitor import source_monitor_loop

import database


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)


# бесконечный цикл — каждую минуту проверяем напоминания
async def reminder_loop(bot: Bot):
    while True:
        try:
            await process_due_reminders(bot)
            await process_schedule_tick(bot)
        except Exception as e:
            logger.error(f"Reminder loop error: {e}")
        await asyncio.sleep(60)


# запускаем веб-панель если включена в конфиге
async def start_web_panel():
    if not WEB_PANEL_ENABLED:
        return
    try:
        import uvicorn
        from webpanel import app as panel_app
        config = uvicorn.Config(panel_app, host="0.0.0.0", port=WEB_PANEL_PORT,
                                log_level="warning")
        server = uvicorn.Server(config)
        asyncio.create_task(server.serve())
        logger.info(f"Web panel started on port {WEB_PANEL_PORT}")
    except ImportError:
        logger.warning("uvicorn/fastapi not installed — web panel disabled")


# точка входа
async def main():
    if not BOT_TOKEN or BOT_TOKEN == "YOUR_BOT_TOKEN_HERE":
        logger.error("BOT_TOKEN не задан!")
        sys.exit(1)

    if DB_TYPE == "postgres":
        import database_pg as db_module
        for attr in dir(db_module):
            if not attr.startswith("_"):
                setattr(database, attr, getattr(db_module, attr))
        logger.info("Using PostgreSQL database")
    else:
        logger.info("Using SQLite database")

    await database.init_db()
    await sdb.init_schedule_db()

    logger.info(f"Bot started (mode={BOT_MODE}, db={DB_TYPE})")

    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher(storage=MemoryStorage())

    dp.include_router(router)
    dp.include_router(sched_router)

    asyncio.create_task(reminder_loop(bot))
    asyncio.create_task(schedule_loop(bot))
    asyncio.create_task(source_monitor_loop(bot))
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
        await dp.start_polling(bot, allowed_updates=[
            "message", "callback_query", "my_chat_member", "chat_member"
        ])


if __name__ == "__main__":
    asyncio.run(main())