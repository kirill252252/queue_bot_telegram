"""
Legacy compatibility wrapper for the schedule loop.
The actual scheduling logic lives in schedule_manager.py.
"""
import asyncio
import logging

from aiogram import Bot

from schedule_manager import process_schedule_tick

logger = logging.getLogger(__name__)


async def schedule_tick(bot: Bot):
    await process_schedule_tick(bot)


async def schedule_loop(bot: Bot):
    while True:
        try:
            await schedule_tick(bot)
        except Exception as e:
            logger.error(f"Schedule tick error: {e}")
        await asyncio.sleep(60)
