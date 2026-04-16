"""
Мониторинг внешних источников расписания.
Поддерживает Telegram каналы/группы и ВКонтакте.
"""
import asyncio
import logging
import aiohttp
from datetime import datetime
from typing import Optional

from aiogram import Bot

import schedule_db as sdb
from schedule_parser import parse_schedule_change

logger = logging.getLogger(__name__)


# ─── Telegram channel monitoring ─────────────────────────────────────────────

async def check_telegram_source(bot: Bot, source: dict, chat_id: int) -> Optional[str]:
    """
    Читает последние посты из Telegram канала/группы.
    source_id — username канала (@mychannel) или chat_id.
    Возвращает last_post_id если нашли что-то новое.
    """
    source_channel = source["source_id"]
    last_post_id = source.get("last_post_id")

    try:
        # Используем getUpdates не подходит для чужих каналов
        # Используем getChatHistory через MTProto — здесь через forwardMessages trick
        # Простой способ: бот должен быть подписчиком канала и получать updates
        # Для публичных каналов используем web scraping через t.me/s/channel
        
        if source_channel.startswith("@"):
            username = source_channel[1:]
        else:
            username = source_channel

        url = f"https://t.me/s/{username}"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers={
                "User-Agent": "Mozilla/5.0"
            }) as resp:
                if resp.status != 200:
                    return None
                html = await resp.text()

        # Парсим посты из HTML
        import re
        posts = re.findall(
            r'<div class="tgme_widget_message_text[^"]*"[^>]*>(.*?)</div>',
            html, re.DOTALL
        )
        
        # Получаем ID последнего поста
        post_ids = re.findall(r'data-post="[^/]+/(\d+)"', html)
        
        if not posts or not post_ids:
            return None

        latest_id = post_ids[-1]
        if latest_id == last_post_id:
            return None  # ничего нового

        # Берём новые посты
        new_posts = []
        for i, (pid, post) in enumerate(zip(post_ids, posts)):
            if last_post_id and pid <= last_post_id:
                continue
            # Убираем HTML теги
            clean = re.sub(r'<[^>]+>', '', post).strip()
            if clean:
                new_posts.append(clean)

        if not new_posts:
            return latest_id

        # Анализируем каждый пост на изменения расписания
        groups = await sdb.get_chat_groups(chat_id)
        if not groups:
            return latest_id

        for post_text in new_posts:
            keywords = ["расписание", "пара", "занятие", "отмена", "перенос", 
                       "замена", "расп", "лекция", "семинар"]
            if not any(kw in post_text.lower() for kw in keywords):
                continue

            result = await parse_schedule_change(post_text)
            if not result or not result.get("changes"):
                continue

            group_map = {g["group_name"].lower(): g for g in groups}
            applied = []
            for change in result["changes"]:
                gname = change.get("group")
                targets = [group_map.get(gname.lower())] if gname else groups
                for g in [x for x in targets if x]:
                    await sdb.save_override(g["id"], change)
                    applied.append(f"{change['type']}: {change.get('subject','?')}")

            if applied:
                try:
                    await bot.send_message(
                        chat_id,
                        f"📢 <b>Обнаружено изменение расписания</b> из {source_channel}:\n\n"
                        + "\n".join(f"• {a}" for a in applied),
                        parse_mode="HTML"
                    )
                except Exception as e:
                    logger.warning(f"Cannot notify chat {chat_id}: {e}")

        return latest_id

    except Exception as e:
        logger.error(f"check_telegram_source error: {e}")
        return None


# ─── VKontakte monitoring ─────────────────────────────────────────────────────

async def check_vk_source(bot: Bot, source: dict, chat_id: int) -> Optional[str]:
    """
    Читает последние посты из группы/паблика ВКонтакте.
    source_id — short_name группы или -group_id.
    Требует VK_TOKEN в переменных окружения.
    """
    import os
    vk_token = os.getenv("VK_TOKEN")
    if not vk_token:
        logger.warning("VK_TOKEN not set, skipping VK source")
        return None

    source_id = source["source_id"]
    last_post_id = source.get("last_post_id")

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                "https://api.vk.com/method/wall.get",
                params={
                    "domain": source_id,
                    "count": 10,
                    "access_token": vk_token,
                    "v": "5.131"
                }
            ) as resp:
                data = await resp.json()

        if "error" in data:
            logger.error(f"VK API error: {data['error']}")
            return None

        items = data.get("response", {}).get("items", [])
        if not items:
            return None

        latest_id = str(items[0]["id"])
        if latest_id == last_post_id:
            return None

        groups = await sdb.get_chat_groups(chat_id)
        if not groups:
            return latest_id

        new_posts = []
        for item in items:
            if last_post_id and str(item["id"]) <= last_post_id:
                break
            text = item.get("text", "").strip()
            # Проверяем прикреплённые фото
            photos = []
            for attach in item.get("attachments", []):
                if attach["type"] == "photo":
                    sizes = attach["photo"]["sizes"]
                    best = max(sizes, key=lambda s: s.get("width", 0))
                    photos.append(best["url"])
            if text or photos:
                new_posts.append({"text": text, "photos": photos})

        for post in new_posts:
            text = post["text"]
            keywords = ["расписание", "пара", "занятие", "отмена", "перенос",
                       "замена", "расп", "лекция", "семинар"]
            if not any(kw in text.lower() for kw in keywords):
                continue

            image_bytes = None
            if post["photos"]:
                try:
                    async with aiohttp.ClientSession() as session:
                        async with session.get(post["photos"][0]) as r:
                            image_bytes = await r.read()
                except Exception:
                    pass

            result = await parse_schedule_change(text, image_bytes)
            if not result or not result.get("changes"):
                continue

            group_map = {g["group_name"].lower(): g for g in groups}
            applied = []
            for change in result["changes"]:
                gname = change.get("group")
                targets = [group_map.get(gname.lower())] if gname else groups
                for g in [x for x in targets if x]:
                    await sdb.save_override(g["id"], change)
                    applied.append(f"{change['type']}: {change.get('subject','?')}")

            if applied:
                try:
                    await bot.send_message(
                        chat_id,
                        f"📢 <b>Изменение расписания</b> из ВКонтакте ({source_id}):\n\n"
                        + "\n".join(f"• {a}" for a in applied),
                        parse_mode="HTML"
                    )
                except Exception as e:
                    logger.warning(f"Cannot notify chat {chat_id}: {e}")

        return latest_id

    except Exception as e:
        logger.error(f"check_vk_source error: {e}")
        return None


# ─── Main monitoring loop ─────────────────────────────────────────────────────

async def source_monitor_loop(bot: Bot, interval_minutes: int = 15):
    """Проверяем все источники каждые N минут."""
    while True:
        try:
            sources = await sdb.get_all_sources()
            for source in sources:
                chat_id = source["chat_id"]
                stype = source["source_type"]
                new_post_id = None

                if stype == "telegram":
                    new_post_id = await check_telegram_source(bot, source, chat_id)
                elif stype == "vk":
                    new_post_id = await check_vk_source(bot, source, chat_id)

                if new_post_id:
                    await sdb.update_source_checkpoint(source["id"], new_post_id)

        except Exception as e:
            logger.error(f"source_monitor_loop error: {e}")

        await asyncio.sleep(interval_minutes * 60)
