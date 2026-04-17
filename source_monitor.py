import asyncio
import logging
import aiohttp
import os
import re
from datetime import datetime

from aiogram import Bot

import schedule_db as sdb
from schedule_parser import parse_schedule_change

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# shared HTTP session (меньше токенов/нагрузки)
# ─────────────────────────────────────────────

_http_session: aiohttp.ClientSession | None = None


async def get_session():
    global _http_session
    if _http_session is None or _http_session.closed:
        _http_session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=20)
        )
    return _http_session


# ─────────────────────────────────────────────
# TELEGRAM
# ─────────────────────────────────────────────

async def check_telegram_source(bot: Bot, source: dict, chat_id: int):
    source_channel = source["source_id"]
    last_post_id = source.get("last_post_id")

    try:
        username = source_channel.lstrip("@")

        session = await get_session()
        url = f"https://t.me/s/{username}"

        async with session.get(url, headers={"User-Agent": "Mozilla/5.0"}) as resp:
            if resp.status != 200:
                return None
            html = await resp.text()

        post_ids = re.findall(r'data-post="[^/]+/(\d+)"', html)
        posts = re.findall(
            r'<div class="tgme_widget_message_text[^"]*"[^>]*>(.*?)</div>',
            html, re.DOTALL
        )

        if not post_ids:
            return None

        # нормальный порядок (старые -> новые)
        items = list(zip(post_ids, posts))
        items.reverse()

        new_last_id = post_ids[-1]

        if last_post_id and new_last_id == last_post_id:
            return None

        groups = await sdb.get_chat_groups(chat_id)
        if not groups:
            return new_last_id

        for pid, post in items:
            if last_post_id and int(pid) <= int(last_post_id):
                continue

            text = re.sub(r"<[^>]+>", "", post).strip()
            if not text:
                continue

            keywords = ["расписание", "пара", "отмена", "перенос", "замена"]
            if not any(k in text.lower() for k in keywords):
                continue

            result = await parse_schedule_change(text)
            if not result or not result.get("changes"):
                continue

            group_map = {g["group_name"].lower(): g for g in groups}

            applied = []
            for change in result["changes"]:
                gname = (change.get("group") or "").lower()
                targets = [group_map.get(gname)] if gname else groups

                for g in [x for x in targets if x]:
                    await sdb.save_override(g["id"], change)
                    # FIX: использовать 'action', а не 'type' (поле называется action)
                    action = change.get("action") or change.get("type") or "изменение"
                    subject = change.get("subject") or "?"
                    applied.append(f"{action}: {subject}")

            if applied:
                await bot.send_message(
                    chat_id,
                    "📢 <b>Изменение расписания (Telegram)</b>\n\n"
                    + "\n".join(f"• {a}" for a in applied),
                    parse_mode="HTML"
                )

        return new_last_id

    except Exception as e:
        logger.error(f"Telegram monitor error: {e}")
        return None


# ─────────────────────────────────────────────
# VK
# ─────────────────────────────────────────────

async def check_vk_source(bot: Bot, source: dict, chat_id: int):
    vk_token = os.getenv("VK_TOKEN")
    if not vk_token:
        return None

    source_id = source["source_id"]
    last_post_id = source.get("last_post_id")

    try:
        session = await get_session()

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

        items = data.get("response", {}).get("items", [])
        if not items:
            return None

        latest_id = str(items[0]["id"])

        if last_post_id and latest_id == last_post_id:
            return None

        groups = await sdb.get_chat_groups(chat_id)
        if not groups:
            return latest_id

        items.reverse()

        for item in items:
            pid = str(item["id"])

            if last_post_id and int(pid) <= int(last_post_id):
                continue

            text = item.get("text", "").strip()

            if not any(k in text.lower() for k in ["расп", "пара", "отмена", "замена"]):
                continue

            image_bytes = None
            attachments = item.get("attachments", [])

            if attachments:
                try:
                    # Ищем первую фотографию во вложениях
                    for att in attachments:
                        if att.get("type") == "photo":
                            sizes = att["photo"].get("sizes", [])
                            if sizes:
                                url = sizes[-1]["url"]
                                async with session.get(url) as r:
                                    image_bytes = await r.read()
                                break
                except Exception as e:
                    logger.warning(f"VK photo download error: {e}")

            result = await parse_schedule_change(text, image_bytes)

            if not result or not result.get("changes"):
                continue

            applied = []
            for change in result["changes"]:
                for g in groups:
                    await sdb.save_override(g["id"], change)
                action = change.get("action") or change.get("type") or "изменение"
                subject = change.get("subject") or "?"
                applied.append(f"{action}: {subject}")

            if applied:
                await bot.send_message(
                    chat_id,
                    "📢 <b>Изменение расписания (VK)</b>\n"
                    f"Источник: {source_id}\n\n"
                    + "\n".join(f"• {a}" for a in applied),
                    parse_mode="HTML"
                )

        return latest_id

    except Exception as e:
        logger.error(f"VK monitor error: {e}")
        return None


async def close_session():
    global _http_session

    if _http_session and not _http_session.closed:
        await _http_session.close()
        _http_session = None
        logger.info("Source monitor session closed")


# ─────────────────────────────────────────────
# LOOP
# ─────────────────────────────────────────────

async def source_monitor_loop(bot: Bot, interval_minutes: int = 15):
    while True:
        try:
            sources = await sdb.get_all_sources()

            for source in sources:
                stype = source["source_type"]

                if stype == "telegram":
                    new_id = await check_telegram_source(bot, source, source["chat_id"])
                elif stype == "vk":
                    new_id = await check_vk_source(bot, source, source["chat_id"])
                else:
                    logger.warning(f"Unknown source type: {stype}")
                    continue

                if new_id:
                    await sdb.update_source_checkpoint(source["id"], new_id)

        except Exception as e:
            logger.error(f"Monitor loop error: {e}")

        await asyncio.sleep(interval_minutes * 60)
