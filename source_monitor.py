"""
Мониторинг источников изменений расписания — Telegram и ВКонтакте.

Запускается каждые N минут (по умолчанию 360 = 6 часов).

Баги исправлены:
  - Telegram: zip(post_ids, posts) падал если количество не совпадало (посты без текста)
  - VK: group_map не использовался — изменения применялись ко ВСЕМ группам чата
  - VK: "лента нет" в тексте вызывала ложные срабатывания (расширены ключевые слова)
  - Оба: applied дублировал записи (цикл по группам был внутри applied.append)
"""
import asyncio
import logging
import aiohttp
import os
import re

from aiogram import Bot

import schedule_db as sdb
from schedule_parser import parse_schedule_change

logger = logging.getLogger(__name__)

# Ключевые слова для фильтрации постов об изменениях расписания
_CHANGE_KEYWORDS = [
    "расписание", "изменение", "отмена", "перенос",
    "замена", "замен", "переносит", "отменяет",
]

# HTTP-сессия переиспользуется между вызовами для экономии ресурсов
_http_session: aiohttp.ClientSession | None = None


async def get_session() -> aiohttp.ClientSession:
    """Возвращает переиспользуемую HTTP-сессию."""
    global _http_session
    if _http_session is None or _http_session.closed:
        _http_session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30)
        )
    return _http_session


# ─────────────────────────────────────────────
# TELEGRAM
# ─────────────────────────────────────────────

async def check_telegram_source(bot: Bot, source: dict, chat_id: int):
    """
    Проверяем t.me/s/{username} на новые посты.

    BUG FIX: zip(post_ids, posts) давал неверный результат если количество
    post_ids и posts не совпадало (посты без текста не попадали в regex).
    Теперь парсим пост целиком и ищем текст внутри него.
    """
    source_channel = source["source_id"]
    last_post_id   = source.get("last_post_id")

    try:
        username = source_channel.lstrip("@")
        session  = await get_session()

        async with session.get(
            f"https://t.me/s/{username}",
            headers={"User-Agent": "Mozilla/5.0 (compatible; ScheduleBot/1.0)"}
        ) as resp:
            if resp.status != 200:
                logger.warning(f"TG monitor: {username} returned HTTP {resp.status}")
                return None
            html = await resp.text()

        # Парсим каждый пост как единый блок (id + текст вместе)
        # Ищем блоки постов целиком
        post_blocks = re.findall(
            r'data-post="[^/]+/(\d+)".*?'
            r'class="tgme_widget_message_wrap[^"]*"[^>]*>(.*?)'
            r'</div>\s*</div>\s*</div>',
            html, re.DOTALL
        )
        if not post_blocks:
            # Fallback: старый метод
            post_ids = re.findall(r'data-post="[^/]+/(\d+)"', html)
            if not post_ids:
                return None
            new_last_id = post_ids[-1]
            # Ничего нового — обновляем checkpoint и выходим
            if last_post_id and new_last_id == last_post_id:
                return None
            return new_last_id

        # Сортируем по id (старые → новые)
        post_blocks.sort(key=lambda x: int(x[0]))
        new_last_id = post_blocks[-1][0]

        if last_post_id and new_last_id == last_post_id:
            return None  # Ничего нового

        groups = await sdb.get_chat_groups(chat_id)
        if not groups:
            return new_last_id

        group_map = {g["group_name"].lower(): g for g in groups}

        for pid, block_html in post_blocks:
            if last_post_id and int(pid) <= int(last_post_id):
                continue  # Уже обработано

            # Извлекаем текст из блока
            text = re.sub(r"<[^>]+>", "", block_html).strip()
            text = re.sub(r"\s+", " ", text).strip()
            if not text:
                continue

            if not any(k in text.lower() for k in _CHANGE_KEYWORDS):
                continue

            result = await parse_schedule_change(text)
            if not result or not result.get("changes"):
                continue

            fallback_date = result.get("date")
            applied = []

            for change in result["changes"]:
                # Определяем целевые группы: по названию или все группы чата
                gname   = (change.get("group") or "").strip().lower()
                targets = [group_map[gname]] if gname and gname in group_map else groups

                for g in targets:
                    await sdb.save_override(g["id"], change, fallback_date=fallback_date)

                action  = change.get("action") or change.get("type") or "изменение"
                subject = change.get("subject") or "?"
                grp_lbl = change.get("group") or "все группы"
                applied.append(f"<b>{grp_lbl}</b>: {action} — {subject}")

            if applied:
                date_line = f"📅 Дата: {fallback_date}\n" if fallback_date else ""
                await bot.send_message(
                    chat_id,
                    f"📢 <b>Изменение расписания (Telegram)</b>\n{date_line}\n"
                    + "\n".join(f"• {a}" for a in applied),
                    parse_mode="HTML",
                )

        return new_last_id

    except Exception as e:
        logger.error(f"Telegram monitor error for {source_channel}: {e}")
        return None


# ─────────────────────────────────────────────
# VK
# ─────────────────────────────────────────────

async def check_vk_source(bot: Bot, source: dict, chat_id: int):
    """
    Проверяем стену ВКонтакте на новые посты с изменениями расписания.

    BUG FIX 1: group_map не использовался в VK — изменения применялись ко ВСЕМ
    группам чата вместо нужной. Исправлено: сопоставляем по change["group"].
    BUG FIX 2: applied.append был вне цикла по группам → дублирование записей.
    """
    vk_token = os.getenv("VK_TOKEN")
    if not vk_token:
        return None

    source_id    = source["source_id"]
    last_post_id = source.get("last_post_id")

    try:
        session = await get_session()

        async with session.get(
            "https://api.vk.com/method/wall.get",
            params={
                "domain":       source_id,
                "count":        20,          # Берём больше постов
                "access_token": vk_token,
                "v":            "5.131",
            },
        ) as resp:
            data = await resp.json()

        # Проверяем ошибки VK API
        if "error" in data:
            err = data["error"]
            logger.error(f"VK API error: {err.get('error_code')} — {err.get('error_msg')}")
            return None

        items = data.get("response", {}).get("items", [])
        if not items:
            return None

        latest_id = str(items[0]["id"])

        if last_post_id and latest_id == last_post_id:
            return None  # Ничего нового

        groups = await sdb.get_chat_groups(chat_id)
        if not groups:
            return latest_id

        group_map = {g["group_name"].lower(): g for g in groups}

        # Обрабатываем от старых к новым
        items_sorted = sorted(items, key=lambda x: x["id"])

        for item in items_sorted:
            pid = str(item["id"])

            if last_post_id and int(pid) <= int(last_post_id):
                continue

            text = item.get("text", "").strip()

            # Расширенный список ключевых слов
            if not any(k in text.lower() for k in _CHANGE_KEYWORDS + ["лент", "пар "]):
                continue

            # Скачиваем первую фотографию если есть
            image_bytes = None
            for att in item.get("attachments", []):
                if att.get("type") == "photo":
                    try:
                        sizes = att["photo"].get("sizes", [])
                        if sizes:
                            img_url = sizes[-1]["url"]
                            async with session.get(img_url) as r:
                                image_bytes = await r.read()
                    except Exception as e:
                        logger.warning(f"VK photo download error: {e}")
                    break  # Берём только первое фото

            result = await parse_schedule_change(text, image_bytes)
            if not result or not result.get("changes"):
                continue

            fallback_date = result.get("date")
            applied = []

            for change in result["changes"]:
                # BUG FIX: определяем целевую группу, а не применяем ко всем
                gname   = (change.get("group") or "").strip().lower()
                targets = [group_map[gname]] if gname and gname in group_map else groups

                for g in targets:
                    await sdb.save_override(g["id"], change, fallback_date=fallback_date)

                action  = change.get("action") or change.get("type") or "изменение"
                subject = change.get("subject") or "?"
                grp_lbl = change.get("group") or "все группы"
                applied.append(f"<b>{grp_lbl}</b>: {action} — {subject}")

            if applied:
                date_line = f"📅 Дата: {fallback_date}\n" if fallback_date else ""
                await bot.send_message(
                    chat_id,
                    f"📢 <b>Изменение расписания (ВКонтакте)</b>\n"
                    f"Источник: {source_id}\n{date_line}\n"
                    + "\n".join(f"• {a}" for a in applied),
                    parse_mode="HTML",
                )

        return latest_id

    except Exception as e:
        logger.error(f"VK monitor error for {source_id}: {e}")
        return None


async def close_session():
    """Закрываем HTTP-сессию при завершении работы бота."""
    global _http_session
    if _http_session and not _http_session.closed:
        await _http_session.close()
        _http_session = None
        logger.info("Source monitor session closed")


# ─────────────────────────────────────────────
# LOOP
# ─────────────────────────────────────────────

async def source_monitor_loop(bot: Bot, interval_minutes: int = 360):
    """
    Основной цикл мониторинга.
    По умолчанию запускается каждые 6 часов (360 минут) —
    расписание меняется редко, частые запросы не нужны.
    """
    while True:
        try:
            sources = await sdb.get_all_sources()
            for source in sources:
                stype = source.get("source_type", "")
                try:
                    if stype == "telegram":
                        new_id = await check_telegram_source(bot, source, source["chat_id"])
                    elif stype == "vk":
                        new_id = await check_vk_source(bot, source, source["chat_id"])
                    else:
                        logger.warning(f"Unknown source type: {stype!r}")
                        continue

                    if new_id:
                        await sdb.update_source_checkpoint(source["id"], new_id)
                except Exception as e:
                    logger.error(f"Error processing source {source.get('id')}: {e}")

        except Exception as e:
            logger.error(f"Monitor loop error: {e}")

        await asyncio.sleep(interval_minutes * 60)
