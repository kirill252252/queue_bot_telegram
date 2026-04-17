"""
Распознавание расписания с изображений через Google Gemini API.
Поддерживает фото и скриншоты расписания.
Единый модуль для всех OCR-задач (используется schedule_handlers.py и source_monitor.py).
"""

import base64
import json
import logging
import re
import os
from typing import Optional

import aiohttp

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# Google Gemini config
# ─────────────────────────────────────────────

GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")

if not GOOGLE_API_KEY:
    logger.warning("GOOGLE_API_KEY is not set — OCR disabled")

GEMINI_MODEL = "gemini-2.0-flash"

GEMINI_URL = (
    f"https://generativelanguage.googleapis.com/v1beta/models/"
    f"{GEMINI_MODEL}:generateContent?key={GOOGLE_API_KEY}"
)

# ─────────────────────────────────────────────
# PROMPTS
# ─────────────────────────────────────────────

SCHEDULE_PROMPT = """
Ты — парсер расписания учебных занятий.

Верни ТОЛЬКО JSON, без пояснений и markdown:

{
  "group_name": "string или null",
  "lessons": [
    {
      "weekday": 1,
      "lesson_num": 1,
      "subject": "string",
      "teacher": "string или null",
      "room": "string или null",
      "time_start": "HH:MM",
      "time_end": "HH:MM"
    }
  ]
}

weekday: 1=Пн, 2=Вт, 3=Ср, 4=Чт, 5=Пт, 6=Сб, 7=Вс.
Если не удаётся распознать — верни: {"error": "не удалось распознать"}
"""

CHANGE_PROMPT = """
Ты — парсер изменений расписания учебных занятий.

Проанализируй текст или изображение и верни ТОЛЬКО JSON, без пояснений и markdown:

{
  "changes": [
    {
      "action": "cancel" | "reschedule" | "add" | "room_change" | "teacher_change",
      "group": "название группы или null",
      "lesson_num": 1,
      "weekday": 1,
      "date": "YYYY-MM-DD или null",
      "subject": "предмет или null",
      "time_start": "HH:MM или null",
      "time_end": "HH:MM или null",
      "room": "аудитория или null",
      "teacher": "преподаватель или null",
      "comment": "комментарий или null"
    }
  ]
}

action: cancel=отмена, reschedule=перенос, add=добавление, room_change=смена аудитории, teacher_change=замена преподавателя.
weekday: 1=Пн ... 7=Вс.
Если изменений не найдено — верни: {"changes": []}
"""


# ─────────────────────────────────────────────
# CORE — Gemini REST call
# ─────────────────────────────────────────────

def _image_to_base64(image_bytes: bytes) -> str:
    return base64.b64encode(image_bytes).decode()


def _extract_json(text: str) -> Optional[dict]:
    """Извлекаем JSON из ответа модели, убирая markdown-обёртку."""
    # Убираем ```json ... ``` или просто ``` ... ```
    cleaned = re.sub(r"```(?:json)?\s*", "", text).strip().rstrip("`").strip()

    # Пробуем распарсить целиком
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        pass

    # Ищем первый JSON-объект
    match = re.search(r"\{.*\}", cleaned, re.DOTALL)
    if match:
        try:
            return json.loads(match.group())
        except json.JSONDecodeError:
            pass

    return None


async def _call_gemini(parts: list[dict], max_tokens: int = 2048) -> Optional[dict]:
    """
    parts — список объектов для Gemini API:
      {"text": "..."}
      {"inline_data": {"mime_type": "image/jpeg", "data": "base64..."}}
    """
    if not GOOGLE_API_KEY:
        logger.error("GOOGLE_API_KEY is not set — cannot call Gemini")
        return None

    body = {
        "contents": [{"parts": parts}],
        "generationConfig": {
            "temperature": 0.1,
            "maxOutputTokens": max_tokens,
        },
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                GEMINI_URL,
                json=body,
                headers={"Content-Type": "application/json"},
            ) as resp:

                if resp.status != 200:
                    error_text = await resp.text()
                    logger.error(f"Gemini API error {resp.status}: {error_text}")
                    return None

                data = await resp.json()

                # Извлекаем текст ответа
                try:
                    content = data["candidates"][0]["content"]["parts"][0]["text"]
                except (KeyError, IndexError) as e:
                    logger.error(f"Unexpected Gemini response structure: {e}\n{data}")
                    return None

                return _extract_json(content)

    except aiohttp.ClientError as e:
        logger.error(f"Gemini network error: {e}")
        return None
    except Exception as e:
        logger.error(f"Gemini call exception: {e}")
        return None


# ─────────────────────────────────────────────
# PARSE SCHEDULE IMAGE
# ─────────────────────────────────────────────

async def parse_schedule_image(
    image_bytes: bytes, media_type: str = "image/jpeg"
) -> Optional[dict]:
    """
    Распознаём базовое расписание с фотографии.
    Возвращает: {"group_name": ..., "lessons": [...]}
    """
    b64 = _image_to_base64(image_bytes)

    parts = [
        {"text": SCHEDULE_PROMPT},
        {"inline_data": {"mime_type": media_type, "data": b64}},
    ]

    result = await _call_gemini(parts, max_tokens=4096)

    if not result or not isinstance(result, dict):
        return None

    if "error" in result:
        logger.warning(f"Gemini schedule parse error: {result['error']}")
        return None

    # Фильтруем невалидные занятия
    required_keys = ("weekday", "lesson_num", "subject", "time_start", "time_end")
    lessons = [
        l for l in result.get("lessons", [])
        if all(k in l for k in required_keys)
    ]
    result["lessons"] = lessons
    return result


# ─────────────────────────────────────────────
# PARSE CHANGE IMAGE
# ─────────────────────────────────────────────

async def parse_change_image(
    image_bytes: bytes, media_type: str = "image/jpeg"
) -> Optional[dict]:
    """
    Распознаём изменения расписания с фотографии.
    Возвращает: {"changes": [...]}
    """
    b64 = _image_to_base64(image_bytes)

    parts = [
        {"text": CHANGE_PROMPT},
        {"inline_data": {"mime_type": media_type, "data": b64}},
    ]

    return await _call_gemini(parts, max_tokens=2048)


# ─────────────────────────────────────────────
# PARSE CHANGE TEXT
# ─────────────────────────────────────────────

async def parse_change_text(text: str) -> Optional[dict]:
    """
    Распознаём изменения расписания из текстового сообщения.
    Возвращает: {"changes": [...]}
    """
    keywords = ["отмен", "перенос", "замен", "пар", "лекц", "семинар", "расписани"]
    if not any(k in text.lower() for k in keywords):
        return None

    parts = [{"text": f"{CHANGE_PROMPT}\n\nТекст сообщения:\n{text}"}]
    return await _call_gemini(parts, max_tokens=1024)


# ─────────────────────────────────────────────
# PARSE CHANGE (TEXT + OPTIONAL IMAGE) — для source_monitor
# ─────────────────────────────────────────────

async def parse_schedule_change(
    text: str = "",
    image_bytes: Optional[bytes] = None,
    media_type: str = "image/jpeg",
) -> Optional[dict]:
    """
    Универсальный парсер изменений — принимает текст и/или изображение.
    Используется source_monitor.py для разбора постов из VK и Telegram.
    Возвращает: {"changes": [...]}
    """
    parts = [{"text": CHANGE_PROMPT}]

    if image_bytes:
        b64 = _image_to_base64(image_bytes)
        parts.append({"inline_data": {"mime_type": media_type, "data": b64}})

    if text:
        parts.append({"text": f"\nТекст сообщения:\n{text}"})

    if len(parts) == 1:
        # Только промпт, нет данных
        return None

    return await _call_gemini(parts, max_tokens=2048)


# ─────────────────────────────────────────────
# FORMAT SCHEDULE — форматирование для отображения
# ─────────────────────────────────────────────

def format_schedule(lessons: list[dict]) -> str:
    days = {1: "Пн", 2: "Вт", 3: "Ср", 4: "Чт", 5: "Пт", 6: "Сб", 7: "Вс"}

    by_day: dict[int, list] = {}
    for l in lessons:
        by_day.setdefault(l["weekday"], []).append(l)

    lines = []
    for wd in sorted(by_day):
        lines.append(f"\n<b>📅 {days.get(wd, str(wd))}</b>")
        for l in sorted(by_day[wd], key=lambda x: x["lesson_num"]):
            teacher = f" — {l['teacher']}" if l.get("teacher") else ""
            room = f" [{l['room']}]" if l.get("room") else ""
            lines.append(
                f"{l['lesson_num']}. {l['time_start']}–{l['time_end']} "
                f"<b>{l['subject']}</b>{teacher}{room}"
            )

    return "\n".join(lines) if lines else "Расписание пусто"
