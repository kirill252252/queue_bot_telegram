"""
Распознавание расписания с изображений через OpenAI Vision API.
Поддерживает фото и скриншоты расписания.
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
# OpenAI config
# ─────────────────────────────────────────────

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

if not OPENAI_API_KEY:
    logger.warning("OPENAI_API_KEY is not set — OCR disabled")

OPENAI_URL = "https://api.openai.com/v1/chat/completions"

HEADERS = {
    "Authorization": f"Bearer {OPENAI_API_KEY}",
    "Content-Type": "application/json",
}

# ─────────────────────────────────────────────
# PROMPTS
# ─────────────────────────────────────────────

SCHEDULE_PROMPT = """
Ты — парсер расписания учебных занятий.

Верни ТОЛЬКО JSON:

{
  "group_name": "string|null",
  "lessons": [
    {
      "weekday": 1,
      "lesson_num": 1,
      "subject": "string",
      "teacher": "string|null",
      "room": "string|null",
      "time_start": "HH:MM",
      "time_end": "HH:MM"
    }
  ]
}

weekday: 1=Пн ... 7=Вс
"""

CHANGE_PROMPT = """
Ты — парсер изменений расписания.

Верни JSON:
{
  "changes": []
}
"""

# ─────────────────────────────────────────────
# utils
# ─────────────────────────────────────────────

async def image_to_base64(image_bytes: bytes) -> str:
    return base64.b64encode(image_bytes).decode()


# ─────────────────────────────────────────────
# OpenAI Vision call
# ─────────────────────────────────────────────

async def call_openai_vision(image_b64: str, prompt: str, media_type="image/jpeg"):
    body = {
        "model": "gpt-4o-mini",
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": prompt},
                    {
                        "type": "image_url",
                        "image_url": {
                            "url": f"data:{media_type};base64,{image_b64}"
                        }
                    }
                ]
            }
        ],
        "max_tokens": 2000
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(OPENAI_URL, json=body, headers=HEADERS) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    logger.error(f"OpenAI Vision error {resp.status}: {text}")
                    return None

                data = await resp.json()
                content = data["choices"][0]["message"]["content"]

                match = re.search(r"\{.*\}", content, re.DOTALL)
                if match:
                    return json.loads(match.group())

                return json.loads(content)

    except Exception as e:
        logger.error(f"OpenAI Vision exception: {e}")
        return None


# ─────────────────────────────────────────────
# parse schedule
# ─────────────────────────────────────────────

async def parse_schedule_image(image_bytes: bytes, media_type="image/jpeg") -> Optional[dict]:
    b64 = await image_to_base64(image_bytes)

    result = await call_openai_vision(b64, SCHEDULE_PROMPT, media_type)

    if not result or not isinstance(result, dict):
        return None

    lessons = result.get("lessons", [])
    valid = []

    for l in lessons:
        if all(k in l for k in ("weekday", "lesson_num", "subject", "time_start", "time_end")):
            valid.append(l)

    result["lessons"] = valid
    return result


# ─────────────────────────────────────────────
# parse changes image
# ─────────────────────────────────────────────

async def parse_change_image(image_bytes: bytes, media_type="image/jpeg") -> Optional[dict]:
    b64 = await image_to_base64(image_bytes)
    return await call_openai_vision(b64, CHANGE_PROMPT, media_type)


# ─────────────────────────────────────────────
# parse text changes
# ─────────────────────────────────────────────

async def parse_change_text(text: str) -> Optional[dict]:
    keywords = ["отмен", "перенос", "замен", "пар", "лекц", "семинар"]

    if not any(k in text.lower() for k in keywords):
        return None

    # текст тоже через vision endpoint (упрощённо через chat)
    body = {
        "model": "gpt-4o-mini",
        "messages": [
            {"role": "user", "content": f"{CHANGE_PROMPT}\n\n{text}"}
        ],
        "max_tokens": 800
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(OPENAI_URL, json=body, headers=HEADERS) as resp:
                if resp.status != 200:
                    return None

                data = await resp.json()
                content = data["choices"][0]["message"]["content"]

                match = re.search(r"\{.*\}", content, re.DOTALL)
                return json.loads(match.group() if match else content)

    except Exception as e:
        logger.error(f"OpenAI text error: {e}")
        return None


# ─────────────────────────────────────────────
# format schedule
# ─────────────────────────────────────────────

def format_schedule(lessons: list[dict]) -> str:
    days = {1:"Пн",2:"Вт",3:"Ср",4:"Чт",5:"Пт",6:"Сб",7:"Вс"}

    by_day = {}
    for l in lessons:
        by_day.setdefault(l["weekday"], []).append(l)

    lines = []

    for wd in sorted(by_day):
        lines.append(f"\n<b>📅 {days.get(wd, wd)}</b>")

        for l in sorted(by_day[wd], key=lambda x: x["lesson_num"]):
            teacher = f" — {l.get('teacher')}" if l.get("teacher") else ""
            room = f" [{l.get('room')}] " if l.get("room") else ""

            lines.append(
                f"{l['lesson_num']}. {l['time_start']}–{l['time_end']} "
                f"<b>{l['subject']}</b>{teacher}{room}"
            )

    return "\n".join(lines) if lines else "Расписание пусто"