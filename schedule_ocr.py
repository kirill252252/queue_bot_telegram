"""
Распознавание расписания с изображений через Claude Vision API.
Поддерживает фото и скриншоты расписания в любом формате.
"""
import base64
import json
import logging
import re
import os
from typing import Optional

import aiohttp

logger = logging.getLogger(__name__)

CLAUDE_API_URL = "https://api.anthropic.com/v1/messages"

CLAUDE_API_KEY = os.getenv("ANTHROPIC_API_KEY")

if not CLAUDE_API_KEY:
    raise RuntimeError("ANTHROPIC_API_KEY is not set")


BASE_HEADERS = {
    "Content-Type": "application/json",
    "anthropic-version": "2023-06-01",
    "x-api-key": CLAUDE_API_KEY,
}

SCHEDULE_PROMPT = """Ты — парсер расписания учебных занятий. 
Проанализируй изображение и извлеки расписание.

Верни ТОЛЬКО валидный JSON без пояснений, в формате:
{
  "group_name": "название группы если видно, иначе null",
  "lessons": [
    {
      "weekday": 1,
      "lesson_num": 1,
      "subject": "Математика",
      "teacher": "Иванов И.И.",
      "room": "101",
      "time_start": "08:00",
      "time_end": "09:35"
    }
  ]
}

weekday: 1=Пн ... 7=Вс
"""

CHANGE_PROMPT = """Ты — парсер изменений в расписании.
Верни JSON:
{
  "changes": []
}
"""


async def image_to_base64(image_bytes: bytes) -> str:
    return base64.b64encode(image_bytes).decode()


async def call_claude_vision(image_b64: str, prompt: str,
                            media_type: str = "image/jpeg") -> Optional[dict]:

    body = {
        "model": "claude-sonnet-4-20250514",
        "max_tokens": 2000,
        "messages": [{
            "role": "user",
            "content": [
                {
                    "type": "image",
                    "source": {
                        "type": "base64",
                        "media_type": media_type,
                        "data": image_b64,
                    }
                },
                {"type": "text", "text": prompt}
            ]
        }]
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                CLAUDE_API_URL,
                json=body,
                headers=BASE_HEADERS
            ) as resp:

                if resp.status != 200:
                    text = await resp.text()
                    logger.error(f"Claude API error {resp.status}: {text}")
                    return None

                data = await resp.json()
                content = data["content"][0]["text"]

                match = re.search(r'\{.*\}', content, re.DOTALL)
                if match:
                    return json.loads(match.group())

                return json.loads(content)

    except Exception as e:
        logger.error(f"Claude Vision error: {e}")
        return None


async def call_claude_text(text: str, prompt: str) -> Optional[dict]:
    body = {
        "model": "claude-sonnet-4-20250514",
        "max_tokens": 1000,
        "messages": [{
            "role": "user",
            "content": f"{prompt}\n\n{text}"
        }]
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                CLAUDE_API_URL,
                json=body,
                headers=BASE_HEADERS
            ) as resp:

                if resp.status != 200:
                    return None

                data = await resp.json()
                content = data["content"][0]["text"]

                match = re.search(r'\{.*\}', content, re.DOTALL)
                if match:
                    return json.loads(match.group())

                return json.loads(content)

    except Exception as e:
        logger.error(f"Claude text error: {e}")
        return None


async def parse_schedule_image(image_bytes: bytes,
                              media_type: str = "image/jpeg") -> Optional[dict]:

    b64 = await image_to_base64(image_bytes)
    result = await call_claude_vision(b64, SCHEDULE_PROMPT, media_type)

    if not result or "error" in result:
        return None

    lessons = result.get("lessons", [])

    valid = []
    for l in lessons:
        if all(k in l for k in ("weekday", "lesson_num", "subject", "time_start", "time_end")):
            valid.append(l)

    result["lessons"] = valid
    return result


async def parse_change_image(image_bytes: bytes,
                            media_type: str = "image/jpeg") -> Optional[dict]:

    b64 = await image_to_base64(image_bytes)
    return await call_claude_vision(b64, CHANGE_PROMPT, media_type)


async def parse_change_text(text: str) -> Optional[dict]:
    keywords = ["отмен", "перенос", "замен", "пар", "лекц", "семинар"]
    if not any(k in text.lower() for k in keywords):
        return None

    return await call_claude_text(text, CHANGE_PROMPT)


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
            room = f" [{l.get('room')}]" if l.get("room") else ""

            lines.append(
                f"{l['lesson_num']}. {l['time_start']}–{l['time_end']} "
                f"<b>{l['subject']}</b>{teacher}{room}"
            )

    return "\n".join(lines) if lines else "Расписание пусто"