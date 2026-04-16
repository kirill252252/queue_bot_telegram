"""
Распознавание расписания через OpenAI Vision API (GPT-4o).
"""
import base64
import json
import logging
import re
import os
from typing import Optional

import aiohttp

logger = logging.getLogger(__name__)

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

OPENAI_URL = "https://api.openai.com/v1/chat/completions"

if not OPENAI_API_KEY:
    logger.warning("OPENAI_API_KEY is not set — OCR disabled")


HEADERS = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {OPENAI_API_KEY}"
}

MODEL = "gpt-4o-mini"


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

Если не можешь распознать — верни:
{"error": "не удалось распознать"}
"""


CHANGE_PROMPT = """
Ты — парсер изменений расписания.

Верни JSON:
{
  "changes": []
}

action:
cancel | add | reschedule | room_change | teacher_change
"""


def image_to_base64(image_bytes: bytes) -> str:
    return base64.b64encode(image_bytes).decode()


async def call_openai_vision(image_b64: str, prompt: str) -> Optional[dict]:
    if not OPENAI_API_KEY:
        return None

    body = {
        "model": MODEL,
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": prompt},
                    {
                        "type": "image_url",
                        "image_url": {
                            "url": f"data:image/jpeg;base64,{image_b64}"
                        }
                    }
                ]
            }
        ],
        "max_tokens": 2000
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(OPENAI_URL, headers=HEADERS, json=body) as resp:

                if resp.status != 200:
                    text = await resp.text()
                    logger.error(f"OpenAI API error {resp.status}: {text}")
                    return None

                data = await resp.json()
                content = data["choices"][0]["message"]["content"]

                match = re.search(r'\{.*\}', content, re.DOTALL)
                if match:
                    return json.loads(match.group())

                return json.loads(content)

    except Exception as e:
        logger.error(f"OpenAI Vision error: {e}")
        return None


async def call_openai_text(text: str, prompt: str) -> Optional[dict]:
    if not OPENAI_API_KEY:
        return None

    body = {
        "model": MODEL,
        "messages": [
            {
                "role": "user",
                "content": f"{prompt}\n\n{text}"
            }
        ],
        "max_tokens": 1000
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(OPENAI_URL, headers=HEADERS, json=body) as resp:

                if resp.status != 200:
                    return None

                data = await resp.json()
                content = data["choices"][0]["message"]["content"]

                match = re.search(r'\{.*\}', content, re.DOTALL)
                if match:
                    return json.loads(match.group())

                return json.loads(content)

    except Exception as e:
        logger.error(f"OpenAI text error: {e}")
        return None


async def parse_schedule_image(image_bytes: bytes, media_type="image/jpeg"):
    b64 = image_to_base64(image_bytes)
    return await call_openai_vision(b64, SCHEDULE_PROMPT)


async def parse_change_image(image_bytes: bytes, media_type="image/jpeg"):
    b64 = image_to_base64(image_bytes)
    return await call_openai_vision(b64, CHANGE_PROMPT)


async def parse_change_text(text: str):
    keywords = ["отмен", "перенос", "замен", "пар", "лекц", "семинар"]
    if not any(k in text.lower() for k in keywords):
        return None

    return await call_openai_text(text, CHANGE_PROMPT)


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

    return "\n".join(lines)