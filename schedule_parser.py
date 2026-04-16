"""
Парсер расписания через OpenAI Vision API.
Принимает фото расписания и возвращает структурированные данные.
"""
import json
import logging
import base64
import aiohttp
import os
from typing import Optional

logger = logging.getLogger(__name__)

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

OPENAI_URL = "https://api.openai.com/v1/chat/completions"

HEADERS = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {OPENAI_API_KEY}"
}

MODEL = "gpt-4o-mini"


SYSTEM_PROMPT = """Ты — парсер расписания учебных занятий. 
Тебе дают фотографию расписания. Извлеки все занятия и верни ТОЛЬКО JSON без пояснений.

Формат ответа:
{
  "groups": [
    {
      "name": "название группы",
      "schedule": [
        {
          "weekday": 1,
          "subject": "название предмета",
          "time_start": "09:00",
          "time_end": "10:30",
          "room": "аудитория или null",
          "teacher": "преподаватель или null"
        }
      ]
    }
  ]
}

weekday: 1=Пн ... 7=Вс
Верни ТОЛЬКО JSON.
"""


def _b64(image_bytes: bytes) -> str:
    return base64.b64encode(image_bytes).decode()


async def _call_openai(messages: list, max_tokens: int = 2000) -> Optional[dict]:
    if not OPENAI_API_KEY:
        logger.error("OPENAI_API_KEY is not set")
        return None

    payload = {
        "model": MODEL,
        "messages": messages,
        "max_tokens": max_tokens
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(OPENAI_URL, json=payload, headers=HEADERS) as resp:

                if resp.status != 200:
                    text = await resp.text()
                    logger.error(f"OpenAI API error {resp.status}: {text}")
                    return None

                data = await resp.json()
                raw = data["choices"][0]["message"]["content"]

                raw = raw.replace("```json", "").replace("```", "").strip()
                return json.loads(raw)

    except Exception as e:
        logger.error(f"OpenAI request error: {e}")
        return None


async def parse_schedule_image(image_bytes: bytes, mime_type: str = "image/jpeg") -> Optional[dict]:
    image_b64 = _b64(image_bytes)

    messages = [
        {
            "role": "user",
            "content": [
                {"type": "text", "text": SYSTEM_PROMPT},
                {
                    "type": "image_url",
                    "image_url": {
                        "url": f"data:{mime_type};base64,{image_b64}"
                    }
                }
            ]
        }
    ]

    return await _call_openai(messages, max_tokens=4096)


# ───────────────────────────────────────────────────────────────
# CHANGE PARSER
# ───────────────────────────────────────────────────────────────

CHANGE_SYSTEM_PROMPT = """Ты анализируешь сообщения об изменении расписания.

Верни ТОЛЬКО JSON:

{
  "changes": [
    {
      "type": "cancel" | "reschedule" | "add",
      "group": "название группы или null",
      "weekday": 1-7 или null,
      "date": "DD.MM.YYYY или null",
      "subject": "предмет или null",
      "time_start": "HH:MM или null",
      "time_end": "HH:MM или null",
      "new_time_start": "HH:MM или null",
      "new_time_end": "HH:MM или null",
      "new_weekday": 1-7 или null,
      "new_date": "DD.MM.YYYY или null",
      "comment": "комментарий или null"
    }
  ]
}

Верни только JSON.
"""


async def parse_schedule_change(text: str, image_bytes: Optional[bytes] = None,
                               mime_type: str = "image/jpeg") -> Optional[dict]:

    content = []

    if image_bytes:
        content.append({
            "type": "image_url",
            "image_url": {
                "url": f"data:{mime_type};base64,{_b64(image_bytes)}"
            }
        })

    if text:
        content.append({"type": "text", "text": text})

    if not content:
        return None

    messages = [
        {
            "role": "user",
            "content": [
                {"type": "text", "text": CHANGE_SYSTEM_PROMPT},
                *content
            ]
        }
    ]

    return await _call_openai(messages, max_tokens=2000)