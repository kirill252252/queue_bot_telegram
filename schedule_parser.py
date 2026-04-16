"""
Парсер расписания через Claude Vision API.
Принимает фото расписания и возвращает структурированные данные.
"""
import json
import logging
import base64
import aiohttp
from datetime import datetime, time
from typing import Optional

logger = logging.getLogger(__name__)

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

weekday: 1=Пн, 2=Вт, 3=Ср, 4=Чт, 5=Пт, 6=Сб, 7=Вс
Если группа одна и не указана явно — используй "Группа 1".
Верни ТОЛЬКО JSON, без markdown-блоков и пояснений."""


async def parse_schedule_image(image_bytes: bytes, mime_type: str = "image/jpeg") -> Optional[dict]:
    """Отправляет фото в Claude API и получает структуру расписания."""
    image_b64 = base64.b64encode(image_bytes).decode()

    payload = {
        "model": "claude-sonnet-4-20250514",
        "max_tokens": 4096,
        "system": SYSTEM_PROMPT,
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "image",
                        "source": {
                            "type": "base64",
                            "media_type": mime_type,
                            "data": image_b64
                        }
                    },
                    {
                        "type": "text",
                        "text": "Извлеки расписание из этого изображения."
                    }
                ]
            }
        ]
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                "https://api.anthropic.com/v1/messages",
                json=payload,
                headers={"Content-Type": "application/json"}
            ) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    logger.error(f"Claude API error {resp.status}: {text}")
                    return None
                data = await resp.json()
                raw = data["content"][0]["text"].strip()
                # убираем ```json если есть
                raw = raw.replace("```json", "").replace("```", "").strip()
                return json.loads(raw)
    except Exception as e:
        logger.error(f"parse_schedule_image error: {e}")
        return None


async def parse_schedule_change(text: str, image_bytes: Optional[bytes] = None,
                                 mime_type: str = "image/jpeg") -> Optional[dict]:
    """
    Анализирует текстовое сообщение или фото с изменением расписания.
    Возвращает список изменений.
    """
    change_system = """Ты анализируешь сообщение об изменении расписания учебных занятий.
Верни ТОЛЬКО JSON без пояснений.

Формат:
{
  "changes": [
    {
      "type": "cancel" | "reschedule" | "add",
      "group": "название группы или null если для всех",
      "weekday": 1-7 или null,
      "date": "DD.MM.YYYY или null",
      "subject": "предмет или null",
      "time_start": "HH:MM или null",
      "time_end": "HH:MM или null",
      "new_time_start": "HH:MM или null (для reschedule)",
      "new_time_end": "HH:MM или null (для reschedule)",
      "new_weekday": 1-7 или null (для reschedule),
      "new_date": "DD.MM.YYYY или null (для reschedule)",
      "comment": "комментарий или null"
    }
  ]
}

type: cancel=отмена, reschedule=перенос, add=добавление
Верни ТОЛЬКО JSON."""

    content = []
    if image_bytes:
        content.append({
            "type": "image",
            "source": {
                "type": "base64",
                "media_type": mime_type,
                "data": base64.b64encode(image_bytes).decode()
            }
        })
    if text:
        content.append({"type": "text", "text": text})
    if not content:
        return None

    payload = {
        "model": "claude-sonnet-4-20250514",
        "max_tokens": 2048,
        "system": change_system,
        "messages": [{"role": "user", "content": content}]
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                "https://api.anthropic.com/v1/messages",
                json=payload,
                headers={"Content-Type": "application/json"}
            ) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
                raw = data["content"][0]["text"].strip()
                raw = raw.replace("```json", "").replace("```", "").strip()
                return json.loads(raw)
    except Exception as e:
        logger.error(f"parse_schedule_change error: {e}")
        return None
