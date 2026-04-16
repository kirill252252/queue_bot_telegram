"""
Распознавание расписания с изображений через Claude Vision API.
Поддерживает фото и скриншоты расписания в любом формате.
"""
import base64
import json
import logging
import re
from typing import Optional

import aiohttp

logger = logging.getLogger(__name__)

CLAUDE_API_URL = "https://api.anthropic.com/v1/messages"

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
  ],
  "period": "описание периода если указан"
}

weekday: 1=Пн, 2=Вт, 3=Ср, 4=Чт, 5=Пт, 6=Сб, 7=Вс
lesson_num: номер пары (1, 2, 3...)
time_start/time_end: формат HH:MM
teacher и room могут быть null если не указаны.

Если расписание не найдено или изображение нечёткое — верни {"error": "описание проблемы"}"""

CHANGE_PROMPT = """Ты — парсер изменений в расписании учебных занятий.
Проанализируй текст/изображение и извлеки изменения.

Верни ТОЛЬКО валидный JSON без пояснений:
{
  "changes": [
    {
      "action": "cancel",
      "date": "2024-01-15",
      "lesson_num": 2,
      "subject": "Математика",
      "note": "пояснение"
    }
  ]
}

action может быть:
- "cancel" — пара отменена
- "reschedule" — перенос (добавь time_start, time_end, новую дату)
- "add" — добавляется пара (добавь subject, time_start, time_end)
- "room_change" — смена аудитории (добавь room)
- "teacher_change" — замена преподавателя (добавь teacher)

date формат YYYY-MM-DD. Если дата не указана явно — попробуй вычислить из контекста.
Если изменений не найдено — верни {"changes": []}"""


async def image_to_base64(image_bytes: bytes) -> str:
    return base64.b64encode(image_bytes).decode()


async def call_claude_vision(image_b64: str, prompt: str,
                              media_type: str = "image/jpeg") -> Optional[dict]:
    headers = {
        "Content-Type": "application/json",
        "anthropic-version": "2023-06-01",
    }
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
            async with session.post(CLAUDE_API_URL, json=body, headers=headers) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    logger.error(f"Claude API error {resp.status}: {text}")
                    return None
                data = await resp.json()
                content = data["content"][0]["text"]
                # извлекаем JSON из ответа
                json_match = re.search(r'\{.*\}', content, re.DOTALL)
                if json_match:
                    return json.loads(json_match.group())
                return json.loads(content)
    except Exception as e:
        logger.error(f"Claude Vision error: {e}")
        return None


async def call_claude_text(text: str, prompt: str) -> Optional[dict]:
    """Анализируем текст на предмет изменений в расписании."""
    headers = {
        "Content-Type": "application/json",
        "anthropic-version": "2023-06-01",
    }
    body = {
        "model": "claude-sonnet-4-20250514",
        "max_tokens": 1000,
        "messages": [{
            "role": "user",
            "content": f"{prompt}\n\nТекст для анализа:\n{text}"
        }]
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(CLAUDE_API_URL, json=body, headers=headers) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
                content = data["content"][0]["text"]
                json_match = re.search(r'\{.*\}', content, re.DOTALL)
                if json_match:
                    return json.loads(json_match.group())
                return json.loads(content)
    except Exception as e:
        logger.error(f"Claude text error: {e}")
        return None


async def parse_schedule_image(image_bytes: bytes,
                                media_type: str = "image/jpeg") -> Optional[dict]:
    """Распознаём расписание с изображения."""
    b64 = await image_to_base64(image_bytes)
    result = await call_claude_vision(b64, SCHEDULE_PROMPT, media_type)

    if not result:
        return None
    if "error" in result:
        logger.warning(f"Schedule parse error: {result['error']}")
        return None

    lessons = result.get("lessons", [])
    # валидация
    valid_lessons = []
    for l in lessons:
        if not all(k in l for k in ("weekday", "lesson_num", "subject", "time_start", "time_end")):
            continue
        valid_lessons.append(l)

    result["lessons"] = valid_lessons
    logger.info(f"Parsed {len(valid_lessons)} lessons from image")
    return result


async def parse_change_image(image_bytes: bytes,
                              media_type: str = "image/jpeg") -> Optional[dict]:
    """Распознаём изменения в расписании с изображения."""
    b64 = await image_to_base64(image_bytes)
    return await call_claude_vision(b64, CHANGE_PROMPT, media_type)


async def parse_change_text(text: str) -> Optional[dict]:
    """Анализируем текстовое сообщение на предмет изменений в расписании."""
    # быстрая проверка — есть ли вообще ключевые слова
    keywords = ["отмен", "перенос", "замен", "пар", "лекц", "семинар",
                "занятие", "расписание", "аудитор", "преподавател"]
    text_lower = text.lower()
    if not any(kw in text_lower for kw in keywords):
        return None

    return await call_claude_text(text, CHANGE_PROMPT)


def format_schedule(lessons: list[dict]) -> str:
    """Форматируем расписание для вывода в Telegram."""
    days = {1: "Пн", 2: "Вт", 3: "Ср", 4: "Чт", 5: "Пт", 6: "Сб", 7: "Вс"}
    by_day = {}
    for l in lessons:
        wd = l["weekday"]
        if wd not in by_day:
            by_day[wd] = []
        by_day[wd].append(l)

    lines = []
    for wd in sorted(by_day.keys()):
        lines.append(f"\n<b>📅 {days.get(wd, wd)}</b>")
        for l in sorted(by_day[wd], key=lambda x: x["lesson_num"]):
            teacher = f" — {l['teacher']}" if l.get("teacher") else ""
            room = f" [{l['room']}]" if l.get("room") else ""
            lines.append(
                f"  {l['lesson_num']}. {l['time_start']}–{l['time_end']} "
                f"<b>{l['subject']}</b>{teacher}{room}"
            )
    return "\n".join(lines) if lines else "Расписание пусто"
