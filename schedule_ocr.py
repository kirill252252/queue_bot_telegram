"""
Распознавание расписания с изображений через Groq API (LLaMA Vision).
Поддерживает фото и скриншоты расписания, а также текстовый парсинг изменений.
Единый модуль для всех AI-задач (используется schedule_handlers.py и source_monitor.py).
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
# Groq API config
# ─────────────────────────────────────────────

GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")

if not GROQ_API_KEY:
    logger.warning("GROQ_API_KEY is not set — AI features disabled")

GROQ_URL = "https://api.groq.com/openai/v1/chat/completions"

# Модель с поддержкой изображений (vision)
VISION_MODEL = os.getenv("GROQ_VISION_MODEL", "meta-llama/llama-4-scout-17b-16e-instruct")

# Модель для текстовых задач (быстрее и дешевле)
TEXT_MODEL = os.getenv("GROQ_TEXT_MODEL", "llama-3.3-70b-versatile")


# ─────────────────────────────────────────────
# PROMPTS
# ─────────────────────────────────────────────

SCHEDULE_PROMPT = """
Ты — точный парсер расписания учебных занятий аэрокосмического колледжа.

ФОРМАТ ТАБЛИЦЫ:
- Дни недели написаны вертикально слева (ПОНЕДЕЛЬНИК, ВТОРНИК, СРЕДА и т.д.)
- «Лента» = номер пары (lesson_num). «Ауд» = аудитория (room).
- В одной таблице несколько групп — у каждой свои колонки. Название группы в заголовке (Гр. ПК-10-25).
- Преподаватель написан под названием предмета в той же ячейке.
- Времени начала/конца нет — оставляй time_start и time_end пустыми строками "".

ПРАВИЛА ЧЁТНЫХ/НЕЧЁТНЫХ НЕДЕЛЬ:
- Если в одной ячейке ленты два предмета на двух строках (или через дробь «/»):
  → ПЕРВЫЙ предмет: week_type=1 (нечётная неделя)
  → ВТОРОЙ предмет: week_type=2 (чётная неделя)
  → Создай ДВЕ отдельные записи с одинаковым weekday и lesson_num, но разными week_type.
- Один предмет в ячейке → week_type=0 (каждую неделю).

МЕРОПРИЯТИЯ И ПРОПУСКИ:
- «Разговоры о важном», «Внеклассное мероприятие», «Классный час» → is_event=1, НЕ пропускай их (добавь в JSON).
- Пустые ячейки, «————», «лент нет», «лента нет» → пропускай.

Верни ТОЛЬКО JSON, без пояснений и markdown:

{
  "groups": [
    {
      "group_name": "ПК-10-25",
      "lessons": [
        {
          "weekday": 1,
          "lesson_num": 1,
          "subject": "Математика",
          "teacher": "Дубиненко ЕП",
          "room": "С-5",
          "time_start": "",
          "time_end": "",
          "week_type": 0,
          "is_event": 0
        }
      ]
    }
  ]
}

weekday: 1=Пн, 2=Вт, 3=Ср, 4=Чт, 5=Пт, 6=Сб, 7=Вс.
week_type: 0=каждую неделю, 1=нечётные недели, 2=чётные недели.
is_event: 0=обычное занятие, 1=мероприятие (без очереди).
Если не удаётся распознать — верни: {"error": "не удалось распознать"}
"""

CHANGE_PROMPT = """
Ты — парсер изменений расписания учебных занятий аэрокосмического колледжа.

На изображении или в тексте — лист «Изменения в расписании».
Особенности формата:
- В заголовке указана дата и день недели (например: «На 20.04.2026. (ПОНЕДЕЛЬНИК) - Вторая неделя»)
- Таблица содержит колонки: Группа | Лента | Дисциплина | Преподаватель | Аудитория
- «Лента» — это номер пары, используй как lesson_num
- «Лента нет» или пустая лента — занятие отменено (action: cancel)
- «По расписанию» в дисциплине — занятие проходит по обычному расписанию (пропускай)
- Строка «Уходит: Группа ХХХ (практика)» — это примечание, не изменение расписания (пропускай)
- Одна строка = одно изменение для одной группы

Верни ТОЛЬКО JSON, без пояснений и markdown:

{
  "date": "YYYY-MM-DD или null",
  "changes": [
    {
      "action": "cancel" | "reschedule" | "add" | "room_change" | "teacher_change",
      "group": "название группы, например ПК-10-25",
      "lesson_num": 1,
      "weekday": 1,
      "date": "YYYY-MM-DD или null",
      "subject": "предмет или null",
      "time_start": null,
      "time_end": null,
      "room": "аудитория или null",
      "teacher": "преподаватель или null",
      "comment": "комментарий или null"
    }
  ]
}

action: cancel=отмена (лента нет), reschedule=перенос времени/аудитории, add=новое занятие, room_change=смена аудитории, teacher_change=замена преподавателя.
weekday: 1=Пн, 2=Вт, 3=Ср, 4=Чт, 5=Пт, 6=Сб, 7=Вс.
Если изменений не найдено — верни: {"date": null, "changes": []}
"""


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def _image_to_base64(image_bytes: bytes) -> str:
    return base64.b64encode(image_bytes).decode()


def _extract_json(text: str) -> Optional[dict]:
    """Извлекаем JSON из ответа модели, убирая markdown-обёртку."""
    cleaned = re.sub(r"```(?:json)?\s*", "", text).strip().rstrip("`").strip()

    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        pass

    match = re.search(r"\{.*\}", cleaned, re.DOTALL)
    if match:
        try:
            return json.loads(match.group())
        except json.JSONDecodeError:
            pass

    return None


def _groq_headers() -> dict:
    return {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {GROQ_API_KEY}",
    }


# ─────────────────────────────────────────────
# CORE — Groq REST call (vision)
# ─────────────────────────────────────────────

async def _call_groq_vision(
    prompt: str,
    image_bytes: bytes,
    media_type: str = "image/jpeg",
    max_tokens: int = 4096,
) -> Optional[dict]:
    """
    Отправляем изображение + промпт в Groq Vision API.
    """
    if not GROQ_API_KEY:
        logger.error("GROQ_API_KEY is not set — cannot call Groq Vision")
        return None

    b64 = _image_to_base64(image_bytes)
    data_url = f"data:{media_type};base64,{b64}"

    body = {
        "model": VISION_MODEL,
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": prompt},
                    {"type": "image_url", "image_url": {"url": data_url}},
                ],
            }
        ],
        "max_tokens": max_tokens,
        "temperature": 0.1,
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                GROQ_URL,
                json=body,
                headers=_groq_headers(),
            ) as resp:
                if resp.status != 200:
                    error_text = await resp.text()
                    logger.error(f"Groq Vision API error {resp.status}: {error_text}")
                    return None

                data = await resp.json()

                try:
                    content = data["choices"][0]["message"]["content"]
                except (KeyError, IndexError) as e:
                    logger.error(f"Unexpected Groq response structure: {e}\n{data}")
                    return None

                return _extract_json(content)

    except aiohttp.ClientError as e:
        logger.error(f"Groq Vision network error: {e}")
        return None
    except Exception as e:
        logger.error(f"Groq Vision call exception: {e}")
        return None


# ─────────────────────────────────────────────
# CORE — Groq REST call (text only)
# ─────────────────────────────────────────────

async def _call_groq_text(
    prompt: str,
    max_tokens: int = 1024,
) -> Optional[dict]:
    """
    Отправляем текстовый запрос в Groq (без изображения).
    """
    if not GROQ_API_KEY:
        logger.error("GROQ_API_KEY is not set — cannot call Groq")
        return None

    body = {
        "model": TEXT_MODEL,
        "messages": [
            {"role": "user", "content": prompt}
        ],
        "max_tokens": max_tokens,
        "temperature": 0.1,
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                GROQ_URL,
                json=body,
                headers=_groq_headers(),
            ) as resp:
                if resp.status != 200:
                    error_text = await resp.text()
                    logger.error(f"Groq Text API error {resp.status}: {error_text}")
                    return None

                data = await resp.json()

                try:
                    content = data["choices"][0]["message"]["content"]
                except (KeyError, IndexError) as e:
                    logger.error(f"Unexpected Groq response structure: {e}\n{data}")
                    return None

                return _extract_json(content)

    except aiohttp.ClientError as e:
        logger.error(f"Groq Text network error: {e}")
        return None
    except Exception as e:
        logger.error(f"Groq Text call exception: {e}")
        return None


# ─────────────────────────────────────────────
# PARSE SCHEDULE IMAGE
# ─────────────────────────────────────────────

async def parse_schedule_image(
    image_bytes: bytes, media_type: str = "image/jpeg"
) -> Optional[dict]:
    """
    Распознаём базовое расписание с фотографии.
    Возвращает: {"groups": [{"group_name": ..., "lessons": [...]}, ...]}
    Для обратной совместимости также поддерживает старый формат {"group_name": ..., "lessons": [...]}.
    """
    result = await _call_groq_vision(
        prompt=SCHEDULE_PROMPT,
        image_bytes=image_bytes,
        media_type=media_type,
        max_tokens=8192,
    )

    if not result or not isinstance(result, dict):
        return None

    if "error" in result:
        logger.warning(f"Groq schedule parse error: {result['error']}")
        return None

    required_keys = ("weekday", "lesson_num", "subject")

    def _normalize_lessons(raw_lessons: list) -> list:
        """Нормализуем занятия: добавляем week_type, is_event, автодетект мероприятий."""
        out = []
        for l in raw_lessons:
            if not all(k in l for k in required_keys):
                continue
            l.setdefault("week_type", 0)
            l.setdefault("is_event", 0)
            # Автодетект мероприятий по ключевым словам (резерв если модель не проставила)
            subj = l.get("subject", "").lower()
            event_kw = ["разговор", "внеклассное", "классный час", "воспитательное"]
            if any(kw in subj for kw in event_kw):
                l["is_event"] = 1
            out.append(l)
        return out

    # Новый формат: несколько групп
    if "groups" in result:
        for group in result["groups"]:
            group["lessons"] = _normalize_lessons(group.get("lessons", []))
        result["groups"] = [g for g in result["groups"] if g.get("lessons")]
        return result

    # Старый/упрощённый формат: одна группа
    lessons = _normalize_lessons(result.get("lessons", []))
    result["lessons"] = lessons
    # Оборачиваем в общий формат
    return {
        "groups": [{
            "group_name": result.get("group_name") or "Группа",
            "lessons": lessons,
        }]
    }


# ─────────────────────────────────────────────
# PARSE CHANGE IMAGE
# ─────────────────────────────────────────────

async def parse_change_image(
    image_bytes: bytes, media_type: str = "image/jpeg"
) -> Optional[dict]:
    """
    Распознаём изменения расписания с фотографии.
    Возвращает: {"date": "YYYY-MM-DD", "changes": [...]}
    """
    result = await _call_groq_vision(
        prompt=CHANGE_PROMPT,
        image_bytes=image_bytes,
        media_type=media_type,
        max_tokens=4096,
    )
    # Нормализуем: если нет поля date — добавляем null
    if result and "changes" in result and "date" not in result:
        result["date"] = None
    return result


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

    prompt = f"{CHANGE_PROMPT}\n\nТекст сообщения:\n{text}"
    return await _call_groq_text(prompt, max_tokens=1024)


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
    if image_bytes:
        # Если есть картинка — используем vision модель
        prompt = CHANGE_PROMPT
        if text:
            prompt += f"\n\nТекст поста:\n{text}"
        return await _call_groq_vision(
            prompt=prompt,
            image_bytes=image_bytes,
            media_type=media_type,
            max_tokens=2048,
        )
    elif text:
        # Только текст — используем текстовую модель (быстрее)
        return await parse_change_text(text)
    else:
        return None


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
            ts = l.get("time_start") or ""
            te = l.get("time_end") or ""
            time_str = f" {ts}–{te}" if ts and te else ""
            lines.append(
                f"{l['lesson_num']}.{time_str} "
                f"<b>{l['subject']}</b>{teacher}{room}"
            )

    return "\n".join(lines) if lines else "Расписание пусто"
