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
OCR_DEBUG = os.getenv("SCHEDULE_OCR_DEBUG", "").strip().lower() in {"1", "true", "yes", "on"}


# ─────────────────────────────────────────────
# PROMPTS
# ─────────────────────────────────────────────

SCHEDULE_PROMPT = """
Ты — точный парсер расписания учебных занятий. Анализируй таблицу ОЧЕНЬ внимательно.

══════════════════════ СТРУКТУРА ТАБЛИЦЫ ══════════════════════
• Дни недели написаны вертикально слева (ПОНЕДЕЛЬНИК, ВТОРНИК, СРЕДА, ЧЕТВЕРГ, ПЯТНИЦА, СУББОТА)
• «Лента» (или «лента») = номер пары → lesson_num
• «Ауд» / «Ауд.» = аудитория → room
• Преподаватель написан ПОД названием предмета в той же ячейке
• Одна таблица может содержать НЕСКОЛЬКО групп — у каждой отдельные столбцы
• Название группы в заголовке блока (например: «Гр. П-5-24», «Гр. ПК-10-25»)
• Времени нет на фото → time_start="" и time_end="" ВСЕГДА

══════════════════ КАК ОПРЕДЕЛЯТЬ ЧЁТНЫЕ/НЕЧЁТНЫЕ НЕДЕЛИ ══════════════════

КЛЮЧЕВОЕ ПРАВИЛО: Одна «лента» (номер пары) может содержать ДВЕ строки в таблице.
Это означает, что на этой паре две недели идут РАЗНЫЕ варианты.

ВЕРХНЯЯ строка в ячейке = НЕДЕЛЯ 1 (нечётная, week_type=1)
НИЖНЯЯ строка в ячейке = НЕДЕЛЯ 2 (чётная,   week_type=2)

Возможные комбинации для одной ленты:
┌─────────────────────────────────────────────────────────────────┐
│ ВАРИАНТ А: Одна строка с предметом                              │
│   → week_type=0 (занятие КАЖДУЮ неделю), одна запись            │
│   Пример: Лента 1 = «Разработка программных модулей Петрова АА» │
│                                                                 │
│ ВАРИАНТ Б: Две строки, оба предмета                             │
│   → Верхний: week_type=1, нижний: week_type=2, ДВЕ записи       │
│   Пример: Лента 2:                                              │
│     ┌─ Инструментальные средства Наприенко ЕМ    ← week_type=1  │
│     └─ Программирование web-приложений Вахитов РГ ← week_type=2 │
│                                                                 │
│ ВАРИАНТ В: Верхняя строка = «-----» или ПУСТО, нижняя = предмет │
│   → week_type=2 (только чётная неделя), одна запись             │
│   Пример: Лента 1 Среда:                                        │
│     ┌─ «——————————————»           ← пусто/прочерк               │
│     └─ Математическое моделирование Мережникова ЕМ ← week_type=2 │
│                                                                 │
│ ВАРИАНТ Г: Верхняя строка = предмет, нижняя = «-----» или ПУСТО │
│   → week_type=1 (только нечётная неделя), одна запись           │
│   Пример: Лента 5 Среда:                                        │
│     ┌─ Иностранный язык Данилова АА  ← week_type=1              │
│     └─ «——————————————»              ← пусто/прочерк            │
└─────────────────────────────────────────────────────────────────┘

Прочерки в таблице выглядят как: «------», «——————», «- - - -», «----------»

══════════════════════ МЕРОПРИЯТИЯ ══════════════════════
«Разговоры о важном», «Внеклассное мероприятие», «Классный час», «Разговор о важном»
→ is_event=1 (добавляй в JSON, но очередь для них не создаётся)
Все остальные занятия → is_event=0

══════════════════════ ПРОПУСКАЙ ══════════════════════
• Ячейки где обе строки пустые или оба прочерка
• Строки «лент нет», «лента нет», пустые ленты

══════════════════════ ФОРМАТ ОТВЕТА ══════════════════════
Верни ТОЛЬКО JSON без пояснений и markdown:

{
  "groups": [
    {
      "group_name": "П-5-24",
      "lessons": [
        {
          "weekday": 1,
          "lesson_num": 1,
          "subject": "Инструментальные средства разработки ПО",
          "teacher": "Наприенко ЕМ",
          "room": "509",
          "time_start": "",
          "time_end": "",
          "week_type": 0,
          "is_event": 0
        }
      ]
    }
  ]
}

weekday: 1=Пн, 2=Вт, 3=Ср, 4=Чт, 5=Пт, 6=Сб, 7=Вс
week_type: 0=каждую, 1=нечётные, 2=чётные
is_event: 0=обычное, 1=мероприятие
Если не распознать → {"error": "не удалось распознать"}
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


SCHEDULE_REVIEW_PROMPT_TEMPLATE = """
Ты выполняешь вторую проверку уже распознанного расписания по тому же изображению.

Тебе дан черновой JSON. Исправь его, если в нём есть ошибки, и верни только JSON в ТОМ ЖЕ формате.

Проверь особенно:
1. Внеклассные мероприятия и «Разговоры о важном» не считаются парой и не должны сдвигать lesson_num обычных занятий.
2. Нельзя объединять одинаковые подряд пары. Если у двух соседних лент одинаковые предмет, преподаватель и аудитория, это всё равно ДВЕ записи с разными lesson_num.
3. Если в одной ленте две строки, это одна и та же пара:
   - верхняя строка = week_type 1
   - нижняя строка = week_type 2
   - обе записи должны иметь один и тот же lesson_num
4. Если верхняя половина ячейки пустая/прочерк, а нижняя содержит предмет, это только week_type 2.
5. Если нижняя половина ячейки пустая/прочерк, а верхняя содержит предмет, это только week_type 1.
6. Не превращай одну дробную ячейку в две разные последовательные пары.
7. Не теряй занятия из лент 1/2/3 и т.д., даже если предметы одинаковые.

Черновой JSON:
{draft_json}
"""


SCHEDULE_CELL_PROMPT = """
Ты анализируешь расписание занятий с изображения.

Сначала определи ЯЧЕЙКИ по видимому номеру пары в колонке «лента», а не плоский список предметов.

Главные правила:
1. Если у двух соседних строк разные цифры в колонке «лента», это две разные пары, даже если текст одинаковый.
2. Если под одной и той же цифрой внутри ячейки две строки, это одна пара с разделением по неделям.
3. Верхняя строка под одним номером пары = нечётная неделя.
4. Нижняя строка под одним номером пары = чётная неделя.
5. Пустой верх + заполненный низ = только чётная неделя.
6. Заполненный верх + пустой низ = только нечётная неделя.
7. Мероприятия «Разговоры о важном» / «Внеклассное мероприятие» не считаются обычной парой: lesson_num=0, is_event=1.

Критичные примеры:
- Понедельник:
  lesson_num 1 = обычная пара.
  lesson_num 2 = одна ячейка с двумя строками:
    верх = «Инструментальные средства разработки программного обеспечения»
    низ  = «Программирование web-приложений»
  Это ОДИН lesson_num=2, а не пары 2 и 3.
- Вторник:
  lesson_num 1 и lesson_num 2 могут иметь одинаковый текст.
  Если цифры слева разные, это две разные пары.
- Среда:
  lesson_num 1 = only even
  lesson_num 2 = odd/even split
  lesson_num 3 = обычная отдельная пара, даже если текст похож на верх lesson_num 2
  lesson_num 5 = only odd

Верни только JSON:
{
  "groups": [
    {
      "group_name": "П-5-24",
      "cells": [
        {
          "weekday": 1,
          "lesson_num": 2,
          "week_mode": "odd_even",
          "is_event": 0,
          "top": {
            "subject": "Инструментальные средства разработки программного обеспечения",
            "teacher": "Наприенко ЕМ",
            "room": "509"
          },
          "bottom": {
            "subject": "Программирование web-приложений",
            "teacher": "Вахитов РГ",
            "room": "410"
          }
        }
      ]
    }
  ]
}

Допустимые week_mode:
- "every_week"
- "odd_even"
- "odd_only"
- "even_only"
- "event"
- "empty"

Если top или bottom отсутствует, ставь null.
Если не удалось распознать -> {"error":"не удалось распознать"}
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


def _debug_dump_schedule(stage: str, payload: Optional[dict]) -> None:
    if OCR_DEBUG and isinstance(payload, dict):
        logger.info("schedule_ocr %s: %s", stage, json.dumps(payload, ensure_ascii=False))


EVENT_KEYWORDS = (
    "разговор о важном",
    "разговоры о важном",
    "внеклассное",
    "классный час",
    "воспитательное",
)


VALID_WEEK_MODES = {
    "every_week",
    "odd_even",
    "odd_only",
    "even_only",
    "event",
    "empty",
}


def _is_placeholder_text(value: Optional[str]) -> bool:
    text = str(value or "").strip().lower()
    if not text:
        return True
    if text in {"-", "--", "---", "----", "-----", "------", "--------", "----------", "нет", "лента нет", "ленты нет"}:
        return True
    return bool(re.fullmatch(r"[-—–_\s.]+", text))


def _normalize_cell_part(raw_part: Optional[dict]) -> Optional[dict]:
    if not isinstance(raw_part, dict):
        return None

    subject = str(raw_part.get("subject") or "").strip()
    teacher = str(raw_part.get("teacher") or "").strip()
    room = str(raw_part.get("room") or "").strip()

    if _is_placeholder_text(subject):
        return None

    return {
        "subject": subject,
        "teacher": teacher or None,
        "room": room or None,
    }


def _make_lesson_from_part(
    weekday: int,
    lesson_num: int,
    part: dict,
    week_type: int,
    is_event: int,
) -> dict:
    return {
        "weekday": weekday,
        "lesson_num": lesson_num,
        "subject": part["subject"],
        "teacher": part.get("teacher"),
        "room": part.get("room"),
        "time_start": "",
        "time_end": "",
        "week_type": week_type,
        "is_event": is_event,
    }


def _expand_schedule_cells(result: dict) -> Optional[dict]:
    """
    Разворачиваем OCR-ответ в виде ячеек по номеру пары в список lessons.
    """
    if not result or not isinstance(result, dict):
        return None
    if "error" in result:
        return None

    groups = result.get("groups")
    if not isinstance(groups, list):
        return None

    expanded_groups = []
    for group in groups:
        if not isinstance(group, dict) or "cells" not in group:
            continue

        lessons = []
        for raw_cell in group.get("cells", []):
            if not isinstance(raw_cell, dict):
                continue

            try:
                weekday = int(raw_cell.get("weekday"))
                lesson_num = int(raw_cell.get("lesson_num") or 0)
            except (TypeError, ValueError):
                continue

            week_mode = str(raw_cell.get("week_mode") or "").strip().lower()
            if week_mode and week_mode not in VALID_WEEK_MODES:
                week_mode = ""

            is_event = 1 if int(raw_cell.get("is_event") or 0) else 0
            top = _normalize_cell_part(raw_cell.get("top"))
            bottom = _normalize_cell_part(raw_cell.get("bottom"))

            if week_mode == "empty":
                continue

            if is_event or week_mode == "event":
                part = top or bottom
                if part:
                    lessons.append(_make_lesson_from_part(weekday, 0, part, 0, 1))
                continue

            if not week_mode:
                if top and bottom:
                    week_mode = "odd_even"
                elif top and not bottom:
                    week_mode = "every_week"
                elif bottom and not top:
                    week_mode = "even_only"
                else:
                    continue

            if week_mode == "every_week":
                part = top or bottom
                if part:
                    lessons.append(_make_lesson_from_part(weekday, lesson_num, part, 0, 0))
            elif week_mode == "odd_even":
                if top:
                    lessons.append(_make_lesson_from_part(weekday, lesson_num, top, 1, 0))
                if bottom:
                    lessons.append(_make_lesson_from_part(weekday, lesson_num, bottom, 2, 0))
            elif week_mode == "odd_only":
                if top:
                    lessons.append(_make_lesson_from_part(weekday, lesson_num, top, 1, 0))
            elif week_mode == "even_only":
                if bottom:
                    lessons.append(_make_lesson_from_part(weekday, lesson_num, bottom, 2, 0))

        if lessons:
            expanded_groups.append({
                "group_name": group.get("group_name") or "Р“СЂСѓРїРїР°",
                "lessons": lessons,
            })

    return {"groups": expanded_groups} if expanded_groups else None


def _normalize_lessons(raw_lessons: list) -> list[dict]:
    """
    Приводим уроки к единому виду после OCR.
    """
    out = []
    for raw in raw_lessons:
        if not isinstance(raw, dict):
            continue

        lesson = dict(raw)

        try:
            lesson["weekday"] = int(lesson.get("weekday"))
            lesson["lesson_num"] = int(lesson.get("lesson_num") or 0)
        except (TypeError, ValueError):
            continue

        subject = str(lesson.get("subject") or "").strip()
        if not subject:
            continue
        lesson["subject"] = subject

        lesson["teacher"] = str(lesson.get("teacher") or "").strip() or None
        lesson["room"] = str(lesson.get("room") or "").strip() or None
        lesson["time_start"] = str(lesson.get("time_start") or "").strip()
        lesson["time_end"] = str(lesson.get("time_end") or "").strip()

        try:
            lesson["week_type"] = int(lesson.get("week_type") or 0)
        except (ValueError, TypeError):
            lesson["week_type"] = 0

        try:
            lesson["is_event"] = int(lesson.get("is_event") or 0)
        except (ValueError, TypeError):
            lesson["is_event"] = 0

        if any(keyword in subject.lower() for keyword in EVENT_KEYWORDS):
            lesson["is_event"] = 1

        out.append(lesson)

    return out


def _dedupe_lessons(lessons: list[dict]) -> list[dict]:
    seen = set()
    result = []

    for lesson in sorted(
        lessons,
        key=lambda item: (
            int(item.get("weekday") or 0),
            int(item.get("lesson_num") or 0),
            int(item.get("week_type") or 0),
            str(item.get("subject") or ""),
            str(item.get("teacher") or ""),
            str(item.get("room") or ""),
            int(item.get("is_event") or 0),
        ),
    ):
        key = (
            int(lesson.get("weekday") or 0),
            int(lesson.get("lesson_num") or 0),
            str(lesson.get("subject") or ""),
            str(lesson.get("teacher") or ""),
            str(lesson.get("room") or ""),
            int(lesson.get("week_type") or 0),
            int(lesson.get("is_event") or 0),
        )
        if key in seen:
            continue
        seen.add(key)
        result.append(lesson)

    return result


def _repair_group_lessons(lessons: list[dict]) -> list[dict]:
    """
    Локальная защита от самых частых OCR-ошибок:
    - мероприятие не считается парой и не должно сдвигать обычные lesson_num
    - точные дубли одной и той же записи схлопываем
    """
    by_day: dict[int, list[dict]] = {}
    for lesson in lessons:
        by_day.setdefault(int(lesson.get("weekday") or 0), []).append(dict(lesson))

    repaired = []
    for weekday in sorted(by_day):
        day_lessons = by_day[weekday]
        events = []
        regular = []

        for lesson in day_lessons:
            if int(lesson.get("is_event") or 0):
                event_lesson = dict(lesson)
                event_lesson["lesson_num"] = 0
                events.append(event_lesson)
            else:
                regular.append(dict(lesson))

        regular_numbers = sorted(
            {
                int(lesson.get("lesson_num") or 0)
                for lesson in regular
                if int(lesson.get("lesson_num") or 0) > 0
            }
        )

        # Если OCR посчитал мероприятие "нулевой" парой как первую ленту,
        # сдвигаем обычные занятия обратно.
        if events and regular_numbers and regular_numbers[0] > 1 and 1 not in regular_numbers:
            shifted_regular = []
            for lesson in regular:
                shifted = dict(lesson)
                shifted["lesson_num"] = max(1, int(shifted.get("lesson_num") or 0) - 1)
                shifted_regular.append(shifted)
            regular = shifted_regular

        repaired.extend(events)
        repaired.extend(regular)

    return _dedupe_lessons(repaired)


def _normalize_schedule_result(result: dict) -> Optional[dict]:
    if not result or not isinstance(result, dict):
        return None

    if "error" in result:
        logger.warning(f"Groq schedule parse error: {result['error']}")
        return None

    if "groups" in result:
        groups = []
        for group in result.get("groups", []):
            if not isinstance(group, dict):
                continue
            lessons = _repair_group_lessons(_normalize_lessons(group.get("lessons", [])))
            if not lessons:
                continue
            groups.append({
                "group_name": group.get("group_name") or "Группа",
                "lessons": lessons,
            })
        return {"groups": groups} if groups else None

    lessons = _repair_group_lessons(_normalize_lessons(result.get("lessons", [])))
    if not lessons:
        return None

    return {
        "groups": [{
            "group_name": result.get("group_name") or "Группа",
            "lessons": lessons,
        }]
    }


async def _review_schedule_parse(
    image_bytes: bytes,
    media_type: str,
    draft_result: dict,
) -> Optional[dict]:
    """
    Второй проход по тому же изображению: просим модель проверить готовый JSON
    и исправить lesson_num/week_type, если она схлопнула пары или сдвинула ленты.
    """
    prompt = SCHEDULE_REVIEW_PROMPT_TEMPLATE.format(
        draft_json=json.dumps(draft_result, ensure_ascii=False, indent=2)
    )
    reviewed = await _call_groq_vision(
        prompt=prompt,
        image_bytes=image_bytes,
        media_type=media_type,
        max_tokens=8192,
    )
    return reviewed if isinstance(reviewed, dict) else None


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
    cell_result = await _call_groq_vision(
        prompt=SCHEDULE_CELL_PROMPT,
        image_bytes=image_bytes,
        media_type=media_type,
        max_tokens=8192,
    )
    _debug_dump_schedule("cell_result", cell_result)
    expanded_cells = _expand_schedule_cells(cell_result)
    _debug_dump_schedule("expanded_cells", expanded_cells)
    if expanded_cells:
        normalized_cells = _normalize_schedule_result(expanded_cells)
        _debug_dump_schedule("normalized_cells", normalized_cells)
        if normalized_cells:
            return normalized_cells

    initial_result = await _call_groq_vision(
        prompt=SCHEDULE_PROMPT,
        image_bytes=image_bytes,
        media_type=media_type,
        max_tokens=8192,
    )
    _debug_dump_schedule("initial_result", initial_result)

    if not initial_result or not isinstance(initial_result, dict):
        return None

    result = initial_result
    reviewed_result = await _review_schedule_parse(
        image_bytes=image_bytes,
        media_type=media_type,
        draft_result=initial_result,
    )
    _debug_dump_schedule("reviewed_result", reviewed_result)
    if reviewed_result:
        result = reviewed_result

    normalized = _normalize_schedule_result(result)
    _debug_dump_schedule("normalized_result", normalized)
    if normalized:
        return normalized

    return _normalize_schedule_result(initial_result)


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


# ─────────────────────────────────────────────
# Функция разделения
# ─────────────────────────────────────────────

def split_by_week(lessons: list[dict]):
    even = []
    odd = []

    for l in lessons:
        wt = int(l.get("week_type") or 0)

        # 0 = всегда
        if wt == 0:
            even.append(l)
            odd.append(l)

        # 1 = нечётная
        elif wt == 1:
            odd.append(l)

        # 2 = чётная
        elif wt == 2:
            even.append(l)

    return even, odd
