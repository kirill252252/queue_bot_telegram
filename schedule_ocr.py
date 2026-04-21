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
from io import BytesIO
from typing import Optional

import aiohttp
import numpy as np
from PIL import Image

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


SCHEDULE_DAY_PROMPT_TEMPLATE = """
Ты видишь только один день расписания для одной группы: {day_name}.
В этом фрагменте слева есть видимые номера пар, рядом аудитории, справа содержимое ячеек.

Правила:
1. Если цифры слева разные, это разные пары, даже если текст одинаковый.
2. Если под одной цифрой внутри одной ячейки две строки, это одна пара с делением по неделям:
   - верх = week_mode "odd_even" верхняя часть
   - низ  = week_mode "odd_even" нижняя часть
3. Верх заполнен, низ пустой/прочерк -> "odd_only"
4. Верх пустой/прочерк, низ заполнен -> "even_only"
5. Одна обычная строка -> "every_week"
6. Внеклассное мероприятие / Разговоры о важном -> lesson_num 0, week_mode "event", is_event 1
7. Не объединяй одинаковые пары с разными цифрами.

Верни только JSON:
{{
  "weekday": {weekday},
  "cells": [
    {{
      "lesson_num": 2,
      "week_mode": "odd_even",
      "is_event": 0,
      "top": {{"subject": "...", "teacher": "...", "room": "..."}},
      "bottom": {{"subject": "...", "teacher": "...", "room": "..."}}
    }}
  ]
}}

Если ячейка пустая, не включай её.
Если top или bottom отсутствует, ставь null.
Если не удалось распознать, верни {{"weekday": {weekday}, "cells": []}}.
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


DAY_NAMES = {
    1: "Понедельник",
    2: "Вторник",
    3: "Среда",
    4: "Четверг",
    5: "Пятница",
    6: "Суббота",
    7: "Воскресенье",
}


def _group_positions(positions: list[int], max_gap: int = 1) -> list[list[int]]:
    if not positions:
        return []

    grouped = [[positions[0]]]
    for value in positions[1:]:
        if value - grouped[-1][-1] <= max_gap:
            grouped[-1].append(value)
        else:
            grouped.append([value])
    return grouped


def _group_centers(positions: list[int], max_gap: int = 1) -> list[int]:
    return [int(round(sum(group) / len(group))) for group in _group_positions(sorted(positions), max_gap=max_gap)]


def _max_dark_run(values: np.ndarray) -> int:
    best = 0
    current = 0
    for flag in values:
        if flag:
            current += 1
            best = max(best, current)
        else:
            current = 0
    return best


def _detect_day_bands(image_bytes: bytes) -> list[tuple[int, int, int]]:
    """
    Пытаемся найти полосы дней в типовом расписании колледжа по горизонтальным линиям таблицы.
    Возвращает список кортежей: (weekday, top, bottom).
    """
    image = Image.open(BytesIO(image_bytes)).convert("L")
    arr = np.array(image)
    height, width = arr.shape
    threshold = min(175, max(110, int(arr.mean() * 0.82)))
    dark = arr < threshold

    row_runs = [_max_dark_run(row) for row in dark]
    line_rows = [idx for idx, run in enumerate(row_runs) if run >= max(120, int(width * 0.22))]
    row_centers = [value for value in _group_centers(line_rows, max_gap=2) if value > 40]

    if len(row_centers) < 12:
        return []

    clusters = _group_positions(row_centers, max_gap=40)
    if len(clusters) < 5:
        return []

    clusters = clusters[:5]
    day_bands = []
    day_start = clusters[0][0]
    for weekday, cluster in enumerate(clusters, start=1):
        day_end = cluster[-1]
        day_bands.append((weekday, day_start, day_end))
        day_start = day_end

    return day_bands


def _detect_content_bounds(image_bytes: bytes) -> Optional[tuple[int, int]]:
    """
    Ищем вертикальные границы: после колонки с днями и до правой границы таблицы.
    """
    image = Image.open(BytesIO(image_bytes)).convert("L")
    arr = np.array(image)
    height, _width = arr.shape
    threshold = min(175, max(110, int(arr.mean() * 0.82)))
    dark = arr < threshold

    col_runs = [_max_dark_run(col) for col in dark.T]
    line_cols = [idx for idx, run in enumerate(col_runs) if run >= max(140, int(height * 0.12))]
    col_centers = _group_centers(line_cols, max_gap=2)

    if len(col_centers) < 4:
        return None

    left = max(0, col_centers[1] - 2)
    right = col_centers[-1]
    if right - left < 150:
        return None
    return left, right


def _crop_image_bytes(image_bytes: bytes, box: tuple[int, int, int, int]) -> bytes:
    image = Image.open(BytesIO(image_bytes))
    cropped = image.crop(box)
    output = BytesIO()
    cropped.save(output, format="PNG")
    return output.getvalue()


async def _parse_schedule_by_day_crops(
    image_bytes: bytes,
    media_type: str,
    group_name: str,
) -> Optional[dict]:
    day_bands = _detect_day_bands(image_bytes)
    content_bounds = _detect_content_bounds(image_bytes)

    if len(day_bands) < 5 or not content_bounds:
        return None

    left, right = content_bounds
    parsed_lessons = []

    for weekday, top, bottom in day_bands:
        crop_bytes = _crop_image_bytes(image_bytes, (left, top, right, bottom))
        prompt = SCHEDULE_DAY_PROMPT_TEMPLATE.format(
            day_name=DAY_NAMES.get(weekday, str(weekday)),
            weekday=weekday,
        )
        day_result = await _call_groq_vision(
            prompt=prompt,
            image_bytes=crop_bytes,
            media_type="image/png",
            max_tokens=2048,
        )
        _debug_dump_schedule(f"day_crop_{weekday}", day_result)
        if not isinstance(day_result, dict):
            continue

        cells_payload = {
            "groups": [{
                "group_name": group_name or "Группа",
                "cells": [
                    {
                        **cell,
                        "weekday": weekday,
                    }
                    for cell in day_result.get("cells", [])
                    if isinstance(cell, dict)
                ],
            }]
        }
        expanded = _expand_schedule_cells(cells_payload)
        if not expanded:
            continue
        group_lessons = expanded["groups"][0].get("lessons", [])
        parsed_lessons.extend(group_lessons)

    if not parsed_lessons:
        return None

    return {
        "groups": [{
            "group_name": group_name or "Группа",
            "lessons": parsed_lessons,
        }]
    }


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


def _schedule_candidate_score(result: Optional[dict]) -> int:
    if not result or not isinstance(result, dict):
        return -1

    groups = result.get("groups", [])
    lessons = []
    weekdays = set()
    for group in groups:
        for lesson in group.get("lessons", []):
            lessons.append(lesson)
            weekdays.add(int(lesson.get("weekday") or 0))

    score = len(lessons) * 10 + len(weekdays) * 20
    split_pairs = sum(1 for lesson in lessons if int(lesson.get("week_type") or 0) in (1, 2))
    every_week = sum(1 for lesson in lessons if int(lesson.get("week_type") or 0) == 0)
    score += split_pairs * 3 + every_week
    return score


def _normalize_lessons(raw_lessons: list) -> list[dict]:
    """
    Приводим уроки к единому виду после OCR.
    Мероприятия (is_event) всегда получают lesson_num=0.
    """
    out = []
    for raw in raw_lessons:
        if not isinstance(raw, dict):
            continue

        lesson = dict(raw)

        try:
            lesson["weekday"] = int(lesson.get("weekday"))
        except (TypeError, ValueError):
            continue

        subject = str(lesson.get("subject") or "").strip()
        if not subject:
            continue
        lesson["subject"] = subject

        # Определяем is_event ДО установки lesson_num
        try:
            lesson["is_event"] = int(lesson.get("is_event") or 0)
        except (ValueError, TypeError):
            lesson["is_event"] = 0

        # Автоопределение мероприятий по ключевым словам
        if any(keyword in subject.lower() for keyword in EVENT_KEYWORDS):
            lesson["is_event"] = 1

        # lesson_num: мероприятия = 0, остальные = как есть
        try:
            raw_num = int(lesson.get("lesson_num") or 0)
        except (TypeError, ValueError):
            raw_num = 0

        if lesson["is_event"]:
            lesson["lesson_num"] = 0
        else:
            lesson["lesson_num"] = raw_num

        lesson["teacher"] = str(lesson.get("teacher") or "").strip() or None
        lesson["room"] = str(lesson.get("room") or "").strip() or None
        lesson["time_start"] = str(lesson.get("time_start") or "").strip()
        lesson["time_end"] = str(lesson.get("time_end") or "").strip()

        try:
            lesson["week_type"] = int(lesson.get("week_type") or 0)
        except (ValueError, TypeError):
            lesson["week_type"] = 0

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
    Улучшенная пост-обработка OCR-результата:
    - мероприятия (is_event) → lesson_num=0, не влияют на нумерацию
    - consecutive дубли с одинаковыми lesson_num + content + week_type → схлопываем
    - сдвиг lesson_num назад ТОЛЬКО если мероприятие явно сдвинуло нумерацию
    - сохраняем исходную нумерацию regular-занятий (пропуски допустимы)
    """
    by_day: dict[int, list[dict]] = {}
    for lesson in lessons:
        by_day.setdefault(int(lesson.get("weekday") or 0), []).append(dict(lesson))

    repaired = []
    for weekday in sorted(by_day):
        day_lessons = by_day[weekday]
        events = []
        regular = []

        # 1. Разделяем мероприятия и обычные занятия
        for lesson in day_lessons:
            if int(lesson.get("is_event") or 0):
                event_lesson = dict(lesson)
                event_lesson["lesson_num"] = 0
                events.append(event_lesson)
            else:
                regular.append(dict(lesson))

        # 2. Удаляем consecutive дубли внутри regular (одинаковый lesson_num + content + week_type)
        deduped_regular = []
        prev_key = None
        for lesson in sorted(regular, key=lambda x: (
            int(x.get("lesson_num") or 0),
            int(x.get("week_type") or 0),
            str(x.get("subject") or ""),
        )):
            key = (
                int(lesson.get("lesson_num") or 0),
                int(lesson.get("week_type") or 0),
                str(lesson.get("subject") or ""),
                str(lesson.get("teacher") or ""),
                str(lesson.get("room") or ""),
            )
            if key != prev_key:
                deduped_regular.append(lesson)
                prev_key = key

        # 3. Сдвиг lesson_num: только если мероприятие явно сдвинуло нумерацию
        # Т.е.: есть events И первая regular пара имеет номер > 1 И lesson_num=1 отсутствует
        regular_numbers = sorted(
            {
                int(lesson.get("lesson_num") or 0)
                for lesson in deduped_regular
                if int(lesson.get("lesson_num") or 0) > 0
            }
        )

        if events and regular_numbers and regular_numbers[0] > 1 and 1 not in regular_numbers:
            # Мероприятие сдвинуло нумерацию → сдвигаем regular на -1
            for lesson in deduped_regular:
                old_num = int(lesson.get("lesson_num") or 0)
                if old_num > 0:
                    lesson["lesson_num"] = old_num - 1

        # 4. Собираем результат
        repaired.extend(events)
        repaired.extend(deduped_regular)

    return _dedupe_lessons(repaired)


def _correct_week_types_after_flat_ocr(result: dict) -> dict:
    """
    Исправляет ошибку плоского OCR-формата: модель часто назначает week_type=1
    всем одиночным парам (должно быть 0) и перепутывает верхнюю/нижнюю строки.

    Алгоритм для каждого дня:
    - Если НЕТ ни одной пары с week_type=0 → подозреваем ошибку
    - Одиночная пара с week_type=1 без парного week_type=2 → устанавливаем 0
    - Две пары с одним lesson_num (wt=1 и wt=2) → меняем местами
      (OCR выдаёт верхнюю строку как wt=2, нижнюю как wt=1 — обратно правильному)
    """
    if not result or "groups" not in result:
        return result

    corrected_groups = []
    for group in result.get("groups", []):
        lessons = list(group.get("lessons", []))

        # Группируем по дням
        by_day: dict = {}
        for l in lessons:
            wd = int(l.get("weekday") or 0)
            by_day.setdefault(wd, []).append(l)

        fixed_all = []
        for wd, day_lessons in sorted(by_day.items()):
            # Только обычные занятия (не мероприятия)
            regular = [l for l in day_lessons if not int(l.get("is_event") or 0)]
            events  = [l for l in day_lessons if int(l.get("is_event") or 0)]

            wt0 = sum(1 for l in regular if int(l.get("week_type") or 0) == 0)
            wt1 = sum(1 for l in regular if int(l.get("week_type") or 0) == 1)
            wt2 = sum(1 for l in regular if int(l.get("week_type") or 0) == 2)

            # Признак ошибки: нет ни одного week_type=0 И много week_type=1
            is_likely_wrong = (wt0 == 0 and wt1 > 0)

            if not is_likely_wrong:
                fixed_all.extend(day_lessons)
                continue

            # Группируем по lesson_num
            by_num: dict = {}
            for l in regular:
                num = int(l.get("lesson_num") or 0)
                by_num.setdefault(num, []).append(l)

            fixed_regular = []
            for num in sorted(by_num):
                variants = by_num[num]
                if len(variants) == 1:
                    # Единственная запись — занятие каждую неделю
                    l = dict(variants[0])
                    l["week_type"] = 0
                    fixed_regular.append(l)
                elif len(variants) == 2:
                    # Дробная пара:
                    # OCR назначает верхней строке wt=2, нижней — wt=1 (обратно)
                    # Сортируем по (wt, subject) → [wt=1(нижняя OCR), wt=2(верхняя OCR)]
                    sorted_v = sorted(variants, key=lambda x: (
                        int(x.get("week_type") or 0),
                        str(x.get("subject") or "")
                    ))
                    # Меняем местами: первый (wt=1, нижняя) → wt=2(чётная)
                    #                  второй (wt=2, верхняя) → wt=1(нечётная)
                    l_lower = dict(sorted_v[0])  # OCR: wt=1 (нижняя строка)
                    l_upper = dict(sorted_v[1])  # OCR: wt=2 (верхняя строка)
                    l_lower["week_type"] = 2  # нижняя = чётная неделя
                    l_upper["week_type"] = 1  # верхняя = нечётная неделя
                    fixed_regular.append(l_upper)  # нечётная первой
                    fixed_regular.append(l_lower)  # чётная второй
                else:
                    # 3+ вариантов — нестандартный случай, оставляем как есть
                    fixed_regular.extend(variants)

            fixed_all.extend(events)
            fixed_all.extend(fixed_regular)

        corrected_groups.append({**group, "lessons": fixed_all})

    return {**result, "groups": corrected_groups}


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
    normalized_cells = None
    if expanded_cells:
        normalized_cells = _normalize_schedule_result(expanded_cells)
        _debug_dump_schedule("normalized_cells", normalized_cells)

    crop_group_name = "Группа"
    if isinstance(cell_result, dict):
        try:
            crop_group_name = (
                cell_result.get("groups", [{}])[0].get("group_name")
                or crop_group_name
            )
        except Exception:
            pass

    day_crop_result = await _parse_schedule_by_day_crops(
        image_bytes=image_bytes,
        media_type=media_type,
        group_name=crop_group_name,
    )
    _debug_dump_schedule("day_crop_result", day_crop_result)
    normalized_day_crop = _normalize_schedule_result(day_crop_result) if day_crop_result else None
    _debug_dump_schedule("normalized_day_crop", normalized_day_crop)

    best_candidate = normalized_cells
    if _schedule_candidate_score(normalized_day_crop) > _schedule_candidate_score(best_candidate):
        best_candidate = normalized_day_crop

    initial_result = await _call_groq_vision(
        prompt=SCHEDULE_PROMPT,
        image_bytes=image_bytes,
        media_type=media_type,
        max_tokens=8192,
    )
    _debug_dump_schedule("initial_result", initial_result)

    if not initial_result or not isinstance(initial_result, dict):
        return best_candidate

    result = initial_result
    reviewed_result = await _review_schedule_parse(
        image_bytes=image_bytes,
        media_type=media_type,
        draft_result=initial_result,
    )
    _debug_dump_schedule("reviewed_result", reviewed_result)
    if reviewed_result:
        result = reviewed_result

    # Применяем коррекцию week_type к результатам плоского формата
    # (ячеечный формат уже имеет правильные week_type через _expand_schedule_cells)
    normalized = _correct_week_types_after_flat_ocr(_normalize_schedule_result(result))
    _debug_dump_schedule("normalized_result", normalized)
    if _schedule_candidate_score(normalized) > _schedule_candidate_score(best_candidate):
        best_candidate = normalized

    fallback_normalized = _correct_week_types_after_flat_ocr(_normalize_schedule_result(initial_result))
    if _schedule_candidate_score(fallback_normalized) > _schedule_candidate_score(best_candidate):
        best_candidate = fallback_normalized

    return best_candidate


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
