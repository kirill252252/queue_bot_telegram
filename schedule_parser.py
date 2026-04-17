"""
schedule_parser.py — обёртка над schedule_ocr для обратной совместимости.
Весь реальный код парсинга живёт в schedule_ocr.py (Google Gemini API).
source_monitor.py импортирует parse_schedule_change отсюда.
"""

from schedule_ocr import (  # noqa: F401  (re-export)
    parse_schedule_image,
    parse_schedule_change,
    parse_change_image,
    parse_change_text,
    format_schedule,
)
