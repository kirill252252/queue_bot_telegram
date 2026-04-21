import sys
import types
import unittest
from datetime import date

sys.modules.setdefault("aiosqlite", types.ModuleType("aiosqlite"))

import schedule_db
import schedule_ocr
from schedule_group_match import (
    build_group_lookup,
    normalize_group_name,
    resolve_target_groups,
)


class WeekTypeTests(unittest.TestCase):
    def test_reference_week_flips_every_monday(self):
        reference = date(2026, 4, 20)

        self.assertEqual(schedule_db.get_week_type_for_date(date(2026, 4, 20), reference, 1), 1)
        self.assertEqual(schedule_db.get_week_type_for_date(date(2026, 4, 26), reference, 1), 1)
        self.assertEqual(schedule_db.get_week_type_for_date(date(2026, 4, 27), reference, 1), 2)
        self.assertEqual(schedule_db.get_week_type_for_date(date(2026, 5, 4), reference, 1), 1)

    def test_reference_week_supports_dates_before_anchor(self):
        reference = date(2026, 4, 20)
        self.assertEqual(schedule_db.get_week_type_for_date(date(2026, 4, 13), reference, 1), 2)

    def test_even_reference_type_is_respected(self):
        reference = date(2026, 4, 20)
        self.assertEqual(schedule_db.get_week_type_for_date(date(2026, 4, 20), reference, 2), 2)
        self.assertEqual(schedule_db.get_week_type_for_date(date(2026, 4, 27), reference, 2), 1)


class GroupMatchTests(unittest.TestCase):
    def test_normalize_group_name_ignores_prefixes_and_separators(self):
        self.assertEqual(normalize_group_name("гр. П-5-24"), normalize_group_name("П 5 24"))
        self.assertEqual(normalize_group_name("Группа П-5-24"), normalize_group_name("п-5-24"))

    def test_resolve_target_groups_matches_normalized_name(self):
        groups = [
            {"id": 1, "group_name": "П-5-24"},
            {"id": 2, "group_name": "ИС-1-24"},
        ]
        lookup = build_group_lookup(groups)

        targets = resolve_target_groups({"group": "гр. П-5-24"}, groups, lookup)
        self.assertEqual([g["id"] for g in targets], [1])

    def test_resolve_target_groups_returns_all_groups_when_group_missing(self):
        groups = [
            {"id": 1, "group_name": "П-5-24"},
            {"id": 2, "group_name": "ИС-1-24"},
        ]
        lookup = build_group_lookup(groups)

        targets = resolve_target_groups({}, groups, lookup)
        self.assertEqual([g["id"] for g in targets], [1, 2])

    def test_resolve_target_groups_does_not_fall_back_to_all_on_unknown_group(self):
        groups = [{"id": 1, "group_name": "П-5-24"}]
        lookup = build_group_lookup(groups)

        self.assertEqual(resolve_target_groups({"group": "ПК-10-25"}, groups, lookup), [])


class EditableFieldTests(unittest.TestCase):
    def test_week_controls_are_allowed_for_manual_updates(self):
        required = {"skip_queue", "week_type", "is_event"}
        self.assertTrue(required.issubset(schedule_db._ALLOWED_LESSON_UPDATE_FIELDS))


class OcrPostProcessTests(unittest.TestCase):
    def test_event_row_does_not_shift_regular_lessons(self):
        lessons = [
            {
                "weekday": 1,
                "lesson_num": 1,
                "subject": "Разговоры о важном",
                "teacher": "Куратор",
                "room": "",
                "week_type": 0,
                "is_event": 1,
            },
            {
                "weekday": 1,
                "lesson_num": 2,
                "subject": "Инструментальные средства разработки программного обеспечения",
                "teacher": "Наприенко ЕМ",
                "room": "509",
                "week_type": 0,
                "is_event": 0,
            },
            {
                "weekday": 1,
                "lesson_num": 3,
                "subject": "Дискретная математика",
                "teacher": "Бронникова ЕН",
                "room": "212",
                "week_type": 0,
                "is_event": 0,
            },
        ]

        repaired = schedule_ocr._repair_group_lessons(lessons)
        regular = [lesson for lesson in repaired if not lesson.get("is_event")]
        event = next(lesson for lesson in repaired if lesson.get("is_event"))

        self.assertEqual(event["lesson_num"], 0)
        self.assertEqual([lesson["lesson_num"] for lesson in regular], [1, 2])

    def test_day_without_event_is_not_shifted(self):
        lessons = [
            {
                "weekday": 2,
                "lesson_num": 1,
                "subject": "Разработка программных модулей",
                "teacher": "Петрова АА",
                "room": "С-6",
                "week_type": 0,
                "is_event": 0,
            },
            {
                "weekday": 2,
                "lesson_num": 3,
                "subject": "История России",
                "teacher": "Андриевская НМ",
                "room": "303",
                "week_type": 0,
                "is_event": 0,
            },
        ]

        repaired = schedule_ocr._repair_group_lessons(lessons)
        self.assertEqual([lesson["lesson_num"] for lesson in repaired], [1, 3])


if __name__ == "__main__":
    unittest.main()
