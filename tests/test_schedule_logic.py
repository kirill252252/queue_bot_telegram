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

    def test_expand_schedule_cells_preserves_split_and_duplicate_numbers(self):
        parsed = {
            "groups": [{
                "group_name": "П-5-24",
                "cells": [
                    {
                        "weekday": 1,
                        "lesson_num": 1,
                        "week_mode": "every_week",
                        "is_event": 0,
                        "top": {
                            "subject": "Инструментальные средства разработки программного обеспечения",
                            "teacher": "Наприенко ЕМ",
                            "room": "509",
                        },
                        "bottom": None,
                    },
                    {
                        "weekday": 1,
                        "lesson_num": 2,
                        "week_mode": "odd_even",
                        "is_event": 0,
                        "top": {
                            "subject": "Инструментальные средства разработки программного обеспечения",
                            "teacher": "Наприенко ЕМ",
                            "room": "509",
                        },
                        "bottom": {
                            "subject": "Программирование web-приложений",
                            "teacher": "Вахитов РГ",
                            "room": "410",
                        },
                    },
                    {
                        "weekday": 3,
                        "lesson_num": 1,
                        "week_mode": "even_only",
                        "is_event": 0,
                        "top": None,
                        "bottom": {
                            "subject": "Математическое моделирование",
                            "teacher": "Мережникова ЕМ",
                            "room": "404",
                        },
                    },
                    {
                        "weekday": 3,
                        "lesson_num": 5,
                        "week_mode": "odd_only",
                        "is_event": 0,
                        "top": {
                            "subject": "Иностранный язык в профессиональной деятельности",
                            "teacher": "Данилова АА",
                            "room": "Л610",
                        },
                        "bottom": None,
                    },
                ],
            }]
        }

        expanded = schedule_ocr._expand_schedule_cells(parsed)
        lessons = expanded["groups"][0]["lessons"]

        monday = [(lesson["lesson_num"], lesson["subject"], lesson["week_type"]) for lesson in lessons if lesson["weekday"] == 1]
        wednesday = [(lesson["lesson_num"], lesson["subject"], lesson["week_type"]) for lesson in lessons if lesson["weekday"] == 3]

        self.assertEqual(
            monday,
            [
                (1, "Инструментальные средства разработки программного обеспечения", 0),
                (2, "Инструментальные средства разработки программного обеспечения", 1),
                (2, "Программирование web-приложений", 2),
            ],
        )
        self.assertEqual(
            wednesday,
            [
                (1, "Математическое моделирование", 2),
                (5, "Иностранный язык в профессиональной деятельности", 1),
            ],
        )


if __name__ == "__main__":
    unittest.main()
