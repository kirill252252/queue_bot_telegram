from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton


def schedule_main_keyboard(chat_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="📸 Загрузить новое расписание",
            callback_data=f"sched_upload_new:{chat_id}"
        )],
        [InlineKeyboardButton(
            text="📋 Вся неделя",
            callback_data=f"sched_show:{chat_id}"
        )],
        [InlineKeyboardButton(
            text="📅 На сегодня",
            callback_data=f"sched_today:{chat_id}"
        )],
    ])