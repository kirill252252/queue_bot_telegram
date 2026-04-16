from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton


def schedule_main_keyboard(chat_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📸 Загрузить расписание", callback_data=f"sched_upload_new")],
        [InlineKeyboardButton(text="📋 Вся неделя", callback_data=f"sched_show")],
        [InlineKeyboardButton(text="📅 Сегодня", callback_data=f"sched_today")],
        [InlineKeyboardButton(text="🔕 Настройка очередей", callback_data=f"schedule_skip:{chat_id}")],
        [InlineKeyboardButton(text="📡 Источники", callback_data=f"schedule_sources:{chat_id}")],
    ])