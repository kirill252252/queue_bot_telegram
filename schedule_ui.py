from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton


def schedule_main_keyboard(chat_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📸 Загрузить расписание", callback_data="sched_upload_new")],
        [
            InlineKeyboardButton(text="📋 Вся неделя", callback_data="sched_show"),
            InlineKeyboardButton(text="📅 Сегодня",    callback_data="sched_today"),
        ],
        [
            InlineKeyboardButton(text="1️⃣ Нечётная",   callback_data="sched_show_odd"),
            InlineKeyboardButton(text="2️⃣ Чётная",     callback_data="sched_show_even"),
        ],
        [
            InlineKeyboardButton(text="✏️ Редактировать",    callback_data=f"sched_edit:{chat_id}"),
            InlineKeyboardButton(text="📋 Изменить на дату", callback_data=f"sched_override:{chat_id}"),
        ],
        [
            InlineKeyboardButton(text="🔔 Звонки",           callback_data=f"sched_bells:{chat_id}"),
            InlineKeyboardButton(text="🔕 Очереди",          callback_data=f"schedule_skip:{chat_id}"),
        ],
        [
            InlineKeyboardButton(text="📣 Уведомления",      callback_data=f"sched_notify:{chat_id}"),
            InlineKeyboardButton(text="📡 Источники",        callback_data=f"schedule_sources:{chat_id}"),
        ],
    ])
