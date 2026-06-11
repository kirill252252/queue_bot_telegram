from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove

# постоянная клавиатура внизу экрана в личке
def pm_reply_keyboard() -> ReplyKeyboardMarkup:
    
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📋 Мои очереди"), KeyboardButton(text="🔍 Найти очередь")],
            [KeyboardButton(text="👤 Профиль / Ник"),  KeyboardButton(text="❓ Помощь")],
        ],
        resize_keyboard=True,
        persistent=True,
    )

# инлайн-меню главного экрана в личке
def pm_main_keyboard(has_queues: bool = True) -> InlineKeyboardMarkup:
    
    buttons = []
    if has_queues:
        buttons.append([InlineKeyboardButton(text="📋 Мои очереди", callback_data="pm_myqueues")])
        buttons.append([InlineKeyboardButton(text="🔍 Найти очередь", callback_data="pm_start")])
    else:
        buttons.append([InlineKeyboardButton(text="🔍 Найти очередь", callback_data="pm_start")])
    buttons.append([InlineKeyboardButton(text="👤 Мой профиль / Ник", callback_data="show_me")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

# список чатов для выбора очереди
def pm_chat_select_keyboard(queues_by_chat: list[dict], chat_names: dict) -> InlineKeyboardMarkup:
    seen, buttons = set(), []
    for q in queues_by_chat:
        cid = q["chat_id"]
        if cid in seen:
            continue
        seen.add(cid)
        name = chat_names.get(cid, f"Чат {cid}")
        buttons.append([InlineKeyboardButton(text=f"💬 {name}", callback_data=f"pm_chat:{cid}")])
    if not buttons:
        buttons.append([InlineKeyboardButton(text="— нет доступных групп —", callback_data="noop")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

# список очередей в выбранном чате
def pm_queue_select_keyboard(queues: list[dict], chat_id: int) -> InlineKeyboardMarkup:
    buttons = []
    for q in queues:
        slots = f" ({q['max_slots']} мест)" if q["max_slots"] > 0 else ""
        buttons.append([InlineKeyboardButton(
            text=f"📋 {q['name']}{slots}",
            callback_data=f"pm_queue:{q['id']}"
        )])
    buttons.append([InlineKeyboardButton(text="◀️ Назад к группам", callback_data="pm_start")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

# кнопки действий внутри очереди в личке
def pm_queue_actions_keyboard(queue_id: int, user_in: bool,
                              is_closed: bool, chat_id: int,
                              user_is_first: bool = False,
                              is_full: bool = False,
                              is_subscribed: bool = False,
                              is_frozen: bool = False) -> InlineKeyboardMarkup:
    buttons = []
    if not is_closed:
        if user_in:
            if user_is_first and not is_frozen:
                buttons.append([InlineKeyboardButton(
                    text="✅ Я прошёл, следующий!",
                    callback_data=f"done_next:{queue_id}"
                )])
            buttons.append([InlineKeyboardButton(
                text="🚪 Выйти из очереди",
                callback_data=f"pm_leave:{queue_id}"
            )])
            if is_frozen:
                buttons.append([InlineKeyboardButton(
                    text="🔥 Разморозить место",
                    callback_data=f"unfreeze:{queue_id}"
                )])
            else:
                buttons.append([
                    InlineKeyboardButton(text="🧊 Заморозить место", callback_data=f"freeze_menu:{queue_id}"),
                    InlineKeyboardButton(text="🔀 Обменяться", callback_data=f"swap_menu:{queue_id}"),
                ])
        else:
            if is_full:
                if is_subscribed:
                    buttons.append([InlineKeyboardButton(
                        text="🔕 Отписаться от уведомления",
                        callback_data=f"unsubscribe:{queue_id}"
                    )])
                else:
                    buttons.append([InlineKeyboardButton(
                        text="🔔 Уведомить когда освободится место",
                        callback_data=f"subscribe:{queue_id}"
                    )])
            else:
                buttons.append([InlineKeyboardButton(
                    text="✋ Занять место",
                    callback_data=f"pm_join:{queue_id}"
                )])
    buttons.append([
        InlineKeyboardButton(text="🔄 Обновить", callback_data=f"pm_queue:{queue_id}"),
        InlineKeyboardButton(text="◀️ Назад", callback_data=f"pm_chat:{chat_id}"),
    ])
    buttons.append([
        InlineKeyboardButton(text="🏠 Главное меню", callback_data="pm_home"),
    ])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

# кнопки очередей в группе — без админских действий, управление через /admin в личке
def queue_list_keyboard(queues: list[dict], is_admin: bool = False) -> InlineKeyboardMarkup:
    buttons = [[InlineKeyboardButton(text=f"📋 {q['name']}",
                                      callback_data=f"view_queue:{q['id']}")]
               for q in queues]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

# действия с конкретной очередью в группе — только пользовательские кнопки
def queue_actions_keyboard(queue_id: int, user_in: bool,
                           is_admin: bool = False, is_closed: bool = False,
                           user_is_first: bool = False) -> InlineKeyboardMarkup:
    buttons = []
    if not is_closed:
        if user_in:
            if user_is_first:
                buttons.append([InlineKeyboardButton(
                    text="✅ Я прошёл, следующий!",
                    callback_data=f"done_next:{queue_id}"
                )])
            buttons.append([InlineKeyboardButton(
                text="🚪 Выйти из очереди",
                callback_data=f"leave:{queue_id}"
            )])
        else:
            buttons.append([InlineKeyboardButton(
                text="✋ Занять место",
                callback_data=f"join:{queue_id}"
            )])
    buttons.append([
        InlineKeyboardButton(text="🔄 Обновить", callback_data=f"view_queue:{queue_id}"),
        InlineKeyboardButton(text="◀️ Назад",    callback_data="back_to_list"),
    ])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

# настройки очереди для админа
def queue_settings_keyboard(queue_id: int, notify_leave: bool,
                            remind_min: int, auto_kick: bool) -> InlineKeyboardMarkup:
    leave_icon  = "🔔" if notify_leave else "🔕"
    kick_icon   = "⚡" if auto_kick    else "🔕"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text=f"{leave_icon} Анонс выхода: {'вкл' if notify_leave else 'выкл'}",
            callback_data=f"toggle_leave_notif:{queue_id}"
        )],
        [InlineKeyboardButton(
            text=f"{kick_icon} Авто-кик: {'вкл' if auto_kick else 'выкл'}",
            callback_data=f"toggle_autokick:{queue_id}"
        )],
        [InlineKeyboardButton(
            text=f"⏱ Таймаут: {remind_min} мин",
            callback_data=f"set_remind:{queue_id}"
        )],
        [InlineKeyboardButton(text="◀️ Назад", callback_data=f"view_queue:{queue_id}")],
    ])

def kick_members_keyboard(queue_id: int, members: list[dict]) -> InlineKeyboardMarkup:
    buttons = [[InlineKeyboardButton(
        text=f"#{m['position']} {m['display_name']}",
        callback_data=f"kick:{queue_id}:{m['user_id']}"
    )] for m in members]
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data=f"view_queue:{queue_id}")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def confirm_keyboard(action: str, entity_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Да",  callback_data=f"confirm_{action}:{entity_id}"),
        InlineKeyboardButton(text="❌ Нет", callback_data=f"view_queue:{entity_id}"),
    ]])

def cancel_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_fsm")
    ]])

def nick_group_select_keyboard(queues: list[dict], chat_names: dict,
                                action: str) -> InlineKeyboardMarkup:

    seen, buttons = set(), []
    for q in queues:
        cid = q["chat_id"]
        if cid in seen:
            continue
        seen.add(cid)
        name = chat_names.get(cid, f"Чат {cid}")
        buttons.append([InlineKeyboardButton(
            text=f"💬 {name}",
            callback_data=f"{action}_nick_group:{cid}"
        )])
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data="show_me")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def me_keyboard(has_any_nick: bool) -> InlineKeyboardMarkup:
    buttons = [[InlineKeyboardButton(text="✏️ Установить ник для группы",
                                      callback_data="set_nick_choose_group")]]
    if has_any_nick:
        buttons.append([InlineKeyboardButton(text="🗑 Сбросить ник для группы",
                                              callback_data="reset_nick_choose_group")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

# выбор времени заморозки
def freeze_keyboard(queue_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="5 мин",  callback_data=f"freeze:{queue_id}:5"),
            InlineKeyboardButton(text="10 мин", callback_data=f"freeze:{queue_id}:10"),
            InlineKeyboardButton(text="15 мин", callback_data=f"freeze:{queue_id}:15"),
            InlineKeyboardButton(text="30 мин", callback_data=f"freeze:{queue_id}:30"),
        ],
        [InlineKeyboardButton(text="❌ Отмена", callback_data=f"pm_queue:{queue_id}")],
    ])

# список участников для обмена позицией
def swap_select_keyboard(queue_id: int, members: list[dict], my_user_id: int) -> InlineKeyboardMarkup:
    buttons = []
    for m in members:
        if m["user_id"] == my_user_id:
            continue
        buttons.append([InlineKeyboardButton(
            text=f"#{m['position']} {m['display_name']}",
            callback_data=f"swap_request:{queue_id}:{m['user_id']}"
        )])
    buttons.append([InlineKeyboardButton(text="❌ Отмена", callback_data=f"pm_queue:{queue_id}")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

# подтверждение/отклонение запроса на обмен
def swap_confirm_keyboard(request_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Принять обмен", callback_data=f"swap_accept:{request_id}"),
        InlineKeyboardButton(text="❌ Отклонить",     callback_data=f"swap_decline:{request_id}"),
    ]])


# ── Админ-панель в личке ──────────────────────────────────────────────────────

def admin_panel_keyboard(chat_id: int, chat_name: str) -> InlineKeyboardMarkup:
    """Главное меню админ-панели для конкретной группы."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📋 Очереди",         callback_data=f"adm_queues:{chat_id}")],
        [InlineKeyboardButton(text="➕ Создать очередь",  callback_data=f"adm_create:{chat_id}")],
        [InlineKeyboardButton(text="👥 Бот-администраторы", callback_data=f"adm_admins:{chat_id}")],
        [InlineKeyboardButton(text="🌍 Часовой пояс",    callback_data=f"adm_tz:{chat_id}")],
        [InlineKeyboardButton(text="📊 Статистика",      callback_data=f"adm_stats:{chat_id}")],
    ])


def admin_queue_list_keyboard(queues: list[dict], chat_id: int) -> InlineKeyboardMarkup:
    """Список очередей в админ-панели."""
    buttons = [[InlineKeyboardButton(
        text=f"{'🟢' if q['is_active'] else '🔴'} {q['name']}",
        callback_data=f"adm_queue:{q['id']}"
    )] for q in queues]
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data=f"adm_home:{chat_id}")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def admin_queue_actions_keyboard(queue_id: int, chat_id: int,
                                  is_closed: bool) -> InlineKeyboardMarkup:
    """Действия с конкретной очередью в админ-панели."""
    buttons = []
    if not is_closed:
        buttons.append([
            InlineKeyboardButton(text="🔒 Закрыть",  callback_data=f"adm_close:{queue_id}"),
            InlineKeyboardButton(text="🗑 Удалить",  callback_data=f"adm_delete:{queue_id}"),
        ])
        buttons.append([
            InlineKeyboardButton(text="👢 Кикнуть участника",
                                 callback_data=f"adm_kick_menu:{queue_id}"),
            InlineKeyboardButton(text="➕ Добавить участника",
                                 callback_data=f"adm_add_member:{queue_id}"),
        ])
        buttons.append([InlineKeyboardButton(text="🔗 Ссылка-приглашение",
                                              callback_data=f"adm_invite:{queue_id}")])
    buttons.append([
        InlineKeyboardButton(text="⚙️ Настройки", callback_data=f"adm_settings:{queue_id}"),
        InlineKeyboardButton(text="📥 CSV",        callback_data=f"adm_export:{queue_id}"),
    ])
    buttons.append([
        InlineKeyboardButton(text="📋 Создать по шаблону", callback_data=f"adm_clone:{queue_id}"),
    ])
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data=f"adm_queues:{chat_id}")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def admin_queue_settings_keyboard(queue_id: int, chat_id: int, notify_leave: bool,
                                   remind_min: int, auto_kick: bool) -> InlineKeyboardMarkup:
    leave_icon = "🔔" if notify_leave else "🔕"
    kick_icon  = "⚡" if auto_kick    else "🔕"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text=f"{leave_icon} Анонс выхода: {'вкл' if notify_leave else 'выкл'}",
            callback_data=f"adm_toggle_leave:{queue_id}"
        )],
        [InlineKeyboardButton(
            text=f"{kick_icon} Авто-кик: {'вкл' if auto_kick else 'выкл'}",
            callback_data=f"adm_toggle_kick:{queue_id}"
        )],
        [InlineKeyboardButton(
            text=f"⏱ Таймаут: {remind_min} мин",
            callback_data=f"adm_set_remind:{queue_id}"
        )],
        [InlineKeyboardButton(text="◀️ Назад", callback_data=f"adm_queue:{queue_id}")],
    ])


def admin_kick_keyboard(queue_id: int, members: list[dict]) -> InlineKeyboardMarkup:
    buttons = [[InlineKeyboardButton(
        text=f"#{m['position']} {m['display_name']}",
        callback_data=f"adm_kick:{queue_id}:{m['user_id']}"
    )] for m in members]
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data=f"adm_queue:{queue_id}")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def admin_chat_select_keyboard(chats: list[dict]) -> InlineKeyboardMarkup:
    """Выбор группы для управления — если у админа несколько групп."""
    buttons = [[InlineKeyboardButton(
        text=f"💬 {c.get('title') or c['chat_id']}",
        callback_data=f"adm_home:{c['chat_id']}"
    )] for c in chats]
    return InlineKeyboardMarkup(inline_keyboard=buttons)