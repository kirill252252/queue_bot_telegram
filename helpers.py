def format_queue_info(queue: dict, members: list[dict]) -> str:
    name     = queue["name"]
    desc     = queue.get("description") or ""
    max_s    = queue["max_slots"]
    active   = queue["is_active"]
    count    = len(members)
    status   = "🟢 Открыта" if active else "🔴 Закрыта"
    slots_s  = f"{count}/{max_s}" if max_s > 0 else str(count)

    lines = [f"📋 <b>{name}</b>  {status}"]
    if desc:
        lines.append(f"ℹ️ {desc}")
    lines.append(f"👥 Участников: {slots_s}")
    lines.append("")
    if members:
        lines.append("<b>Очередь:</b>")
        for m in members:
            lines.append(f"  <b>#{m['position']}</b> {m['display_name']}")
    else:
        lines.append("😶 Очередь пуста")
    return "\n".join(lines)

def format_queue_list(queues: list[dict]) -> str:
    if not queues:
        return "В этом чате пока нет активных очередей.\nСоздайте первую! 👇"
    lines = ["<b>Активные очереди в этом чате:</b>", ""]
    for q in queues:
        slot = f" (макс. {q['max_slots']})" if q["max_slots"] > 0 else ""
        lines.append(f"📋 <b>{q['name']}</b>{slot}")
    return "\n".join(lines)

def format_pm_my_queues(entries: list[dict]) -> str:
    if not entries:
        return (
            "У тебя нет активных очередей.\n\n"
            "Используй /start чтобы выбрать группу и встать в очередь."
        )
    lines = ["<b>Твои активные очереди:</b>", ""]
    prev_chat = None
    for e in entries:
        if e["chat_id"] != prev_chat:
            chat_label = e.get("chat_name") or f"Чат {e['chat_id']}"
            lines.append(f"\n💬 <b>{chat_label}</b>")
            prev_chat = e["chat_id"]
        lines.append(f"  📋 {e['queue_name']}  →  место <b>#{e['position']}</b>")
    return "\n".join(lines)
