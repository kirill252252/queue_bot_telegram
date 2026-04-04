"""
Web admin panel — FastAPI + Jinja2.
Run alongside the bot. Access at http://yourserver:8080
Password set via WEB_PANEL_PASSWORD in .env
"""
import asyncio
import secrets
from datetime import datetime

from fastapi import FastAPI, Request, Depends, HTTPException, status
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials

import database as db
from config import WEB_PANEL_PASSWORD

app = FastAPI(title="Queue Bot Admin", docs_url=None, redoc_url=None)
security = HTTPBasic()

PANEL_USER = "admin"


def check_auth(credentials: HTTPBasicCredentials = Depends(security)):
    ok_user = secrets.compare_digest(credentials.username.encode(), PANEL_USER.encode())
    ok_pass = secrets.compare_digest(credentials.password.encode(), WEB_PANEL_PASSWORD.encode())
    if not (ok_user and ok_pass):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Неверный пароль",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username


HTML_STYLE = """
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
         background: #0f0f0f; color: #e0e0e0; }
  .header { background: #1a1a2e; padding: 16px 32px; display: flex;
            align-items: center; gap: 12px; border-bottom: 1px solid #2a2a4a; }
  .header h1 { font-size: 20px; color: #7c7cff; }
  .container { max-width: 1100px; margin: 0 auto; padding: 24px 32px; }
  .stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
           gap: 16px; margin-bottom: 32px; }
  .stat-card { background: #1a1a2e; border-radius: 12px; padding: 20px;
               border: 1px solid #2a2a4a; text-align: center; }
  .stat-card .num { font-size: 36px; font-weight: 700; color: #7c7cff; }
  .stat-card .label { font-size: 12px; color: #888; margin-top: 4px; }
  .section-title { font-size: 16px; font-weight: 600; color: #aaa;
                   margin: 24px 0 12px; text-transform: uppercase; letter-spacing: 1px; }
  table { width: 100%; border-collapse: collapse; background: #1a1a2e;
          border-radius: 12px; overflow: hidden; border: 1px solid #2a2a4a; }
  th { background: #13132a; padding: 12px 16px; text-align: left;
       font-size: 12px; color: #888; text-transform: uppercase; letter-spacing: 0.5px; }
  td { padding: 12px 16px; border-top: 1px solid #2a2a3a; font-size: 14px; }
  tr:hover td { background: #1e1e38; }
  .badge { display: inline-block; padding: 2px 8px; border-radius: 99px;
           font-size: 11px; font-weight: 600; }
  .badge-green { background: #1a3a2a; color: #4caf82; }
  .badge-red { background: #3a1a1a; color: #cf6679; }
  .badge-blue { background: #1a2a3a; color: #6694cf; }
  a { color: #7c7cff; text-decoration: none; }
  a:hover { text-decoration: underline; }
  .empty { color: #555; font-style: italic; }
  .refresh { float: right; font-size: 12px; color: #555; }
</style>
"""


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request, user: str = Depends(check_auth)):
    stats = await db.get_global_stats()
    chats = await db.get_known_chats()

    chat_rows = ""
    for c in chats:
      queues = await db.get_chat_queues(c["chat_id"])
      s = await db.get_stats(c["chat_id"])
      chat_rows += f"""
        <tr>
          <td><a href="/chat/{c['chat_id']}">{c.get('title') or c['chat_id']}</a></td>
          <td>{c['chat_id']}</td>
          <td>{s['active_queues']} / {s['total_queues']}</td>
          <td>{s['total_members']}</td>
          <td>{s['unique_users']}</td>
        </tr>"""

    now = datetime.utcnow().strftime("%H:%M:%S UTC")
    html = f"""<!DOCTYPE html><html><head><title>Queue Bot Admin</title>{HTML_STYLE}</head><body>
    <div class="header">
      <span style="font-size:24px">🤖</span>
      <h1>Queue Bot — Панель администратора</h1>
      <span class="refresh">Обновлено: {now} &nbsp;<a href="/">[↻]</a></span>
    </div>
    <div class="container">
      <div class="stats">
        <div class="stat-card"><div class="num">{stats['total_chats']}</div><div class="label">Групп</div></div>
        <div class="stat-card"><div class="num">{stats['active_queues']}</div><div class="label">Активных очередей</div></div>
        <div class="stat-card"><div class="num">{stats['total_queues']}</div><div class="label">Всего очередей</div></div>
        <div class="stat-card"><div class="num">{stats['total_members']}</div><div class="label">Участников</div></div>
        <div class="stat-card"><div class="num">{stats['total_users']}</div><div class="label">Пользователей</div></div>
      </div>
      <div class="section-title">Группы</div>
      <table>
        <thead><tr><th>Название</th><th>Chat ID</th><th>Очереди (акт/всего)</th><th>Участников</th><th>Уник. юзеров</th></tr></thead>
        <tbody>{chat_rows or '<tr><td colspan=5 class="empty">Нет групп</td></tr>'}</tbody>
      </table>
    </div></body></html>"""
    return HTMLResponse(html)


@app.get("/chat/{chat_id}", response_class=HTMLResponse)
async def chat_detail(chat_id: int, user: str = Depends(check_auth)):
    chats = await db.get_known_chats()
    chat = next((c for c in chats if c["chat_id"] == chat_id), None)
    title = chat["title"] if chat else str(chat_id)

    queues = await db.get_chat_queues(chat_id)
    # Also get inactive
    import aiosqlite
    async with aiosqlite.connect(db.DB_PATH) as conn:
        conn.row_factory = aiosqlite.Row
        cur = await conn.execute(
            "SELECT * FROM queues WHERE chat_id=? AND is_active=0 ORDER BY created_at DESC LIMIT 10",
            (chat_id,))
        closed = [dict(r) for r in await cur.fetchall()]

    queue_html = ""
    for q in queues + closed:
        members = await db.get_queue_members(q["id"])
        status_badge = '<span class="badge badge-green">открыта</span>' if q["is_active"] else '<span class="badge badge-red">закрыта</span>'
        slots = f"{len(members)}/{q['max_slots']}" if q["max_slots"] else str(len(members))
        member_rows = "".join(
            f"<tr><td>#{m['position']}</td><td>{m['display_name']}</td>"
            f"<td>{'@'+m['username'] if m.get('username') else '—'}</td>"
            f"<td>{m['user_id']}</td><td>{m.get('joined_at','')}</td></tr>"
            for m in members
        ) or "<tr><td colspan=5 class='empty'>Очередь пуста</td></tr>"

        queue_html += f"""
        <div style="margin-bottom:24px">
          <div style="display:flex;align-items:center;gap:10px;margin-bottom:8px">
            <span style="font-size:15px;font-weight:600">{q['name']}</span>
            {status_badge}
            <span class="badge badge-blue">{slots} мест</span>
            <span style="color:#555;font-size:12px">#{q['id']}</span>
          </div>
          <table>
            <thead><tr><th>#</th><th>Имя</th><th>Username</th><th>User ID</th><th>Вступил</th></tr></thead>
            <tbody>{member_rows}</tbody>
          </table>
        </div>"""

    html = f"""<!DOCTYPE html><html><head><title>{title}</title>{HTML_STYLE}</head><body>
    <div class="header">
      <a href="/" style="color:#555;font-size:20px">←</a>
      <span style="font-size:24px">💬</span>
      <h1>{title}</h1>
    </div>
    <div class="container">
      <div class="section-title">Очереди</div>
      {queue_html or '<p class="empty">Нет очередей</p>'}
    </div></body></html>"""
    return HTMLResponse(html)
