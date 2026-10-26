import os
import json
import time
import hashlib
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import asyncpg
from fastapi.middleware.cors import CORSMiddleware

DATABASE_URL = os.environ.get("DATABASE_URL")

class ConnectionManager:
    def __init__(self):
        self.active_connections: dict[str, WebSocket] = {}
        self.spam_tracker = {} # Для защиты от спама
        
    async def connect(self, login: str, websocket: WebSocket):
        await websocket.accept()
        self.active_connections[login] = websocket
        
    def disconnect(self, login: str):
        if login in self.active_connections:
            del self.active_connections[login]
            
    async def send_personal_message(self, message: str, recipient: str):
        if recipient in self.active_connections:
            try: await self.active_connections[recipient].send_text(message)
            except Exception: self.disconnect(recipient)
            
    def is_spam(self, login: str) -> bool:
        now = time.time()
        if login not in self.spam_tracker:
            self.spam_tracker[login] = []
        # Очищаем старые записи (старше 5 секунд)
        self.spam_tracker[login] = [t for t in self.spam_tracker[login] if now - t < 5]
        
        # Если больше 10 сообщений за 5 секунд - это спам
        if len(self.spam_tracker[login]) >= 10:
            return True
            
        self.spam_tracker[login].append(now)
        return False

manager = ConnectionManager()
db_pool = None

async def init_db():
    global db_pool
    if not DATABASE_URL: return
    try:
        db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5, statement_cache_size=0)
        print("✅ База данных готова!")
    except Exception as e:
        print(f"❌ Ошибка БД: {e}")

@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    yield
    if db_pool: await db_pool.close()

app = FastAPI(lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    login = None
    try:
        while True:
            data = await websocket.receive_text()
            if data == "ping": continue
            parsed = json.loads(data)
            action = parsed.get("action")

            if action == "register":
                user_login = parsed.get("login").lower()
                pwd = parsed.get("password")
                nick = parsed.get("nickname")
                uname = parsed.get("username").lower()
                bday = parsed.get("birthday")
                pwd_hash = hashlib.sha256(pwd.encode()).hexdigest()
                
                async with db_pool.acquire() as conn:
                    try:
                        await conn.execute(
                            "INSERT INTO users (login, password_hash, nickname, username, birthday) VALUES ($1, $2, $3, $4, $5)",
                            user_login, pwd_hash, nick, uname, bday
                        )
                        await websocket.send_text(json.dumps({"status": "ok", "msg": "Аккаунт создан"}))
                    except asyncpg.exceptions.UniqueViolationError:
                        await websocket.send_text(json.dumps({"status": "error", "msg": "Этот номер/почта или @username уже заняты!"}))

            elif action == "login":
                login = parsed.get("login").lower()
                pwd = parsed.get("password")
                pwd_hash = hashlib.sha256(pwd.encode()).hexdigest()
                async with db_pool.acquire() as conn:
                    user = await conn.fetchrow("SELECT * FROM users WHERE login=$1 AND password_hash=$2", login, pwd_hash)
                    if not user:
                        await websocket.send_text(json.dumps({"status": "error", "msg": "Неверный логин или пароль"}))
                        continue
                    
                    manager.active_connections[login] = websocket
                    await conn.execute("UPDATE users SET last_seen=NOW() WHERE login=$1", login)
                    
                    # Загружаем комнаты
                    rooms = await conn.fetch("SELECT r.* FROM rooms r JOIN room_members m ON r.id = m.room_id WHERE m.user_login = $1", login)
                    room_list = []
                    for r in rooms:
                        if not r["is_group"]:
                            other = await conn.fetchval("SELECT user_login FROM room_members WHERE room_id=$1 AND user_login!=$2", r["id"], login)
                            room_list.append({"id": r["id"], "name": other or "Удален", "is_group": False})
                        else: room_list.append({"id": r["id"], "name": r["name"], "is_group": True})
                    
                    # Загружаем контакты и блокировки
                    blocked = [b["blocked_login"] for b in await conn.fetch("SELECT blocked_login FROM blocks WHERE owner_login=$1", login)]
                    contacts = [c["contact_login"] for c in await conn.fetch("SELECT contact_login FROM contacts WHERE owner_login=$1", login)]
                    
                    all_users = [{"login": u["login"], "nickname": u["nickname"], "username": u["username"]} for u in await conn.fetch("SELECT login, nickname, username FROM users WHERE login != $1", login)]
                    
                    settings = {"hide_online": user["hide_online"], "auto_reply": user["auto_reply"], "nickname": user["nickname"], "username": user["username"]}
                    
                    await websocket.send_text(json.dumps({"status": "ok", "rooms": room_list, "all_users": all_users, "blocked": blocked, "contacts": contacts, "settings": settings}))

            elif action == "update_settings" and login:
                async with db_pool.acquire() as conn:
                    await conn.execute("UPDATE users SET hide_online=$1, auto_reply=$2 WHERE login=$3", 
                                       parsed.get("hide_online", False), parsed.get("auto_reply", ""), login)
                    await websocket.send_text(json.dumps({"status": "ok", "msg": "Настройки сохранены"}))

            elif action == "block_user" and login:
                target = parsed.get("target")
                async with db_pool.acquire() as conn:
                    await conn.execute("INSERT INTO blocks (owner_login, blocked_login) VALUES ($1, $2) ON CONFLICT DO NOTHING", login, target)
                    await websocket.send_text(json.dumps({"action": "blocked", "msg": f"{target} в чёрном списке"}))

            elif action == "add_contact" and login:
                target = parsed.get("target")
                async with db_pool.acquire() as conn:
                    await conn.execute("INSERT INTO contacts (owner_login, contact_login) VALUES ($1, $2) ON CONFLICT DO NOTHING", login, target)
                    await websocket.send_text(json.dumps({"status": "ok", "msg": f"{target} добавлен в контакты"}))

            elif action == "start_dm" and login:
                target = parsed.get("target")
                async with db_pool.acquire() as conn:
                    existing = await conn.fetchval("""
                        SELECT r.id FROM rooms r 
                        JOIN room_members m1 ON r.id = m1.room_id 
                        JOIN room_members m2 ON r.id = m2.room_id 
                        WHERE r.is_group = FALSE AND m1.user_login = $1 AND m2.user_login = $2
                    """, login, target)
                    
                    if not existing:
                        room_id = await conn.fetchval("INSERT INTO rooms (is_group) VALUES (FALSE) RETURNING id")
                        await conn.execute("INSERT INTO room_members (room_id, user_login) VALUES ($1, $2), ($1, $3)", room_id, login, target)
                    else: room_id = existing
                    await websocket.send_text(json.dumps({"action": "dm_started", "room_id": room_id, "target": target}))

            elif action == "get_history" and login:
                room_id = parsed.get("room_id")
                async with db_pool.acquire() as conn:
                    msgs = await conn.fetch("SELECT * FROM messages WHERE room_id=$1 ORDER BY id ASC LIMIT 100", room_id)
                    history = []
                    for m in msgs:
                        sender_info = await conn.fetchrow("SELECT nickname, username FROM users WHERE login=$1", m["sender_login"])
                        sender_name = sender_info["nickname"] if sender_info else "Unknown"
                        history.append({"sender": m["sender_login"], "sender_name": sender_name, "text": m["text"], "image": m["image"], "time": m["created_at"].strftime("%H:%M")})
                    await websocket.send_text(json.dumps({"action": "history", "messages": history, "room_id": room_id}))

            elif action == "send_message" and login:
                if manager.is_spam(login):
                    await websocket.send_text(json.dumps({"status": "error", "msg": "Слишком много сообщений! Подождите (Защита от спама)."}))
                    continue

                room_id, text, img = parsed.get("room_id"), parsed.get("text", ""), parsed.get("image", "")
                async with db_pool.acquire() as conn:
                    await conn.execute("INSERT INTO messages (room_id, sender_login, text, image) VALUES ($1, $2, $3, $4)", room_id, login, text, img)
                    members = await conn.fetch("SELECT user_login FROM room_members WHERE room_id=$1", room_id)
                    
                    # Получаем имя отправителя
                    sender_info = await conn.fetchrow("SELECT nickname FROM users WHERE login=$1", login)
                    sender_name = sender_info["nickname"] if sender_info else "Unknown"
                    
                    for m in members:
                        if m["user_login"] != login:
                            await manager.send_personal_message(json.dumps({"action": "new_message", "room_id": room_id, "sender": login, "sender_name": sender_name, "text": text, "image": img}), m["user_login"])
                            
                            # Автоответчик
                            rec_settings = await conn.fetchrow("SELECT auto_reply FROM users WHERE login=$1", m["user_login"])
                            if rec_settings and rec_settings["auto_reply"]:
                                await manager.send_personal_message(json.dumps({"action": "new_message", "room_id": room_id, "sender": m["user_login"], "sender_name": "Бот (Автоответ)", "text": rec_settings['auto_reply'], "image": ""}), login)

    except WebSocketDisconnect:
        if login: manager.disconnect(login)
