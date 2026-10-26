import os
import json
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import asyncpg
import hashlib
from fastapi.middleware.cors import CORSMiddleware

DATABASE_URL = os.environ.get("DATABASE_URL")

class ConnectionManager:
    def __init__(self):
        self.active_connections: dict[str, WebSocket] = {}
    async def connect(self, nickname: str, websocket: WebSocket):
        await websocket.accept()
        self.active_connections[nickname] = websocket
    def disconnect(self, nickname: str):
        if nickname in self.active_connections:
            del self.active_connections[nickname]
    async def send_personal_message(self, message: str, recipient: str):
        if recipient in self.active_connections:
            try: await self.active_connections[recipient].send_text(message)
            except Exception: self.disconnect(recipient)

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
    nickname = None
    try:
        while True:
            data = await websocket.receive_text()
            if data == "ping": continue
            parsed = json.loads(data)
            action = parsed.get("action")

            if action == "login":
                nickname, pwd = parsed.get("nickname"), parsed.get("password")
                pwd_hash = hashlib.sha256(pwd.encode()).hexdigest()
                async with db_pool.acquire() as conn:
                    user = await conn.fetchrow("SELECT * FROM users WHERE nickname=$1", nickname)
                    if not user:
                        await conn.execute("INSERT INTO users (nickname, password_hash) VALUES ($1, $2)", nickname, pwd_hash)
                    
                    manager.active_connections[nickname] = websocket
                    
                    rooms = await conn.fetch("SELECT r.* FROM rooms r JOIN room_members m ON r.id = m.room_id WHERE m.nickname = $1", nickname)
                    room_list = []
                    for r in rooms:
                        if not r["is_group"]:
                            other = await conn.fetchval("SELECT nickname FROM room_members WHERE room_id=$1 AND nickname!=$2", r["id"], nickname)
                            room_list.append({"id": r["id"], "name": other or "Удален", "is_group": False})
                        else: room_list.append({"id": r["id"], "name": r["name"], "is_group": True})
                    
                    all_users = [u["nickname"] for u in await conn.fetch("SELECT nickname FROM users WHERE nickname != $1", nickname)]
                    blocked = [b["blocked"] for b in await conn.fetch("SELECT blocked FROM blocks WHERE owner=$1", nickname)]
                    settings = {"hide_online": user["hide_online"], "auto_reply": user["auto_reply"]}
                    
                    await websocket.send_text(json.dumps({"status": "ok", "rooms": room_list, "all_users": all_users, "blocked": blocked, "settings": settings}))

            elif action == "update_settings" and nickname:
                async with db_pool.acquire() as conn:
                    await conn.execute("UPDATE users SET hide_online=$1, auto_reply=$2 WHERE nickname=$3", 
                                       parsed.get("hide_online", False), parsed.get("auto_reply", ""), nickname)
                    await websocket.send_text(json.dumps({"status": "ok", "msg": "Настройки сохранены"}))

            elif action == "block_user" and nickname:
                target = parsed.get("target")
                async with db_pool.acquire() as conn:
                    await conn.execute("INSERT INTO blocks (owner, blocked) VALUES ($1, $2) ON CONFLICT DO NOTHING", nickname, target)
                    await websocket.send_text(json.dumps({"action": "blocked", "msg": f"{target} в чёрном списке"}))

            elif action == "start_dm" and nickname:
                target = parsed.get("target")
                async with db_pool.acquire() as conn:
                    # Проверка на блокировку
                    is_blocked = await conn.fetchrow("SELECT 1 FROM blocks WHERE owner=$1 AND blocked=$2", target, nickname)
                    if is_blocked:
                        await websocket.send_text(json.dumps({"status": "error", "msg": "Вы в чёрном списке у этого пользователя"}))
                        continue
                    
                    existing = await conn.fetchval("""
                        SELECT r.id FROM rooms r 
                        JOIN room_members m1 ON r.id = m1.room_id 
                        JOIN room_members m2 ON r.id = m2.room_id 
                        WHERE r.is_group = FALSE AND m1.nickname = $1 AND m2.nickname = $2
                    """, nickname, target)
                    
                    if not existing:
                        room_id = await conn.fetchval("INSERT INTO rooms (is_group) VALUES (FALSE) RETURNING id")
                        await conn.execute("INSERT INTO room_members (room_id, nickname) VALUES ($1, $2), ($1, $3)", room_id, nickname, target)
                    else: room_id = existing
                    await websocket.send_text(json.dumps({"action": "dm_started", "room_id": room_id, "target": target}))

            elif action == "create_group" and nickname:
                name = parsed.get("name")
                async with db_pool.acquire() as conn:
                    room_id = await conn.fetchval("INSERT INTO rooms (name, is_group) VALUES ($1, TRUE) RETURNING id", name)
                    await conn.execute("INSERT INTO room_members (room_id, nickname) VALUES ($1, $2)", room_id, nickname)
                    await websocket.send_text(json.dumps({"action": "group_created", "room_id": room_id, "name": name}))

            elif action == "get_history" and nickname:
                room_id = parsed.get("room_id")
                async with db_pool.acquire() as conn:
                    msgs = await conn.fetch("SELECT * FROM messages WHERE room_id=$1 ORDER BY id ASC LIMIT 100", room_id)
                    history = [{"sender": m["sender"], "text": m["text"], "image": m["image"], "time": m["created_at"].strftime("%H:%M")} for m in msgs]
                    await websocket.send_text(json.dumps({"action": "history", "messages": history, "room_id": room_id}))

            elif action == "send_message" and nickname:
                room_id, text, img = parsed.get("room_id"), parsed.get("text", ""), parsed.get("image", "")
                async with db_pool.acquire() as conn:
                    await conn.execute("INSERT INTO messages (room_id, sender, text, image) VALUES ($1, $2, $3, $4)", room_id, nickname, text, img)
                    members = await conn.fetch("SELECT nickname FROM room_members WHERE room_id=$1", room_id)
                    for m in members:
                        if m["nickname"] != nickname:
                            await manager.send_personal_message(json.dumps({"action": "new_message", "room_id": room_id, "sender": nickname, "text": text, "image": img}), m["nickname"])
                            
                            rec_settings = await conn.fetchrow("SELECT auto_reply FROM users WHERE nickname=$1", m["nickname"])
                            if rec_settings and rec_settings["auto_reply"]:
                                await manager.send_personal_message(json.dumps({"action": "new_message", "room_id": room_id, "sender": m["nickname"], "text": f"[Автоответ] {rec_settings['auto_reply']}", "image": ""}), nickname)

    except WebSocketDisconnect:
        if nickname: manager.disconnect(nickname)
