import os
import json
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import asyncpg
import hashlib
from datetime import datetime
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
                    if not user: await conn.execute("INSERT INTO users (nickname, password_hash) VALUES ($1, $2)", nickname, pwd_hash)
                    
                    manager.active_connections[nickname] = websocket
                    await conn.execute("UPDATE users SET last_seen=NOW() WHERE nickname=$1", nickname)
                    
                    rooms = await conn.fetch("SELECT r.* FROM rooms r JOIN room_members m ON r.id = m.room_id WHERE m.nickname = $1", nickname)
                    room_list = []
                    for r in rooms:
                        if not r["is_group"]:
                            other = await conn.fetchval("SELECT nickname FROM room_members WHERE room_id=$1 AND nickname!=$2", r["id"], nickname)
                            ou = await conn.fetchrow("SELECT last_seen, hide_online FROM users WHERE nickname=$1", other) if other else None
                            status = "Скрыт" if (ou and ou["hide_online"]) else (f"Был(а) в {ou['last_seen'].strftime('%H:%M')}" if ou else "Недавно")
                            room_list.append({"id": r["id"], "name": other or "Удален", "is_group": False, "status": status})
                        else: room_list.append({"id": r["id"], "name": r["name"], "is_group": True, "status": "Группа"})
                    
                    all_users = [u["nickname"] for u in await conn.fetch("SELECT nickname FROM users WHERE nickname != $1", nickname)]
                    await websocket.send_text(json.dumps({"status": "ok", "rooms": room_list, "all_users": all_users}))

            elif action == "get_history" and nickname:
                room_id = parsed.get("room_id")
                async with db_pool.acquire() as conn:
                    msgs = await conn.fetch("SELECT * FROM messages WHERE room_id=$1 ORDER BY id ASC LIMIT 100", room_id)
                    history = [{"id": m["id"], "sender": m["sender"], "text": m["text"], "image": m["image"], "voice": m["voice"], "reply_to": m["reply_to"], "edited": m["edited"], "time": m["created_at"].strftime("%H:%M")} for m in msgs]
                    await websocket.send_text(json.dumps({"action": "history", "messages": history, "room_id": room_id}))

            elif action == "send_message" and nickname:
                room_id, text, img, voice, r_id = parsed.get("room_id"), parsed.get("text", ""), parsed.get("image", ""), parsed.get("voice", ""), parsed.get("reply_to")
                async with db_pool.acquire() as conn:
                    msg_id = await conn.fetchval("INSERT INTO messages (room_id, sender, text, image, voice, reply_to) VALUES ($1, $2, $3, $4, $5, $6) RETURNING id", room_id, nickname, text, img, voice, r_id)
                    members = await conn.fetch("SELECT nickname FROM room_members WHERE room_id=$1", room_id)
                    for m in members:
                        if m["nickname"] != nickname:
                            await manager.send_personal_message(json.dumps({"action": "new_message", "room_id": room_id, "id": msg_id, "sender": nickname, "text": text, "image": img, "voice": voice, "reply_to": r_id}), m["nickname"])

            elif action == "edit_message" and nickname:
                msg_id, new_text = parsed.get("id"), parsed.get("text")
                async with db_pool.acquire() as conn:
                    await conn.execute("UPDATE messages SET text=$1, edited=TRUE WHERE id=$2 AND sender=$3", new_text, msg_id, nickname)
                    room_id = await conn.fetchval("SELECT room_id FROM messages WHERE id=$1", msg_id)
                    members = await conn.fetch("SELECT nickname FROM room_members WHERE room_id=$1", room_id)
                    for m in members:
                        await manager.send_personal_message(json.dumps({"action": "message_edited", "id": msg_id, "text": new_text}), m["nickname"])

            elif action == "delete_message" and nickname:
                msg_id = parsed.get("id")
                async with db_pool.acquire() as conn:
                    room_id = await conn.fetchval("SELECT room_id FROM messages WHERE id=$1", msg_id)
                    await conn.execute("DELETE FROM messages WHERE id=$1 AND sender=$2", msg_id, nickname)
                    members = await conn.fetch("SELECT nickname FROM room_members WHERE room_id=$1", room_id)
                    for m in members:
                        await manager.send_personal_message(json.dumps({"action": "message_deleted", "id": msg_id}), m["nickname"])

            elif action == "start_dm" and nickname:
                target = parsed.get("target")
                async with db_pool.acquire() as conn:
                    existing = await conn.fetchval("SELECT r.id FROM rooms r JOIN room_members m1 ON r.id = m1.room_id JOIN room_members m2 ON r.id = m2.room_id WHERE r.is_group = FALSE AND m1.nickname = $1 AND m2.nickname = $2", nickname, target)
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

    except WebSocketDisconnect:
        if nickname: manager.disconnect(nickname)
