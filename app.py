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
        self.spam_tracker = {}
        
    async def connect(self, login: str, websocket: WebSocket):
        await websocket.accept()
        self.active_connections[login] = websocket
        
    def disconnect(self, login: str):
        if login in self.active_connections: del self.active_connections[login]
            
    async def send_personal_message(self, message: str, recipient: str):
        if recipient in self.active_connections:
            try: await self.active_connections[recipient].send_text(message)
            except Exception: self.disconnect(recipient)
            
    def is_spam(self, login: str) -> bool:
        now = time.time()
        if login not in self.spam_tracker: self.spam_tracker[login] = []
        self.spam_tracker[login] = [t for t in self.spam_tracker[login] if now - t < 5]
        if len(self.spam_tracker[login]) >= 10: return True
        self.spam_tracker[login].append(now)
        return False

manager = ConnectionManager()
db_pool = None

async def init_db():
    global db_pool
    if not DATABASE_URL: return
    try:
        db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5, statement_cache_size=0)
        print("✅ Database ready!")
    except Exception as e:
        print(f"❌ DB Error: {e}")

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
                lang = parsed.get("language", "en")
                pwd_hash = hashlib.sha256(pwd.encode()).hexdigest()
                
                async with db_pool.acquire() as conn:
                    try:
                        await conn.execute("INSERT INTO users (login, password_hash, nickname, username, birthday, language) VALUES ($1, $2, $3, $4, $5, $6)", user_login, pwd_hash, nick, uname, bday, lang)
                        await websocket.send_text(json.dumps({"status": "ok", "msg": "Account created"}))
                    except asyncpg.exceptions.UniqueViolationError:
                        await websocket.send_text(json.dumps({"status": "error", "msg": "Email/Phone or @username is already taken!"}))

            elif action == "login":
                login = parsed.get("login").lower()
                pwd = parsed.get("password")
                pwd_hash = hashlib.sha256(pwd.encode()).hexdigest()
                async with db_pool.acquire() as conn:
                    user = await conn.fetchrow("SELECT * FROM users WHERE login=$1", login)
                    if not user:
                        await websocket.send_text(json.dumps({"status": "error", "msg": "User not found"})); continue
                    
                    if user["password_hash"] == 'BANNED':
                        await websocket.send_text(json.dumps({"status": "error", "msg": "Your account has been banned due to multiple reports."})); continue

                    if user["password_hash"] != pwd_hash:
                        await websocket.send_text(json.dumps({"status": "error", "msg": "Invalid password"})); continue
                    
                    manager.active_connections[login] = websocket
                    await conn.execute("UPDATE users SET last_seen=NOW() WHERE login=$1", login)
                    
                    rooms = await conn.fetch("SELECT r.* FROM rooms r JOIN room_members m ON r.id = m.room_id WHERE m.user_login = $1", login)
                    room_list = []
                    for r in rooms:
                        if not r["is_group"]:
                            other = await conn.fetchval("SELECT user_login FROM room_members WHERE room_id=$1 AND user_login!=$2", r["id"], login)
                            room_list.append({"id": r["id"], "name": other or "Deleted", "is_group": False})
                        else: room_list.append({"id": r["id"], "name": r["name"], "is_group": True})
                    
                    blocked = [b["blocked_login"] for b in await conn.fetch("SELECT blocked_login FROM blocks WHERE owner_login=$1", login)]
                    all_users = [{"login": u["login"], "nickname": u["nickname"], "username": u["username"]} for u in await conn.fetch("SELECT login, nickname, username FROM users WHERE login != $1", login)]
                    settings = {"hide_online": user["hide_online"], "auto_reply": user["auto_reply"], "nickname": user["nickname"], "username": user["username"], "language": user["language"]}
                    
                    await websocket.send_text(json.dumps({"status": "ok", "rooms": room_list, "all_users": all_users, "blocked": blocked, "settings": settings}))

            elif action == "update_settings" and login:
                async with db_pool.acquire() as conn:
                    await conn.execute("UPDATE users SET hide_online=$1, auto_reply=$2, language=$3 WHERE login=$4", parsed.get("hide_online", False), parsed.get("auto_reply", ""), parsed.get("language", "en"), login)
                    await websocket.send_text(json.dumps({"status": "ok", "msg": "Settings saved"}))
            
            # ... (остальной код report_user, start_dm, get_history, send_message остается таким же, как в прошлом сообщении) ...
