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
            try:
                await self.active_connections[recipient].send_text(message)
            except Exception:
                self.disconnect(recipient)

manager = ConnectionManager()
db_pool = None

async def init_db():
    global db_pool
    if not DATABASE_URL: return
    try:
        db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5, statement_cache_size=0)
        async with db_pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    nickname TEXT PRIMARY KEY,
                    password_hash TEXT,
                    contact TEXT UNIQUE
                );
                CREATE TABLE IF NOT EXISTS messages (
                    id SERIAL PRIMARY KEY,
                    sender TEXT,
                    recipient TEXT,
                    text TEXT,
                    created_at TIMESTAMP DEFAULT NOW()
                );
            """)
        print("✅ База данных подключена!")
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
    # Ждем первого сообщения для авторизации
    await websocket.accept()
    nickname = None
    try:
        while True:
            data = await websocket.receive_text()
            if data == "ping": continue
            parsed = json.loads(data)
            action = parsed.get("action")

            if action == "register":
                # Проверка, занят ли ник
                nick = parsed.get("nickname")
                contact = parsed.get("contact")
                pwd = parsed.get("password")
                pwd_hash = hashlib.sha256(pwd.encode()).hexdigest()
                
                async with db_pool.acquire() as conn:
                    try:
                        await conn.execute("INSERT INTO users (nickname, password_hash, contact) VALUES ($1, $2, $3)", nick, pwd_hash, contact)
                        await websocket.send_text(json.dumps({"status": "ok", "msg": "Регистрация успешна"}))
                    except asyncpg.exceptions.UniqueViolationError:
                        await websocket.send_text(json.dumps({"status": "error", "msg": "Никнейм или контакт уже заняты!"}))

            elif action == "login":
                nickname = parsed.get("nickname")
                pwd = parsed.get("password")
                pwd_hash = hashlib.sha256(pwd.encode()).hexdigest()
                
                async with db_pool.acquire() as conn:
                    user = await conn.fetchrow("SELECT * FROM users WHERE nickname=$1 AND password_hash=$2", nickname, pwd_hash)
                    if user:
                        manager.active_connections[nickname] = websocket
                        await websocket.send_text(json.dumps({"status": "ok", "msg": "Вход выполнен"}))
                        await websocket.send_text(json.dumps({"action": "get_users", "users": [r["nickname"] for r in await conn.fetch("SELECT nickname FROM users WHERE nickname != $1", nickname)]}))
                    else:
                        await websocket.send_text(json.dumps({"status": "error", "msg": "Неверный ник или пароль"}))

            elif action == "send_message":
                recipient = parsed.get("recipient")
                text = parsed.get("text")
                if nickname:
                    async with db_pool.acquire() as conn:
                        await conn.execute("INSERT INTO messages (sender, recipient, text) VALUES ($1, $2, $3)", nickname, recipient, text)
                    msg_payload = json.dumps({"action": "new_message", "sender": nickname, "text": text})
                    await manager.send_personal_message(msg_payload, recipient)

    except WebSocketDisconnect:
        if nickname: manager.disconnect(nickname)
