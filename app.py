import os
import json
import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import asyncpg
from fastapi.middleware.cors import CORSMiddleware

DATABASE_URL = os.environ.get("DATABASE_URL")

# Храним активные подключения в словаре: {"nickname": websocket}
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
    if not DATABASE_URL:
        print("⚠️ DATABASE_URL не задан!")
        return
    try:
        db_pool = await asyncpg.create_pool(
            DATABASE_URL, 
            min_size=1, 
            max_size=5,
            statement_cache_size=0 # ВАЖНО для Supabase
        )
        async with db_pool.acquire() as conn:
            # Обновляем таблицу сообщений для поддержки ЛС (добавляем sender и recipient)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    nickname TEXT PRIMARY KEY,
                    last_seen TIMESTAMP DEFAULT NOW()
                );
                CREATE TABLE IF NOT EXISTS messages (
                    id SERIAL PRIMARY KEY,
                    sender TEXT,
                    recipient TEXT,
                    text TEXT,
                    created_at TIMESTAMP DEFAULT NOW()
                );
            """)
        print("✅ База данных подключена и готова.")
    except Exception as e:
        print(f"❌ Ошибка подключения к БД: {e}")

@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    yield
    if db_pool:
        await db_pool.close()

app = FastAPI(lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.websocket("/ws/{nickname}")
async def websocket_endpoint(websocket: WebSocket, nickname: str):
    await manager.connect(nickname, websocket)
    print(f"✅ Пользователь {nickname} подключился")
    
    try:
        while True:
            data = await websocket.receive_text()
            if data == "ping": continue
            
            try:
                parsed = json.loads(data)
                action = parsed.get("action")
                
                if action == "send_message":
                    recipient = parsed.get("recipient")
                    text = parsed.get("text")
                    
                    # Сохраняем в базу
                    if db_pool:
                        async with db_pool.acquire() as conn:
                            await conn.execute(
                                "INSERT INTO messages (sender, recipient, text) VALUES ($1, $2, $3)",
                                nickname, recipient, text
                            )
                    
                    # Отправляем получателю
                    msg_payload = json.dumps({
                        "action": "new_message",
                        "sender": nickname,
                        "text": text
                    })
                    await manager.send_personal_message(msg_payload, recipient)
                    
                elif action == "get_users":
                    # Отправляем список всех, кто есть в базе (кроме себя)
                    if db_pool:
                        async with db_pool.acquire() as conn:
                            users = await conn.fetch("SELECT nickname FROM users WHERE nickname != $1", nickname)
                            user_list = [u["nickname"] for u in users]
                            await websocket.send_text(json.dumps({"action": "user_list", "users": user_list}))
                            
                elif action == "register":
                    # Регистрируем пользователя в БД при входе
                    if db_pool:
                        async with db_pool.acquire() as conn:
                            await conn.execute("INSERT INTO users (nickname) VALUES ($1) ON CONFLICT DO NOTHING", nickname)
                            
            except Exception as e:
                print(f"Ошибка обработки данных от {nickname}: {e}")
                
    except WebSocketDisconnect:
        manager.disconnect(nickname)
        print(f"❌ Пользователь {nickname} отключился")
