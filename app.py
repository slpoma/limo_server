import os
import json
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import asyncpg
from fastapi.middleware.cors import CORSMiddleware

DATABASE_URL = os.environ.get("DATABASE_URL")

class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: str, sender_ws: WebSocket = None):
        for connection in self.active_connections:
            if connection != sender_ws:
                try:
                    await connection.send_text(message)
                except Exception:
                    self.disconnect(connection)

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
            statement_cache_size=0 # ВАЖНО для Supabase Pooler!
        )
        async with db_pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS messages (
                    id SERIAL PRIMARY KEY,
                    author TEXT,
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

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            if data == "ping": continue
            try:
                parsed = json.loads(data)
                if parsed.get("type") == "message":
                    author = parsed.get("author", "Unknown")
                    text = parsed.get("text", "")
                    
                    if db_pool:
                        async with db_pool.acquire() as conn:
                            await conn.execute(
                                "INSERT INTO messages (author, text) VALUES ($1, $2)",
                                author, text
                            )
                    
                    await manager.broadcast(data, websocket)
            except Exception:
                pass
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception:
        manager.disconnect(websocket)

@app.get("/")
def read_root():
    return {"status": "running", "database": "connected" if db_pool else "disconnected"}
