import json
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

# Разрешаем CORS (на всякий случай, если клиент будет в браузере)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        # Безопасное удаление, чтобы сервер не падал
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: str, sender_ws: WebSocket):
        # Рассылаем всем, КРОМЕ отправителя
        for connection in self.active_connections:
            if connection != sender_ws:
                try:
                    await connection.send_text(message)
                except Exception:
                    # Если соединение умерло - удаляем его
                    self.disconnect(connection)

manager = ConnectionManager()

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            
            # Heartbeat (пинг) - просто пропускаем, ничего не рассылаем
            if data == "ping":
                continue
            
            # Проверяем, что это валидный JSON (защита от спама мусором)
            try:
                parsed = json.loads(data)
                if parsed.get("type") == "message":
                    # Рассылаем всем, кроме того, кто отправил
                    await manager.broadcast(data, websocket)
            except json.JSONDecodeError:
                pass # Игнорируем мусор
                
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception:
        manager.disconnect(websocket)

@app.get("/")
def read_root():
    return {
        "status": "LIMO Server is running", 
        "websocket": "wss://limo-server-pd9t.onrender.com/ws",
        "active_users": len(manager.active_connections)
    }
