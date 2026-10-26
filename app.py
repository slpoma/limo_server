import flet as ft
import json
import asyncio
import websockets
import os
import sys
import base64
from datetime import datetime

try:
    desktop = os.path.join(os.path.expanduser("~"), "Desktop")
    log_file = open(os.path.join(desktop, "slpoma.log"), "w", encoding="utf-8")
    sys.stdout, sys.stderr = log_file, log_file
except: pass

SERVER_WS_URL = "wss://limo-server-pd9t.onrender.com/ws"
BG_DEEP = "#0B0B0F"; BG_PANEL = "#181820"; BG_CARD = "#23232D"
ACCENT = "#8C52FF"; TEXT = "#F2F2F5"; TEXT_MUTED = "#8A8A98"; DANGER = "#FF5C7A"

async def main(page: ft.Page):
    page.title = "ЛИМО"; page.bgcolor = BG_DEEP; page.padding = 0
    page.theme_mode = ft.ThemeMode.DARK; page.window_width = 1200; page.window_height = 800

    S = {"me": None, "ws": None, "current_room": None, "current_name": "", "rooms": [], "all_users": [], "blocked": [], "img_to_send": None, "settings": {}}

    def toast(msg, color=None):
        page.snack_bar = ft.SnackBar(content=ft.Text(msg, color=TEXT), bgcolor=color or ACCENT)
        page.snack_bar.open = True; page.update()

    # --- WEBSOCKET ---
    async def ws_listen():
        while True:
            try:
                async with websockets.connect(SERVER_WS_URL, open_timeout=20) as ws:
                    S["ws"] = ws
                    if S.get("pending_auth"):
                        await ws.send(json.dumps(S["pending_auth"])); S["pending_auth"] = None
                    
                    async for msg in ws:
                        if msg == "pong": continue
                        data = json.loads(msg)
                        
                        if data.get("status") == "ok":
                            if data.get("rooms") is not None:
                                S["rooms"] = data["rooms"]; S["all_users"] = data["all_users"]; S["blocked"] = data["blocked"]; S["settings"] = data["settings"]
                                update_rooms(); show_main_ui()
                            elif data.get("msg") == "Настройки сохранены": toast("Настройки сохранены")
                        elif data.get("status") == "error": toast(data["msg"], DANGER)
                        elif data.get("action") == "new_message":
                            if S["current_room"] == data["room_id"]: add_bubble(data.get("id"), data["text"], data["sender"], "", data.get("image"), own=False)
                        elif data.get("action") == "history":
                            chat_list.controls.clear()
                            for m in data["messages"]:
                                add_bubble(m.get("id"), m["text"], m["sender"], m["time"], m["image"], own=(m["sender"]==S["me"]))
                        elif data.get("action") == "dm_started":
                            S["current_room"] = data["room_id"]; select_room(data["room_id"], data["target"])
                        elif data.get("action") == "group_created":
                            S["current_room"] = data["room_id"]; select_room(data["room_id"], data["name"])
                        elif data.get("action") == "blocked": S["blocked"].append(data["msg"].split()[0]); toast(data["msg"], DANGER)
            except Exception as e: print(f"WS: {e}")
            finally: await asyncio.sleep(4)

    # --- UI ---
    root = ft.Stack(expand=True)
    rooms_col = ft.Column(spacing=4, scroll=ft.ScrollMode.AUTO)
    chat_list = ft.ListView(expand=True, spacing=10, padding=20, auto_scroll=True)
    chat_title = ft.Text("Выберите чат", color=TEXT, size=18, weight=ft.FontWeight.BOLD)
    msg_input = ft.TextField(hint_text="Сообщение...", expand=True, border=ft.InputBorder.NONE, filled=True, fill_color="transparent", text_style=ft.TextStyle(color=TEXT), cursor_color=ACCENT)

    def update_rooms():
        rooms_col.controls.clear()
        for r in S["rooms"]:
            icon = ft.icons.GROUP if r["is_group"] else ft.icons.PERSON
            rooms_col.controls.append(ft.Container(padding=10, border_radius=12, ink=True, on_click=lambda e, room=r: page.run_task(select_room_async, room["id"], room["name"]), content=ft.Row([ft.Container(width=40, height=40, border_radius=20, bgcolor=BG_CARD, alignment=ft.alignment.center, content=ft.Icon(icon, color=ACCENT)), ft.Text(r["name"], color=TEXT, size=15)])))
        page.update()

    async def select_room_async(room_id, name):
        S["current_room"] = room_id; S["current_name"] = name; chat_title.value = name; chat_list.controls.clear(); page.update()
        if S["ws"]: await S["ws"].send(json.dumps({"action": "get_history", "room_id": room_id}))

    def add_bubble(msg_id, text, author, time_str, img_url="", own=False):
        bg = ACCENT if own else BG_CARD; align = ft.MainAxisAlignment.END if own else ft.MainAxisAlignment.START
        content_col = [ft.Text(author, color=TEXT_MUTED, size=11, weight=ft.FontWeight.BOLD)]
        if img_url: content_col.append(ft.Image(src=img_url, width=200, border_radius=10))
        if text: content_col.append(ft.Text(text, color=TEXT, size=14))
        content_col.append(ft.Text(time_str, color=TEXT_MUTED, size=10, text_align=ft.TextAlign.RIGHT))
        bubble = ft.Container(data=msg_id, bgcolor=bg, border_radius=16, padding=12, content=ft.Column(content_col, tight=True, spacing=4))
        chat_list.controls.append(ft.Row([bubble], alignment=align)); page.update()

    async def send_message_async(e=None):
        text = (msg_input.value or "").strip()
        if (text or S["img_to_send"]) and S["current_room"] and S["ws"]:
            await S["ws"].send(json.dumps({"action": "send_message", "room_id": S["current_room"], "text": text, "image": S["img_to_send"]}))
            add_bubble(None, text, S["me"], datetime.now().strftime("%H:%M"), S["img_to_send"], own=True)
            msg_input.value = ""; S["img_to_send"] = None; page.update()

    msg_input.on_submit = send_message_async

    def pick_image(e):
        def on_result(res: ft.FilePickerResultEvent):
            if res.files:
                with open(res.files[0].path, "rb") as f: S["img_to_send"] = f"data:image/jpeg;base64,{base64.b64encode(f.read()).decode('utf-8')}"
                toast("Фото прикреплено!")
        fp = ft.FilePicker(on_result=on_result); page.overlay.append(fp); fp.pick_files(allow_multiple=False, file_type=ft.FilePickerFileType.IMAGE); page.update()

    async def open_new_chat(e):
        def start_dm(user): page.run_task(S["ws"].send, json.dumps({"action": "start_dm", "target": user})); page.dialog.close()
        def create_group(e): page.run_task(S["ws"].send, json.dumps({"action": "create_group", "name": group_input.value})); page.dialog.close()
        group_input = ft.TextField(hint_text="Название группы")
        users_list = ft.Column([ft.TextButton(u, on_click=lambda e, u=u: start_dm(u)) for u in S["all_users"]], scroll=ft.ScrollMode.AUTO, height=200)
        dlg = ft.AlertDialog(title=ft.Text("Новый чат"), content=ft.Column([group_input, ft.ElevatedButton("Создать группу", on_click=create_group), ft.Divider(), users_list]), bgcolor=BG_PANEL)
        page.dialog = dlg; dlg.open = True; page.update()

    # --- НАСТРОЙКИ ПРОФИЛЯ ---
    def open_settings(e):
        hide_switch = ft.Switch(value=S["settings"].get("hide_online", False), active_color=ACCENT)
        auto_reply = ft.TextField(hint_text="Текст автоответа", value=S["settings"].get("auto_reply", ""), width=300)
        blocked_list = ft.Column([ft.Text(f"❌ {b}", color=DANGER) for b in S["blocked"]])
        
        def save_settings(e):
            page.run_task(S["ws"].send, json.dumps({"action": "update_settings", "hide_online": hide_switch.value, "auto_reply": auto_reply.value, "wallpaper": "Графит"}))
            page.dialog.close()

        dlg = ft.AlertDialog(
            title=ft.Text("Настройки профиля"), 
            content=ft.Column([
                ft.Row([ft.Text("Скрыть онлайн", color=TEXT), ft.Container(expand=True), hide_switch]),
                ft.Text("Автоответчик:", color=TEXT),
                auto_reply,
                ft.Divider(),
                ft.Text("Чёрный список:", color=TEXT),
                blocked_list
            ]), 
            actions=[ft.TextButton("Сохранить", on_click=save_settings), ft.TextButton("Закрыть", on_click=lambda e: page.dialog.close())],
            bgcolor=BG_PANEL
        )
        page.dialog = dlg; dlg.open = True; page.update()

    def show_main_ui():
        if len(root.controls) > 0 and isinstance(root.controls[0], ft.Row): return
        sidebar = ft.Container(width=70, bgcolor="#121218", padding=10, content=ft.Column([ft.Container(width=50, height=50, border_radius=16, bgcolor=ACCENT, alignment=ft.alignment.center, content=ft.Text("Л", color=TEXT, size=24, weight=ft.FontWeight.BOLD)), ft.Container(expand=True), ft.IconButton(ft.icons.SETTINGS_ROUNDED, icon_color=TEXT_MUTED, on_click=open_settings)]))
        chats_panel = ft.Container(width=300, bgcolor=BG_PANEL, padding=15, content=ft.Column([ft.Row([ft.Text("Чаты", color=TEXT, size=22, weight=ft.FontWeight.BOLD), ft.Container(expand=True), ft.IconButton(ft.icons.ADD, icon_color=ACCENT, on_click=open_new_chat)]), rooms_col]))
        chat_col = ft.Column([ft.Container(padding=15, bgcolor=BG_PANEL, content=chat_title), ft.Container(expand=True, content=chat_list), ft.Container(padding=15, bgcolor=BG_PANEL, content=ft.Row([ft.IconButton(ft.icons.ATTACH_FILE, icon_color=TEXT_MUTED, on_click=pick_image), ft.Container(expand=True, bgcolor=BG_CARD, border_radius=24, content=msg_input, padding=ft.padding.symmetric(0, 10)), ft.Container(width=50, height=50, border_radius=25, bgcolor=ACCENT, alignment=ft.alignment.center, ink=True, on_click=send_message_async, content=ft.Icon(ft.icons.SEND, color=TEXT))]))], expand=True, spacing=0)
        root.controls = [ft.Row([sidebar, chats_panel, chat_col], expand=True, spacing=0)]; page.update()

    # --- ВХОД ---
    auth_root = ft.Container(expand=True, alignment=ft.alignment.center)
    nick_in = ft.TextField(hint_text="Никнейм", width=350, filled=True, fill_color=BG_CARD, border_radius=14, text_style=ft.TextStyle(color=TEXT))
    pwd_in = ft.TextField(hint_text="Пароль", password=True, width=350, filled=True, fill_color=BG_CARD, border_radius=14, text_style=ft.TextStyle(color=TEXT))
    
    def do_login(e):
        if not nick_in.value: return toast("Введите ник")
        S["me"] = nick_in.value
        S["pending_auth"] = {"action": "login", "nickname": nick_in.value, "password": pwd_in.value}
        auth_root.content = ft.Column([ft.ProgressRing(color=ACCENT), ft.Text("Подключение...", color=TEXT)], horizontal_alignment=ft.CrossAxisAlignment.CENTER)
        page.update()
        if not S.get("ws_started"): S["ws_started"] = True; page.run_task(ws_listen)
    
    auth_root.content = ft.Column([ft.Text("ЛИМО", size=40, color=TEXT, weight=ft.FontWeight.BOLD), ft.Container(height=20), nick_in, pwd_in, ft.Container(width=350, height=50, border_radius=14, bgcolor=ACCENT, alignment=ft.alignment.center, ink=True, on_click=do_login, content=ft.Text("Войти", color=TEXT, weight=ft.FontWeight.BOLD))], horizontal_alignment=ft.CrossAxisAlignment.CENTER)
    root.controls = [auth_root]; page.add(root)

if __name__ == "__main__":
    ft.app(target=main)
