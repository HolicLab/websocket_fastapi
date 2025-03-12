from fastapi import FastAPI, WebSocket, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.logger import logger
from starlette.websockets import WebSocketDisconnect
import json
import asyncio

app = FastAPI()
# templates = Jinja2Templates(directory="templates")   # html파일을 서비스할 수 있는 jinja설정 (/templates 폴더사용)

# # 연결된 모든 웹소켓 클라이언트 저장
# connected_clients = set()

# # 웹소켓 연결
# @app.get("/client")
# async def client(request: Request):
#     # /templates/client.html파일을 response
#     return templates.TemplateResponse("client.html", {"request":request})

# 웹소켓 설정 ws://127.0.0.1:8080/ws 로 접속할 수 있음
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    print(f"client connected : {websocket.client}")
    await websocket.accept() # client의 websocket접속 허용
    
    await websocket.send_json({
        "type": "welcome", 
        "text": f"Welcome client : {websocket.client}"
    })
    
    try:
        while True:
            data = await websocket.receive_text()  # client 메시지 수신대기
            print(f"Received message from client: {data}")  # 로그 추가
            try:
                json_data = json.loads(data)
                message_text = json_data.get("message", "No message key found")
            except json.JSONDecodeError:
                message_text = data  # 일반 문자열로 처리
            
            response_message = {"type": "response", "text": f"Message was: {message_text}"}
            
            await websocket.send_json(response_message)
                
    except WebSocketDisconnect:
        print(f"client disconnected : {websocket.client}")
