from fastapi import FastAPI, WebSocket, Request
from fastapi.responses import HTMLResponse
from fastapi.logger import logger
from starlette.websockets import WebSocketDisconnect
import json
import asyncio
import logging
import os
from ppg_save import save_to_csv, process_buffer, buffers

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

# 저장 디렉토리 생성
if not os.path.exists("ppg_datas"):
    os.makedirs("ppg_datas")
    os.chmod("ppg_datas", 0o777)

# 웹소켓 설정
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    logger.info(f"client connected : {websocket.client}")
    await websocket.accept() # client의 websocket접속 허용
    
    try:
        session_id = None
        
        while True:
            data = await websocket.receive_text()  # client 메시지 수신대기
            logger.info(f"Received message from client: {data}")  # 로그 추가
            try:
                json_data = json.loads(data)
                session_id = json_data.get("session_id", "No session_id key found")
                ppg_value = json_data.get("ppg_value", "No ppg_value key found")
                date = json_data.get("date", "No date key found")
            except json.JSONDecodeError:
                logger.info("Received data is not JSON")
            
            if session_id:
                if session_id not in buffers:
                    buffers[session_id] = []
                    asyncio.create_task(process_buffer(session_id))
                
                buffers[session_id].append((session_id, ppg_value, date))
            
            response_message = {"type": "response", "session_id": f"{session_id}", "ppg_value": f"{ppg_value}", "date": f"{date}"}
            logger.info(f"session_id : {session_id}, ppg_value : {ppg_value}, date : {date}")
            
            await websocket.send_json(response_message)
                
    except WebSocketDisconnect:
        logger.info(f"client disconnected : {websocket.client}")
        if session_id and session_id in buffers:
            await save_to_csv(buffers[session_id], f"ppg_data_{session_id}.csv")
            del buffers[session_id]
            