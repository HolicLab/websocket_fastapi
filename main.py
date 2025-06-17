from fastapi import FastAPI, WebSocket, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.logger import logger
from starlette.websockets import WebSocketDisconnect
import logging
from application import Task
from domain import focus_data
import uvicorn
from datetime import datetime
import pytz
import asyncio
from pydantic import BaseModel
from mapping_table import UlidMapper
from typing import Optional

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

# 2000번부터 시작하는 매퍼 생성
mapper = UlidMapper(start_id=2000)

class SessionComplete(BaseModel):
    session_id: str
    user_id: str
    avg_focus: Optional[float] = None

@app.post("/avg_focus", status_code = 200)
async def complete_session(session_data: SessionComplete):
    logger.info(f"세션 종료 요청 받음: {session_data.dict()}")
    try:
        user_id_as_int = await mapper.get_numeric_id(session_data.user_id)
        session_id = session_data.session_id
        avg_focus = session_data.avg_focus
    except Exception as e:
        logger.error(f"세션 종료 처리 중 예외 발생: {str(e)}")
        raise HTTPException(status_code=500, detail=f"서버 내부 오류: {str(e)}")

# 웹소켓 설정
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    #### 3. 웹소켓 연결 
    logger.info(f"client connected : {websocket.client}")
    await websocket.accept() # client의 websocket접속 허용
    
    task = Task(websocket)
    
    process_task = asyncio.create_task(task.process_message_queue())
    task.background_tasks.add(process_task)
    process_task.add_done_callback(task.background_tasks.discard)
    
    try:
        while True:
            data = await websocket.receive_text()
            
            # 큐에 메시지 추가 (처리는 다른 태스크에서 수행)
            await task.input_queue.put(data)
            
    ##### 6. 웹소켓 연결 종료
    except WebSocketDisconnect:
        logger.info(f"client disconnected : {websocket.client}")
        logger.info(f"받은 총 데이터 개수 : {task.count}")

    finally:
        ##### 7. 정리 작업
        try:
            await task.cleanup()
        except Exception as cleanup_error:
            logger.error(f"정리 작업 중 오류: {str(cleanup_error)}")
        
if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=18001, reload=False)