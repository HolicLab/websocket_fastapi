from fastapi import FastAPI, WebSocket
from fastapi.responses import HTMLResponse
from fastapi.logger import logger
from starlette.websockets import WebSocketDisconnect
import logging
from application import Task
from domain import focus_data
import uvicorn
from datetime import datetime
import pytz
from mapping_table import UlidMapper

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

# 저장 디렉토리 생성
# if not os.path.exists("ppg_datas"):
#     os.makedirs("ppg_datas")
#     os.chmod("ppg_datas", 0o777)

# 2000번부터 시작하는 매퍼 생성
mapper = UlidMapper(start_id=2000)

# 웹소켓 설정
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    #### 3. 웹소켓 연결 
    logger.info(f"client connected : {websocket.client}")
    await websocket.accept() # client의 websocket접속 허용
    task = Task(websocket)
    
    try:
        while True:
            ##### 1. client 메시지 수신대기
            data = await websocket.receive_text()
            
            ##### 2. task.ppg_data에 수신한 ppg, session_id 등을 저장
            await task.receive_ppg_data(data)              
            
            ##### 3. task.ppg_data.user_id를 숫자로 매핑
            task.ppg_data.user_id = await mapper.get_numeric_id(task.ppg_data.user_id)
            if task.count == 1:
                logger.info(f"변환된 user_id(int) : {task.ppg_data.user_id}")
            
            #-------------- 인공지능 처리 -------------------#
            # predictor.on_receive_ppg(task.ppg_data.ppg_value, task.ppg_data.date)
            
            #------------------------------------------------#
            
            ##### 4. task.ppg_data.user_id를 다시 ulid로 매핑
            task.ppg_data.user_id = await mapper.get_ulid(task.ppg_data.user_id)
            if task.count == 100:
                logger.info(f"변환된 user_id(ulid) : {task.ppg_data.user_id}")
            
            # 현재 시간
            seoul_timezone = pytz.timezone('Asia/Seoul')
            now = datetime.now(seoul_timezone)
            formatted_date = now.strftime("%Y-%m-%dT%H:%M:%S.%f%z")
            
            ##### 5. focus_test_data (ai모델 출력값 임의 설정) 생성
            focus_test_data = focus_data(
                user_id=f"{task.ppg_data.user_id}",
                session_id=f"{task.ppg_data.session_id}",
                focus_rate=0,
                level=0,
                time=f"{formatted_date}"
            )
            
            ##### 6. focus_test_data 스마트 워치에 전송
            await task.send_data_to_watch(focus_test_data)
            #-------------------------#
            
    ##### 6. 웹소켓 연결 종료
    except WebSocketDisconnect:
        logger.info(f"client disconnected : {websocket.client}")
        logger.info(f"받은 총 데이터 개수 : {task.count}")
    finally:
        ##### 7. 정리 작업
        await task.cleanup()
        
if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=18001, reload=False)