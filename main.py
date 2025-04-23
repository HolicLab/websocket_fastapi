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
    # 웹소켓 연결 
    logger.info(f"client connected : {websocket.client}")
    await websocket.accept() # client의 websocket접속 허용
    task = Task(websocket)
    
    try:
        while True:
            data = await websocket.receive_text()       # client 메시지 수신대기
            await task.receive_ppg_data(data)               # 메시지 수신
            
            # user_id(ulid)를 숫자로 매핑(테스트)
            task.ppg_data.user_id = mapper.get_numeric_id(task.ppg_data.user_id)
            if task.count == 1:
                logger.info(f"변환된 user_id : {task.ppg_data.user_id}")
            
            #---- 인공지능 처리 ------#
            # predictor.on_receive_ppg(task.ppg_data.ppg_value, task.ppg_data.date)
            
            # 인공지능 결과값(워치로 전송 되는지 테스트용)
            #-----------------------#

            # 이 부분은 인공지능 결과 데이터를 가정(워치로 보내지는지, cpu 서버로 post 요청을 하는지 테스트트)
            seoul_timezone = pytz.timezone('Asia/Seoul')
            now = datetime.now(seoul_timezone)
            formatted_date = now.strftime("%Y-%m-%dT%H:%M:%S.%f%z")
            
            # 숫자를 ULID로 변환(테스트)
            task.ppg_data.user_id = mapper.get_ulid(task.ppg_data.user_id)
            if task.count == 100:
                logger.info(f"변환된 user_id : {task.ppg_data.user_id}")
            
            focus_test_data = focus_data(
                user_id=f"{task.ppg_data.user_id}",
                session_id=f"{task.ppg_data.session_id}",
                focus_rate=0,
                level=0,
                time=f"{formatted_date}"
            )
            await task.send_data_to_watch(focus_test_data)
            #-------------------------#
            
    except WebSocketDisconnect:
        logger.info(f"client disconnected : {websocket.client}")
        logger.info(f"받은 총 데이터 개수 : {task.count}")
    finally:
        # 연결이 끊기면 반드시 정리 작업 수행
        await task.cleanup()
        
if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=18001, reload=False)