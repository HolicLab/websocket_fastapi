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

### 딥러닝 모듈 바인딩 ###
import sys
import os
sys.path.append(os.path.abspath("../Real_Time_Predict_PKG"))
from deeplearning_pkg.real_time_predict import RealTimePredictor
###

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

predictor = RealTimePredictor()

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
            # 1. PPG 한개 수신
            await task.receive_ppg_data(data)
            # 🔥 여기서 수신 데이터 정상 여부 검사
            if task.ppg_data.date is None or task.ppg_data.ppg_value is None or task.ppg_data.session_id is None:
                logger.error(f"수신 데이터 누락 - date: {task.ppg_data.date}, value: {task.ppg_data.ppg_value}, session: {task.ppg_data.session_id}")
                continue  # 이 프레임은 스킵하고 다음거 받아
            task.ppg_data.user_id_as_int = await mapper.get_numeric_id(task.ppg_data.user_id)

            # 2. predictor로 ppg_value만 전달 (focus_score 반환 안받음)
            predictor.on_receive_ppg(
                task.ppg_data.ppg_value,
                task.ppg_data.date,
                task.ppg_data.user_id_as_int,
                task.ppg_data.session_id
            )

            # 3. summarize_focus 호출해서, 1분치가 준비됐는지 확인
            summary_result = predictor.summarize_focus(
                task.ppg_data.user_id_as_int,
                task.ppg_data.session_id
            )

            if summary_result is not None:
                avg_focus, focus_level = summary_result

                # task.ppg_data.user_id를 ULID로 복원
                task.ppg_data.user_id = await mapper.get_ulid(task.ppg_data.user_id)

                # 현재 시간
                seoul_timezone = pytz.timezone('Asia/Seoul')
                now = datetime.now(seoul_timezone)
                formatted_date = now.strftime("%Y-%m-%dT%H:%M:%S.%f%z")

                # focus_data 패킹
                focus_test_data = focus_data(
                    user_id=f"{task.ppg_data.user_id}",
                    session_id=f"{task.ppg_data.session_id}",
                    focus_rate=avg_focus,  # 1분 평균
                    level=focus_level,     # 1분 집중 레벨
                    time=formatted_date
                )

                await task.send_data_to_watch(focus_test_data)

            #------------------------------------------------#
            
            ############### TEST ###############
            # ##### 4. task.ppg_data.user_id를 다시 ulid로 매핑
            # task.ppg_data.user_id = await mapper.get_ulid(task.ppg_data.user_id)
            # if task.count == 100:
            #     logger.info(f"변환된 user_id(ulid) : {task.ppg_data.user_id}")
            
            # # 현재 시간
            # seoul_timezone = pytz.timezone('Asia/Seoul')
            # now = datetime.now(seoul_timezone)
            # formatted_date = now.strftime("%Y-%m-%dT%H:%M:%S.%f%z")
            
            # ##### 5. focus_test_data (ai모델 출력값 임의 설정) 생성
            # focus_test_data = focus_data(
            #     user_id=f"{task.ppg_data.user_id}",
            #     session_id=f"{task.ppg_data.session_id}",
            #     focus_rate=0,
            #     level=0,
            #     time=f"{formatted_date}"
            # )
            
            ##### 6. focus_test_data 스마트 워치에 전송
            # await task.send_data_to_watch(focus_test_data)
            ####################################

            #-------------------------#
            
    ##### 6. 웹소켓 연결 종료
    except WebSocketDisconnect:
        logger.info(f"client disconnected : {websocket.client}")
        logger.info(f"받은 총 데이터 개수 : {task.count}")
        
        predictor.cleanup_session(task.ppg_data.user_id_as_int, task.ppg_data.session_id)

    finally:
        ##### 7. 정리 작업
        await task.cleanup()
        
if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=18001, reload=False)