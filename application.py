from dataclasses import dataclass
from fastapi import WebSocket
import json
from domain import ppg_data, focus_data
import asyncio
import aiohttp
import logging
from datetime import datetime
from mapping_table import UlidMapper
import pytz

### 딥러닝 모듈 바인딩 ###
import sys
import os
sys.path.append(os.path.abspath("../Real_Time_Predict_PKG"))
from deeplearning_pkg.real_time_predict import RealTimePredictor
###

predictor = RealTimePredictor()

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MAX_BUFF_SIZE = 1000

# 2000번부터 시작하는 매퍼 생성
mapper = UlidMapper(start_id=2000)


# 로그인 관련 클래스
class AuthService:
    def __init__(self):
        self.session = aiohttp.ClientSession()
        
    # 토큰 읽어오는 메서드(로그인)
    async def get_token(self, user_id, session_id):
        url = "https://youngwon.site/users/gpu/login"
        async with self.session.post(url, json={"user_id": user_id, "session_id": session_id}) as response:
            if response.status == 200:
                result = await response.json()
                logger.info(result.get("access_token"))
                return result.get("access_token")
            print("false")
            return None

    async def close(self):
        await self.session.close()


# 웹소켓 동작 수행 클래스
class Task:
    def __init__(self, websocket: WebSocket):
        self.websocket = websocket
        self.auth_service = AuthService()
        self.focus_queues = {}                          # 🔥 수정 (큐 방식으로 변환)
        self.input_queue = asyncio.Queue()              # 입력 메시지 저장 큐
        self.background_tasks = set()                   # 백그라운드 태스크 추적용 세트
        self.count = 0
        self.running = True                             # 처리 루프 제어 변수
        self.last_processed_user_id = None              # AI의 cleanup()을 위한 변수들
        self.last_processed_session_id = None
        self.last_processed_user_id_as_int = None
        
    # 메시지 큐 처리 메서드 
    async def process_message_queue(self):
        """입력 큐에서 메시지를 처리하는 비동기 태스크"""
        while self.running:
            try:
                # 큐에서 메시지 가져오기
                message = await self.input_queue.get()
                
                # JSON 파싱
                if isinstance(message, str):
                    data = json.loads(message)
                else:
                    data = message
                
                # PPG 데이터 생성
                current_data = ppg_data()
                current_data.user_id = data.get("user_id", "No user_id key found")
                current_data.session_id = data.get("session_id", "No session_id key found")
                current_data.ppg_value = data.get("ppg_value", "No ppg_value key found")
                
                # 시간 처리
                time_str = data.get("time", None)
                if time_str:
                    try:
                        current_data.time = datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%S.%f%z")
                        current_data.date = current_data.time
                    except Exception as e:
                        logger.error(f"타임 포맷 오류 발생: {e}")
                        current_data.time = datetime.now()
                        current_data.date = current_data.time
                else:
                    current_data.time = datetime.now()
                    current_data.date = current_data.time
                
                # logger.info(f"Processing message: {current_data}")
                self.count += 1
                
                # 데이터 유효성 검사
                if current_data.date is None or current_data.ppg_value is None or current_data.session_id is None:
                    logger.error(f"수신 데이터 누락 - date: {current_data.date}, value: {current_data.ppg_value}, session: {current_data.session_id}")
                    self.input_queue.task_done()
                    continue  # 다음 메시지로 넘어가기
                
                # 사용자 ID를 숫자 ID로 매핑
                current_data.user_id_as_int = await mapper.get_numeric_id(current_data.user_id)
                
                # 사용자 ID와 세션 ID 저장
                self.last_processed_user_id = current_data.user_id
                self.last_processed_session_id = current_data.session_id
                self.last_processed_user_id_as_int = current_data.user_id_as_int
                
                # 예측기로 전달
                predictor.on_receive_ppg(
                    current_data.ppg_value,
                    current_data.date,
                    current_data.user_id_as_int,
                    current_data.session_id
                )
                
                # 요약 확인
                summary_result = predictor.summarize_focus(
                    current_data.user_id_as_int,
                    current_data.session_id
                )
                
                if summary_result is not None:
                    logger.info(f"summary_result 결과 : {summary_result}")
                    avg_focus, focus_level = summary_result
                    
                    # ULID로 다시 매핑
                    original_user_id = await mapper.get_ulid(current_data.user_id_as_int)
                    
                    # 현재 시간
                    seoul_timezone = pytz.timezone('Asia/Seoul')
                    now = datetime.now(seoul_timezone)
                    formatted_date = now.strftime("%Y-%m-%dT%H:%M:%S.%f%z")
                    
                    # 포커스 데이터 패킹
                    focus_test_data = focus_data(
                        user_id=original_user_id,
                        session_id=current_data.session_id,
                        focus_rate=avg_focus,
                        level=focus_level,
                        time=formatted_date
                    )
                    
                    logger.info(f"워치 전송 데이터 : {focus_test_data}")
                    await self.send_data_to_watch(focus_test_data)
                
                # 작업 완료 표시
                self.input_queue.task_done()
                
            except Exception as e:
                logger.error(f"메시지 처리 중 오류 발생: {str(e)}")
                try:
                    self.input_queue.task_done()
                except:
                    pass

    # 세션별 큐 가져오기 (없을 경우 새로 생성)
    async def get_session_queue(self, session_id, user_id):
        if session_id not in self.focus_queues:
            self.focus_queues[session_id] = asyncio.Queue()
            
            # 해당 세션의 큐를 처리할 태스크 생성
            queue_task = asyncio.create_task(self.process_session_queue(user_id, session_id))
            queue_task.add_done_callback(self.background_tasks.discard)
            self.background_tasks.add(queue_task)
        return self.focus_queues[session_id]


    # 워치에 집중도 데이터 전송 -> process_session_queue (백그라운드 작업, 5초 주기로 버퍼 확인 후 전송)
    async def send_data_to_watch(self, focus_data: focus_data):
        text = {
            "user_id": focus_data.user_id,
            "session_id": focus_data.session_id,
            "focus_rate": focus_data.focus_rate,
            "level": focus_data.level,
            "time": focus_data.time
        }
        await self.websocket.send_json(text)
        
        queue = await self.get_session_queue(focus_data.session_id, focus_data.user_id)
        await queue.put(focus_data)               
                      
    # 백그라운드 작업(cpu로 주기적 post 전송)
    async def process_session_queue(self, user_id: str, session_id: str):
        access_token = await self.auth_service.get_token(user_id, session_id)
        if not access_token:
            logger.info(f"user : {user_id} gpu 로그인 실패")
            return
        
        logger.info(f"user : {user_id} gpu 로그인 완료")
        buffer = []   # 로컬 버퍼
        
        while True:
            try:
                # 데이터 기다리기
                focus_data = await self.focus_queues[session_id].get()
                buffer.append(focus_data)
                self.focus_queues[session_id].task_done()
                
                # 버퍼가 가득차거나 타임아웃 후 버퍼에 항목이 있으면 처리
                if len(buffer) > 0:
                    url = "https://youngwon.site/study/data"
                    headers = {"Authorization": f"Bearer {access_token}"}
                    
                    focus_result = []
                    for focus_obj in buffer:
                        focus_dict = {
                            "focus_rate": focus_obj.focus_rate,
                            "level": focus_obj.level,
                            "time": focus_obj.time
                        }
                        focus_result.append(focus_dict)

                    payload = {
                        "session_id": session_id,
                        "datas": focus_result
                    }

                    async with aiohttp.ClientSession() as session:
                        try:
                            async with session.post(url, json=payload, headers=headers) as response:
                                if response.status == 200 or response.status == 201:
                                    logger.info(f"데이터 전송 성공: {payload}, 데이터 수: {len(focus_result)}")
                                    buffer.clear()  # 로컬 버퍼 지우기
                                else:
                                    error_text = await response.text()
                                    logger.error(f"데이터 전송 실패: {response.status}, 오류: {error_text}")
                        except Exception as e:
                            logger.error(f"요청 중 예외 발생: {str(e)}")

            except Exception as e:
                logger.error(f"큐 처리 중 오류: {str(e)}")
                
    async def cleanup_last_session(self):
        """마지막으로 처리된 세션을 정리"""
        if self.last_processed_user_id_as_int and self.last_processed_session_id:
            predictor.cleanup_session(
                self.last_processed_user_id_as_int, 
                self.last_processed_session_id
            )
            logger.info(f"세션 정리 완료: 사용자 {self.last_processed_user_id}, 세션 {self.last_processed_session_id}")
            return True
        else:
            logger.warning("처리된 메시지가 없어 세션 정리를 수행하지 않았습니다.")
            return False
            
    # 태스크 정리 메서드 추가
    async def cleanup(self):
        
        self.running = False

        if not self.input_queue.empty():
            await self.input_queue.join()
        
        await self.cleanup_last_session()
        
        # 나머지 버퍼 정리
        for session_id, queue in self.focus_queues.items():
            items = []
            while not queue.empty():
                try:
                    item = queue.get_nowait()
                    items.append(item)
                    queue.task_done()
                except asyncio.QueueEmpty:
                    break
            
            if items:
                access_token = await self.auth_service.get_token(items[0].user_id, session_id)
                if access_token:
                    url = "https://youngwon.site/study/data"
                    headers = {"Authorization": f"Bearer {access_token}"}
                    focus_result = [
                        {
                            "focus_rate": f.focus_rate,
                            "level": f.level,
                            "time": f.time
                        } for f in items
                    ]
                    payload = {
                        "session_id": session_id,
                        "datas": focus_result
                    }
                    async with aiohttp.ClientSession() as session:
                        retry_count = 3
                        while retry_count > 0:
                            try:
                                async with session.post(url, json=payload, headers=headers) as response:
                                    if response.status in [200, 201]:
                                        logger.info(f"종료 시 데이터 전송 성공: {session_id}, 데이터 수: {len(focus_result)}")
                                        break
                                    else:
                                        error_text = await response.text()
                                        logger.error(f"종료 시 전송 실패: {response.status}, 오류: {error_text}")
                            except Exception as e:
                                logger.error(f"종료 시 전송 중 예외 발생: {str(e)}")
                            
                            retry_count -= 1
                            await asyncio.sleep(1)  # 1초 대기 후 재시도
        
        # 모든 백그라운드 태스크 취소
        for task in self.background_tasks:
            task.cancel()
            
        # 취소된 태스크가 완료될 때까지 대기 (예외 처리)
        if self.background_tasks:
            await asyncio.gather(*self.background_tasks, return_exceptions=True)
            
        # 세션 닫기
        await self.auth_service.close()
        logger.info("모든 백그라운드 태스크가 정리되었습니다.")
        