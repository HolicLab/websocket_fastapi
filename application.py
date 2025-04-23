from dataclasses import dataclass
from fastapi import WebSocket
import json
from domain import ppg_data, focus_data
import asyncio
import aiohttp
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MAX_BUFF_SIZE = 1000

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
        self.focus_buffers = {}
        self.ppg_data = ppg_data()
        self.background_tasks = set()  # 백그라운드 태스크 추적용 세트
        
    # ppg 데이터 수신
    async def receive_ppg_data(self, json_data: json):
        try:
            session_id = None
            if isinstance(json_data, str):
                json_data = json.loads(json_data)
                
            self.ppg_data.user_id = json_data.get("user_id", "No user_id key found")
            self.ppg_data.session_id = json_data.get("session_id", "No session_id key found")
            self.ppg_data.ppg_value = json_data.get("ppg_value", "No ppg_value key found")
            self.ppg_data.time = json_data.get("time", "No time key found")
            
            logger.info(f"Received message from client: {self.ppg_data}")  # 로그 추가
        except json.JSONDecodeError:
            logger.info("Received data is not JSON")
                    
    # 워치에 집중도 데이터 전송
    async def send_data_to_watch(self, focus_data: focus_data):
        text = {
            "user_id": focus_data.user_id,
            "session_id": focus_data.session_id,
            "focus_rate": focus_data.focus_rate,
            "level": focus_data.level,
            "time": focus_data.time
        }
        await self.websocket.send_json(text)
        
        # 버퍼에 집중도 결과값 저장
        if focus_data.session_id not in self.focus_buffers:
            self.focus_buffers[focus_data.session_id] = []
            task = asyncio.create_task(self.send_cpu_server(focus_data.user_id, focus_data.session_id))
            
            # 태스크 완료 시 세트에서 제거하는 콜백 추가
            task.add_done_callback(self.background_tasks.discard)
            
            # 세트에 태스크 추가
            self.background_tasks.add(task)
        
        self.focus_buffers[focus_data.session_id].append(focus_data)                
                      
    # 백그라운드 작업(cpu로 주기적 post 전송)
    async def send_cpu_server(self, user_id: str, session_id: str):
        access_token = await self.auth_service.get_token(user_id, session_id)
        if not access_token:
            return
        
        logger.info(f"user : {user_id} gpu 로그인 완료")
        while True:
            # 일정 버퍼 이상일 시
            if len(self.focus_buffers[session_id]) >= MAX_BUFF_SIZE:
                url = "https://youngwon.site/study/data"
                headers = {"Authorization": f"Bearer {access_token}"}
                
                focus_result = []
                for focus_obj in self.focus_buffers[session_id]:
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
                                logger.info(f"데이터 전송 성공: {session_id}, 데이터 수: {len(focus_result)}")
                                await self.process_buffer(session_id)
                            else:
                                error_text = await response.text()
                                logger.error(f"데이터 전송 실패: {response.status}, 오류: {error_text}")
                    except Exception as e:
                        logger.error(f"요청 중 예외 발생: {str(e)}")
            # 적절한 대기 시간
            await asyncio.sleep(5) 
            
    # 버퍼 비우기
    async def process_buffer(self, session_id: str):
        self.focus_buffers[session_id].clear()
        
    async def close(self):
        await self.auth_service.close()
        
    # 태스크 정리 메서드 추가
    async def cleanup(self):
        # 모든 백그라운드 태스크 취소
        for task in self.background_tasks:
            task.cancel()
            
        # 취소된 태스크가 완료될 때까지 대기 (예외 처리)
        if self.background_tasks:
            await asyncio.gather(*self.background_tasks, return_exceptions=True)
            
        # 세션 닫기
        await self.auth_service.close()
        
        logger.info("모든 백그라운드 태스크가 정리되었습니다.")
        
        
# 테스트      
# async def main():
#     authservice = AuthService()
#     token = await authservice.get_token("01JS4H9F14TFCF0MX0A8K0R75Y", "01JSGKWS92KNCHBQC54VMWCE9W")      
#     await authservice.close()
#     return token  

# 테스트 
# if __name__ == "__main__":
#     # 이벤트 루프 생성 및 코루틴 실행
#     loop = asyncio.get_event_loop()
#     result = loop.run_until_complete(main())
#     print(f"최종 결과: {result}")