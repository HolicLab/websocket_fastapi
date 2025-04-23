from dataclasses import dataclass
from fastapi import WebSocket
import json
from domain import ppg_data, focus_data
import asyncio
import aiohttp
import logging
from datetime import datetime, timedelta

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
        self.recieve_buffers = {}
        self.send_buffers = {}
        self.auth_service = AuthService()
        self.ppg_data = ppg_data()
        self.focus_data = focus_data()
        self.websocket = websocket
        
    async def set_focus_data(self, ):
        session_id = None
        
        
    # 워치에 집중도 데이터 전송
    async def send_watch(self):
        text = {
            "user_id": self.focus_data.user_id,
            "session_id": self.focus_data.session_id,
            "focus_rate": self.focus_data.focus_rate,
            "level": self.focus_data.level,
            "time": self.focus_data.time
        }
        await self.websocket.send_json(text)
        
    # 버퍼에(메모리) 데이터 저장
    async def set_ppg_data(self, json_data: json):
        session_id = None
        if isinstance(json_data, str):
            json_data = json.loads(json_data)
            
        self.ppg_data.user_id = json_data.get("user_id", "No user_id key found")
        self.ppg_data.session_id = json_data.get("session_id", "No session_id key found")
        self.ppg_data.ppg_value = json_data.get("ppg_value", "No ppg_value key found")
        self.ppg_data.date = json_data.get("date", "No date key found")
        
        if session_id not in self.recieve_buffers:
            self.recieve_buffers[session_id] = []
            asyncio.create_task(self.send_cpu_server(self.ppg_data.user_iduser_id, self.ppg_data.session_id))    
    
        # 버퍼에 추가
        self.recieve_buffers[session_id].append(self.recieve)
        
    # 백그라운드 작업(cpu로 주기적 post 전송)
    async def send_cpu_server(self, user_id: str, session_id: str):
        access_token = await self.auth_service.get_token(user_id, session_id)
        if not access_token:
            return
        
        logger.info(f"user : {user_id} gpu 로그인 완료")
        while True:
            # 일정 버퍼 이상일 시
            if len(self.recieve_buffers[session_id]) >= MAX_BUFF_SIZE:
                url = "https://youngwon.site/study/data"
                headers = {"Authorization": f"Bearer {access_token}"}
                
                # /study/data post 요청 테스트 데이터
                test_data_list = []
                for i in range(3):
                    test_data = {
                        "focus_rate": 0.1,
                        "level": 1,
                        "time" : f"test{i}"
                    }
                    test_data_list.append(test_data)

                payload = {
                    "session_id": session_id,
                    "datas": test_data_list
                }

                async with aiohttp.ClientSession() as session:
                    try:
                        async with session.post(url, json=payload, headers=headers) as response:
                            if response.status == 200 or response.status == 201:
                                logger.info(f"데이터 전송 성공: {session_id}, 데이터 수: {len(test_data_list)}")
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
        self.recieve_buffers[session_id].clear()
        
    async def close(self):
        await self.auth_service.close()
        
        
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