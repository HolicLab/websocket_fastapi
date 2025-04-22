from dataclasses import dataclass
import json
from domain import recieve_data, send_data
import asyncio
import aiohttp

MAX_BUFF_SIZE = 1000

# 로그인 관련 클래스
class AuthService:
    def __init__(self):
        self.session = aiohttp.ClientSession()
        
    # 토큰 읽어오는 메서드(로그인)
    async def get_token(self, user_id, session_id):
        url = "https://youngwon.site/gpu/login"
        async with self.session.post(url, json={"user_id": user_id, "session_id": session_id}) as response:
            if response.status == 200:
                result = await response.json()
                return result.get("token")
            return None

    async def close(self):
        await self.session.close()


# 웹소켓 동작 수행 클래스
class Task:
    def __init__(self):
        self.recieve_buffers = {}
        self.auth_service = AuthService()
        
    # 버퍼에(메모리) 데이터 저장
    async def set_data(self, json_data: json):
        session_id = None
        if isinstance(json_data, str):
            json_data = json.loads(json_data)
            
        user_id = json_data.get("user_id", "No user_id key found")
        session_id = json_data.get("session_id", "No session_id key found")
        ppg_value = json_data.get("ppg_value", "No ppg_value key found")
        date = json_data.get("date", "No date key found")
        
        self.recieve = recieve_data(user_id=user_id, session_id=session_id, ppg_value=ppg_value, date=date)
        
        if session_id not in self.recieve_buffers:
            self.recieve_buffers[session_id] = []
            asyncio.create_task(self.send_cpu_server(user_id, session_id))    
    
        # 버퍼에 추가
        self.recieve_buffers[session_id].append(self.recieve)
        
    # 버퍼 비우기
    async def process_buffer(self, session_id):
        self.recieve_buffers[session_id].clear()
        
    # 백그라운드 작업(cpu로 주기적 post 전송)
    async def send_cpu_server(self, user_id, session_id):
        token = await self.auth_service.get_token(user_id, session_id)
        if not token:
            return
        
        while True:
            if len(self.recieve_buffers[session_id]) >= MAX_BUFF_SIZE:
                # 데이터 처리 로직
                url = "https://youngwon.site/gpu/process"
                headers = {"Authorization": f"Bearer {token}"}
                data = [item.__dict__ for item in self.recieve_buffers[session_id]]
                
                async with aiohttp.ClientSession() as session:
                    async with session.post(url, json=data, headers=headers) as response:
                        if response.status == 200:
                            await self.process_buffer(session_id)
            # 적절한 대기 시간
            await asyncio.sleep(1)
        
    async def close(self):
        await self.auth_service.close()
        

    