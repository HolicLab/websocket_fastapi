import asyncio
import websockets
import json
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_pytorch_websocket():
    try:
        # PyTorch 컨테이너의 WebSocket 서버에 연결
        uri = "ws://pytorch:18002"
        
        logger.info(f"Connecting to PyTorch WebSocket: {uri}")
        
        async with websockets.connect(uri) as websocket:
            # 테스트 데이터 전송
            test_data = {
                "session_id": "test_session_123",
                "ppg_value": 75.5,
                "date": "2024-03-27T12:00:00"
            }
            
            logger.info(f"Sending test data to PyTorch: {test_data}")
            await websocket.send(json.dumps(test_data))
            
            # 응답 대기
            response = await websocket.recv()
            logger.info(f"Received response from PyTorch: {response}")
    
    except Exception as e:
        logger.error(f"WebSocket connection to PyTorch error: {e}")
        
async def main():
    await test_pytorch_websocket()

if __name__ == "__main__":
    asyncio.run(main())