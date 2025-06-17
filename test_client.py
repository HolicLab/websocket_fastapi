import asyncio
import websockets
import json
import time
from datetime import datetime

async def send_ppg_data():
    uri = "ws://localhost:18001/ws"
    user_id = "01JS4H9F14TFCF0MX0A8K0R75Y"         # name : youngwon, id : lka1116@naver.com, password : qpqp1234
    session_id = "01JSVXV1H2TFQXA38SMX24ZGF1"
    
    try:
        async with websockets.connect(uri) as websocket:
            print("웹소켓 서버에 연결되었습니다.")
            
            # 10개의 PPG 데이터 전송
            for i in range(10):
                # 실제 PPG 값은 보통 0.5~3.0 사이
                ppg_value = 1.5 + (i * 0.1)
                
                # 데이터 패킷 구성
                data = {
                    "user_id": user_id,
                    "session_id": session_id,
                    "ppg_value": ppg_value,
                    "time": datetime.now().isoformat() + "Z"
                }
                
                # 데이터 전송
                await websocket.send(json.dumps(data))
                print(f"전송: {data}")
                
                # 응답 대기 (비동기 처리)
                try:
                    response_task = asyncio.create_task(websocket.recv())
                    response = await asyncio.wait_for(response_task, timeout=5.0)
                    print(f"수신: {response}")
                except asyncio.TimeoutError:
                    print("응답 타임아웃")
                except Exception as e:
                    print(f"응답 수신 중 오류: {str(e)}")
                    break
                
                await asyncio.sleep(1)  # 1초 대기
            
            print("테스트 완료. 연결 종료.")
    except Exception as conn_error:
        print(f"연결 중 오류 발생: {str(conn_error)}")

if __name__ == "__main__":
    asyncio.run(send_ppg_data())