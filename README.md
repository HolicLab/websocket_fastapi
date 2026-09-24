## WebSocket Server by FastAPI

> 스마트워치 PPG(광혈류) 데이터를 실시간 수신 → AI 모델로 집중도 추론 → 워치·백엔드(CPU 서버)로 결과를 전달하는 **GPU 추론 서버용 WebSocket 게이트웨이**

## 📌 개요

- **프로젝트명**: HOLIC – 실시간 학습 집중도 측정 서비스의 WebSocket 서버
- **수행 기간**: 2025.03 ~ 2025.06
- **역할**
  - 스마트워치 ↔ AI 추론 모델 ↔ 백엔드(CPU) 서버를 잇는 실시간 데이터 파이프라인
  - 워치에서 초당 수십 건씩 들어오는 PPG 데이터를 끊김 없이 수신·처리
  - 추론된 집중도(평균 집중도, 레벨)를 워치에 즉시 반환하고, 백엔드에 세션 단위로 저장
- **특징**
  - `asyncio` 기반 비동기 처리로 수신·추론·전송을 분리
  - 세션별 큐 + 백그라운드 태스크로 백엔드 전송을 비동기화
  - ULID ↔ 숫자 ID 매핑 테이블로 AI 모델 입력 형식 호환

## 🛠 사용 기술

| 구분 | 기술 |
| --- | --- |
| Language | Python 3.10+ |
| Framework | FastAPI, Starlette WebSocket, Uvicorn |
| 비동기 처리 | asyncio (`Queue`, `Lock`, `create_task`), aiohttp |
| 데이터 모델 | dataclasses, Pydantic |
| AI 연동 | `deeplearning_pkg.real_time_predict.RealTimePredictor` (외부 패키지) |
| 인프라 | Docker, Docker Compose (Ubuntu 22.04) |
| 기타 | pytz (Asia/Seoul 타임존), JSON 파일 기반 매핑 저장소 |

## 📁 폴더 구조

```
websocket_fastapi/
├── main.py              # FastAPI 앱 진입점 (WebSocket /ws, POST /avg_focus)
├── application.py       # 핵심 로직: Task(연결 단위 처리), AuthService(GPU 로그인)
├── domain.py            # 송·수신 데이터 모델 (ppg_data, focus_data)
├── mapping_table.py     # UlidMapper: ULID ↔ 숫자 ID 매핑 테이블
├── ulid_mapping.json    # 매핑 테이블 영속 저장 파일
├── test_client.py       # WebSocket 테스트 클라이언트 (PPG 더미 데이터 전송)
├── Dockerfile           # Ubuntu 22.04 + FastAPI 실행 환경
├── docker-compose.yml   # 컨테이너 구성 (포트 18001)
└── README.md
```

- 실행에 필요한 최소 파일: `main.py`, `application.py`, `domain.py`, `mapping_table.py`
- AI 모델 패키지는 상위 디렉터리 `../Real_Time_Predict_PKG`에 위치해야 함

## 🏗 시스템 구조

```
┌──────────────┐   PPG (WebSocket)    ┌────────────────────────────┐
│  Smart Watch │ ───────────────────▶ │   WebSocket Server (GPU)   │
│              │ ◀─────────────────── │                            │
└──────────────┘  집중도 결과 (JSON)    │  ┌──────────────────────┐  │
                                      │  │ RealTimePredictor    │  │
                                      │  │ (집중도 추론 모델)     │  │
                                      │  └──────────────────────┘  │
                                      └─────────────┬──────────────┘
                                                    │ HTTPS POST (Bearer Token)
                                                    ▼
                                      ┌────────────────────────────┐
                                      │  Backend API (CPU Server)  │
                                      │  /users/gpu/login          │
                                      │  /study/data               │
                                      └────────────────────────────┘
```

### 데이터 처리 흐름

1. 워치가 `/ws`로 WebSocket 연결 → 연결마다 `Task` 인스턴스 생성
2. 수신 루프는 메시지를 `input_queue`에 **넣기만** 함 (수신 지연 최소화)
3. 별도 태스크(`process_message_queue`)가 큐에서 꺼내 처리
   - JSON 파싱 → `ppg_data` 생성 → 필수값 검증
   - `user_id`(ULID) → 숫자 ID 변환
   - `predictor.on_receive_ppg()`로 모델에 PPG 누적
   - `predictor.summarize_focus()` 결과가 나오면 집중도 산출
4. 집중도 결과를 ULID로 되돌려 워치에 `send_json` 전송
5. 동시에 세션별 큐(`focus_queues[session_id]`)에 적재
6. 세션별 백그라운드 태스크가 GPU 로그인 후 토큰으로 백엔드 `/study/data`에 POST
7. 연결 종료 시 `cleanup()` 실행
   - 남은 입력 큐 처리 완료 대기
   - 모델 세션 정리 (`predictor.cleanup_session`)
   - 세션 큐의 잔여 데이터 백엔드 전송 (최대 3회 재시도)
   - 백그라운드 태스크 취소 및 HTTP 세션 종료

## 💡 핵심 설계

### 1. 수신과 처리의 분리 (Producer–Consumer)
- 수신 루프: `receive_text()` → `input_queue.put()`만 수행
- 처리 태스크: 파싱·매핑·추론·전송 담당
- 효과: 모델 추론이 느려져도 WebSocket 수신이 막히지 않음

### 2. 연결 단위 상태 격리 (`Task` 클래스)
- WebSocket 연결마다 독립된 `Task` 인스턴스 생성
- 입력 큐, 세션 큐, 백그라운드 태스크, 인증 세션을 연결별로 관리
- 다중 사용자 동시 접속 시에도 상태 충돌 없음

### 3. 세션별 비동기 전송 큐
- `session_id`별 `asyncio.Queue` + 전담 백그라운드 태스크를 지연 생성
- 워치 응답과 백엔드 저장을 분리 → 백엔드 지연이 실시간 응답에 영향 없음
- 전송 실패 시 로컬 버퍼 유지 → 다음 전송 시 함께 재전송

### 4. ULID ↔ 숫자 ID 매핑 (`UlidMapper`)
- 서비스는 ULID 문자열, AI 모델은 정수 ID를 사용 → 양방향 매핑 테이블로 해결
- 2000번부터 순차 발급, `ulid_mapping.json`에 영속화
- `asyncio.Lock` + Double-checked locking으로 동시 발급 시 중복 방지
- 파일 쓰기는 `run_in_executor`로 오프로딩 → 이벤트 루프 블로킹 방지

### 5. 안정적인 종료 처리
- `finally` 블록에서 항상 `cleanup()` 호출
- 미전송 데이터 flush + 재시도 로직으로 데이터 유실 최소화
- 모든 백그라운드 태스크를 `cancel` 후 `gather`로 안전하게 회수

## ✨ 주요 기능

- **실시간 PPG 수신**: WebSocket `/ws` 엔드포인트
- **실시간 집중도 추론**: `RealTimePredictor` 연동, 평균 집중도·레벨 산출
- **워치 피드백**: 추론 결과를 즉시 워치로 전송
- **백엔드 연동**: GPU 전용 로그인 → 토큰 기반 집중도 데이터 저장
- **ID 매핑**: ULID ↔ 숫자 ID 자동 발급·조회
- **세션 종료 API**: `POST /avg_focus` (세션 평균 집중도 수신)
- **로그**: 연결/해제, 수신 건수, 전송 성공·실패 로깅

## 📨 데이터 형식

**워치 → 서버 (PPG)**
```json
{
  "user_id": "01JS4H9F14TFCF0MX0A8K0R75Y",
  "session_id": "01JSVXV1H2TFQXA38SMX24ZGF1",
  "ppg_value": 1.52,
  "time": "2025-05-01T13:00:00.000000+0900"
}
```

**서버 → 워치 (집중도)**
```json
{
  "user_id": "01JS4H9F14TFCF0MX0A8K0R75Y",
  "session_id": "01JSVXV1H2TFQXA38SMX24ZGF1",
  "focus_rate": 0.82,
  "level": 3,
  "time": "2025-05-01T13:00:07.123456+0900"
}
```

**서버 → 백엔드 (`POST /study/data`)**
```json
{
  "session_id": "01JSVXV1H2TFQXA38SMX24ZGF1",
  "datas": [
    { "focus_rate": 0.82, "level": 3, "time": "2025-05-01T13:00:07.123456+0900" }
  ]
}
```

## 🚀 실행 방법

```bash
# 1) Docker 컨테이너 생성
docker-compose up --build -d

# 2) 직접 실행 (포트 18001)
python3 main.py

# 3) 로그 파일로 출력하며 백그라운드 실행
python3 main.py > test.log 2>&1 &

# 4) 테스트 클라이언트로 동작 확인
python3 test_client.py
```

- 서버가 켜져 있지 않으면 스마트워치에서 학습 시작 불가
- 컨테이너 기동 시 자동 실행하려면 `Dockerfile`의 아래 줄 주석 해제
  ```dockerfile
  CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "18001", "--reload"]
  ```

### 백그라운드 프로세스 종료 (코드 수정 후 재시작 시)

```bash
# 프로세스 조회 → python3 main.py 의 PID 확인
ps -ef | grep main.py

# 종료
kill -9 <PID>
```
