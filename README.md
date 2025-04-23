### WebSocket_Server_by_fastAPI

### 실행 방법 - 4가지 방법 중 택 1
```bash
# 위치: /holic/websocket_fastapi

# 컨테이너 생성(생성 되어있음)
docker-compose up --build -d

# 그냥 실행
python3 main.py

# test.log파일에 로그 결과 출력하며 실행
python3 main.py > test.log 2>&1

# test.log파일에 로그 결과 출력 & 백그라운드 실행 (현재 상태)
python3 main.py > test.log 2>&1 &

#  Dockerfile에서 아래 줄 주석 풀면  컨테이너 실행시 자동으로 실행됨(테스트 필요)
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "18001", "--reload"]
```
- 서버 웹소켓 안 켜두면 스마트 워치에서 학습 시작x 
- 현재는 background로 켜둠, reload(자동 저장) 안됨
#### 백그라운드 실행 상태 종료하는 법(코드 수정시 껐다 켜야함)
```bash
# 프로세스 조회
ps -ef

# ex) root      258820  104157  0 17:57 pts/0    00:00:00 python3 main.py
# 위 형식으로 뜨는 프로세스를 종료해야 꺼짐(pid : 258820)
kill -9 258820

# 파일 실행
```
### 파일 구성
├── ./Dockerfile
├── ./README.md
├── ./application.py                                  # 웹소켓 동작 제어 (Task)
├── ./docker-compose.yml           
├── ./domain.py                                       # 기본 자료구조(송, 수신 데이터)
├── ./main.py                                           # 메인 실행
├── ./mapping_table.py                           # 매핑 테이블 클래스(ulid -> 숫자, 숫자 -> ulid)
├── ./test.log                                            # 로그파일
└── ./ulid_mapping.json                           # 매핑 테이블(json)
- 전부 도커 컨테이너 내부에 있음
- main.py, application.py domain.py, mapping_table.py만 같은 디렉터리 아래 있으면 된다.

### 시스템 구성
```python
# domain.py (전송, 수신 데이터 형식 설정)

# 워치 -> 서버로 전달받은 데이터(PPG 데이터 자료구조)
@dataclass
class ppg_data:
    user_id: str | int = None
    session_id: str = None
    ppg_value: float = 0.0
    time: datetime = None

# AI모델 -> 서버로 전달받은 데이터(집중도 결과 데이터 자료구조)
@dataclass
class focus_data:
    user_id: str | int = None
    session_id: str = None
    focus_rate: float = 0.0
    level: int = 0
    time: datetime = None

# 웹소켓 연결 별로 인스턴스 생성(웹 소켓 관리용 class)
class Task:
    def __init__(self, websocket: WebSocket):
        self.websocket = websocket
        self.auth_service = AuthService()                             # gpu -> cpu post 전송 위한 로그인
        self.focus_buffers = {}                                               # 집중도 데이터 cpu 전송을 위한 버퍼
        self.ppg_data = ppg_data()                                       # 수신한 ppg_data 임시 저장
        self.background_tasks = set() 
        self.count = 0
    async def receive_ppg_data(self, json_data: json):                         # ppg 데이터 수신
    async def send_data_to_watch(self, focus_data: focus_data):       # 워치에 집중도 데이터 전송
    async def send_cpu_server(self, user_id: str, session_id: str):       # 자동 호출
    async def process_buffer(self, session_id: str):                               # 자동 호출
    async def cleanup(self):                                                                   # 웹소켓 종료시 호출

# 매핑 테이블 클래스
class UlidMapper:
    def __init__(self, file_path="ulid_mapping.json", start_id=2000):    # 2000번 부터 1씩 증가
    def _load_mapping(self):                           #  파일에서 매핑 정보를 로드
    def _write_to_file(self, mapping_data):      # 실제 파일 쓰기 작업을 수행하는 동기 메서드 추가
    async def _save_mapping(self):                   # 매핑 정보를 파일에 저장 (async 추가)
    async def get_numeric_id(self, ulid: str) -> int:             # ULID에 해당하는 숫자 ID를 반환, 없으면 새로 생성 (async 추가)
    async def get_ulid(self, numeric_id: int) -> str:       # 숫자 ID에 해당하는 ULID를 반환

# 사용 예시(매퍼)
# 2000번부터 시작하는 매퍼 생성 
mapper = UlidMapper(start_id=2000) 

# ULID를 숫자로 변환
user_id_numeric = mapper.get_numeric_id("01JS4H9F14TFCF0MX0A8K0R75Y")  # 2000 반환 

# 숫자를 ULID로 변환 
original_ulid = mapper.get_ulid(user_id_numeric)     # "01JS4H9F14TFCF0MX0A8K0R75Y" 반환

```

### 현재 코드 동작 구조
```
1. main 실행 중
2. 웹소켓 연결 대기
3. 웹소켓 연결 완료
4. 각 웹소켓 연결 별 Task 인스턴스 생성
5. while문 무한 반복 (대략 7초 당 200개 ppg 데이터 수신)
	1. client(스마트 워치) ppg 데이터 한 개 수신
	2. task.ppg_data에 수신한 ppg, session_id 등을 저장
	3. task.ppg_data.user_id를 숫자로 매핑                                      - 숫자 변환 테스트용
	4. task.ppg_data.user_id를 다시 ulid로 매핑                               - ulid 변환 테스트용
	5. focus_test_data (ai모델 출력값 임의 설정) 생성                     - post 전송 테스트용
	6. focus_test_data 스마트 워치에 결과값 전송 
		1. 스마트 워치에 결과값 전송
		2. task.focus_buffers[session_id]에 결과값 저장
		3. if MAX_BUFF_SIZE 넘을 경우 post로 cpu서버에 전송 후 버퍼 clear()
6. 웹소켓 연결 종료
7. 정리 작업(버퍼의 남은 데이터를 cpu 서버로 전송)
```
