import json
import os
import logging
import asyncio 

logger = logging.getLogger(__name__)

class UlidMapper:
    def __init__(self, file_path="ulid_mapping.json", start_id=2000):
        """
        ULID와 숫자 ID 간의 매핑을 관리하는 클래스
        
        Args:
            file_path (str): 매핑 저장 JSON 파일 경로
            start_id (int): ID 시작 값 (기본값: 2000)
        """
        self.file_path = file_path
        self.start_id = start_id
        self.ulid_to_id = {}  # ULID -> 숫자 ID
        self.id_to_ulid = {}  # 숫자 ID -> ULID
        self.next_id = start_id  # 다음 사용할 ID
        self.lock = asyncio.Lock()  # 동시성 제어를 위한 비동기 락
        
        # 파일이 존재하면 로드
        if os.path.exists(file_path):
            self._load_mapping()
        
    #  파일에서 매핑 정보를 로드
    def _load_mapping(self):
        try:
            with open(self.file_path, 'r') as f:
                mapping_data = json.load(f)
                
            self.ulid_to_id = {str(k): int(v) for k, v in mapping_data.get('ulid_to_id', {}).items()}
            
            # id_to_ulid 및 next_id 재구성
            self.id_to_ulid = {int(k): str(v) for k, v in mapping_data.get('id_to_ulid', {}).items()}
            
            # 다음 ID 계산
            if self.id_to_ulid:
                self.next_id = max(int(k) for k in self.id_to_ulid.keys()) + 1
            else:
                self.next_id = self.start_id
                
            logger.info(f"매핑 파일 로드 완료: {len(self.ulid_to_id)}개 항목, 다음 ID: {self.next_id}")
        except Exception as e:
            logger.error(f"매핑 파일 로드 실패: {str(e)}")
            # 로드 실패 시 기본값 유지
    
    # 실제 파일 쓰기 작업을 수행하는 동기 메서드 추가
    def _write_to_file(self, mapping_data):
        with open(self.file_path, 'w') as f:
            json.dump(mapping_data, f)
    
    # 매핑 정보를 파일에 저장 (async 추가)
    async def _save_mapping(self):
        try:
            mapping_data = {
                'ulid_to_id': self.ulid_to_id,
                'id_to_ulid': self.id_to_ulid
            }
            
            # 파일 작업은 블로킹 작업이므로 run_in_executor로 실행
            await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self._write_to_file(mapping_data)
            )
                
        except Exception as e:
            logger.error(f"매핑 파일 저장 실패: {str(e)}")
      
    # ULID에 해당하는 숫자 ID를 반환, 없으면 새로 생성 (async 추가)
    async def get_numeric_id(self, ulid: str) -> int:
        # 락 없이 먼저 확인 (성능 최적화)
        if ulid in self.ulid_to_id:
            return self.ulid_to_id[ulid]
        
        # 없으면 락을 획득하고 다시 확인 (다른 스레드가 이미 추가했을 수 있음)
        async with self.lock:
            # 다시 확인
            if ulid in self.ulid_to_id:
                return self.ulid_to_id[ulid]
            
            # 새 매핑 생성
            new_id = self.next_id
            self.next_id += 1
            
            # 매핑 업데이트
            self.ulid_to_id[ulid] = new_id
            self.id_to_ulid[new_id] = ulid
            
            # 파일에 저장
            await self._save_mapping()
            
            logger.info(f"새 매핑 생성: {ulid} -> {new_id}")
            return new_id
            
    # 숫자 ID에 해당하는 ULID를 반환
    async def get_ulid(self, numeric_id: int) -> str:
        return self.id_to_ulid.get(numeric_id)