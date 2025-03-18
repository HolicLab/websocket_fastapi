import csv
import os
import asyncio
import logging
# 대략 1분
BUFFER_SIZE = 1500

buffers = {}

async def save_to_csv(buffer, filename):
    try:
        file_path = os.path.join("ppg_datas", filename)
        file_exists = os.path.isfile(file_path)
        with open(file_path, mode='a', newline='') as file:
            writer = csv.writer(file)
            if not file_exists:
                writer.writerow(["session_id", "ppg_value", "date"])
            for row in buffer:
                writer.writerow(row)
    except Exception as e:
        logger.error(f"Error saving to CSV: {e}")
    
    # 파일이 새로 생성된 경우 권한 변경
    if not file_exists:
        os.chmod(file_path, 0o777)
            
async def process_buffer(session_id):
    while True:
        global buffers
        if session_id in buffers:
            if len(buffers[session_id]) >= BUFFER_SIZE:
                await save_to_csv(buffers[session_id], f"ppg_data_{session_id}.csv")
                buffers[session_id].clear()
        await asyncio.sleep(5)