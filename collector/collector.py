import json
import requests
import redis
import time

from altair import sequence

# 1. Redis 연결 설정
# Docker 환경이면 host="redis", 로컬 테스트면 host="localhost"
r = redis.Redis(host="127.0.0.1", port=6379, db=0, decode_responses=True)

# 2. 위키피디아 실시간 스트림 URL
url = 'https://stream.wikimedia.org/v2/stream/recentchange'


def start_collecting():
    print("📡 위키피디아 실시간 데이터 수집 및 Redis 저장 시작...")
    while True: # 열결이 끊겨도 무한 반복
        try:
            # 스트림 데이터 가져오기
            response = requests.get(url, stream=True, headers={'User-Agent': 'WikiTrend-Project'})

            for line in response.iter_lines():
                if line:
                    decoded_line = line.decode('utf-8')

                    # 'data: '로 시작하는 실제 데이터만 추출
                    if decoded_line.startswith('data: '):
                        try:
                            raw_json = decoded_line[6:]
                            data = json.loads(raw_json)

                            # 영어 위키피디아 데이터만 필터링
                            if data.get('wiki') == 'enwiki':
                                # 3. Redis에 데이터 저장
                                # 'recent_changes'라는 리스트의 왼쪽에 데이터 삽입 (LPUSH)
                                r.lpush("recent_changes", json.dumps(data))
                                # 최신 10000개까지만 유지 (메모리 관리)
                                r.ltrim("recent_changes", 0, 99999)

                                # main.py를 위한 데이터 묶음 만들기
                                recent_list = r.lrange("recent_changes", 0, 20)
                                sequence_data = [json.loads(item) for item in recent_list]

                                # 시간순으로 정렬
                                sequence_data.sort(key=lambda x: x.get('timestamp', 0))

                                r.set("latest_sequence", json.dumps(sequence_data))

                                print(f"🚀 Saved: {data.get('title')[:30]}...")

                        except json.JSONDecodeError:
                            continue
        except Exception as e:
            print(f"❌ 에러 발생: {e}")


if __name__ == "__main__":
    start_collecting()