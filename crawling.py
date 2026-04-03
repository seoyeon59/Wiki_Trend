import json
import requests

# 1. 설정
output_file = "dataset/enwiki_raw_log_2.txt"  # 또는 .jsonl
target_count = 10000
current_count = 0
url = 'https://stream.wikimedia.org/v2/stream/recentchange'

print(f"📡 [Raw Log 수집] 컬럼 구분 없이 {target_count}개 통째로 저장 시작...")

try:
    headers = {'User-Agent': 'Mozilla/5.0'}
    response = requests.get(url, stream=True, headers=headers)

    # 'w' 모드로 열어서 매번 새로 수집하거나, 'a' 모드로 이어쓰기 하세요.
    with open(output_file, mode='a', encoding='utf-8') as f:
        for line in response.iter_lines():
            if line:
                decoded_line = line.decode('utf-8')

                # 'data: '로 시작하는 진짜 데이터만 추출
                if decoded_line.startswith('data: '):
                    try:
                        # 1. JSON으로 변환 (영어 위키 체크를 위해)
                        raw_json_str = decoded_line[6:]
                        data_dict = json.loads(raw_json_str)

                        # 2. 영어 위키피디아 필터링
                        if data_dict.get('wiki') != 'enwiki':
                            continue

                        # 3. [핵심] 가공 없이 데이터 전체를 한 줄로 저장
                        # 문자열 그대로 저장하고 끝에 줄바꿈(\n)만 추가합니다.
                        f.write(raw_json_str + "\n")
                        f.flush()  # 실시간으로 파일에 기록

                        current_count += 1
                        print(f"🚀 [{current_count}/{target_count}] 수집 중: {data_dict.get('title', 'N/A')[:30]}")

                        if current_count >= target_count:
                            print(f"\n✨ 완료! 모든 원본 데이터가 '{output_file}'에 저장되었습니다.")
                            break

                    except Exception:
                        continue

except Exception as e:
    print(f"❌ 오류 발생: {e}")