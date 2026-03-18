import json
import csv
import requests

# 1. 설정
csv_file = "enwiki_1000_data.csv"
target_count = 1000
current_count = 0
url = 'https://stream.wikimedia.org/v2/stream/recentchange'

print(f"📡 [영어 위키 전용] {target_count}개 수집 시작...")

try:
    headers = {'User-Agent': 'Mozilla/5.0'}
    response = requests.get(url, stream=True, headers=headers)

    with open(csv_file, mode='a', newline='', encoding='utf-8') as f:
        writer = None

        for line in response.iter_lines():
            if line:
                decoded_line = line.decode('utf-8')

                if decoded_line.startswith('data: '):
                    try:
                        json_str = decoded_line[6:]
                        data = json.loads(json_str)

                        # --- [추가] 영어 위키피디아(enwiki) 필터링 ---
                        # 'wiki' 키값이 'enwiki'인 데이터만 통과시킵니다.
                        if data.get('wiki') != 'enwiki':
                            continue
                        # ------------------------------------------

                        # meta 제외
                        if 'meta' in data: del data['meta']

                        flat_data = {}


                        def flatten(x, name=''):
                            if type(x) is dict:
                                for a in x: flatten(x[a], name + a + '_')
                            else:
                                flat_data[name[:-1]] = x


                        flatten(data)

                        if writer is None:
                            fieldnames = sorted(flat_data.keys())
                            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
                            writer.writeheader()

                        writer.writerow(flat_data)
                        f.flush()

                        current_count += 1
                        print(f"🇺🇸 [{current_count}/{target_count}] 완료: {data.get('title', 'N/A')[:30]}...")

                        if current_count >= target_count:
                            print(f"\n✨ 영어 위키 데이터 {target_count}개 수집 완료!")
                            break

                    except json.JSONDecodeError:
                        continue
                    except Exception:
                        continue

except Exception as e:
    print(f"❌ 접속 실패: {e}")