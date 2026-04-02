# 라이브러라 할당
import pandas as pd
import json
import numpy as np

# 파일 불러오기
file_path = "../data/enwiki_raw_log.txt"
df = pd.read_json(file_path, lines=True)

# 데이터 구조 확인
print("--- 데이터 상단 5행 ---")
print(df.head())

print("\n--- 전체 컬럼 목록 ---")
print(df.columns)

# 컬럼별 결측치 확인
print("\n--- 컬럼별 결측치 확인 ---")
print(df.isnull().sum())

# 결측치 비율(%) 확인
print("\n--- 컬럼별 결측치 비율 ---")
print((df.isnull().sum() / len(df)) * 100)

# 결측치 90%가 넘는 컬럼 삭제
df = df.drop(columns=['patrolled','log_id','log_type',
                      'log_action','log_params','log_action_comment'])


# meta 컬럼 삭제
df = df.drop(columns=['meta'])


# length의 결측값은 old와 new 다 0으로 채우기
# why? 결과적으로 내용의 길이가 바뀌지 않았다는 의미이기 떄문이다

# length를 length_old와 length_new로 컬럼 불리하기
def split_length(row):
    if isinstance(row,dict):
        return pd.Series([row.get('old',0), row.get('new',0)])
    # NAN이면 0으로 채움
    else:
        return pd.Series([0,0])

# length 컬럼 적용하여 새로운 두 컬럼 생성
df[['length_old', 'length_new']] = df['length'].apply(split_length)

# 불리된 후에도 결측치 남을 것을 대비해 0으로 채우기
df['length_old'] = df['length_old'].fillna(0).astype(int)
df['length_new'] = df['length_new'].fillna(0).astype(int)

# len_diff 계산하기
df['len_diff'] = df['length_new'] - df['length_old']

print('length컬럼 처리 완료')

# length 컬럼 삭제
df = df.drop(columns=['length'])


# $schema의 value counts
print("\n--- $schema의 value_counts 확인 ---")
print(df['$schema'].value_counts())

# $schema 열 삭제
df = df.drop(columns=['$schema'])


# 시간 간격 계산 time_delta
# timestamp 컬럼을 초 단위 숫자에서 '날짜/시간' 형식으로 변환
df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')

# 데이터를 '문서 제목'별로 묶고, 그 안에서 '시간순'으로 정렬
df = df.sort_values(by=['title', 'timestamp'])

# 문서별(title)로 그룹을 지어 직전 행과의 시간 차이 계산
df['time_delta'] = df.groupby('title')['timestamp'].diff()

# 시간 차이를 '초(seconds)' 단위의 숫자로 변환
df['time_delta_sec'] = df['time_delta'].dt.total_seconds()

# 첫 편집은 직전 기록이 없어서 NaN(결측치)이 생기므로 0으로 채워줍니다.
df['time_delta_sec'] = df['time_delta_sec'].fillna(0)

# 결과 확인
print("✅ 시간 간격 계산 완료!")
print(df[['title', 'timestamp', 'time_delta_sec']].head(10))


# user 컬럼과 bot 컬럼 내용물 확인
print("user 컬럼 확인")
print(df['user'].value_counts())
print("\n")
print("\n")
print("bot 컬럼 확인")
print(df['bot'].value_counts())

print("user 컬럼 내림차순 확인")
print(df['user'].value_counts().sort_values(ascending=False).head(15))


# minor nan 편집
# 체크박스 선택인데 선택을 안했다 = 일반 편집이다
df['minor'] = df['minor'].fillna(False)


# revision nan 처리
# 버전 정보가 없다는 건 지식의 누적 기록이 없는 상태를 위미
df['revision'] = df['revision'].fillna(0)


# id의 경우 DB table에서 pk로 쓰여야함으로 살려둠 (ML에는 사용 X)
# id가 null 값이면 행 삭제
df = df.dropna(subset=['id'])


# 확실히 필요 없는 열들 리스트
# notify_url을 포함하여 서버 정보 등 분석에 불필요한 것들을 모았습니다.
cols_to_drop = ['notify_url', 'server_name', 'server_script_path', 'server_url', 'title_url']

# 리스트에 있는 컬럼 중 실제로 데이터프레임에 존재하는 것만 골라 삭제
df = df.drop(columns=[c for c in cols_to_drop if c in df.columns])

print("✅ notify_url 및 불필요한 컬럼 삭제 완료!")
print(f"현재 남은 컬럼: {list(df.columns)}")


# csv 파일 생성
df.to_csv('wiki_trend.csv', index=False, encoding='utf-8')