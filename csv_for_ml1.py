# 라이브러리 할당
import pandas as pd
import numpy as np
import re

# 데이터셋 불러오기
df = pd.read_csv('wiki_trend.csv')

# df 값 보기
print(f"df.shape : {df.shape}")
print("\n[df.info 확인]")
print(df.info())
print(f"\n[df.head 확인] \n {df.head()}")


# 'is_first_post' 컬럼 생성
df['is_first_post'] = df['time_delta'].isnull()

## 컬럼별 Nan 처리
# namespace nan값 처리
# 0(Main): 본문, 1(Talk): 토론 등 위키의 공간 구분
# nan은 0으로 채우기
df['namespace'] = df['namespace'].fillna(0)

# parsedcomment Nan값 처리
# 'No comment'로 채우기
df['parsedcomment'] = df['parsedcomment'].fillna('No comment')


## type 컬럼
# type 컬럼 내용 확인
print(df['type'].value_counts())

# 컬럼별로 전처리 시작
df = pd.get_dummies(df, columns=['type'], drop_first=True)


## namespace 컬럼
# namespce 컬럼 확인
print(df['namespace'].value_counts())

# 데이터가 적은 namespace를 'Others'로 묶어 모델 단순화 진행
# 빈도가 낮은 항목(50개 미만) 항목을 모두 한 그룹으로 통합
def group_minor_ns(ns):
    # 상위 주요 namespace만 유지 (0: 본문, 14: 카테고리, 2: 사용자, 3: 사용자토론 등)
    if ns in [0, 14, 2, 3, 4, 1]:
        return str(ns)
    else:
        return 'others'

df['ns_grouped'] = df['namespace'].apply(group_minor_ns)


# 정재된 컬럼으로 원핫인코딩 수행
df = pd.get_dummies(df, columns=['ns_grouped'], drop_first=True)

# namespace 컬럼 삭제
df = df.drop(columns=['namespace'])


## title 컬럼
# 제목 글자 수 추출
df['title_len'] = df['title'].str.len()


## comment 컬럼
# comment 컬럼 확인
print(df['comment'].value_counts())

# comment(편집 용약) 컬럼 수치화
# 결측치 처리 (앞서 했던 대로 'No comment'로 통일)
df['comment'] = df['comment'].fillna('No comment').astype(str)

# 편집의 성격을 나타내는 핵심 키워드 추출 (이진 변수 생성
# 'revert'가 포함되면 편집 갈등 가능성 높으므로 1, 아니면 0
df['is_revert'] = df['comment'].str.contains('revert|rvv|undo', case=False).astype(int)

# 'update'나 'added'가 포함되면 정보 추가 가능성이 높음
df['is_update'] = df['comment'].str.contains('update|added|expand', case=False).astype(int)

# 'typo'나 'fix'가 포함되면 사소한 수정일 가능성이 높음
df['is_minor_fix'] = df['comment'].str.contains('typo|fix|correction|clean up', case=False).astype(int)

# 코멘트 글자 수 추출
df['comment_len'] = df['comment'].str.len()

# comment 컬럼 삭제
df = df.drop(columns=['comment'])


## user 컬럼
# user 컬럼 확인
print(df['user'].value_counts())

# user 컬럼의 수치 지표화
# 사용자별 편집 빈도 계산 (Frequency Encoding)
user_counts = df['user'].value_counts().to_dict()
df['user_activity_score'] = df['user'].map(user_counts)

# 헤비 에디터 여부 (상위 10% 수준인 50회 이상 편집 기준)
df['is_pro_editor'] = (df['user_activity_score'] >= 50).astype(int)

# user 컬럼 삭제
df = df.drop(columns=['user'])


## minor 컬럼
# minor 컬럼 확인
print(df['minor'].value_counts())

# 0.0, False 모두 0으로 처리, 1.0을 1로 처리
df['minor'] = df['minor'].replace({'False': 0, 'True': 1}).astype(float).astype(int)


## revision 컬럼
# revision 컬럼은 현재 수정 버전의 고유 버전
# 성숙도와 수정 이력을 알려줌
# 낮은 숫자 : 최근에 만들어졌거나 수정이 거의 없었던 초기 단계의 문서일 확률이 높음
# 높은 숫자 : 수많은 사람의 손을 거쳐 내영이 정교해지고 검증된, 성숙한 문서임을 의미

# revision 컬럼 내용 확인
print(df['revision'].value_counts())

# old와 new 값 추출
df[['rev_old', 'rev_new']] = df['revision'].apply(
    lambda x: pd.Series([x.get('old', 0), x.get('new', 0)]) if isinstance(x, dict) else pd.Series([0, 0])
)

# 딕셔너리가 아닌 숫자로 들어온 경우
df.loc[df['rev_new'] == 0, 'rev_new'] = pd.to_numeric(df['revision'], errors='coerce').fillna(0)

# 타입 정수형으로 통일
df['rev_old'] = df['rev_old'].astype(int)
df['rev_new'] = df['rev_new'].astype(int)

# 버전 차이 계산
df['rev_diff'] = df['rev_new'] - df['rev_old']

df = df.drop(columns=['revision'])


## wiki 컬럼
print(df['wiki'].value_counts())
df = df.drop(columns=['wiki'])


## parsedcomment 컬럼
# parsedcomment 컬럼 확인
print(df['parsedcomment'].value_counts())

# [전처리] parsedcomment HTML 태그 제거 및 텍스트 정제
def clean_html(raw_html):
    if not isinstance(raw_html, str):
        return "No comment"
    # HTML 태그 제거 (정규표현식 사용)
    clean_text = re.sub(r'<.*?>', '', raw_html)
    # 불필요한 공백 제거
    clean_text = clean_text.strip()
    return clean_text if clean_text else "No comment"

# 태그가 제거된 순수 텍스트 컬럼 생성
df['comment_cleaned'] = df['parsedcomment'].apply(clean_html)


# [추가 피처] 특정 섹션 수정 여부 확인 (autocomment 패턴)
# "/* ... */" 형태는 특정 섹션을 수정했다는 의미이므로 이진 변수화
df['is_section_edit'] = df['parsedcomment'].str.contains('class="autocomment"', na=False).astype(int)

# 정제된 텍스트의 길이 추출
df['comment_clean_len'] = df['comment_cleaned'].str.len()

df = df.drop(columns=(['parsedcomment']))
df = df.drop(columns=['comment_cleaned'])


## time_delta 컬럼 Nan 전처리
# 매우 큰 값으로 채우기(채워두기먄)
df['time_delta'] = df['time_delta'].fillna(9999999)


# 필요없는 컬럼 삭제
df = df.drop(columns=['id', 'time_delta', 'rev_old', 'rev_new'])

# csv 파일 생성
df.to_csv('ml.csv', index=False, encoding='utf-8')