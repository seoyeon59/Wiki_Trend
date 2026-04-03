# 라이브러리 할당
import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import StandardScaler

# csv 파일 불러오기
df = pd.read_csv('../dataset/ml.csv')


## [STEP 1] 분석용 핵심 변수 생성
# time__delta_sec 역수 처리 (작을수록 중요하므로, 값이 클수록 중요하게 변환)
# 분모 0 방지를 위해 +1
df['time_inv'] = 1/(df['time_delta_sec'] + 1)

# isolation Forest와 Linear Regressino이 학습을 잘 할 수 있도록 '수치형' 데이터만 선택
# 숫자형인 컬럼만 자동으로 수집
features = df.select_dtypes(include=[np.number]).columns.tolist()

# 분석에 방해되는 식별자나 타켓 후보는 제거 (length_old, length_new, time_delate_sec)
to_exclude = ['length_old', 'length_new', 'time_delta_sec']
features = [f for f in features if f not in to_exclude]


# X 값 할당
X = df[features]

# 데이터 스케일링
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)


## [SETP 2] Isolation Forest로 '종합 이상치 점수(KEI)' 산출
# 모든 변수를 고려했을 때 '이 편집은 정말 특이하다!' 라는 점수 뽑음
model_if = IsolationForest(contamination=0.05, random_state=42)
df['anomaly_score'] = model_if.fit(X_scaled).decision_function(X_scaled) * -1

# 점수 정규화 (0~1)
df['target_kei'] = (df['anomaly_score'] - df['anomaly_score'].min()) / \
                   (df['anomaly_score'].max() - df['anomaly_score'].min())


## [STEP 3] Linear Regression으로 모든 변수의 가중치 역산
# 모델 생성 및 학습
model_lr = LinearRegression()
model_lr.fit(X, df['target_kei'])

# 모든 변수의 가중치를 데이터프레임으로 정리
importance_df = pd.DataFrame({
    'Feature': features,
    'Weight': model_lr.coef_
}).sort_values(by='Weight', ascending=False)

print("📊 모든 컬럼을 활용한 KEI 기여도")
print(importance_df)


# =================================================================
# [결과 적용] 도출된 가중치를 활용한 최종 KEI 산출
# =================================================================

# importance_df를 딕셔너리로 변환하여 가중치 맵핑
weights = importance_df.set_index('Feature')['Weight'].to_dict()

# 모든 변수에 가중치를 곱하여 KEI 합산
df['KEI_final'] = 0
for feature, weight in weights.items():
    df['KEI_final'] += df[feature] * weight

# 가장 핫한 문서 TOP 20 확인
print(df.groupby('title')['KEI_final'].mean().sort_values(ascending=False).head(20))


# csv 파일 생성
df.to_csv('ml2.csv', index=False, encoding='utf-8')