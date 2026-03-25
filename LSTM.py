# 라이브러리 할당
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import tensorflow as tf
import random

from sklearn.preprocessing import RobustScaler
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense, Dropout
from tensorflow.keras.callbacks import EarlyStopping
from tensorflow.keras.optimizers import Adam
from tensorflow.keras.layers import Input
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

# 랜덤 시드 고정
def fix_seeds(seed=42):
    random.seed(seed)
    np.random.seed(seed)
    tf.random.set_seed(seed)

fix_seeds(85)

# 파일 업로드
df = pd.read_csv('ml2.csv')

# 타이틀, 타임스템프로 정렬
df = df.sort_values(by=['title', 'timestamp'])

# 사용할 컬럼 선택
features = [
    'bot', 'minor', 'length_old', 'length_new',
       'len_diff', 'time_delta_sec', 'is_first_post', 'type_edit', 'type_log',
       'type_new', 'ns_grouped_1', 'ns_grouped_14', 'ns_grouped_2',
       'ns_grouped_3', 'ns_grouped_4', 'ns_grouped_others', 'title_len',
       'is_revert', 'is_update', 'is_minor_fix', 'comment_len',
       'user_activity_score', 'is_pro_editor', 'rev_diff', 'is_section_edit',
       'comment_clean_len', 'time_inv'
]

target_column = 'KEI_final'
all_cols = features + [target_column]

# df 복사
df_scaled = df.copy()

# 데이터 스케일링
scaler = RobustScaler()
df_scaled[features] = scaler.fit_transform(df[features])


# 윈도우 생성 함수
def create_raw_sequences(data, window_size, feature_cols):
    X, y = [], []
    for title in data['title'].unique():
        # 문서별로 필터링
        title_data = data[data['title'] == title][feature_cols].values

        # 문서의 총 편집 횟수가 윈도우 크기보다 클 때만 생성
        if len(title_data) > window_size:
            for i in range(len(title_data) - window_size):
                X.append(title_data[i:i + window_size])
                y.append(title_data[i + window_size, -1])
    return np.array(X), np.array(y)

# 윈도우 크기
window_size = 10

X, y = create_raw_sequences(df_scaled, window_size, features)

print(f"📊 최종 확인")
print(f"- 설정된 윈도우 크기: {window_size}")
print(f"- 생성된 총 학습 시퀀스 수: {len(X)}")

if len(X) < 100:
    print("⚠️ 주의: 데이터 수가 너무 적습니다! window_size를 줄이는 것을 권장합니다.")


# train/test 데이터셋 분리
# 시계열 특성상 순차 분리를 위해 전체 길이에서 0.8:0.2로 나눠줌
split = int(len(X) * 0.8)
X_train, X_test = X[:split], X[split:]
y_train, y_test = y[:split], y[split:]

print(f"X_train의 형태: {X_train.shape}")
# 만약 () 또는 (0,) 처럼 나온다면 데이터가 비어있는 것입니다.

# LSTM 모델 설계
model = Sequential([
    # 1. 입력 데이터의 형태(Window Size, Feature Count)를 명시적으로 정의
    Input(shape=(X_train.shape[1], X_train.shape[2])),

    # 2. 첫 번째 LSTM 층 (input_shape는 이제 지웁니다)
    LSTM(64, return_sequences=True),

    # 3. 두 번째 LSTM 층
    LSTM(32, return_sequences=False),

    # 4. Dense 층 (특징 추출 및 최종 출력)
    Dense(16, activation='relu'),
    Dense(1)  # 최종 예측 KEI 지수
])

model.compile(optimizer=Adam(learning_rate=0.0005), loss='mse')

early_stop = EarlyStopping(
    monitor='val_loss',  # 검증 오차를 지켜보다가
    patience=10,  # 10번 넘게 안 좋아지면 멈춤
    restore_best_weights=True  # 가장 성적 좋았던 때로 되돌리기
)

# KEI 값에 비례하여 가중치 계산 (값이 클수록 중요하게 처리)
# y_train이 0~1 사이이므로, 여기에 적절한 상수를 더하거나 곱해 가중치를 만듭니다.
sample_weights = np.ones_like(y_train)
sample_weights[y_train > 0.3] = 5.0  # KEI가 0.3보다 크면 5배 더 중요하게 취급
sample_weights[y_train > 0.5] = 10.0 # KEI가 0.5보다 크면 10배 더 중요하게 취급

# 모델 학습
history = model.fit(
    X_train, y_train,
    shuffle=False,
    epochs=30,
    batch_size=16,
    validation_data=(X_test, y_test),
    sample_weight=sample_weights,
    verbose=1
)


# 시각화 결과 확인
# 학습 손실(Loss) 그래프 시각화
plt.figure(figsize=(10, 5))
plt.plot(history.history['loss'], label='Train Loss')
plt.plot(history.history['val_loss'], label='Val Loss')
plt.title('Model Loss Progress')
plt.xlabel('Epochs')
plt.ylabel('Loss (MSE)')
plt.legend()
plt.show()

# 결과 확인
# 테스트 데이터에 대한 예측 수행
y_pred = model.predict(X_test)

plt.figure(figsize=(15, 6))
plt.plot(y_test[:200], label='Actual KEI', color='blue', alpha=0.7)
plt.plot(y_pred[:200], label='Predicted KEI', color='red', linestyle='--', alpha=0.9)
plt.title('Actual vs Predicted KEI Trend (Test Set)')
plt.legend()
plt.show()

# 성능 지표 계산
mae = mean_absolute_error(y_test, y_pred)
mse = mean_squared_error(y_test, y_pred)
rmse = np.sqrt(mse)
r2 = r2_score(y_test, y_pred)

print(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
print(f"🎯 LSTM 모델 트렌드 예측 성능 결과")
print(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
print(f"✅ MAE  (평균 절대 오차): {mae:.4f}")
print(f"   -> 실제 KEI 지수와 평균적으로 이만큼 차이남")
print(f"✅ RMSE (평균 제곱근 오차): {rmse:.4f}")
print(f"   -> 큰 오차에 민감한 지표 (낮을수록 우수)")
print(f"✅ R2 Score (결정계수): {r2:.4f}")
print(f"   -> 1에 가까울수록 모델이 트렌드를 완벽히 설명함")
print(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

# 4. 모델 파일로 박제 (이 파일이 앱에 들어갈 최종 AI입니다)
model.save('wiki_trend.keras')
print("🚀 wiki_trend 최종 AI 모델 저장 완료!")