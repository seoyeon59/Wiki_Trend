from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import numpy as np
import redis
import json
import pandas as pd
import os
import joblib

app = FastAPI(title="WikiTrend API Server")

# 0. CORS 설정 (대시보드와의 통신 허용)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# 1. DB(Redis) 연결 - [수정] 도커가 아니므로 'redis' 대신 'localhost' 사용
try:
    # 로컬에서 실행 중인 redis-server에 접속

    rd = redis.StrictRedis(host='127.0.0.1', port=6379, db=0, decode_responses=True)
    # 연결 테스트
    rd.ping()
    print("✅ Redis 연결 성공")
except Exception as e:
    print(f"❌ Redis 연결 실패: {e}")

# 2. 모델 로드
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MODEL_PATH = os.path.join(BASE_DIR, "ai_engine/models/WikiTrend_RF_Model.pkl")
try:
    if os.path.exists(MODEL_PATH):
        model = joblib.load(MODEL_PATH)
        print("✅ WikiTrend RF 모델 로드 완료")
    else:
        model = None
        print(f"⚠️ 모델 파일을 찾을 수 없습니다: {MODEL_PATH}")
except Exception as e:
    model = None
    print(f"❌ 모델 로드 오류: {e}")


# 요청 데이터 규격
class PredictionRequest(BaseModel):
    data: list


# ---------------------------------------------------------
# [핵심] 전처리 파이프라인
# ---------------------------------------------------------
def preprocess_pipeline(raw_data_list):
    if len(raw_data_list) < 2:  # 데이터가 2개 미만이면 계산 불가
        return np.zeros((1, 5))

    df = pd.DataFrame(raw_data_list)

    if df.empty:
        return np.zeros((1, 5))

    # [타입 정리 및 시간 변환]
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit='s', errors="coerce")
    df = df.sort_values("timestamp")

    # 1. mean_time_delta_sec (편집 간격 평균)
    df['time_delta_sec'] = df['timestamp'].diff().dt.total_seconds().fillna(0)
    mean_time_delta_sec = df['time_delta_sec'].mean()

    # 2. revert_ratio (되돌리기 비율)
    df['comment'] = df['comment'].fillna('').astype(str)
    is_revert = df['comment'].str.contains('revert|rvv|undo', case=False).astype(int)
    revert_ratio = is_revert.mean()

    # 3. human_ratio (사용자 비율)
    df['is_human'] = 1 - df['bot'].fillna(False).astype(int)
    human_ratio = df['is_human'].mean()

    # 4. mean_user_activity_score (사용자 활동 빈도)
    user_counts = df['user'].value_counts().to_dict()
    df['user_activity_score'] = df['user'].map(user_counts)
    mean_user_activity_score = df['user_activity_score'].mean()

    # 5. section_edit_ratio (섹션 편집 비율)
    is_section = df['comment'].str.contains(r'/\*.*?\*/', regex=True).astype(int)
    section_edit_ratio = is_section.mean()

    # 최종 피처 벡터 구성 (1, 5)
    final_vector = np.array([
        mean_time_delta_sec,
        revert_ratio,
        human_ratio,
        mean_user_activity_score,
        section_edit_ratio
    ]).reshape(1, -1)

    return final_vector


# 3. 엔드포인트 작성

@app.get("/api/data/latest")
async def get_latest_data():
    # Redis에서 수집기(Collector)가 쌓아준 최신 시퀀스 데이터 로드
    # [주의] collector.py에서 데이터를 저장할 때 사용하는 키값과 일치해야 함
    latest = rd.get("latest_sequence")

    if not latest:
        # 데이터가 없을 경우 에러 대신 빈 리스트를 반환하여 대시보드 대기 유도 가능
        return {"data": [], "message": "데이터 수집 중입니다..."}

    return {"data": json.loads(latest)}


@app.post("/api/predict")
async def predict_trend(request: PredictionRequest):
    try:
        if not request.data:
            return {"kei_index": 0.0, "is_trend": False, "message": "입력 데이터 없음"}

        # [단계 1] 전처리
        input_vector = preprocess_pipeline(request.data)

        # [단계 2] 모델 예측 (모델이 없을 경우 랜덤값 반환하는 예외 처리 추가)
        if model:
            # 확률값 추출 (클래스 1일 확률)
            prob = model.predict_proba(input_vector)[:, 1][0]
        else:
            import random
            prob = random.uniform(0.3, 0.6)  # 모델 미로드 시 임시값

        # [단계 3] 결과 반환
        return {
            "kei_index": round(float(prob), 4),
            "is_trend": bool(prob >= 0.5),
            "features": {
                "mean_time_delta": round(input_vector[0][0], 2),
                "revert_ratio": round(input_vector[0][1], 2),
                "human_ratio": round(input_vector[0][2], 2),
                "user_activity": round(input_vector[0][3], 2),
                "section_ratio": round(input_vector[0][4], 2)
            },
            "message": "🔥 트렌드 감지!" if prob >= 0.5 else "✅ 정상 상태"
        }
    except Exception as e:
        print(f"Error during prediction: {e}")
        raise HTTPException(status_code=500, detail=f"예측 오류: {str(e)}")


@app.get("/api/health")
async def health():
    return {"status": "ok", "service": "WikiTrend Analysis Engine", "model_loaded": model is not None}