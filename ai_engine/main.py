from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import pickle
import numpy as np
import redis
import json
import pandas as pd

app = FastAPI(title="WikiTrend API Server")

# 0. CORS 설정
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# 1. DB(Redis) 연결
try:
    rd = redis.StrictRedis(host='redis', port=6379, db=0, decode_responses=True)
except Exception as e:
    print(f"❌ Redis 연결 실패: {e}")

# 2. 모델 로드 (Random Forest)
# 학습 시 5개의 피처로 학습된 'WikiTrend_RF_Model.pkl' 파일을 사용합니다.
try:
    with open("models/WikiTrend_RF_Model.pkl", "rb") as f:
        model = pickle.load(f)
    print("✅ WikiTrend RF 모델 로드 완료 (5 Features)")
except Exception as e:
    print(f"❌ 모델 로드 오류: {e}")


# 요청 데이터 규격 (수집된 10개 이상의 편집 로그 리스트)
class PredictionRequest(BaseModel):
    data: list


# ---------------------------------------------------------
# [핵심] 전처리 파이프라인 (분석 및 모델링 코드 통합)
# ---------------------------------------------------------
def preprocess_pipeline(raw_data_list):
    """
    개별 편집 로그 리스트를 모델 입력용 5대 지표 통계치로 변환
    """
    df = pd.DataFrame(raw_data_list)

    # [타입 정리 및 시간 변환]
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit='s', errors="coerce")
    df = df.sort_values("timestamp")

    # 1. mean_time_delta_sec 생성 (이전 편집과의 시간 차 평균)
    # 분석 코드 preprocess.py의 diff() 로직 반영
    df['time_delta_sec'] = df['timestamp'].diff().dt.total_seconds().fillna(0)
    mean_time_delta_sec = df['time_delta_sec'].mean()

    # 2. revert_ratio 생성 (csv_for_ml1.py 및 학습 코드 반영)
    df['comment'] = df['comment'].fillna('').astype(str)
    is_revert = df['comment'].str.contains('revert|rvv|undo', case=False).astype(int)
    revert_ratio = is_revert.mean()

    # 3. human_ratio 생성 (1 - bot 비율)
    df['is_human'] = 1 - df['bot'].fillna(False).astype(int)
    human_ratio = df['is_human'].mean()

    # 4. mean_user_activity_score 생성 (사용자 활동성 빈도 점수)
    # 현재 윈도우 내에서의 사용자 빈도 점수 계산 로직
    user_counts = df['user'].value_counts().to_dict()
    df['user_activity_score'] = df['user'].map(user_counts)
    mean_user_activity_score = df['user_activity_score'].mean()

    # 5. section_edit_ratio 생성 (/* ... */ 패턴 포함 비율)
    # parsedcomment의 'autocomment' 또는 comment의 섹션 기호 확인
    is_section = df['comment'].str.contains(r'/\*.*?\*/', regex=True).astype(int)
    section_edit_ratio = is_section.mean()

    # [최종 피처 벡터 구성] - 학습 코드의 features 순서 엄격 준수
    # features = ["mean_time_delta_sec", "revert_ratio", "human_ratio",
    #             "mean_user_activity_score", "section_edit_ratio"]

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
    latest = rd.get("latest_sequence")
    if not latest:
        raise HTTPException(status_code=404, detail="실시간 데이터를 수집 중입니다.")
    return {"data": json.loads(latest)}


@app.post("/api/predict")
async def predict_trend(request: PredictionRequest):
    try:
        # [단계 1] 전처리: 10개 로그 -> 1개의 통계 행(5개 피처)으로 압축
        input_vector = preprocess_pipeline(request.data)

        # [단계 2] 모델 예측
        prob = model.predict_proba(input_vector)[:, 1][0]

        # [단계 3] 결과 반환 (KEI 지수 및 트렌드 여부)
        # 0.712 임계값은 학습 코드의 quantile 기준 반영
        return {
            "kei_index": round(float(prob), 4),
            "is_trend": bool(prob >= 0.5),  # 확률 기반 판단
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
        raise HTTPException(status_code=500, detail=f"전처리 및 예측 오류: {str(e)}")


@app.get("/api/health")
async def health():
    return {"status": "ok", "service": "WikiTrend Analysis Engine"}