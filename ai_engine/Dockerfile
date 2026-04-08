FROM python:3.11-slim

WORKDIR /app

# 의존성 설치
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 코드 복사 (ai_engine만 아니라 전체 필요 - models 경로 때문)
COPY . .

# 환경변수 (Docker용 Redis)
ENV REDIS_HOST=redis
ENV REDIS_PORT=6379

EXPOSE 8000

# 서버 실행
CMD ["python", "-m", "uvicorn", "ai_engine.main:app", "--host", "0.0.0.0", "--port", "8000"]