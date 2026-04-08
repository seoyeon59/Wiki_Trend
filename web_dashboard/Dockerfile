FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# 의존성 설치
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 코드 전체 복사
COPY . .

# 환경변수 기본값 (Docker Compose에서 오버라이드)
ENV AI_ENGINE_URL=http://ai_engine:8000

EXPOSE 8501

# Streamlit 실행 (headless 모드, CORS 허용)
CMD ["streamlit", "run", "web_dashboard/app.py", "--server.port=8501", "--server.address=0.0.0.0", "--server.headless=true", "--browser.gatherUsageStats=false"]
