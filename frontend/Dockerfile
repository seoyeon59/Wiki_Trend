# ─── Stage 1: Build ───────────────────────────────────────────────────────────
FROM node:20-alpine AS builder

WORKDIR /app

# package-lock.json이 있으므로 npm 사용
COPY package.json package-lock.json ./

# npm ci: lock 파일 기준으로 정확히 설치 (빠르고 재현성 보장)
RUN npm ci

# 소스 코드 전체 복사
COPY . .

# React 빌드 (VITE_API_URL은 빌드 시 환경변수로 주입 가능)
# 기본값: web_dashboard 서비스의 Streamlit 서버
ARG VITE_API_URL=http://localhost:8000
ENV VITE_API_URL=${VITE_API_URL}

RUN npm run build

# ─── Stage 2: Serve with nginx ────────────────────────────────────────────────
FROM nginx:1.27-alpine

# 기본 nginx 설정 제거 후 커스텀 설정 적용
RUN rm /etc/nginx/conf.d/default.conf
COPY nginx.conf /etc/nginx/conf.d/default.conf

# 빌드 결과물 복사
COPY --from=builder /app/dist /usr/share/nginx/html

EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]
