import streamlit as st
import requests
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime
import time

# 1. 페이지 기본 설정 및 다크 테마 커스텀 CSS
st.set_page_config(page_title="Wiki Trend", layout="wide")

st.markdown("""
    <style>
    /* 배경 및 카드 스타일 */
    .stApp { background-color: #030712; color: white; }
    div[data-testid="stMetricValue"] { color: #3b82f6 !important; font-weight: bold; }
    .stCard { 
        background-color: rgba(17, 24, 39, 0.5); 
        border: 1px solid #1f2937; 
        padding: 20px; 
        border-radius: 12px;
    }
    /* 텍스트 그라데이션 */
    .title-text {
        font-size: 40px; font-weight: bold;
        background: -webkit-linear-gradient(#60a5fa, #22d3ee);
        -webkit-background-clip: text; -webkit-text-fill-color: transparent;
    }
    </style>
    """, unsafe_allow_html=True)

# API 설정 (Docker 내부 네트워크 주소)
API_URL = "http://locale:8000"

# 세션 데이터 (그래프용 누적 데이터)
if 'history' not in st.session_state:
    st.session_state.history = []


# 2. 상태 결정 로직 (React의 RiskGauge.tsx 로직 반영)
def get_status_info(level):
    level_val = level * 100
    if level_val < 33:
        return "Safe", "#10b981", "bg-emerald-500/20"
    if level_val < 66:
        return "Warning", "#f59e0b", "bg-amber-500/20"
    return "Critical", "#ef4444", "bg-red-500/20"


# 3. 데이터 로드 함수
def fetch_api_data():
    try:
        # 최신 로그 로드
        res_data = requests.get(f"{API_URL}/api/data/latest", timeout=2)
        if res_data.status_code != 200: return None, None
        logs = res_data.json().get("data", [])

        # 모델 예측
        res_pred = requests.post(f"{API_URL}/api/predict", json={"data": logs}, timeout=2)
        if res_pred.status_code != 200: return logs, None
        return logs, res_pred.json()
    except:
        return None, None


# ---------------------------------------------------------
# 4. 화면 레이아웃 (App.tsx 구조 반영)
# ---------------------------------------------------------

# Header
col_h1, col_h2 = st.columns([2, 1])
with col_h1:
    st.markdown('<h1 class="title-text">Wiki Trend</h1>', unsafe_allow_html=True)
    st.markdown('<p style="color: #9ca3af;">Trends Excavation by Wikipedia Edits</p>', unsafe_allow_html=True)
with col_h2:
    st.write("")  # 간격 조절
    st.info(f"📡 System Active | {datetime.now().strftime('%H:%M:%S')}")

# 실시간 갱신 영역
placeholder = st.empty()

while True:
    logs, result = fetch_api_data()

    with placeholder.container():
        if result:
            # 상단 메인 그리드 (RiskGauge + TrendChart)
            row1_col1, row1_col2 = st.columns([1, 2])

            with row1_col1:
                st.subheader("🔥 Current Trend State")
                kei = result['kei_index']
                status, color, bg_class = get_status_info(kei)

                # 게이지 차트 (Plotly로 RiskGauge 느낌 구현)
                fig_gauge = go.Figure(go.Indicator(
                    mode="gauge+number",
                    value=kei * 100,
                    domain={'x': [0, 1], 'y': [0, 1]},
                    title={'text': status, 'font': {'color': color, 'size': 24}},
                    gauge={
                        'axis': {'range': [0, 100], 'tickwidth': 1, 'tickcolor': "white"},
                        'bar': {'color': color},
                        'bgcolor': "#1f2937",
                        'borderwidth': 2,
                        'bordercolor': "gray",
                    }
                ))
                fig_gauge.update_layout(paper_bgcolor='rgba(0,0,0,0)', font={'color': "white"}, height=280,
                                        margin=dict(t=30, b=0))
                st.plotly_chart(fig_gauge, use_container_width=True)

            with row1_col2:
                st.subheader("📈 Edits of Last 24 Hours (KEI)")
                # 데이터 히스토리 관리
                st.session_state.history.append({"time": datetime.now().strftime("%H:%M"), "kei": kei})
                if len(st.session_state.history) > 24: st.session_state.history.pop(0)

                chart_df = pd.DataFrame(st.session_state.history)

                # Area Chart (KEITrendChart.tsx 느낌 구현)
                fig_line = go.Figure()
                fig_line.add_trace(go.Scatter(
                    x=chart_df['time'], y=chart_df['kei'],
                    fill='tozeroy', line_color='#3b82f6', name='KEI'
                ))
                fig_line.update_layout(
                    paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
                    xaxis=dict(showgrid=True, gridcolor='#374151'),
                    yaxis=dict(showgrid=True, gridcolor='#374151', range=[0, 1]),
                    font={'color': "#9ca3af"}, height=300, margin=dict(t=20, b=20)
                )
                st.plotly_chart(fig_line, use_container_width=True)

            # 중간 Alert Feed (AlertFeed.tsx 구조 반영)
            st.subheader("🔔 Recent Alert Keywords")
            if logs:
                # 최근 5개 로그를 피그마의 Alert 스타일로 표시
                for log in logs[:5]:
                    with st.expander(f"📄 {log.get('title')} | Score: {kei:.2f}", expanded=True):
                        st.markdown(f"""
                        **Editor:** {log.get('user')} | **Time:** {datetime.now().strftime('%H:%M:%S')}
                        \n**Comment:** {log.get('comment')}
                        """)

            # 하단 통계 카드
            st.divider()
            c1, c2, c3 = st.columns(3)
            c1.metric("Today's Monitored", "6,547,892")
            c2.metric("Active Editors", "142,536")
            c3.metric("Total Edits (24h)", "1,247", delta="12%")

        else:
            st.warning("AI Engine으로부터 데이터를 기다리는 중입니다... (main.py 실행 확인)")

    time.sleep(3)  # 3초마다 갱신