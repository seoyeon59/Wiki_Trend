import streamlit as st
import requests
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime
import time

# 페이지 기본 설정 및 다크 테마 커스텀 CSS
st.set_page_config(page_title="Wiki Trend", layout="wide")

st.markdown("""
    <style>
    .stApp { background-color: #030712; color: white; }
    div[data-testid="stMetricValue"] { color: #3b82f6 !important; font-weight: bold; }
    .stCard { 
        background-color: rgba(17, 24, 39, 0.5); 
        border: 1px solid #1f2937; 
        padding: 20px; 
        border-radius: 12px;
    }
    .title-text {
        font-size: 40px; font-weight: bold;
        background: -webkit-linear-gradient(#60a5fa, #22d3ee);
        -webkit-background-clip: text; -webkit-text-fill-color: transparent;
    }
    </style>
""", unsafe_allow_html=True)

API_URL = "http://localhost:8000"

if 'history' not in st.session_state:
    st.session_state.history = []

def get_status_info(level):
    level_val = level * 100
    if level_val < 33:
        return "Safe", "#10b981", "bg-emerald-500/20"
    if level_val < 66:
        return "Warning", "#f59e0b", "bg-amber-500/20"
    return "Critical", "#ef4444", "bg-red-500/20"

def fetch_api_data():
    try:
        res_data = requests.get(f"{API_URL}/api/data/latest", timeout=2)
        if res_data.status_code != 200:
            return None, None

        result_json = res_data.json()
        logs = result_json.json().get("data", [])
        total_count = result_json.get("total_count", 0) # 전체 개수 가져오기

        res_pred = requests.post(f"{API_URL}/api/predict", json={"data": logs}, timeout=2)
        if res_pred.status_code != 200:
            return logs, None, total_count

        return logs, res_pred.json(), total_count
    except:
        return None, None, 0



# ---------------- UI ----------------

# Header
col_h1, col_h2 = st.columns([2, 1])
with col_h1:
    st.markdown('<h1 class="title-text">Wiki Trend</h1>', unsafe_allow_html=True)
    st.markdown('<p style="color: #9ca3af;">Trends Excavation by Wikipedia Edits</p>', unsafe_allow_html=True)
with col_h2:
    st.info(f"📡 System Active | {datetime.now().strftime('%H:%M:%S')}")

# 데이터 가져오기
logs, result, total_accumulated = fetch_api_data()

# 'realtime_data'라는 이름으로 logs를 연결해줍니다. (하단 카드용)
realtime_data = logs if logs else []

if result:
    row1_col1, row1_col2 = st.columns([1, 2])

    with row1_col1:
        st.subheader("🔥 Current Trend State")

        kei = result['kei_index']
        status, color, _ = get_status_info(kei)

        fig_gauge = go.Figure(go.Indicator(
            mode="gauge+number",
            value=kei * 100,
            domain={'x': [0, 1], 'y': [0, 1]},
            title={'text': status, 'font': {'color': color, 'size': 24}},
            gauge={
                'axis': {'range': [0, 100], 'tickcolor': "white"},
                'bar': {'color': color},
                'bgcolor': "#1f2937",
                'borderwidth': 2,
                'bordercolor': "gray",
            }
        ))

        fig_gauge.update_layout(
            paper_bgcolor='rgba(0,0,0,0)',
            font={'color': "white"},
            height=280,
            margin=dict(t=30, b=0)
        )

        st.plotly_chart(fig_gauge, use_container_width=True, key="gauge_chart")

    with row1_col2:
        st.subheader("📈 Edits of Last 24 Hours (KEI)")

        st.session_state.history.append({
            "time": datetime.now().strftime("%H:%M"),
            "kei": kei
        })

        if len(st.session_state.history) > 24:
            st.session_state.history.pop(0)

        chart_df = pd.DataFrame(st.session_state.history)

        fig_line = go.Figure()
        fig_line.add_trace(go.Scatter(
            x=chart_df['time'],
            y=chart_df['kei'],
            fill='tozeroy',
            line_color='#3b82f6',
            name='KEI'
        ))

        fig_line.update_layout(
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            xaxis=dict(showgrid=True, gridcolor='#374151'),
            yaxis=dict(showgrid=True, gridcolor='#374151', range=[0, 1]),
            font={'color': "#9ca3af"},
            height=300,
            margin=dict(t=20, b=20)
        )

        st.plotly_chart(fig_line, use_container_width=True, key="line_chart")

    # Alert Feed
    st.subheader("🔔 Recent Alert Keywords")

    if logs:
        for log in logs[:5]:
            with st.expander(f"📄 {log.get('title')} | Score: {kei:.2f}", expanded=True):
                st.markdown(f"""
                **Editor:** {log.get('user')} | **Time:** {datetime.now().strftime('%H:%M:%S')}
                \n**Comment:** {log.get('comment')}
                """)

    # 하단 카드
    st.divider()
    c1, c2, c3 = st.columns(3)

    # Today's Monitored -> 현재 Redis에서 가져온 총 로그 계수 (또는 누적치)
    monitored_count = len(realtime_data)
    c1.metric("Current Window Log", f"{total_accumulated}개")

    # Active Editors -> 현재 데이터 내의 유니크한 사용자 수
    active_users = len(set(d.get('user') for d in realtime_data))
    c2.metric("Active Editors", f"{active_users}")

    # Total Edits (Latest) -> 마지막 편집 시간 또는 특정 수치
    latest_edit = realtime_data[-1].get('title') if realtime_data else "N/A"
    c3.metric("Last Edit Target", f"{latest_edit[:10]}...")

else:
    st.warning("AI Engine으로부터 데이터를 기다리는 중입니다...")

# ✅ 자동 새로고침
time.sleep(5)
st.rerun()