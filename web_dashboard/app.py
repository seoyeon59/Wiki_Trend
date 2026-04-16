import os
import streamlit as st
import requests
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime, timedelta
import time

# ─────────────────────────────────────────────
# 페이지 기본 설정 및 전역 CSS
# ─────────────────────────────────────────────
st.set_page_config(page_title="Wiki Trend", layout="wide")

st.markdown("""
    <style>
    /* ── Base ── */
    .stApp { background-color: #030712; color: white; }

    /* ── Metric value color ── */
    div[data-testid="stMetricValue"] {
        color: #3b82f6 !important;
        font-weight: bold;
    }

    /* ── Header title gradient ── */
    .title-text {
        font-size: 40px;
        font-weight: bold;
        background: -webkit-linear-gradient(#60a5fa, #22d3ee);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-bottom: 0px;
    }

    /* ── Universal dashboard card ── */
    .dashboard-card {
        border-radius: 8px;
        padding: 16px;
        background: rgba(255, 255, 255, 0.02);
        border: 1px solid rgba(255, 255, 255, 0.1);
        margin-bottom: 12px;
    }

    /* ── Section divider ── */
    .section-gap { margin-top: 32px; margin-bottom: 8px; }

    /* ── Subheader override ── */
    h3 { color: #FFFFFF !important; }
    </style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────
# 설정 / 세션 상태
# ─────────────────────────────────────────────
API_URL = os.getenv("AI_ENGINE_URL", "http://127.0.0.1:8000")

# history 포맷이 구버전(string time)이면 초기화
if 'history' not in st.session_state:
    st.session_state.history = []
elif (st.session_state.history
      and isinstance(st.session_state.history[0].get('time'), str)):
    st.session_state.history = []

# 4h 롤링 트래커: {key: last_seen_datetime}
if 'user_events_4h' not in st.session_state:
    st.session_state.user_events_4h = {}
if 'title_events_4h' not in st.session_state:
    st.session_state.title_events_4h = {}


# ─────────────────────────────────────────────
# 유틸 함수 (기존 로직 유지)
# ─────────────────────────────────────────────
def get_status_info(level):
    level_val = level * 100
    if level_val < 33:
        return "Calm", "#10b981", "bg-emerald-500/20"
    if level_val < 66:
        return "Rising", "#f59e0b", "bg-amber-500/20"
    return "Hot", "#ef4444", "bg-red-500/20"


def hex_to_rgb(hex_color):
    h = hex_color.lstrip('#')
    return tuple(int(h[i:i+2], 16) for i in (0, 2, 4))


def fetch_api_data():
    try:
        res_data = requests.get(f"{API_URL}/api/data/latest", timeout=2)
        if res_data.status_code != 200:
            return None, None, 0

        result_json = res_data.json()
        logs = result_json.get("data", [])
        total_count = result_json.get("total_count", 0)

        res_pred = requests.post(
            f"{API_URL}/api/predict", json={"data": logs}, timeout=2
        )
        if res_pred.status_code != 200:
            return logs, None, total_count

        return logs, res_pred.json(), total_count
    except:
        return None, None, 0


# ─────────────────────────────────────────────
# ① HEADER
# ─────────────────────────────────────────────
col_h1, col_h2 = st.columns([1, 2])

with col_h1:
    st.markdown('<h1 class="title-text">Wiki Trend</h1>', unsafe_allow_html=True)
    st.markdown(
        '<p style="color:#9ca3af; margin-top:-8px; font-size:14px;">'
        'Trends Excavation by Wikipedia Edits</p>',
        unsafe_allow_html=True,
    )

with col_h2:
    now_str = datetime.now().strftime('%p %I:%M:%S').replace('AM', '오전').replace('PM', '오후')
    st.markdown(f"""
    <div style="
        display: flex;
        flex-direction: column;
        align-items: flex-end;
        justify-content: center;
        height: 100%;
        padding-top: 10px;
    ">
        <div style="
            display: inline-flex;
            align-items: center;
            gap: 8px;
            padding: 8px 20px;
            border-radius: 20px;
            background: #0d1b2a;
            border: 1px solid rgba(0, 136, 255, 0.35);
            color: #0088FF;
            font-size: 14px;
            font-weight: 600;
            margin-bottom: 6px;
        ">
            📡 System Active
        </div>
        <div style="color: #666666; font-size: 12px;">Last Update &nbsp;{now_str}</div>
    </div>
    """, unsafe_allow_html=True)

st.markdown('<hr style="border:1px solid rgba(255,255,255,0.08); margin: 16px 0 24px 0;">', unsafe_allow_html=True)

# ─────────────────────────────────────────────
# 데이터 패칭 (기존 로직 유지)
# ─────────────────────────────────────────────
logs, result, total_accumulated = fetch_api_data()
realtime_data = logs if logs else []

# ─────────────────────────────────────────────
# ② MAIN CONTENT  cols [1, 2]
# ─────────────────────────────────────────────
if result:
    kei = result['kei_index']
    kei_display = round(kei * 100, 1)
    status, color, _ = get_status_info(kei)
    rgb = hex_to_rgb(color)
    color_rgba_bg = f"rgba({rgb[0]},{rgb[1]},{rgb[2]}, 0.15)"

    now_dt = datetime.now()

    main_left, main_right = st.columns([1, 2])

    # ── LEFT: Current Trend Keyword ─────────────────
    with main_left:
        st.subheader("Current Trend Keyword")

        top_title = logs[0].get('title', 'No keyword') if logs else 'No keyword'

        st.markdown(f"""
        <div style="
            min-height: 200px;
            border-radius: 8px;
            border: 1px solid rgba(255, 255, 255, 0.1);
            background: rgba(255, 255, 255, 0.02);
            padding: 28px 24px;
            margin-bottom: 16px;
        ">
            <div style="color:#999999; font-size:11px; letter-spacing:1.5px;
                        text-transform:uppercase; margin-bottom:10px;">
                TOP TRENDING
            </div>
            <div style="font-size:22px; font-weight:bold; color:#FFFFFF; margin-bottom:14px;
                        line-height:1.3;">
                {top_title}
            </div>
            <div style="
                display: inline-flex;
                align-items: center;
                gap: 6px;
                padding: 4px 14px;
                border-radius: 12px;
                background: {color_rgba_bg};
                border: 1px solid {color};
                color: {color};
                font-size: 13px;
                font-weight: 600;
            ">
                ● {status}
            </div>
        </div>
        """, unsafe_allow_html=True)

        # 2개 메트릭
        met_c1, met_c2 = st.columns(2)
        with met_c1:
            st.metric("Current KEI", f"{kei_display}")
        with met_c2:
            if st.session_state.history:
                avg_kei = round(
                    sum(h['kei'] for h in st.session_state.history)
                    / len(st.session_state.history), 1
                )
            else:
                avg_kei = kei_display
            st.metric("24h Average", f"{avg_kei}")

    # ── RIGHT: Edits Chart ──────────────────────────
    with main_right:
        st.subheader("Edits of Last 4 Hours")

        # history에 datetime 객체로 저장 (0~100 스케일)
        st.session_state.history.append({
            "time": now_dt,
            "kei": kei_display,
            "total_count": total_accumulated,   # 4h Edits Count 델타 계산용
        })
        # 최대 2880포인트 (5초 간격 × 4시간)
        if len(st.session_state.history) > 2880:
            st.session_state.history.pop(0)

        # ── 4h 롤링 트래커 업데이트 ──────────────────
        _four_h_ago = now_dt - timedelta(hours=4)

        for _entry in realtime_data:
            _u = _entry.get('user')
            _t = _entry.get('title')
            if _u:
                st.session_state.user_events_4h[_u] = now_dt
            if _t:
                st.session_state.title_events_4h[_t] = now_dt

        # 4시간 넘은 항목 정리
        st.session_state.user_events_4h = {
            u: ts for u, ts in st.session_state.user_events_4h.items()
            if ts >= _four_h_ago
        }
        st.session_state.title_events_4h = {
            tl: ts for tl, ts in st.session_state.title_events_4h.items()
            if ts >= _four_h_ago
        }

        chart_df = pd.DataFrame(st.session_state.history)

        fig_line = go.Figure()
        fig_line.add_trace(go.Scatter(
            x=chart_df['time'],
            y=chart_df['kei'],
            fill='tozeroy',
            fillcolor='rgba(59,130,246,0.12)',
            line=dict(color='#3b82f6', width=2),
            name='KEI',
            hovertemplate='<b>%{x|%H:%M:%S}</b><br>KEI: %{y:.1f}<extra></extra>'
        ))

        x_start = now_dt - timedelta(hours=4)
        x_end   = now_dt

        fig_line.update_layout(
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            xaxis=dict(
                showgrid=True,
                gridcolor='#374151',
                range=[x_start, x_end],
                type='date',
                tickformat='%H:%M',
                color='#9ca3af',
                dtick=3600000,   # 1시간 간격 tick (ms 단위)
            ),
            yaxis=dict(
                showgrid=True,
                gridcolor='#374151',
                range=[0, 100],
                color='#9ca3af',
            ),
            font={'color': "#9ca3af"},
            height=320,
            margin=dict(t=20, b=20, l=40, r=20),
            hovermode='x unified',
        )

        st.plotly_chart(fig_line, use_container_width=True, key="line_chart")

    # ─────────────────────────────────────────────
    # ③ ALERT KEYWORDS 섹션 (expander 제거 → 카드 리스트)
    # ─────────────────────────────────────────────
    st.markdown('<div class="section-gap"></div>', unsafe_allow_html=True)
    st.header("🔔 Recent Alert Keywords")

    if logs:
        for log in logs[:5]:
            title  = log.get('title', 'Unknown')
            user   = log.get('user', 'Unknown')
            edits  = log.get('edits') or 247
            score  = round(kei_display)
            time_ago = "2 min ago"

            if score >= 66:
                icon        = "🔴"
                score_color = "#FF3333"
            elif score >= 33:
                icon        = "🟠"
                score_color = "#FF9500"
            else:
                icon        = "🔵"
                score_color = "#0088FF"

            st.markdown(f"""
            <div style="
                min-height: 70px;
                padding: 16px 20px;
                border-radius: 8px;
                border: 1px solid rgba(255, 255, 255, 0.1);
                background: rgba(255, 255, 255, 0.02);
                margin-bottom: 8px;
                display: flex;
                align-items: center;
                justify-content: space-between;
            ">
                <div style="display:flex; align-items:center; gap:14px; flex:1; min-width:0;">
                    <span style="font-size:20px; flex-shrink:0;">{icon}</span>
                    <div style="min-width:0;">
                        <div style="
                            font-weight:600;
                            color:#FFFFFF;
                            font-size:15px;
                            margin-bottom:4px;
                            white-space:nowrap;
                            overflow:hidden;
                            text-overflow:ellipsis;
                        ">{title}</div>
                        <div style="color:#999999; font-size:12px;">
                            Number of Edits: {edits} &nbsp;•&nbsp; {time_ago}
                        </div>
                    </div>
                </div>
                <div style="
                    font-size:26px;
                    font-weight:bold;
                    color:{score_color};
                    min-width:54px;
                    text-align:right;
                    flex-shrink:0;
                ">{score}</div>
            </div>
            """, unsafe_allow_html=True)
    else:
        st.markdown("""
        <div style="
            padding: 32px;
            text-align: center;
            color: #666;
            border: 1px solid rgba(255,255,255,0.1);
            border-radius: 8px;
        ">
            No alerts currently
        </div>
        """, unsafe_allow_html=True)

    # ─────────────────────────────────────────────
    # ④ 하단 KPI 메트릭 (3컬럼)
    # ─────────────────────────────────────────────
    st.markdown('<div class="section-gap"></div>', unsafe_allow_html=True)
    st.markdown('<hr style="border:1px solid rgba(255,255,255,0.08);">', unsafe_allow_html=True)

    c1, c2, c3 = st.columns(3)

    # ── 4h 집계 계산 ────────────────────────────────
    four_h_ago = now_dt - timedelta(hours=4)
    h4 = [h for h in st.session_state.history
          if h['time'] >= four_h_ago and 'total_count' in h]

    # 고유 문서 수 (4h 롤링 트래커)
    articles_4h = len(st.session_state.title_events_4h)

    # 고유 에디터 수 (4h 롤링 트래커)
    editors_4h = len(st.session_state.user_events_4h)

    # 편집 수: total_count 델타 (현재 누적 - 4시간 전 누적)
    if len(h4) >= 2:
        edits_4h = max(h4[-1]['total_count'] - h4[0]['total_count'], 0)
    else:
        edits_4h = total_accumulated

    c1.metric("Recent Monitored Articles (4h)", f"{articles_4h:,}")
    c2.metric("Active Editors (4h)",            f"{editors_4h:,}")
    c3.metric("Edits Count (4h)",               f"{edits_4h:,}")

else:
    st.warning("AI Engine으로부터 데이터를 기다리는 중입니다...")

# ─────────────────────────────────────────────
# ✅ 자동 새로고침
# ─────────────────────────────────────────────
time.sleep(5)
st.rerun()