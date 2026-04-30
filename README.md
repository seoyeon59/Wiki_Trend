# 🚀 Wiki Trend

> **실시간 영문 위키피디아 스트리밍 데이터 기반 지식 엔트로피 탐지 및 트렌드 모니터링 플랫폼**

### 📝 프로젝트 소개
Wiki Trend는 전 세계 지식의 보고인 영문 위키피디아(English Wikipedia)에서 발생하는 실시간 편집 데이터를 수집하여, 지식의 가치 변동을 지식 엔트로피 지수(KEI)로 정량화합니다. 단순한 수정을 넘어 글로벌 이슈와 트렌드를 실시간으로 감지하고 시각화하는 클라우드 네이티브 기반의 통합 모니터링 솔루션입니다.


### ⚙️ 개발 환경 
*   **Infrastructure:** Kubernetes (K8s), Docker
*   **Data Pipeline:** Wikimedia SSE Stream, Redis (In-memory Cache)
*   **Backend & Analytics:** Python, Scikit-learn, FastAPI
*   **Frontend:** Streamlit (Interactive Dashboard)
*   **Version Control:** Git, GitHub


### 👥 Wiki Trend 멤버
*   **박정원** (서울여대 데이터사이언스학과 23학번)
*   **전서연** (서울여대 데이터사이언스학과 23학번)


### 💻 발전 내용
*   **실시간성 확보:** SSE(Server-Sent Events) 스트림 직접 수집을 통해 지연 시간 최소화
*   **지수 모델링:** 단순 통계가 아닌 정보의 밀도와 사용자 신뢰도를 반영한 **KEI 알고리즘** 독자 개발
*   **지능형 이상 탐지:** Isolation Forest와 NLP를 결합한 하이브리드 이슈 감지 시스템 구축
*   **확장성 설계:** 모든 모듈의 컨테이너화를 통해 쿠버네티스 기반 오토스케일링 지원

---

### 🔄 진행 과정 요약 설명

#### 📥 Data Collection 요약 설명
*   **Source:** Wikimedia Foundation 실시간 EventStreams API 연동
*   **Target:** 영문 위키피디아(`en.wikipedia`) 프로젝트 데이터 선별 수집
*   **Technique:** 고성능 Collector 모듈을 통한 무중단 스트림 리스닝 및 실시간 Feature 정제

---

### ⚙️ Processing 요약 설명
*   **Feature Engineering:** 편집 전후 길이 차이(`len_diff`), 편집 간격(`time_delta`), 사용자 유형(Bot/Human) 등 핵심 변수 추출
*   **Sliding Window:** 10초 단위 윈도우 집계를 통해 시계열 데이터의 추세 정보 생성
*   **Data Storage:** 분석된 고빈도 데이터를 Redis StatefulSet에 캐싱하여 대시보드 응답 속도 최적화

---

### 🧠 Machine Learning 요약 설명

#### <모델 및 주요 함수 설명>
*   **Anomaly Detection:** `Isolation Forest` 모델을 활용하여 다차원 피처 공간에서 통계적 이상치(이슈) 탐지
*   **NLP Intent Classification:** `BERT/RoBERTa` 기반 다국어 모델로 편집 코멘트를 분석하여 반달리즘(Vandalism)과 지식 성장(Knowledge Growth) 분류

#### KEI 수식 설계 - Isolation Regression
*   **Knowledge Entropy Index (KEI):** 정보의 양적 변화량과 시간적 긴급도, 사용자 권한을 결합한 수식 모델
*   **핵심 로직:** 명시적인 정답(y값)이 없는 환경에서 비지도 학습을 통해 데이터의 변동성을 스코어링하고, 과거 이슈 데이터셋을 통해 가중치($\alpha, \beta, \gamma$) 최적화

#### 영어 위키피디아에서의 트랜드 파악 - RandomForest
*   **Feature Importance:** RandomForest Regressor를 통해 각 편집 피처가 '실제 이슈 발생'에 미치는 영향력을 분석
*   **Trend Prediction:** 학습된 모델을 바탕으로 현재 유입되는 데이터가 일시적인 노이즈인지, 혹은 글로벌 트렌드로 발전할 이슈인지 실시간 판별

---

### 💡 <추가 설명>
*   **IP 확보 전략:** 본 프로젝트에서 설계한 '지식 엔트로피 수식' 및 '하이브리드 이상 탐지 방법론'은 기술적 독창성을 바탕으로 SW 저작권 등록 및 BM 특허 출원이 가능한 수준의 고도화된 알고리즘을 포함하고 있습니다.
*   **Cloud Native:** 특정 환경에 종속되지 않는 독립적 아키텍처로 설계되어, 다양한 오픈 데이터 소스 확장이 용이합니다.
