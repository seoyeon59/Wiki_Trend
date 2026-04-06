import redis

# 로컬 Redis 연결
rd = redis.StrictRedis(host='127.0.0.1', port=6379, db=0, decode_responses=True)

try:
    # 'recent_changes' 리스트의 길이 확인
    count = rd.llen("recent_changes")
    print(f"📊 현재 Redis에 저장된 데이터 개수: {count}개")

    # 만약 상위 1개 데이터를 보고 싶다면?
    if count > 0:
        sample = rd.lindex("recent_changes", 0)
        print(f"🔍 최근 데이터 샘플: {sample[:50]}...")

except Exception as e:
    print(f"❌ Redis 연결 확인 필요: {e}")