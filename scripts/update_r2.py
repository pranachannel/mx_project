import oracledb
import pandas as pd
import json
import boto3
import os
from datetime import datetime

# 1. 환경 변수에서 DB 및 R2 인증 정보 가져오기 (나중에 GitHub Actions Secrets에 등록할 예정)
# --- Oracle DB 정보 ---
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_DSN = os.getenv('DB_DSN')

# --- Cloudflare R2 정보 ---
R2_ACCOUNT_ID = os.getenv('R2_ACCOUNT_ID')
R2_ACCESS_KEY = os.getenv('R2_ACCESS_KEY')
R2_SECRET_KEY = os.getenv('R2_SECRET_KEY')
R2_BUCKET_NAME = 'my-project-bucket' # 본인의 R2 버킷 이름으로 변경하세요!

def fetch_data_from_oracle():
    """오라클 DB에서 데이터를 가져와 가공하는 함수"""
    try:
        conn = oracledb.connect(user=DB_USER, password=DB_PASSWORD, dsn=DB_DSN)
        
        # 예시: 유저 활동 데이터를 가져오는 쿼리 (본인의 쿼리에 맞게 수정하세요)
        query = """
            SELECT NICKNAME, IP_ADDRESS, ACCOUNT_TYPE, ACT_00, ACT_06, ACT_12, ACT_18, ACT_24
            FROM USER_ACTIVITY_LOG
            WHERE TRUNC(LOG_DATE) = TRUNC(SYSDATE)
        """
        df = pd.read_sql(query, conn)
        conn.close()

        # 프론트엔드(HTML/JS)에서 읽기 편한 형태로 JSON 데이터 가공
        # (아까 index.html에서 썼던 mockData 형태와 똑같이 만들어줍니다)
        processed_data = []
        for _, row in df.iterrows():
            processed_data.append({
                "nickname": row['NICKNAME'],
                "id": row['IP_ADDRESS'],
                "type": row['ACCOUNT_TYPE'],
                "history": [row['ACT_00'], row['ACT_06'], row['ACT_12'], row['ACT_18'], row['ACT_24']]
            })
        
        return processed_data

    except Exception as e:
        print(f"DB 연결 또는 쿼리 에러: {e}")
        return []

def upload_to_r2(json_data):
    """가공된 JSON 데이터를 Cloudflare R2에 업로드하는 함수"""
    # 날짜별 Prefix 생성 (예: 2026/06/07/data.json)
    today_str = datetime.now().strftime("%Y/%m/%d")
    file_key = f"{today_str}/data.json"
    
    # JSON 문자열로 변환 (한글 깨짐 방지: ensure_ascii=False)
    json_body = json.dumps(json_data, ensure_ascii=False)

    # boto3를 이용해 R2 클라이언트 연결
    s3_client = boto3.client(
        's3',
        endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
        aws_access_key_id=R2_ACCESS_KEY,
        aws_secret_access_key=R2_SECRET_KEY,
        region_name='auto' # R2는 region이 auto입니다.
    )

    try:
        # R2 버킷에 JSON 파일 업로드
        s3_client.put_object(
            Bucket=R2_BUCKET_NAME,
            Key=file_key,
            Body=json_body.encode('utf-8'),
            ContentType='application/json' # 웹에서 열기 쉽게 타입 지정
        )
        print(f"✅ R2 업로드 성공! 파일 경로: {file_key}")
        
        # 메인 화면을 위해 최신 데이터라는 뜻의 파일로도 하나 더 덮어쓰기 해줍니다.
        s3_client.put_object(
            Bucket=R2_BUCKET_NAME,
            Key="latest_data.json",
            Body=json_body.encode('utf-8'),
            ContentType='application/json'
        )
        print("✅ latest_data.json 덮어쓰기 완료!")

    except Exception as e:
        print(f"R2 업로드 에러: {e}")

if __name__ == "__main__":
    print("데이터 추출 및 R2 업로드 시작...")
    
    # 1. DB에서 데이터 가져오기
    data = fetch_data_from_oracle()
    
    # 2. 데이터가 있다면 R2에 업로드
    if data:
        upload_to_r2(data)
    else:
        print("업로드할 데이터가 없습니다.")
