import schedule
import time
import mysql.connector
import pandas as pd
from datetime import datetime

# 데이터프레임 불러오기
df = pd.read_excel('/path/to/your/all.xlsx')  # 실제 경로로 수정

# 컬럼 이름 변경
df.columns = ['날짜', 'sale', 'store_count', 'store_type']

# 날짜 형식으로 변환 후 시간 제거 (날짜만 남기기)
df['날짜'] = pd.to_datetime(df['날짜']).dt.date  # 시간은 제외하고 날짜만 추출

# 마지막 삽입 날짜 파일 경로
last_insert_file = "last_insert_date.txt"

# 마지막 삽입 날짜를 파일에서 읽기
def get_last_insert_date():
    try:
        with open(last_insert_file, "r") as f:
            last_date_str = f.read().strip()
            return datetime.strptime(last_date_str, "%Y-%m-%d").date()
    except FileNotFoundError:
        return datetime(2025, 1, 1).date()  # 처음 시작할 때 2025년 1월 1일부터 시작

# 마지막 삽입 날짜 기록
def update_last_insert_date(date):
    with open(last_insert_file, "w") as f:
        f.write(date.strftime("%Y-%m-%d"))

# DB 연결 함수
def insert_data():
    last_insert_date = get_last_insert_date()  # 마지막 삽입 날짜 가져오기

    # 마지막 삽입 날짜 이후의 데이터 필터링
    df_to_insert = df[df['날짜'] > last_insert_date]
    
    if not df_to_insert.empty:
        row = df_to_insert.iloc[0]  # 첫 번째 데이터 (하루 데이터)

        # DB 연결
        db = mysql.connector.connect(
            host='3.35.236.56',
            port=3306,
            user='kdy',  
            password='0710',
            database='crawling',
            charset='utf8mb4'
        )

        cursor = db.cursor()

        # 데이터 삽입 (id_sale은 AUTO_INCREMENT이므로 삽입하지 않음)
        cursor.execute("""
            INSERT INTO sale (store_type, sale, store_count, sale_date) 
            VALUES (%s, %s, %s, %s)
        """, (row['store_type'], row['sale'], row['store_count'], row['날짜']))

        # 커밋 및 종료
        db.commit()
        cursor.close()
        db.close()

        # 마지막 삽입 날짜 갱신
        update_last_insert_date(row['날짜'])

        print(f"Inserted data for {row['날짜']}")  # 삽입된 날짜 출력
    else:
        print("No new data to insert today.")  # 새로운 데이터가 없을 경우

# 매일 2시에 실행
schedule.every().day.at("02:00").do(insert_data)

# 계속 실행
while True:
    schedule.run_pending()
    time.sleep(60)  # 매분 확인
