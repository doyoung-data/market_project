import time
import mysql.connector
import pandas as pd
import schedule
from datetime import datetime, timedelta

# 데이터프레임 불러오기
df_new = pd.read_csv('./trend_data2.csv', header=0)  # 첫 번째 행을 컬럼 이름으로 사용

# 수동으로 컬럼 이름 지정
df_new.columns = ['DATE', 'seven', 'GS25', 'CU', 'cs']

# 날짜 컬럼을 datetime 형식으로 변환 (시간은 날려버림)
df_new['DATE'] = pd.to_datetime(df_new['DATE']).dt.date  # 시간 부분 제거하고 날짜만 추출

# MySQL 연결
db = mysql.connector.connect(
    host='3.35.236.56',  # 예: MySQL 서버 주소
    user='kdy',  # 사용자명
    password='0710',  # 비밀번호
    database='crawling',  # 사용할 DB
    charset='utf8mb4'
)

cursor = db.cursor()

# 마지막 삽입 날짜 파일 경로
last_insert_file = "last_insert_date_naver.txt"

# 마지막 삽입 날짜를 파일에서 읽기
def get_last_insert_date():
    try:
        with open(last_insert_file, "r") as f:
            last_date_str = f.read().strip()
            return datetime.strptime(last_date_str, "%Y-%m-%d").date()  # 마지막 삽입 날짜 반환
    except FileNotFoundError:
        return datetime(2025, 1, 1).date()  # 처음 시작할 때 2025년 1월 1일부터 시작

# 마지막 삽입 날짜 기록
def update_last_insert_date(date):
    with open(last_insert_file, "w") as f:
        f.write(date.strftime("%Y-%m-%d"))

# 데이터 삽입 함수
def insert_data():
    last_insert_date = get_last_insert_date()  # 마지막 삽입 날짜 가져오기

    # last_insert_date 이후 하루씩 데이터를 가져옵니다.
    next_day_data = df_new[df_new['DATE'] == last_insert_date + timedelta(days=1)]

    # 데이터가 존재하면 삽입
    if not next_day_data.empty:
        # 데이터 삽입
        for index, row in next_day_data.iterrows():
            # 'DATE' 컬럼만 사용 (시간을 무시하고 날짜만 사용)
            date_value = row['DATE']

            cursor.execute(""" 
                INSERT INTO naver_search_trend (DATE, seven, GS25, CU, cs) 
                VALUES (%s, %s, %s, %s, %s)
            """, (date_value, row['seven'], row['GS25'], row['CU'], row['cs']))

        # 커밋 및 종료
        db.commit()

        # 마지막 삽입 날짜 갱신
        update_last_insert_date(next_day_data['DATE'].max())  # 마지막 삽입 날짜 갱신

        print(f"Inserted data for {next_day_data['DATE'].max()}")  # 삽입된 날짜 출력
    else:
        print(f"No new data to insert for {last_insert_date + timedelta(days=1)}.")  # 새로운 데이터가 없을 경우

# 매일 1시에 실행
schedule.every().day.at("01:00").do(insert_data)

# 계속 실행
while True:
    schedule.run_pending()  # 예약된 작업 실행
    time.sleep(60)  # 매분 확인
