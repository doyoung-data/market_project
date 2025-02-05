import time
import mysql.connector
import pandas as pd
import schedule
from datetime import datetime, timedelta
import logging

# 로그 설정 (로그 파일에 기록)
logging.basicConfig(filename='/home/ubuntu/market/naver_schedule.log', 
                    level=logging.DEBUG, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

# 데이터프레임 불러오기
try:
    df_new = pd.read_csv('./trend_data2.csv', header=0)  # 첫 번째 행을 컬럼 이름으로 사용
    logging.info("Data loaded successfully.")
except Exception as e:
    logging.error(f"Error loading data from CSV: {e}")
    raise  # 예외 발생 시 스크립트 중지

# 수동으로 컬럼 이름 지정
df_new.columns = ['DATE', 'seven', 'GS25', 'CU', 'cs']

# 날짜 컬럼을 datetime 형식으로 변환 (시간은 날려버림)
try:
    df_new['DATE'] = pd.to_datetime(df_new['DATE']).dt.date  # 시간 부분 제거하고 날짜만 추출
    logging.info("DATE column converted to datetime format.")
except Exception as e:
    logging.error(f"Error converting DATE column: {e}")
    raise  # 예외 발생 시 스크립트 중지

# MySQL 연결
try:
    db = mysql.connector.connect(
        host='3.35.236.56',
        user='kdy',
        password='0710',
        database='crawling',
        charset='utf8mb4'
    )
    cursor = db.cursor()
    logging.info("Connected to MySQL database successfully.")
except mysql.connector.Error as e:
    logging.error(f"Error connecting to MySQL: {e}")
    raise  # 예외 발생 시 스크립트 중지

# 마지막 삽입 날짜 파일 경로
last_insert_file = "last_insert_date_naver.txt"

# 마지막 삽입 날짜를 파일에서 읽기
def get_last_insert_date():
    try:
        with open(last_insert_file, "r") as f:
            last_date_str = f.read().strip()
            logging.info(f"Last insert date: {last_date_str}")
            return datetime.strptime(last_date_str, "%Y-%m-%d").date()  # 마지막 삽입 날짜 반환
    except FileNotFoundError:
        logging.warning("Last insert date file not found. Defaulting to 2025-01-01.")
        return datetime(2025, 1, 1).date()  # 처음 시작할 때 2025년 1월 1일부터 시작
    except Exception as e:
        logging.error(f"Error reading last insert date: {e}")
        raise  # 예외 발생 시 스크립트 중지

# 마지막 삽입 날짜 기록
def update_last_insert_date(date):
    try:
        with open(last_insert_file, "w") as f:
            f.write(date.strftime("%Y-%m-%d"))
        logging.info(f"Last insert date updated to: {date}")
    except Exception as e:
        logging.error(f"Error updating last insert date: {e}")
        raise  # 예외 발생 시 스크립트 중지

# 데이터 삽입 함수
def insert_data():
    try:
        last_insert_date = get_last_insert_date()  # 마지막 삽입 날짜 가져오기

        # last_insert_date 이후 하루씩 데이터를 가져옵니다.
        next_day_data = df_new[df_new['DATE'] == last_insert_date + timedelta(days=1)]

        # 데이터가 존재하면 삽입
        if not next_day_data.empty:
            # 데이터 삽입
            for index, row in next_day_data.iterrows():
                date_value = row['DATE']
                cursor.execute(""" 
                    INSERT INTO naver_search_trend (DATE, seven, GS25, CU, cs) 
                    VALUES (%s, %s, %s, %s, %s)
                """, (date_value, row['seven'], row['GS25'], row['CU'], row['cs']))

            # 커밋 및 종료
            db.commit()

            # 마지막 삽입 날짜 갱신
            update_last_insert_date(next_day_data['DATE'].max())  # 마지막 삽입 날짜 갱신

            logging.info(f"Inserted data for {next_day_data['DATE'].max()}")  # 삽입된 날짜 출력
        else:
            logging.info(f"No new data to insert for {last_insert_date + timedelta(days=1)}.")  # 새로운 데이터가 없을 경우
    except Exception as e:
        logging.error(f"Error inserting data: {e}")
        db.rollback()  # 오류 발생 시 롤백
        raise  # 예외 발생 시 스크립트 중지

# 매일 1시에 실행
schedule.every().day.at("09:40").do(insert_data)

# 계속 실행
while True:
    try:
        schedule.run_pending()  # 예약된 작업 실행
        time.sleep(60)  # 매분 확인
    except Exception as e:
        logging.error(f"Error in scheduler loop: {e}")
        time.sleep(60)  # 오류 발생시에도 계속 실행되도록
