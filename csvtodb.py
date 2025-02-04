import mysql.connector
import pandas as pd
from datetime import datetime

# 데이터프레임 불러오기
df = pd.read_excel('C:/Users/do543/market_project/market_project/all.xlsx')

# 컬럼 이름 변경
df.columns = ['날짜', 'sale', 'store_count', 'store_type']

# 날짜 형식으로 변환 후 시간 제거 (날짜만 남기기)
df['날짜'] = pd.to_datetime(df['날짜']).dt.date  # 시간은 제외하고 날짜만 추출

# 2025년 1월 1일까지의 데이터 필터링
df_before_20250101 = df[df['날짜'] <= datetime(2025, 1, 1).date()]

# MySQL DB 연결
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
for index, row in df_before_20250101.iterrows():
    cursor.execute("""
        INSERT INTO sale (store_type, sale, store_count, sale_date) 
        VALUES (%s, %s, %s, %s)
    """, (row['store_type'], row['sale'], row['store_count'], row['날짜']))

# 커밋 및 종료
db.commit()
cursor.close()
db.close()
