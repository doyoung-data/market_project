import requests
from bs4 import BeautifulSoup
import pymysql
import datetime
import calendar
import schedule
import time

# 스크립트 실행 함수
def run_script():
    # 1. DB 연결
    conn = pymysql.connect(
        host="3.35.236.56",
        user="jsh",
        password="0929",
        database="crawling",
        charset="utf8mb4"
    )
    cursor = conn.cursor()

    # 2. 크롤링할 URL 설정
    base_url = "https://cu.bgfretail.com/brand_info/news_view.do?category=brand_info&depth2=5&idx="

    # 3. 오늘 날짜와 해당 월의 마지막 날짜 계산
    today = datetime.date.today()
    start_date = today.strftime("%Y-%m-%d")  # 오늘 날짜
    # 해당 월의 마지막 날 계산
    last_day_of_month = calendar.monthrange(today.year, today.month)[1]
    end_date = today.replace(day=last_day_of_month).strftime("%Y-%m-%d")

    # 4. 새로운 데이터 크롤링
    new_data = []
    idx = 1077 + 1  # 가장 최신 idx 다음부터 시작

    while True:  # 데이터를 계속해서 크롤링
        # print(idx)
        url = base_url + str(idx)
        response = requests.get(url)

        if response.status_code == 200:
            soup = BeautifulSoup(response.text, "html.parser")

            # 이벤트 제목
            event_tag = soup.find("thead").find("tr").find("th")
            event_title = event_tag.get_text(strip=True) if event_tag else None

            # 이미지 URL
            img_tag = soup.select_one('td[colspan="2"] img[border="0"]')
            img_url = img_tag["src"] if img_tag else None

            # 편의점 종류 (CU로 고정)
            store_type = "CU"

            # 데이터가 유효한지 체크 (NULL 값일 경우 종료)
            if not event_title or not img_url:
                print("유효하지 않은 데이터 발견. 크롤링 중지.")
                break  # 유효하지 않으면 종료

            # 데이터베이스에서 해당 데이터가 이미 존재하는지 확인 (모든 열을 기준으로 중복 체크)
            cursor.execute("""
                SELECT COUNT(*) FROM event_img 
                WHERE img_url = %s AND start_date = %s AND end_date = %s AND event_title = %s AND store_type = %s
            """, (img_url, start_date, end_date, event_title, store_type))

            # 중복되지 않는 데이터만 추가
            if cursor.fetchone()[0] == 0:
                new_data.append((img_url, start_date, end_date, event_title, store_type))
                print(f"새로운 데이터 추가: {event_title}, img_url: {img_url}")
        
        else:
            print("더 이상 유효한 데이터를 찾을 수 없습니다.")
            break  # 더 이상 유효한 데이터가 없으면 중단

        # 다음 idx로 이동
        idx += 1

    # 5. 새 데이터 DB 저장
    if new_data:
        insert_sql = """
        INSERT INTO event_img (img_url, start_date, end_date, event_title, store_type)
        VALUES (%s, %s, %s, %s, %s)
        """
        cursor.executemany(insert_sql, new_data)
        conn.commit()
        print(f"{len(new_data)}개의 새로운 데이터를 삽입했습니다.")

    # 6. 종료
    cursor.close()
    conn.close()

# # 매일 자정(00:00)에 실행되도록 설정
# schedule.every().day.at("00:00").do(run_script)

# # 주기적으로 실행
# while True:
#     schedule.run_pending()  # 예약된 작업을 확인하고 실행
#     time.sleep(60)  # 1분마다 확인 (이렇게 해야 계속 실행될 수 있어)

# 스케줄링 없이 강제로 함수 실행
run_script()  # 여기를 강제로 호출하면 즉시 실행
