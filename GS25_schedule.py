import schedule
import time
import pymysql
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import logging

# 로그 설정 (로그 파일에 기록)
logging.basicConfig(filename='/home/ubuntu/market/CU_schedule.log', 
                    level=logging.DEBUG, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Chrome WebDriver 경로 설정
CHROMEDRIVER_PATH = "/home/ubuntu/chromedriver-linux64/chromedriver"

# Headless 옵션 설정
chrome_options = Options()
chrome_options.add_argument("--headless")  # GUI 없이 실행
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--disable-gpu")

service = Service(CHROMEDRIVER_PATH)
driver = webdriver.Chrome(service=service, options=chrome_options)  # Headless 모드 적용

# MariaDB 연결 설정
DB_CONFIG = {
    "host": "3.35.236.56",
    "user": "jsh",
    "password": "0929",
    "database": "crawling",
    "charset": "utf8mb4"
}

# GS25 이벤트 첫 페이지 URL
base_url = "http://gs25.gsretail.com/gscvs/en/customer-engagement/event/current-events"

def connect_db():
    return pymysql.connect(**DB_CONFIG)

def is_event_exists(event_info):
    """이미 저장된 이벤트인지 확인"""
    conn = connect_db()
    cursor = conn.cursor()
    query = """
    SELECT COUNT(*) FROM event_img 
    WHERE img_url = %s AND start_date = %s AND end_date = %s 
          AND event_title = %s AND store_type = %s
    """
    cursor.execute(query, (
        event_info["이미지 주소"],
        event_info["이벤트 시작 날짜"],
        event_info["이벤트 끝 날짜"],
        event_info["이벤트 제목"],
        event_info["편의점"]
    ))
    count = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    return count > 0

def save_to_db(event_info):
    """데이터 저장"""
    if is_event_exists(event_info):
        print(f"⏭ 이미 존재하는 이벤트: {event_info['이벤트 제목']}, 저장 생략")
        return

    conn = connect_db()
    cursor = conn.cursor()
    insert_sql = """
    INSERT INTO event_img (img_url, start_date, end_date, event_title, store_type, idx)
    VALUES (%s, %s, %s, %s, %s, 0)
    """
    cursor.execute(insert_sql, (
        event_info["이미지 주소"],
        event_info["이벤트 시작 날짜"],
        event_info["이벤트 끝 날짜"],
        event_info["이벤트 제목"],
        event_info["편의점"]
    ))
    conn.commit()
    cursor.close()
    conn.close()
    print(f"✅ 이벤트 저장 완료: {event_info['이벤트 제목']}")

def scrape_event_page():
    """이벤트 상세 페이지 크롤링"""
    try:
        title = driver.find_element(By.CSS_SELECTOR, ".tit_sect h3.tit strong").text
        start_date = driver.find_element(By.ID, "event-start-date").text
        end_date = driver.find_element(By.ID, "event-end-date").text
        image_url = driver.find_element(By.CSS_SELECTOR, ".event-web-contents img").get_attribute("src")

        event_info = {
            "이벤트 제목": title,
            "이벤트 시작 날짜": start_date,
            "이벤트 끝 날짜": end_date,
            "이미지 주소": image_url,
            "편의점": "GS25"
        }

        save_to_db(event_info)  # DB 저장
    except Exception as e:
        print("Error scraping event page:", e)

def scrape_gs25_events():
    """GS25 이벤트 첫 페이지 크롤링 (한 페이지만)"""
    driver.get(base_url)
    time.sleep(3)

    event_list = driver.find_elements(By.CSS_SELECTOR, ".tblwrap tbody tr")
    
    if not event_list:
        print("⚠ 이벤트 없음. 크롤링 종료")
        return
    
    for i in range(len(event_list)):
        try:
            driver.get(base_url)
            time.sleep(3)

            event_list = driver.find_elements(By.CSS_SELECTOR, ".tblwrap tbody tr")

            # 특정 행에 링크가 없으면 건너뛰기
            event_links = event_list[i].find_elements(By.CSS_SELECTOR, "td.ft_lt a")  
            if not event_links:
                print(f"⚠ {i+1}번째 이벤트에 링크가 없음, 건너뜀")
                continue  

            event_link = event_links[0]  # 첫 번째 링크 클릭
            event_link.click()
            time.sleep(3)

            scrape_event_page()  # 데이터 크롤링 및 저장

            driver.back()
            time.sleep(3)

        except Exception as e:
            print(f"❌ Error processing event {i+1}: {e}")
    print("✅ 크롤링 완료.")

# 1️⃣ 강제 실행 (테스트용)
# scrape_gs25_events()

# # 2️⃣ 자동 실행 (매일 밤 12시)
schedule.every().day.at("11:30").do(scrape_gs25_events)

while True:
    schedule.run_pending()
    time.sleep(60)  # 1분마다 스케줄 확인