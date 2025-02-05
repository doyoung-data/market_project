import schedule
import time
import pymysql
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Chrome WebDriver 경로 설정
CHROMEDRIVER_PATH = r"C:\Users\user\Desktop\chromedriver-win64\chromedriver-win64\chromedriver.exe"

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
last_first_event_title = None  # 마지막 크롤링한 이벤트 제목 저장

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
    """GS25 이벤트 전체 크롤링"""
    global last_first_event_title
    driver.get(base_url)
    time.sleep(3)

    # 현재 첫 번째 이벤트 제목 가져오기
    first_event = driver.find_element(By.CSS_SELECTOR, ".tblwrap tbody tr td.ft_lt a").text

    if last_first_event_title and first_event == last_first_event_title:
        print("🔴 첫 번째 이벤트 제목이 변경되지 않음. 크롤링 중단.")
        return

    last_first_event_title = first_event  # 최신 제목 저장
    print(f"✅ 새로운 이벤트 감지: {first_event}, 크롤링 시작")

    current_page = 1  # 현재 페이지 번호

    while True:
        event_list = driver.find_elements(By.CSS_SELECTOR, ".tblwrap tbody tr")
        for i in range(len(event_list)):
            try:
                driver.get(base_url)
                time.sleep(3)

                event_list = driver.find_elements(By.CSS_SELECTOR, ".tblwrap tbody tr")
                event_link = event_list[i].find_element(By.CSS_SELECTOR, "td.ft_lt a")
                event_link.click()
                time.sleep(3)

                scrape_event_page()  # 데이터 크롤링 및 저장

                driver.back()
                time.sleep(3)

            except Exception as e:
                print(f"Error processing event {i+1} on page {current_page}: {e}")

        # 다음 페이지 버튼 확인 및 클릭
        try:
            next_button = driver.find_element(By.CSS_SELECTOR, ".paging .next")
            if "disabled" in next_button.get_attribute("class"):
                print("🔴 마지막 페이지 도달, 크롤링 종료")
                break
            else:
                next_button.click()
                time.sleep(3)
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, ".paging .on"))
                )
                first_event_on_next_page = driver.find_element(By.CSS_SELECTOR, ".tblwrap tbody tr td.ft_lt a").text
                if first_event_on_next_page == last_first_event_title:
                    print("🔴 첫 번째 이벤트 제목이 변경되지 않음. 크롤링 중단.")
                    break 
                current_page += 1
                print(f"✅ {current_page} 페이지로 이동")
        except Exception as e:
            print("🔴 다음 페이지 버튼을 찾을 수 없음. 크롤링 종료")
            break

# 1️⃣ 강제 실행 (테스트용)
scrape_gs25_events()

# 2️⃣ 자동 실행 (매일 밤 12시)
schedule.every().day.at("00:00").do(scrape_gs25_events)

try:
    while True:
        schedule.run_pending()
        time.sleep(60)  # 1분마다 스케줄 확인
except KeyboardInterrupt:
    print("🔴 프로그램 종료")

# 브라우저 종료 (스케줄 종료 후)
driver.quit()
