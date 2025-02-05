import pymysql
import time
import schedule  # 스케줄 라이브러리 추가
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import re

# ✅ ChromeDriver 경로 설정
CHROMEDRIVER_PATH = r"C:\Users\Admin\Downloads\chromedriver-win64\chromedriver-win64\chromedriver.exe"

# ✅ ChromeDriver 실행 설정 (HEADLESS 모드 활성화)
service = Service(CHROMEDRIVER_PATH)
options = webdriver.ChromeOptions()
options.add_argument("--headless")  # GUI 없이 실행
options.add_argument("--disable-gpu")  # GPU 사용 안 함 (Linux 서버에서 필수)
options.add_argument("--no-sandbox")  # 보안 모드 비활성화 (일부 환경에서 필요)
options.add_argument("--disable-dev-shm-usage")  # /dev/shm 파티션 문제 해결

# ✅ MariaDB 연결 설정
DB_CONFIG = {
    "host": "3.35.236.56",  
    "user": "jsh",  
    "password": "0929",  
    "database": "crawling",  
    "charset": "utf8mb4"
}

# ✅ MariaDB 연결 및 기존 데이터 조회
def connect_db():
    """MariaDB 연결 및 기존 저장된 이벤트 데이터 조회"""
    try:
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # 기존 저장된 이벤트 제목 가져오기 (중복 방지)
        cursor.execute("SELECT event_title FROM event_img")
        existing_titles = {row[0] for row in cursor.fetchall()}  

        return conn, cursor, existing_titles
    except Exception as e:
        print(f"❌ MariaDB 연결 오류: {e}")
        return None, None, set()

def open_event_page():
    """ 세븐일레븐 이벤트 페이지를 새로 엶 (HEADLESS 모드) """
    driver = webdriver.Chrome(service=service, options=options)
    driver.get("http://m.7-eleven.co.kr/product/eventList.asp")
    time.sleep(3)  # 페이지 로딩 대기
    return driver

def scrape_events(driver, existing_titles):
    """ 현재 페이지의 이벤트를 크롤링 """
    wait = WebDriverWait(driver, 10)  # 최대 10초까지 대기
    event_details = []  # 새로운 데이터 저장 리스트

    try:
        # ✅ 이벤트 목록이 로드될 때까지 대기
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "#listUl li a")))

        # ✅ 이벤트 목록 가져오기 (매번 새로 가져옴)
        events = driver.find_elements(By.CSS_SELECTOR, "#listUl li a")
        print(f"🔍 총 {len(events)}개의 이벤트 발견!")

        for event in events:
            try:
                # ✅ 이벤트 ID 추출 (fncGoView(1143) 형태에서 1143 추출)
                event_id_match = re.search(r"fncGoView\((\d+)\)", event.get_attribute("href"))
                if not event_id_match:
                    continue
                event_id = event_id_match.group(1)

                # ✅ 이벤트 제목 가져오기
                title_element = event.find_element(By.TAG_NAME, "strong")
                title = title_element.text.strip()

                # ✅ 이벤트 기간 (YYYY-MM-DD ~ YYYY-MM-DD)
                date_element = event.find_element(By.TAG_NAME, "span")
                date_range = date_element.text.strip()
                start_date, end_date = date_range.split(" ~ ")

                # ✅ 중복 체크 (이미 DB에 있으면 저장 안 함)
                if title in existing_titles:
                    print(f"⚠️ 중복 이벤트 무시: {title}")
                    continue

                # ✅ 이벤트 상세 페이지 이동
                driver.execute_script("arguments[0].click();", event)
                time.sleep(2)  # 상세 페이지 로딩 대기
                
                # ✅ 상세 페이지에서 이미지 URL 가져오기
                img_url = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".event_wrap_view img"))).get_attribute("src")
                convenience_store = "seven"  # ✅ "세븐일레븐"

                # ✅ 새로운 데이터 저장
                event_details.append((img_url, start_date, end_date, title, convenience_store))
                existing_titles.add(title)  # 저장된 데이터 Set에 추가

                # ✅ 실시간 진행 상황 출력
                print(f"[{len(event_details)}] 저장: {title}, {start_date}, {end_date}, {img_url}, {convenience_store}")

                # ✅ 상세 페이지에서 돌아가기
                driver.back()
                time.sleep(2)  # 로딩 대기

                # ✅ 요소 갱신 (stale element 방지)
                events = driver.find_elements(By.CSS_SELECTOR, "#listUl li a")

            except Exception as e:
                print(f"❌ 데이터 수집 오류: {e}")
                continue

    except Exception as e:
        print(f"❌ 크롤링 중 오류 발생: {e}")

    return event_details

def save_data_to_db(conn, cursor, event_details):
    """ MariaDB에 크롤링 데이터를 저장 """
    insert_sql = """
    INSERT INTO event_img (img_url, start_date, end_date, event_title, store_type, idx)
    VALUES (%s, %s, %s, %s, %s, 0)  -- idx 값을 항상 0으로 고정
    """

    if event_details:
        try:
            cursor.executemany(insert_sql, event_details)
            conn.commit()
            print(f"✅ {len(event_details)}개의 새로운 이벤트가 DB에 저장되었습니다!")
        except Exception as e:
            conn.rollback()
            print(f"❌ DB 저장 오류: {e}")
    else:
        print("⚠️ 새로운 데이터가 없어 DB 업데이트하지 않았습니다.")

def run_crawling():
    """ 크롤링 실행 함수 (스케줄러에서 호출) """
    print(f"\n🕛 [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 자동 크롤링 시작...")
    
    # **DB 연결**
    conn, cursor, existing_titles = connect_db()
    if conn is None or cursor is None:
        print("❌ DB 연결 실패로 인해 크롤링을 중단합니다.")
        return

    # **크롤링 실행**
    driver = open_event_page()
    event_details = scrape_events(driver, existing_titles)

    # **DB 저장**
    save_data_to_db(conn, cursor, event_details)

    # **연결 종료**
    cursor.close()
    conn.close()
    driver.quit()

    print(f"✅ [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 크롤링 완료!\n")

# ✅ 매일 00시에 실행
schedule.every().day.at("00:00").do(run_crawling)

# ✅ 스케줄 강제 실행 (테스트용) → 이 한 줄을 실행하면 바로 크롤링이 실행됨
schedule.run_all()

print("✅ 자동 크롤링 스케줄러가 설정되었습니다. (매일 00:00 실행)")

# **무한 루프로 스케줄 실행 유지**
while True:
    schedule.run_pending()
    time.sleep(60)  # 1분마다 체크
