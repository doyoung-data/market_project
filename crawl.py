from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options

# ChromeDriver 경로 설정
CHROMEDRIVER_PATH = r"C:\Users\Admin\Downloads\chromedriver-win64\chromedriver-win64\chromedriver.exe"

# ChromeDriver 실행
service = Service(CHROMEDRIVER_PATH)
driver = webdriver.Chrome(service=service)

# 해당 사이트 열기
url = "https://pyony.com/brands/cu/"
driver.get(url)


# 브라우저 종료
driver.quit()
