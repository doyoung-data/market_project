import requests
from bs4 import BeautifulSoup
import csv
import re

# CSV 파일 이름과 헤더 정의
csv_file = 'cu.csv'
header = ["img_url", "store_type", "event_title", "start_date", "end_date"]

# 크롤링할 URL 범위 설정
base_url = "https://cu.bgfretail.com/brand_info/news_view.do?category=brand_info&depth2=5&idx="
start_idx = 992
end_idx = 1078

# 데이터 저장할 리스트
data = []

# CSV 파일 작성
with open(csv_file, mode="w", newline="", encoding="utf-8") as file:
    writer = csv.writer(file)
    writer.writerow(header)  # 헤더 작성

    for idx in range(start_idx, end_idx + 1):
        url = base_url + str(idx)
        response = requests.get(url)

        if response.status_code == 200:
            soup = BeautifulSoup(response.text, "html.parser")

            # event_img 가져오기 (thead > tr > th class="date")
            img_tag = soup.select_one('td[colspan="2"] img[border="0"]')
            event_img = img_tag["src"] if img_tag else "No Image"

            # 편의점종류는 CU로 고정
            convenience_store = "CU"

            # 업데이트 (thead > tr > th)
            update_tag = soup.find("thead").find("tr").find("th")
            update_text = update_tag.get_text(strip=True) if update_tag else "No Update"

            # 데이터 저장s
            writer.writerow([event_img, convenience_store, update_text])
            print(f"크롤링 완료: {url}")

        else:
            print(f"요청 실패: {url} (status code: {response.status_code})")

print(f"\n 크롤링 데이터가 '{csv_file}' 파일로 저장되었습니다!")