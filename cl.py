import requests
from bs4 import BeautifulSoup

url = "https://pyony.com/brands/cu/202412/?event_type=&category=&item=100&sort=&price=&q="  # 크롤링할 웹 페이지 URL
req = requests.get(url)  # 웹 페이지 요청

if req.status_code == 200:  # 요청 성공 여부 확인
    soup = BeautifulSoup(req.content, "html.parser")

    # 상품 리스트 가져오기
    items = soup.find_all("div", class_="col-md-6")  # 상품 블록 기준 클래스

    for item in items:
        # 상품명
        name_tag = item.find("strong")
        name = name_tag.text.strip() if name_tag else "상품명 없음"

        # 가격
        price_tag = item.find("i", class_="fa-coins")
        price = price_tag.next_sibling.strip() if price_tag else "가격 없음"

        # 프로모션 (예: 1+1, 2+1 등)
        promo_tag = item.find("span", class_="badge bg-cu text-white")
        promo = promo_tag.text.strip() if promo_tag else "프로모션 없음"

        print(f"상품명: {name}, 가격: {price}, 프로모션: {promo}")
else:
    print(f"페이지 요청 실패. 상태 코드: {req.status_code}")
