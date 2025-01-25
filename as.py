#-*- coding: utf-8 -*-
import os
import sys
import urllib.request
import json

client_id = "qztXq0whcdbumihIgdNq"
client_secret = "cvN0t3QQrs"

# 요청 URL
url = "https://openapi.naver.com/v1/datalab/shopping/category/keyword/device"

# 요청 파라미터
body = json.dumps({
    "startDate": "2023-08-01",
    "endDate": "2024-09-30",
    "timeUnit": "month",
    "category": "50000000",  # 패션의류 카테고리 코드
    "keyword": "화장품",  # 검색 키워드
    "device": "pc",  # PC에서의 검색 클릭 추이
    "gender": "f",  # 여성 사용자
    "ages": ["20", "30"]  # 20대와 30대 연령대
})

# 요청 헤더 추가
request = urllib.request.Request(url)
request.add_header("X-Naver-Client-Id", client_id)
request.add_header("X-Naver-Client-Secret", client_secret)
request.add_header("Content-Type", "application/json")

# 요청 및 응답 처리
try:
    response = urllib.request.urlopen(request, data=body.encode("utf-8"))
    rescode = response.getcode()
    if rescode == 200:
        response_body = response.read()
        response_data = json.loads(response_body.decode('utf-8'))
        print(json.dumps(response_data, indent=4, ensure_ascii=False))
    else:
        print("Error Code:", rescode)
except Exception as e:
    print("Error:", e)