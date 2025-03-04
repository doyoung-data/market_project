import mysql.connector
import schedule
import time
import threading
from datetime import datetime, timedelta
from slack_sdk import WebClient
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
import pandas as pd
import os
import numpy as np
import joblib
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from matplotlib import rc
from tensorflow.keras.models import load_model
from tensorflow.keras.losses import MeanSquaredError
import re


# ✅ 한글 폰트 설정 (운영체제별 자동 적용)
plt.rc('font', family='Malgun Gothic')  # Windows (맑은 고딕)
plt.rc('axes', unicode_minus=False)  # 마이너스 기호 깨짐 방지

# ✅ 모델 및 정규화 도구 로드
models = {
    "CU": load_model("test/lstm_CU_model.h5", custom_objects={"mse": MeanSquaredError()}),
    "GS25": load_model("test/lstm_GS25_model.h5", custom_objects={"mse": MeanSquaredError()}),
    "Seven": load_model("test/lstm_Seven_model.h5", custom_objects={"mse": MeanSquaredError()})
}
scalers_X = {
    "CU": joblib.load("test/scaler_CU_X.pkl"),
    "GS25": joblib.load("test/scaler_GS25_X.pkl"),
    "Seven": joblib.load("test/scaler_Seven_X.pkl")
}
scalers_y = {
    "CU": joblib.load("test/scaler_CU_y.pkl"),
    "GS25": joblib.load("test/scaler_GS25_y.pkl"),
    "Seven": joblib.load("test/scaler_Seven_y.pkl")
}

# ✅ 매출 예측 함수
def predict_sales(convenience_store, date_to_predict):
    model = models[convenience_store]
    scaler_X = scalers_X[convenience_store]
    scaler_y = scalers_y[convenience_store]

    # ✅ 데이터 불러오기
    file_path = f"test/all_sale_{convenience_store}.xlsx"
    df = pd.read_excel(file_path)
    df['sale_date'] = pd.to_datetime(df['sale_date'])
    df = df.sort_values(by='sale_date')

    # ✅ 모델이 학습한 컬럼 확인
    expected_features = scaler_X.feature_names_in_

    # ✅ 7일 이동 평균 계산 (누락된 컬럼 자동 추가)
    rolling_columns = {
        '1+1_event_count': '1+1_7일평균',
        '2+1_event_count': '2+1_7일평균',
        'event_img': '예능_7일평균'
    }
    for original_col, rolling_col in rolling_columns.items():
        if original_col in df.columns and rolling_col not in df.columns:
            df[rolling_col] = df[original_col].rolling(window=7, min_periods=1).mean()

    for col in ['sum_amount', 'man10', 'man20', 'man30', 'man40', 'man50', 'man60',
                'woman10', 'woman20', 'woman30', 'woman40', 'woman50', 'woman60']:
        df[f'{col}_7일평균'] = df[col].rolling(window=7, min_periods=1).mean()

    df['store_count_7일평균'] = df['store_count'].rolling(window=7, min_periods=1).mean()

    # ✅ 최근 7일 데이터 선택
    recent_data = df[df['sale_date'] < date_to_predict].iloc[-7:].copy()

    # ✅ 데이터 검증
    if recent_data.empty or len(recent_data) < 7:
        return "❌ 예측을 위한 최근 7일치 데이터가 부족합니다."

    # ✅ 모델이 학습한 컬럼과 일치하도록 변환
    X_input = recent_data[expected_features]

    # ✅ 정규화 및 예측 수행
    X_input_scaled = scaler_X.transform(X_input)
    X_input_reshaped = np.array([X_input_scaled])
    y_pred_scaled = model.predict(X_input_reshaped)
    y_pred = scaler_y.inverse_transform(y_pred_scaled)

    # ✅ 결과 포맷팅
    columns = ['sum_amount', 'man10', 'man20', 'man30', 'man40', 'man50', 'man60',
               'woman10', 'woman20', 'woman30', 'woman40', 'woman50', 'woman60']
    predicted_values = {col: y_pred[0][i] for i, col in enumerate(columns)}

    # ✅ 항아리 차트 생성
    graph_path = generate_gender_sales_graph(convenience_store, date_to_predict, predicted_values)

    # ✅ Slack 메시지 구성
    message = format_prediction_message(convenience_store, date_to_predict, predicted_values)

    return message, graph_path

# ✅ 항아리 차트 생성 함수
def generate_gender_sales_graph(convenience_store, date_to_predict, predicted_values):
    age_groups = ["10대", "20대", "30대", "40대", "50대", "60대 이상"]
    male_values = [predicted_values[f'man{i}0'] for i in range(1, 7)]
    female_values = [predicted_values[f'woman{i}0'] for i in range(1, 7)]

    male_values = np.array(male_values) / 1e8  # 억 원 단위 변환
    female_values = np.array(female_values) / 1e8  # 억 원 단위 변환

    plt.figure(figsize=(8, 6))
    plt.barh(age_groups, male_values, color="#2a50ae", label="남성")  # 어두운 파란색
    plt.barh(age_groups, -female_values, color="#c20000", label="여성")  # 어두운 빨간색

    plt.axvline(x=0, color="black", linewidth=1)  # 가운데 기준선 추가
    plt.xlabel("매출 (억 원)")
    plt.title(f"{convenience_store} {date_to_predict} 성별 매출 예측")
    plt.legend()
    plt.grid(True, linestyle="--", alpha=0.6)

    # ✅ x축을 억 원 단위로 변환
    plt.gca().xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"{x:.1f}억"))

    plt.savefig(f"test/{convenience_store}_{date_to_predict}.png", bbox_inches="tight")
    plt.close()

    return f"test/{convenience_store}_{date_to_predict}.png"

# ✅ Slack 메시지 포맷팅
def format_prediction_message(convenience_store, date_to_predict, predicted_values):
    total_sales = predicted_values["sum_amount"]
    male_total = sum(predicted_values[f"man{i}0"] for i in range(1, 7))
    female_total = sum(predicted_values[f"woman{i}0"] for i in range(1, 7))

    message = f"*📢 {convenience_store} {date_to_predict} 매출 예측 📢*\n"
    message += f"총 매출: {total_sales:,.0f} 원\n"
    message += f"  👨🏻 남자 총 매출액: {male_total:,.0f} 원\n"
    for i in range(1, 7):
        message += f"        {i}0대 남자: {predicted_values[f'man{i}0']:,.0f} 원\n"
    message += f"  👩🏻 여자 총 매출액: {female_total:,.0f} 원\n"
    for i in range(1, 7):
        message += f"        {i}0대 여자: {predicted_values[f'woman{i}0']:,.0f} 원\n"

    return message

# 🟦 Slack API Token 설정
slack_token = "xoxb---"
slack_app_token = "xapp-1---"
client = WebClient(token=slack_token)

# 🟦 MySQL 연결 설정
conn = mysql.connector.connect(
    host='3.35.236.56',
    port=3306,
    user='kdy',
    password='0710',
    database='crawling',
    charset='utf8mb4'
)
# 🟦 Slack 앱 초기화
app = App(token=slack_token)


# 🟦 채널 ID 설정
GS25_CHANNEL_ID = "C08BYUVRW4X"
CU_CHANNEL_ID = "C08CG17U15J"
SEVEN_CHANNEL_ID = "C08DJJT0Y8G"
YOUTUBE_CHANNEL_ID = "C08D3PN7SRL"
NAVER_NEWS_CHANNEL_ID = "C08DQRKQR51"
ALERT_CHANNEL_ID = "C08DRRPN61Z"
PREV_CHANNEL_ID = "C08EX9LFZT6"

# 🟦 편의점 채널 매핑
store_mapping = {
    GS25_CHANNEL_ID: "GS25",
    CU_CHANNEL_ID: "CU",
    SEVEN_CHANNEL_ID: "seven"
}

# 🟦 매출 이상 감지 기준 (growth_deviation 기준)
ANOMALY_THRESHOLDS = {
    "CU": {"high": 1.252, "low": -1.344},
    "GS25": {"high": 1.135, "low": -1.461},
    "seven": {"high": 1.394, "low": -1.374},
}

# 🟦 특정 날짜의 유튜브 및 네이버 뉴스 기사 조회 (최대 3개만 먼저 반환)
def get_ytb_links_by_date_and_store(date, store_type):
    query = f"""
        SELECT video_url FROM ytb_video
        WHERE store_type = '{store_type}' AND published_at LIKE '{date}%';
    """
    cursor = conn.cursor(dictionary=True)
    cursor.execute(query)
    data = cursor.fetchall()
    cursor.close()
    
    return [row["video_url"] for row in data] if data else []  # 리스트로 변환하여 반환


# 🟦 특정 날짜의 네이버 뉴스 기사 조회 (최대 3개만 먼저 반환)
def get_news_links_by_date_and_store(date, store_type):
    query = f"""
        SELECT news_url FROM news_search
        WHERE store_type = '{store_type}' AND news_date LIKE '{date}%'
        ORDER BY news_start DESC;
    """
    cursor = conn.cursor(dictionary=True)
    cursor.execute(query)
    data = cursor.fetchall()
    cursor.close()
    
    return [row["news_url"] for row in data] if data else []


# 🟦 백테스트 시작 날짜 (2024년 10월 1일부터 시작)
simulation_date = datetime(2024, 10, 1)

def format_news_links(news_links, date, store_type):
    """처음 3개 뉴스 링크만 출력하고, 더보기 버튼을 추가하는 함수"""
    if not news_links:
        return "📰 관련 뉴스가 없습니다.", None

    top_news = "\n".join(news_links[:3])  # 처음 3개를 문자열로 변환
    more_news = news_links[3:]  # 나머지 뉴스 링크는 더보기 버튼을 통해 출력

    message = f"📰 {date} {store_type} 관련 뉴스: 🎬\n" + top_news
    
    # 더보기 버튼 추가
    attachments = None
    if more_news:
        attachments = [
            {
                "blocks": [
                    {
                        "type": "actions",
                        "elements": [
                            {
                                "type": "button",
                                "text": {"type": "plain_text", "text": "뉴스 더보기"},
                                "action_id": "show_more_news",
                                "value": f"{date}|{store_type}"
                            }
                        ]
                    }
                ]
            }
        ]
    return message, attachments

def format_ytb_links(ytb_links, date, store_type):
    """처음 3개 유튜브 링크만 출력하고, 더보기 버튼을 추가하는 함수"""
    if not ytb_links:
        return "🎬 관련 유튜브 영상이 없습니다.", None

    top_ytb = "\n".join(ytb_links[:3])  # 처음 3개만 출력
    more_ytb = ytb_links[3:]  # 나머지 유튜브 링크는 더보기 버튼을 통해 출력

    message = f"🎬 {date} {store_type} 유튜브 영상 목록 🎬\n" + top_ytb
    
    # 더보기 버튼 추가
    attachments = None
    if more_ytb:
        attachments = [
            {
                "blocks": [
                    {
                        "type": "actions",
                        "elements": [
                            {
                                "type": "button",
                                "text": {"type": "plain_text", "text": "유튜브 더보기"},
                                "action_id": "show_more_ytb",
                                "value": f"{date}|{store_type}"
                            }
                        ]
                    }
                ]
            }
        ]
    return message, attachments

def detect_sales_anomalies():
    global simulation_date
    today_str = simulation_date.strftime("%Y-%m-%d")

    print(f"🔍 [LOG] {today_str} 매출 이상 감지 체크 중...")

    query = f"""
        SELECT sale_date, store_type, sum_amount, growth_deviation
        FROM all_sale
        WHERE sale_date = '{today_str}';
    """
    cursor = conn.cursor(dictionary=True)
    cursor.execute(query)
    data = cursor.fetchall()
    cursor.close()

    if not data:
        print(f"⚠️ [LOG] {today_str} 데이터 없음")
        return

    for row in data:
        store = row["store_type"]
        deviation = row["growth_deviation"]

        print(f"🔎 [DEBUG] {today_str} {store}: growth_deviation={deviation:.2f}")

        if store in ANOMALY_THRESHOLDS:
            ytb_links = get_ytb_links_by_date_and_store(today_str, store)
            news_links = get_news_links_by_date_and_store(today_str, store)

            ytb_message, ytb_attachments = format_ytb_links(ytb_links, today_str, store)
            news_message, news_attachments = format_news_links(news_links, today_str, store)

            if deviation >= ANOMALY_THRESHOLDS[store]["high"]:
                msg = (
                    f"🚨 {today_str} {store} 매출 급등 감지!\n"
                    f"💰 매출액: {row['sum_amount']:,.0f}원\n"
                    f"📊 편차 (growth_deviation): {deviation:.2f}%\n\n"
                    f"{ytb_message}\n\n"
                    f"{news_message}\n"
                )

                client.chat_postMessage(
                    channel=ALERT_CHANNEL_ID,
                    text=msg,
                    attachments=(ytb_attachments if ytb_attachments else []) +
                                (news_attachments if news_attachments else []),
                    unfurl_links=True,
                    unfurl_media=True
                )
                print(f"✅ [LOG] {store} 매출 급등 감지: {deviation:.2f}%")

            if deviation <= ANOMALY_THRESHOLDS[store]["low"]:
                msg = (
                    f"⚠️ {today_str} {store} 매출 급감 감지!\n"
                    f"💰 매출액: {row['sum_amount']:,.0f}원\n"
                    f"📉 편차 (growth_deviation): {deviation:.2f}%\n\n"
                    f"{ytb_message}\n\n"
                    f"{news_message}\n"
                )
                print(f"📨 [DEBUG] 메시지 내용: {msg}")
                print(f"📎 [DEBUG] 첨부 파일: {(ytb_attachments if ytb_attachments else []) + (news_attachments if news_attachments else [])}")

                client.chat_postMessage(
                    channel=ALERT_CHANNEL_ID,
                    text=msg,
                    attachments=(ytb_attachments if ytb_attachments else []) +
                                (news_attachments if news_attachments else []),
                    unfurl_links=True,
                    unfurl_media=True
                )
                print(f"✅ [LOG] {store} 매출 급감 감지: {deviation:.2f}%")

    simulation_date += timedelta(days=1)  # 날짜 증가



# 🟦 백그라운드에서 1분마다 실행
def schedule_test_check():
    schedule.every(1).minutes.do(detect_sales_anomalies)
    while True:
        schedule.run_pending()
        time.sleep(10)

# 🟦 매출 데이터 조회 함수
def get_sales_data(date, store_type):
    query = f"""
        SELECT sum_amount, sum_amount_growth, avg_sum_amount_growth, growth_deviation
        FROM all_sale
        WHERE sale_date = '{date}' AND store_type = '{store_type}';
    """
    cursor = conn.cursor(dictionary=True)
    cursor.execute(query)
    data = cursor.fetchone()
    cursor.close()
    return data

# 🟦 멘션 이벤트 핸들러
# ✅ Slack 이벤트 핸들러 (멘션 처리)
@app.event("app_mention")
def handle_mention(event, say, client):
    import logging
    logging.basicConfig(level=logging.DEBUG)
    logging.debug(f"📢 이벤트 감지: {event}")  # 이벤트 로그 출력

    text = event.get("text", "").strip()
    text = re.sub(r"<@U[A-Z0-9]+>", "", text).strip().upper()
    channel_id = event.get("channel")

    # 🔹 "대시보드" 명령어 처리
    if text == "대시보드" and channel_id == "C08E48KQWET":
        say(f"📊 태블로 대시보드 링크입니다: https://public.tableau.com/app/profile/.70256853/viz/shared/497MCJW64")
        return

    if text == "티피" and channel_id == "C08E48KQWET":
        say(f"📊 태블로 TP 링크입니다: https://public.tableau.com/app/profile/.70256853/viz/_17404727845250/3store_TP?publish=yes")
        return

    # 🔹 입력에서 날짜 및 편의점 종류 추출
    date_match = re.search(r"\d{4}-\d{2}-\d{2}", text)
    store_match = re.search(r"(GS25|CU|SEVEN)", text)

    # 🔹 만약 사용자가 아무것도 입력하지 않았다면
    if not date_match and not store_match:
        if channel_id in store_mapping:
            say("⚠️ 사용법: `@브니 GS25 2024-10-02` 형식으로 입력해주세요.")
        elif channel_id in [YOUTUBE_CHANNEL_ID, NAVER_NEWS_CHANNEL_ID]:
            say("⚠️ 사용법: `@브니 GS25 2024-10-02` 형식으로 입력해주세요.")
        return

    # 🔹 날짜와 편의점 종류 추출
    date = date_match.group(0) if date_match else None
    store_type = store_match.group(0) if store_match else None

    # ✅ 매출 예측 실행
    if channel_id == PREV_CHANNEL_ID:
        date_match = re.search(r"\d{4}-\d{2}-\d{2}", text)
        store_match = re.search(r"(GS25|CU|SEVEN)", text)

        if date_match and store_match:
            date = date_match.group(0)
            store_type = store_match.group(0)

            # ✅ store_mapping을 사용하여 store_type 변환
            store_type = "Seven" if store_type == "SEVEN" else store_type

            say(f"📅 {date} {store_type} 매출 예측 중...⏳")

            try:
                response_message, graph_path = predict_sales(store_type, date)
                say(response_message)

                # ✅ 예측 그래프 업로드
                client.files_upload_v2(
                    channels=channel_id,
                    file=graph_path,
                    title=f"{store_type} {date} 매출 예측 그래프"
                )
            except Exception as e:
                say(f"⚠️ 매출 예측 중 오류 발생: {e}")
        else:
            say("⚠️ 사용법: `@브니 GS25 2024-10-02` 형식으로 입력해주세요.")

    # 🔹 채널이 올바른 경우 기존 매출 데이터 조회
    if channel_id in store_mapping:
        store_type = store_mapping[channel_id]
        if date:
            data = get_sales_data(date, store_type)
            if data:
                response = (
                    f"📅 {date} {store_type} 매출 분석 📊\n"
                    f"💰 매출액: {data['sum_amount']:,.0f}원\n"
                    f"📈 전날 대비 상승률: {data['sum_amount_growth']:.2f}%\n"
                    f"📊 3사 평균 상승률: {data['avg_sum_amount_growth']:.2f}%\n"
                    f"⚠️ 평균 대비 차이: {data['growth_deviation']:.2f}%"
                )
                say(response)
            else:
                say(f"⚠️ {date}에 대한 {store_type} 매출 데이터가 없습니다.")
        else:
            say("⚠️ 사용법: `@브니 2024-10-02` 형식으로 입력해주세요.")

    # 🔹 유튜브 검색 채널에서 실행
    elif channel_id == YOUTUBE_CHANNEL_ID:
        ytb_links = get_ytb_links_by_date_and_store(date, store_type)
        message, attachments = format_ytb_links(ytb_links, date, store_type)
        if ytb_links:
            say(message,attachments=attachments)
        else:
            say(f"⚠️ {date}에 대한 {store_type} 유튜브 영상이 없습니다.")

    # 🔹 네이버 뉴스 검색 채널에서 실행
    elif channel_id == NAVER_NEWS_CHANNEL_ID:
        news_links = get_news_links_by_date_and_store(date, store_type)
        message, attachments = format_news_links(news_links, date, store_type)

        if news_links:
            say(message,attachments=attachments)
        else:
            say(f"⚠️ {date}에 대한 {store_type} 네이버 뉴스가 없습니다.")


# 🟦 유튜브 더보기 버튼 클릭 처리
@app.action("show_more_ytb")
def handle_show_more_ytb(ack, body, respond):
    ack()
    
    value = body['actions'][0]['value']
    date, store_type = value.split("|")
    
    ytb_urls = get_ytb_links_by_date_and_store(date, store_type)

    if len(ytb_urls) > 3:
        more_ytb = "\n".join(ytb_urls[3:])  # 리스트 → 문자열 변환
        message = f"🎬 {date} {store_type} 추가 유튜브 영상 🎬\n{more_ytb}"
        
        # Slack에서 링크 미리보기를 활성화하도록 설정
        respond(text=message, replace_original=False, unfurl_links=True, unfurl_media=True)
    else:
        respond(text="⚠️ 더 이상 유튜브 영상이 없습니다.", replace_original=False)


    
# 🟦 뉴스 더보기 버튼 클릭 처리
@app.action("show_more_news")
def handle_show_more_news(ack, body, respond):
    ack()
    
    value = body['actions'][0]['value']
    date, store_type = value.split("|")
    
    news_urls = get_news_links_by_date_and_store(date, store_type)
    
    if len(news_urls) > 3:
        more_news = news_urls[3:]
        message = f"📰 {date} {store_type} 추가 뉴스 기사 📰\n" + "\n".join(more_news)
        
        # Slack에서 링크 미리보기를 활성화하도록 설정
        respond(text=message, replace_original=False, unfurl_links=True, unfurl_media=True)
    else:
        respond(text="⚠️ 더 이상 뉴스 기사가 없습니다.", replace_original=False)



# 🟦 실행
if __name__ == "__main__":
    # Slack 앱 실행과 동시에 스케줄러를 백그라운드에서 실행하도록 설정
    threading.Thread(target=schedule_test_check, daemon=True).start()
    # Slack 이벤트 핸들러 실행
    handler = SocketModeHandler(app, slack_app_token)
    handler.start()
    # 메인 스레드를 유지하여 백그라운드 작업이 종료되지 않도록 함
    while True:
        time.sleep(1)