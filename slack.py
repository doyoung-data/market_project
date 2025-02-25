import os
import numpy as np
import pandas as pd
import joblib
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from matplotlib import rc
from tensorflow.keras.models import load_model
from tensorflow.keras.losses import MeanSquaredError
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
import re

# ✅ 한글 폰트 설정 (운영체제별 자동 적용)
plt.rc('font', family='Malgun Gothic')  # Windows (맑은 고딕)
plt.rc('axes', unicode_minus=False)  # 마이너스 기호 깨짐 방지

# ✅ Slack API 설정
slack_token = "b"
slack_app_token = "p"
app = App(token=slack_token)

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

# ✅ Slack 이벤트 핸들러
@app.event("app_mention")
def handle_mention(event, say, client):
    text = event["text"].strip()
    _, store, date = text.split()
    response_message, graph_path = predict_sales(store, date)
    say(response_message)
    client.files_upload_v2(channels=event["channel"], file=graph_path)

if __name__ == "__main__":
    handler = SocketModeHandler(app, slack_app_token)
    handler.start()
