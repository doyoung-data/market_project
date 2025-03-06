import numpy as np
import pandas as pd
import joblib
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from tensorflow.keras.models import load_model
from tensorflow.keras.losses import MeanSquaredError

# ✅ 한글 폰트 설정
plt.rc('font', family='Malgun Gothic')  # Windows 환경
plt.rc('axes', unicode_minus=False)  # 마이너스 기호 깨짐 방지

# ✅ 저장된 모델과 정규화 도구 불러오기
custom_objects = {"mse": MeanSquaredError()}
model = load_model("lstm_seven_model_2024.h5", custom_objects=custom_objects)
scaler_X = joblib.load("scaler_seven_X.pkl")
scaler_y = joblib.load("scaler_seven_y.pkl")

# ✅ 데이터 불러오기
file_path = "./all_sale_seven.xlsx"
df = pd.read_excel(file_path)
df['sale_date'] = pd.to_datetime(df['sale_date'])
df = df.sort_values(by='sale_date')

# ✅ 7일 이동 평균 계산 (기존 피처)
df['1+1_7일평균'] = df['1+1_event_count'].rolling(window=7, min_periods=1).mean()
df['2+1_7일평균'] = df['2+1_event_count'].rolling(window=7, min_periods=1).mean()
df['예능_7일평균'] = df['event_img'].rolling(window=7, min_periods=1).mean().fillna(method='bfill')

# ✅ 연령별 매출 이동 평균 계산
for col in ['sum_amount', 'man10', 'man20', 'man30', 'man40', 'man50', 'man60',
            'woman10', 'woman20', 'woman30', 'woman40', 'woman50', 'woman60']:
    df[f'{col}_7일평균'] = df[col].rolling(window=7, min_periods=1).mean()

# ✅ store_count 7일 이동 평균 추가
if 'store_count' in df.columns:
    df['store_count_7일평균'] = df['store_count'].rolling(window=7, min_periods=1).mean()
else:
    raise ValueError("❌ 'store_count' 컬럼이 없습니다. 데이터를 확인하세요!")

# ✅ 2024년 데이터만 필터링
df = df[(df['sale_date'] >= '2024-01-01') & (df['sale_date'] <= '2024-12-31')]

# ✅ 예측값과 실제값 비교 (sum_amount만)
predicted_values = []
actual_values = []
dates = []

for date in df['sale_date'].unique():
    recent_data = df[df['sale_date'] < date].iloc[-7:].copy()
    if len(recent_data) < 7:
        continue
    
    # ✅ 필요한 컬럼만 선택
    X_input = recent_data[['1+1_7일평균', '2+1_7일평균', '예능_7일평균', 'store_count_7일평균'] + 
                          [f'{col}_7일평균' for col in ['sum_amount', 'man10', 'man20', 'man30', 'man40', 
                                                        'man50', 'man60', 'woman10', 'woman20', 'woman30', 
                                                        'woman40', 'woman50', 'woman60']]]
    
    X_input.fillna(0, inplace=True)
    X_input.fillna(X_input.mean(), inplace=True)
    X_input.fillna(method='ffill', inplace=True)
    
    X_input_scaled = scaler_X.transform(X_input)
    X_input_reshaped = np.array([X_input_scaled])
    y_pred_scaled = model.predict(X_input_reshaped)
    y_pred = scaler_y.inverse_transform(y_pred_scaled)
    
    actual_value = df[df['sale_date'] == date]['sum_amount'].values
    if len(actual_value) > 0:
        predicted_values.append(y_pred[0][0])
        actual_values.append(actual_value[0])
        dates.append(date)

# ✅ 그래프 그리기
plt.figure(figsize=(12, 6))
plt.plot(dates, actual_values, label="실제 총매출", linestyle='-', color='#0b6e69')
plt.plot(dates, predicted_values, label="예측 총매출", linestyle='--', marker='x', color='#6e0b10')
plt.xticks(rotation=45)
plt.ylabel("매출 (원)")
plt.title("2024년 총 매출 : 예측 vs 실제 비교")
plt.legend()
plt.grid(True)

# ✅ y축을 정수로 설정
ax = plt.gca()
ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f'{int(x):,}'))

plt.show()
