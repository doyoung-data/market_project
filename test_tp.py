import numpy as np
import pandas as pd
import joblib
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from tensorflow.keras.models import load_model
from tensorflow.keras.losses import MeanSquaredError

# ✅ 한글 폰트 설정
import platform
if platform.system() == 'Windows':
    plt.rc('font', family='Malgun Gothic')  # Windows (맑은 고딕)
elif platform.system() == 'Darwin':  # MacOS
    plt.rc('font', family='AppleGothic')  # Mac (애플 고딕)
else:
    plt.rc('font', family='NanumGothic')  # Linux (나눔고딕)

plt.rc('axes', unicode_minus=False)  # 마이너스(-) 기호 깨짐 방지

# ✅ 저장된 모델과 정규화 도구 불러오기
custom_objects = {"mse": MeanSquaredError()}
model = load_model("lstm_seven_model_2024.h5", custom_objects=custom_objects)
scaler_X = joblib.load("scaler_seven_X.pkl")
scaler_y = joblib.load("scaler_seven_y.pkl")

# ✅ 최근 7일 데이터 준비 (예: 2024년 9월 24일~9월 30일)
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

# ✅ 7일간의 데이터를 추출 (마지막 7일)
date_to_predict = '2024-10-01'
recent_data = df[df['sale_date'] < date_to_predict].iloc[-7:].copy()

# ✅ 데이터 부족 시 예외 처리
if recent_data.empty or len(recent_data) < 7:
    raise ValueError("❌ 2024-10-01을 예측하기 위한 최근 7일치 데이터가 부족합니다. 데이터를 확인하세요!")

# ✅ 필요한 컬럼만 선택 (store_count 추가됨)
X_input = recent_data[['1+1_7일평균', '2+1_7일평균', '예능_7일평균', 'store_count_7일평균'] + 
                      [f'{col}_7일평균' for col in ['sum_amount', 'man10', 'man20', 'man30', 'man40', 
                                                    'man50', 'man60', 'woman10', 'woman20', 'woman30', 
                                                    'woman40', 'woman50', 'woman60']]]

# ✅ 데이터 정규화
X_input.fillna(0, inplace=True)
X_input.fillna(X_input.mean(), inplace=True)
X_input.fillna(method='ffill', inplace=True)

# ✅ 결측값 확인 후 처리
if X_input.isna().sum().sum() > 0:
    raise ValueError("❌ 예측 입력 데이터에 결측값이 있습니다. 데이터를 확인하세요!")

X_input_scaled = scaler_X.transform(X_input)

# ✅ 모델 입력 형식으로 변환
X_input_reshaped = np.array([X_input_scaled])

# ✅ 예측 수행
y_pred_scaled = model.predict(X_input_reshaped)

# ✅ 예측값 역변환
y_pred = scaler_y.inverse_transform(y_pred_scaled)

# ✅ 예측 결과 출력
columns = ['sum_amount', 'man10', 'man20', 'man30', 'man40', 'man50', 'man60',
           'woman10', 'woman20', 'woman30', 'woman40', 'woman50', 'woman60']

predicted_values = dict(zip(columns, y_pred[0]))

# ✅ 실제 데이터에서 예측 날짜의 값 가져오기
actual_values = df[df['sale_date'] == date_to_predict][columns].values.flatten()

# ✅ 그래프 비교
plt.figure(figsize=(12, 6))
plt.plot(columns, actual_values, label="실제 값", linestyle='-', marker='o', color='#0b6e69')
plt.plot(columns, y_pred[0], label="예측 값", linestyle='--', marker='x', color='#6e0b10')

# ✅ x축 회전 및 간격 조정
plt.xticks(rotation=45, ha='right')

# ✅ y축 지수 표기법(Scientific Notation) 제거
plt.ticklabel_format(style='plain', axis='y')

# ✅ y축 숫자에 천 단위 콤마 추가
ax = plt.gca()
ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f'{int(x):,}'))

plt.ylabel("매출 (원)")
plt.title(f"실제 vs 예측 (2024년 10월 1일)")
plt.legend()
plt.grid(True)
plt.show()

# ✅ 예측값 및 실제값 출력
print(f"📌 {date_to_predict} 예측 결과:")
for key, pred_val, actual_val in zip(columns, y_pred[0], actual_values):
    print(f"{key}: 예측 {pred_val:,.2f}, 실제 {actual_val:,.2f}")
