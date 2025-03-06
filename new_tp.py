from sklearn.metrics import mean_absolute_percentage_error, explained_variance_score
import pandas as pd
import numpy as np
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense, Dropout
from sklearn.preprocessing import MinMaxScaler
import matplotlib.pyplot as plt
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
import joblib

# ✅ 데이터 불러오기
file_path = "./all_sale_seven.xlsx"
df = pd.read_excel(file_path)

# ✅ 날짜 변환 및 정렬 (순서 변경)
df['sale_date'] = pd.to_datetime(df['sale_date'])
df = df.sort_values(by='sale_date')

# ✅ 2024년도 데이터 필터링 (순서 수정)
df = df[df['sale_date'].dt.year == 2024]

# ✅ 데이터가 비어있는지 확인
if df.empty:
    raise ValueError("❌ 2024년도 데이터가 없습니다. 데이터 범위를 확인하세요!")

# ✅ 7일 이동 평균 계산 (기존 피처)
df['1+1_7일평균'] = df['1+1_event_count'].rolling(window=7, min_periods=1).mean()
df['2+1_7일평균'] = df['2+1_event_count'].rolling(window=7, min_periods=1).mean()
df['예능_7일평균'] = df['event_img'].rolling(window=7, min_periods=1).mean().fillna(method='bfill')

# ✅ 연령별 매출 이동 평균
for col in ['sum_amount', 'man10', 'man20', 'man30', 'man40', 'man50', 'man60',
            'woman10', 'woman20', 'woman30', 'woman40', 'woman50', 'woman60']:
    df[f'{col}_7일평균'] = df[col].rolling(window=7, min_periods=1).mean()

# ✅ store_count 7일 이동 평균 추가
if 'store_count' in df.columns:
    df['store_count_7일평균'] = df['store_count'].rolling(window=7, min_periods=1).mean()
else:
    raise ValueError("❌ 'store_count' 컬럼이 없습니다. 데이터를 확인하세요!")

# ✅ NaN 제거
df.dropna(inplace=True)

# ✅ X, y 정의
feature_columns = ['1+1_7일평균', '2+1_7일평균', '예능_7일평균', 'store_count_7일평균'] + \
                  [f'{col}_7일평균' for col in ['sum_amount', 'man10', 'man20', 'man30', 'man40', 
                                                'man50', 'man60', 'woman10', 'woman20', 'woman30', 
                                                'woman40', 'woman50', 'woman60']]

X = df[feature_columns]
y = df[[f'{col}_7일평균' for col in ['sum_amount', 'man10', 'man20', 'man30', 'man40', 
                                    'man50', 'man60', 'woman10', 'woman20', 
                                    'woman30', 'woman40', 'woman50', 'woman60']]]

# ✅ 데이터 정규화
scaler_X = MinMaxScaler()
scaler_y = MinMaxScaler()
X_scaled = scaler_X.fit_transform(X)
y_scaled = scaler_y.fit_transform(y)

# ✅ 시계열 데이터 생성 함수
def create_sequences(X, y, time_steps=7):
    Xs, ys = [], []
    for i in range(len(X) - time_steps):
        Xs.append(X[i:i+time_steps])
        ys.append(y[i+time_steps])
    return np.array(Xs), np.array(ys)

X_seq, y_seq = create_sequences(X_scaled, y_scaled)

# ✅ 훈련/테스트 데이터 분할
train_size = int(len(X_seq) * 0.8)
X_train, X_test = X_seq[:train_size], X_seq[train_size:]
y_train, y_test = y_seq[:train_size], y_seq[train_size:]

# ✅ LSTM 모델 생성
def build_lstm_model(output_dim=13):
    model = Sequential([
        LSTM(64, return_sequences=True, input_shape=(7, X_seq.shape[2])),
        Dropout(0.2),
        LSTM(64, return_sequences=False),
        Dropout(0.2),
        Dense(output_dim)
    ])
    model.compile(optimizer='adam', loss='mse')
    return model

# ✅ 모델 학습
lstm_model = build_lstm_model()
lstm_model.fit(X_train, y_train, epochs=50, batch_size=16, validation_data=(X_test, y_test))

# ✅ 모델 저장 (파일명 변경)
lstm_model.save("lstm_seven_model_2024.h5")
joblib.dump(scaler_X, "scaler_seven_X_2024.pkl")
joblib.dump(scaler_y, "scaler_seven_y_2024.pkl")

# ✅ 예측 수행
y_pred = lstm_model.predict(X_test)

# ✅ 예측값 역변환
y_pred = scaler_y.inverse_transform(y_pred)
y_test = scaler_y.inverse_transform(y_test)

# ✅ 평가 지표 계산
mse = mean_squared_error(y_test, y_pred)
mae = mean_absolute_error(y_test, y_pred)
rmse = np.sqrt(mse)
r2 = r2_score(y_test, y_pred)

# ✅ 추가 평가 지표 계산
mape = mean_absolute_percentage_error(y_test, y_pred)
evs = explained_variance_score(y_test, y_pred)
mbd = np.mean(y_pred - y_test)

print(f'Mean Squared Error (MSE): {mse:.4f}')
print(f'Mean Absolute Error (MAE): {mae:.4f}')
print(f'Root Mean Squared Error (RMSE): {rmse:.4f}')
print(f'R-squared Score (R²): {r2:.4f}')
print(f'Mean Absolute Percentage Error (MAPE): {mape:.4f}')
print(f'Explained Variance Score (EVS): {evs:.4f}')
print(f'Mean Bias Deviation (MBD): {mbd:.4f}')

# ✅ 개별 그래프 출력
dates = df['sale_date'].iloc[-len(y_test):].values
columns = ['sum_amount', 'man10', 'man20', 'man30', 'man40', 'man50', 'man60',
           'woman10', 'woman20', 'woman30', 'woman40', 'woman50', 'woman60']

for i, col in enumerate(columns):
    plt.figure(figsize=(10, 4))
    plt.plot(dates, y_test[:, i], label=f'Real {col}', linestyle='-', marker='o')
    plt.plot(dates, y_pred[:, i], label=f'Predicted {col}', linestyle='--', marker='x')
    plt.xlabel("Date")
    plt.ylabel("Sales")
    plt.title(f'{col} 예측 결과')
    plt.legend()
    plt.grid(True)
    plt.xticks(rotation=45)
    plt.show()
