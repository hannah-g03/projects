# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a40bc079-4900-4630-97c0-902c8c1e2328",
# META       "default_lakehouse_name": "Weather_lakehouse",
# META       "default_lakehouse_workspace_id": "346e1b64-7681-4e36-aaf6-454ec69177f7"
# META     }
# META   }
# META }

# CELL ********************

# Data science & ML
import pandas as pd
import numpy as np
import seaborn as sns
from sklearn.metrics import mean_squared_error, mean_absolute_error
from sklearn.model_selection import TimeSeriesSplit

# Stats & Time series
import statsmodels.api as sm
import scipy.stats as stats
from statsmodels.tsa.stattools import adfuller, acf, pacf
from statsmodels.graphics.tsaplots import plot_acf, plot_pacf
from statsmodels.tsa.seasonal import seasonal_decompose, STL
from statsmodels.tsa.statespace.sarimax import SARIMAX
from statsmodels.tsa.arima.model import ARIMA

# Visualisation
import matplotlib.pyplot as plt
import matplotlib as mpl

# Utilities
from itertools import product
import itertools
import warnings
import os
import json
from datetime import timedelta, datetime 
from time import time
from delta import *

# Spark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode_outer, when, lit
from pyspark.sql.types import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### visualisations and machine learning model on weather dataset : fact_temp

# CELL ********************

# Load dataset
temp_df = spark.table("fact_temp").toPandas()
temp_df.set_index('dt', inplace=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

temp_df.info()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Plot main_temp

sns.set_style('whitegrid')
sns.lineplot(x='dt', y='main_temp', data=temp_df).set(title='main temp - whole dataset')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Removing multiple inputs a day 
df_daily = temp_df.resample('D').mean()

sns.set_style('whitegrid')
sns.lineplot(x='dt', y='main_temp', data=df_daily).set(title='main temp - one entry per day')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### ADF test - Augmented Dicky-Fuller test
# 
# a statistical test that determines if a time series is stationary or not:
# - When the test statistic is lower than the critical value, you reject the null hypothesis and infer that the time series is stationary.

# CELL ********************

# ADF test - stationary 

result = adfuller(df_daily['main_temp'])

# print the test statistic, p-value, and other relevant information
print('ADF Statistic:', result[0])
print('p-value:', result[1])
print('Critical Values:')
for key, value in result[4].items():
    print(key, ':', value)

# if the p-value is less than 0.05, we reject the null hypothesis and conclude that the time series is stationary
if result[1] < 0.05:
    print('The time series is likely stationary.')
else:
    print('The time series is likely non-stationary.')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Take the first difference - eliminates trend and seasonality (first difference of Y at period t = Yt-Yt-1)

first_diff = df_daily.diff()[1:]

sns.set_style('whitegrid')
sns.lineplot(x='dt', y='main_temp', data=first_diff).set(title='main temp - 1st difference')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### ACF plot - Autocorrelation function 
# 
# shows the correlation between a time series and its lagged values
# - observation at the current time spot and the observations at previous time spots
# - starts at lag 0 (which is itself) so therefore should always have a correlation of 1
# - used to identify the order of an AR model

# CELL ********************


fig, axs = plt.subplots(1, 2, figsize=(20, 5))

plot_acf(df_daily['main_temp'], lags=10, title='ACF of main temperature - not differenced', ax=axs[0])
axs[0].set_xlabel('Lags')
axs[0].set_ylabel('Autocorrelation')

plot_acf(first_diff['main_temp'], lags=10, title='ACF of main temperature - first difference', ax=axs[1])
axs[1].set_xlabel('Lags')
axs[1].set_ylabel('Autocorrelation')

plt.show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# can select the order q for model - if plot has a sharp cut-off after lag
# - nothing noticeable 

# MARKDOWN ********************

# ### PACF plot - Partial autocorrelation function
# 
# shows the correlation of a time series with itself at different lags, after removing the effects of the previous lags
# - used to identify the order of an MA model
# 


# CELL ********************

fig, axs = plt.subplots(1, 2, figsize=(20, 5))

plot_pacf(df_daily['main_temp'], lags=10, title='PACF of main temperature - not differenced', ax=axs[0])
axs[0].set_xlabel('Lags')
axs[0].set_ylabel('Partial Autocorrelation')

plot_pacf(first_diff['main_temp'], lags=10, title='PACF of main temperature - first difference', ax=axs[1])
axs[1].set_xlabel('Lags')
axs[1].set_ylabel('Partial Autocorrelation')

plt.show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# PACF - outside the blue boundary tells us the order of the AR model (p)
# - significant lags (2 , 3 , 9)
# - shows seasonality 

# MARKDOWN ********************

# ### Seasonal Decomposition
# 
# breaks down time series data into three components: trend, seasonal, and residual
# 
# - Trend: long-term movement of the data, which can be up or down
# - Seasonal: patterns that occur within a fixed time period
# - Residual: random variation left over after the trend and seasonal components have been accounted for 


# CELL ********************

plt.close('all') 
plt.figure(figsize=(50,10))
result = seasonal_decompose(df_daily['main_temp'], model='additive', period=20)
result.plot()
plt.show()




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

plt.close('all') 
plt.figure(figsize=(50,10))
result = seasonal_decompose(first_diff['main_temp'], model='additive', period=20)
result.plot()
plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Creating series - for modelling 

temp_series = df_daily['main_temp']


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Code from - https://medium.com/thedeephub/mastering-time-series-forecasting-f49f45855c83

def recommend_model(temp_series, seasonality_threshold=0.3, acf_lag=60, stationarity_threshold=0.05, ma_threshold=0.5, ar_threshold=0.5):
    """
    Analyzes the given time series data to recommend AR, MA, ARMA, ARIMA, or SARIMA.
    :param time_series: A Pandas Series with datetime index.
    :param seasonality_threshold: Threshold to decide significant seasonality.
    :param acf_lag: Number of lags to use for autocorrelation and partial autocorrelation Test.
    :param stationarity_threshold: P-value threshold for stationarity test.
    :param ma_threshold: Threshold for significant autocorrelation at lag 1.
    :param ar_threshold: Threshold for significant partial autocorrelation.
    :return: Recommendation string.
    """
    # Step 1: Seasonal Decomposition
    result = seasonal_decompose(temp_series, model='additive', period=acf_lag)
    seasonal_std = result.seasonal.std()

    # Step 2: Test for Stationarity
    dftest = adfuller(temp_series, autolag='AIC')
    p_value = dftest[1]

    # Step 3: Autocorrelation and Partial Autocorrelation Analysis
    lag_acf = acf(temp_series, nlags=acf_lag)
    lag_pacf = pacf(temp_series, nlags=acf_lag)

    # Step 4: Recommendations
    if seasonal_std > seasonality_threshold or any(abs(lag_acf[1:]) > 0.5):
        return "SARIMA"
    elif p_value < stationarity_threshold:
        if all(abs(lag_pacf[2:]) < ar_threshold) and all(abs(lag_acf[2:]) < ma_threshold):
            if abs(lag_pacf[1]) > ar_threshold:
                return "AR"
            elif abs(lag_acf[1]) > ma_threshold:
                return "MA"
            else:
                return "ARMA"
    else:
        return "ARIMA"

recommendation = recommend_model(temp_series)

# Print the recommendation
print(f"Recommended Model: {recommendation}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### SARIMA model - seasonal auto-regressive integrated moving average

# CELL ********************

# Split dataset - train & test 

size = int(len(temp_series)*0.70)
Xtrain, Xtest = temp_series[0:size], temp_series[size:]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Xtrain.head()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Xtest.head()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

plt.figure()
Xtrain.plot()
Xtest.plot()
plt.title('Visual of train vs test set')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Model Training 
mod = sm.tsa.statespace.SARIMAX(Xtrain,
                                order=(1, 0, 1),
                                seasonal_order=(0, 1, 1, 12),
                                enforce_stationarity=False,
                                enforce_invertibility=False)
results = mod.fit(disp=False)
print(results.summary().tables[1])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_predict = pd.DataFrame(results.predict(start=len(Xtrain), end=len(temp_series)))
final_predict.columns = ['pred']
final_predict.head()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

predict_actual = pd.concat((final_predict,Xtest), axis=1)
predict_actual.head()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

plt.figure(figsize=(10,8))
plt.plot(predict_actual.index, predict_actual['pred'], color = 'red', label = 'prediction')
plt.plot(temp_series.index, temp_series, color = 'blue', label = 'actual')
plt.plot(pd.DataFrame(results.predict(start = len(temp_series), end = len(temp_series) + 36)), color = 'yellow', label = 'forecast')
plt.legend()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************


# MARKDOWN ********************

# # ML model - on london dataset (2021-2024)

# CELL ********************

# Load dataset
ldn_df = spark.table("ldn_weather_final").toPandas()
ldn_df.set_index(pd.to_datetime(ldn_df['dt']), inplace=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Removing multiple inputs a day 
ldn_df_resampled = ldn_df[['temp', 'temp_feelslike', 'temp_max', 'temp_min']].resample('D').mean()

# Monthly data

lnd_monthly = ldn_df[['temp', 'temp_feelslike', 'temp_max', 'temp_min']].resample('M').mean()

pd.options.display.float_format = '{:.2f}'.format






# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ldn_df_resampled.head()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

lnd_monthly.head()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sns.set_style('whitegrid')
sns.lineplot(x='dt', y='temp', data=ldn_df_resampled).set(title='temperature')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sns.set_style('whitegrid')
sns.lineplot(x='dt', y='temp', data=lnd_monthly).set(title='temperature')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ADF test - stationary 

result = adfuller(lnd_monthly['temp'])

# print the test statistic, p-value, and other relevant information
print('ADF Statistic:', result[0])
print('p-value:', result[1])
print('Critical Values:')
for key, value in result[4].items():
    print(key, ':', value)

# if the p-value is less than 0.05, we reject the null hypothesis and conclude that the time series is stationary
if result[1] < 0.05:
    print('The time series is likely stationary.')
else:
    print('The time series is likely non-stationary.')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ADF test - stationary 

result = adfuller(ldn_df_resampled['temp'])

# print the test statistic, p-value, and other relevant information
print('ADF Statistic:', result[0])
print('p-value:', result[1])
print('Critical Values:')
for key, value in result[4].items():
    print(key, ':', value)

# if the p-value is less than 0.05, we reject the null hypothesis and conclude that the time series is stationary
if result[1] < 0.05:
    print('The time series is likely stationary.')
else:
    print('The time series is likely non-stationary.')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### - not stationary = need to take first difference

# CELL ********************

fig, axs = plt.subplots(1, 2, figsize=(20, 5))

plot_acf(ldn_df_resampled['temp'], lags=10, title='ACF of temperature - MA terms', ax=axs[0])
axs[0].set_xlabel('nth Lags')
axs[0].set_ylabel('ACF')

plot_pacf(ldn_df_resampled['temp'], lags=10, title='PACF of temperature - AR terms', ax=axs[1])
axs[1].set_xlabel('nth Lags')
axs[1].set_ylabel('PCF')

plt.show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# significant: 1 , 3, (5)

# CELL ********************

## difference (by one)
lnd_monthly['temp_diff'] = lnd_monthly['temp'].diff()
lnd_monthly['temp_diff'].fillna(lnd_monthly['temp_diff'].mean(), inplace=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fig, axs = plt.subplots(1, 2, figsize=(20, 5))

plot_acf(lnd_monthly['temp_diff'], lags=10, title='ACF of temperature (temp_diff) - MA terms', ax=axs[0])
axs[0].set_xlabel('nth Lags')
axs[0].set_ylabel('ACF')

plot_pacf(lnd_monthly['temp_diff'], lags=10, title='PACF of temperature (temp_diff) - AR terms', ax=axs[1])
axs[1].set_xlabel('nth Lags')
axs[1].set_ylabel('PCF')

plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

plt.close('all') 
plt.figure(figsize=(50,10))
result = seasonal_decompose(lnd_monthly['temp_diff'], model='additive', period=12)
result.plot()
plt.show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ldn_df_resampled = ldn_df_resampled.reset_index()
print(ldn_df_resampled)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ldn_df_resampled_2 = ldn_df_resampled.sample(n=1000, random_state=None)
print(ldn_df_resampled_2)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ldn_df_resampled_2 = ldn_df_resampled_2.sort_index()
print(ldn_df_resampled_2)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ldn_df_resampled_2 = ldn_df_resampled_2.set_index("dt")
print(ldn_df_resampled_2)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Creating series - for modelling 

ldn_series = ldn_df_resampled['temp']

ldn_series_2 = ldn_df_resampled_2['temp']

#lnd_monthly_series = lnd_monthly['temp_diff']


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Split dataset - train & test 

size = int(len(ldn_series_2)*0.70)
Xldntrain, Xldntest = ldn_series_2[0:size], ldn_series_2[size:]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Xldntrain.head()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Xldntest.head()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

plt.figure()
Xldntrain.plot()
Xldntest.plot()
plt.title('Visual of train vs test set')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Model Training 
mod = sm.tsa.statespace.SARIMAX(Xldntrain,
                                order=(1, 0, 1),
                                seasonal_order=(1, 1, 2, 12),
                                enforce_stationarity=False,
                                enforce_invertibility=False)
ldn_results = mod.fit(disp=False)
print(ldn_results.summary().tables[1])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ldn_final_predict = pd.DataFrame(ldn_results.predict(start=len(Xldntrain), end=len(ldn_series_2)))
ldn_final_predict.colu#mns = ['prediction']
ldn_final_predict.head()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

predict_actual_ldn = pd.concat((ldn_final_predict,Xldntest), axis=1).reset_index()
predict_actual_ldn.rename(columns={"index": "dt"}, inplace=True)
predict_actual_ldn.head()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

last_temp_value = lnd_monthly['temp'].iloc[size - 1]  # The last known temperature before the test set


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# undifference results : 

undiff_predictions = []
for i, pred in enumerate(predict_actual_ldn['prediction']):
    if i == 0:
        # first prediction: add the last known temp value
        undiff_value = last_temp_value + pred
    else:
        # next predictions: add the differenced value to the previous undifferenced prediction
        undiff_value = undiff_predictions[-1] + pred
    undiff_predictions.append(undiff_value)

# add the undifferenced predictions to the dataFrame
predict_actual_ldn['undiff_prediction'] = undiff_predictions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

predict_actual_ldn.head()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### - saving model (order=(1, 0, 1), seasonal_order=(0, 1, 1, 12)) - predictions and actuals as its own table (for reporting)

# CELL ********************

model_1 = spark.createDataFrame(predict_actual_ldn)

model_1 = model_1.withColumn("dt", col("dt").cast(DateType())) 
model_1 = model_1.withColumn("prediction", col("prediction").cast(FloatType())) 
model_1 = model_1.withColumn("temp_diff", col("temp_diff").cast(FloatType())) 
model_1 = model_1.withColumn("undiff_prediction", col("undiff_prediction").cast(FloatType())) 

#model_1.write.format("delta").mode("overwrite").saveAsTable("ldn_model_1_results")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

model_1.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("ldn_model_1_results")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

plt.plot(predict_actual_ldn['dt'], predict_actual_ldn['undiff_prediction'], color='red', label='Undifferenced Prediction')
plt.plot(lnd_monthly.index[size:], lnd_monthly['temp'][size:], color='blue', label='Actual Temperature')

plt.xlabel('Date')
plt.ylabel('Temperature')
plt.title('Actual vs Predicted Temperatures (Undifferenced)')
plt.legend()
plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

plt.figure(figsize=(10,8))
plt.plot(predict_actual_ldn.index, predict_actual_ldn['pred'], color = 'red', label = 'prediction')
plt.plot(lnd_monthly_series.index, lnd_monthly_series, color = 'blue', label = 'actual')
plt.plot(pd.DataFrame(ldn_results.predict(start = len(lnd_monthly_series), end = len(lnd_monthly_series) + 36)), color = 'yellow', label = 'forecast')
plt.legend()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Creating loop to store results in table 

# orders and seasonal orders
orders = [(1, 0, 1), (1, 1, 1), (2, 0, 1), (2, 1, 1)]
seasonal_orders = [(0, 1, 1, 12), (1, 1, 1, 12), (0, 1, 2, 12), (1, 1, 2, 12)]

results = []
model_id_counter = 1  # counter to increase by 1 for each model

for order in orders:
    for seasonal_order in seasonal_orders:
        
        mod = sm.tsa.statespace.SARIMAX(Xldntrain, order=order, seasonal_order=seasonal_order, enforce_stationarity=False, enforce_invertibility=False)
        ldn_results = mod.fit(disp=False)

        ldn_final_predict = ldn_results.predict(start=len(Xldntrain), end=len(lnd_monthly_series))

        # Mean Squared Error (MSE)
        mse = np.mean((ldn_final_predict - lnd_monthly['temp']) ** 2)

        model_id = model_id_counter 

        # Create a datetime index for the predictions
        dt_index = pd.date_range(start=Xldntrain.index[-1] + pd.Timedelta(days=1), periods=len(ldn_final_predict), freq='M')

        for i, (dt, pred) in enumerate(zip(dt_index, ldn_final_predict)):  # iterator that pairs up the elements from dt_index and ldn_final_predict
            result = {                                                     # enumerate function adds a counter
                'dt': dt,
                'model_id': model_id,
                'order': order,
                'seasonal_order': seasonal_order,
                'mse': mse,
                'prediction': pred,
                'actual': lnd_monthly_series[i]
            }
            results.append(result)

        model_id_counter += 1  

df = pd.DataFrame(results)

spark_df = spark.createDataFrame(df)

spark_df.write.format("delta") \
    .option("overwriteSchema", "true") \
    .mode("overwrite") \
    .saveAsTable("LDN_weather_results")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

### full model (for easy comparison): - change all the way through the code !!

ldn_series = ldn_df_resampled['temp'] # daily 
ldn_series_2 = ldn_df_resampled_2['temp'] # 1000 rows
lnd_monthly_series = lnd_monthly['temp_diff'] # monthly


# Split dataset - train & test 

size = int(len(ldn_series_2)*0.70)
Xldntrain, Xldntest = ldn_series_2[0:size], ldn_series_2[size:]

#######################
plt.figure()
Xldntrain.plot()
Xldntest.plot()
plt.title('Visual of train vs test set')
plt.close()
#######################

# Model Training 
mod = sm.tsa.statespace.SARIMAX(Xldntrain,
                                order=(1, 0, 1),
                                seasonal_order=(1, 1, 2, 12),
                                enforce_stationarity=False,
                                enforce_invertibility=False)
ldn_results = mod.fit(disp=False)
print(ldn_results.summary().tables[1])

ldn_final_predict = pd.DataFrame(ldn_results.predict(start=len(Xldntrain), end=len(ldn_series_2)))
ldn_final_predict.columns = ['prediction']

predict_actual_ldn = pd.concat((ldn_final_predict,Xldntest), axis=1).reset_index()
predict_actual_ldn.rename(columns={"index": "dt"}, inplace=True)

## USE IF USING DIFFERENCED TEMP ##
#last_temp_value = lnd_monthly['temp'].iloc[size - 1]  
#undiff_predictions = []
#for i, pred in enumerate(predict_actual_ldn['prediction']):
##    if i == 0:
#        undiff_value = last_temp_value + pred
#    else:
#        undiff_value = undiff_predictions[-1] + pred
#    undiff_predictions.append(undiff_value)
#
#predict_actual_ldn['undiff_prediction'] = undiff_predictions
############################################

predict_actual_ldn.head()

plt.figure(figsize=(10,8))
plt.plot(predict_actual_ldn['dt'], predict_actual_ldn['prediction'], color = 'red', label = 'prediction')
plt.plot(ldn_series_2.index[size:], ldn_series_2[size:], color = 'blue', label = 'actual')
plt.plot(pd.DataFrame(ldn_results.predict(start = len(ldn_series_2), end = len(ldn_series_2) + 36)), color = 'yellow', label = 'forecast')
plt.legend()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
