import pandas as pd
from fbprophet import Prophet
from statsmodels.tsa.seasonal import seasonal_decompose
from datetime import timedelta
import numpy as np
from resources import STRING
from google.cloud import storage


class AlllerProphet:
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)

    def __init__(self, freq='M', period=3):
        _storage_client = storage.Client.from_service_account_json(STRING.credentials)
        self.freq = freq
        self.period = period
        self._bucket = _storage_client.get_bucket('aller_prophet')
        
    def forecast(self, df):

        for tgt in df.drop(['Ad unit', 'date'], axis=1).columns:
            print(tgt)
            df_ = df[['date', 'Ad unit', tgt]]
            df_tgt = pd.DataFrame()
            for ad_unit in df_['Ad unit'].unique():
                print(ad_unit)
                df_i = df_[df_['Ad unit'] == ad_unit]

                # Decompose
                df_i = df_i.sort_values(by='date', ascending=True)
                df_i = df_i.set_index(df_i['date'])
                result_add = seasonal_decompose(df_i[tgt], model='additive', extrapolate_trend='freq')
                trend = result_add.trend
                seasonal = result_add.seasonal
                resid = result_add.resid
                trend_df_i = pd.concat([trend, seasonal, resid], axis=1)
                trend_df_i.columns = ['trend', 'seasonal', 'resid']
                trend_df_i = trend_df_i.reset_index(drop=True)
                df_i = df_i.reset_index(drop=True)

                # Prophet
                df_i_ph = df_i[['date', tgt]]
                df_i_ph.columns = ['ds', 'y']

                model_ph = Prophet(yearly_seasonality=True, weekly_seasonality=False, daily_seasonality=False,
                                   changepoint_prior_scale=0.1, seasonality_mode='additive',
                                   seasonality_prior_scale=0.1)
                model_ph.fit(df_i_ph)
                future = model_ph.make_future_dataframe(periods=self.period, freq=self.freq)
                forecast_ph = model_ph.predict(future)
                forecast_ph = forecast_ph[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail(self.period)
                forecast_ph.columns = ['date', tgt, 'yhat_lower', 'yhat_upper']
                forecast_ph['date'] = forecast_ph['date'] + timedelta(days=1)

                df_i = pd.concat([df_i, trend_df_i], axis=1)
                df_i = pd.concat([df_i, forecast_ph], axis=0)
                df_i['Ad unit'] = df_i['Ad unit'].fillna(ad_unit)
                df_i['resid'] = df_i['resid'].fillna(0)
                df_i['seasonal'] = df_i['seasonal'].fillna(0)
                df_i['trend'] = df_i['trend'].fillna(0)
                df_i['yhat_lower'] = df_i['yhat_lower'].fillna(df_i[tgt])
                df_i['yhat_upper'] = df_i['yhat_upper'].fillna(df_i[tgt])
                df_i['predict'] = np.where(df_i.index > len(df_i.index) - self.period - 1, 1, 0)
                df_tgt = df_tgt.append(df_i)

            self._bucket.blob('proc_' + tgt + '.csv').upload_from_string(df_tgt.to_csv(), 'text/csv')

AlllerProphet()