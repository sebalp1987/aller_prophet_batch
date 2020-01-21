import pandas as pd
from fbprophet import Prophet
from statsmodels.tsa.seasonal import seasonal_decompose


class AlllerProphet:

    def __init__(self, input_path, target_name):
        self.target_name = target_name
        self.input_path = input_path

    def _load(self):
        df = pd.read_csv(self.input_path, sep=';')
        for i in self.remove_date:
            df = df[df['date'] != i]
        df = df.reset_index(drop=True)

        return df

    def forecast(self):
        df = self._load()
        df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')

        # Decompose
        pd.plotting.register_matplotlib_converters()
        df = df.set_index(df['date'])
        result_add = seasonal_decompose(df[self.target_name], model='additive', extrapolate_trend='freq')
        trend = result_add.trend
        seasonal = result_add.seasonal
        resid = result_add.resid
        trend_df = pd.concat([trend, seasonal, resid], axis=1)
        trend_df.columns = ['trend', 'seasonal', 'resid']

        df = df.reset_index(drop=True)

        df_ph = df[['date', self.target_name]]
        df_ph.columns = ['ds', 'y']

        model_ph = Prophet(yearly_seasonality=True, weekly_seasonality=False, daily_seasonality=False,
                           changepoint_prior_scale=0.1, seasonality_mode='additive',
                           seasonality_prior_scale=0.1)
        model_ph.fit(df_ph)
        future = model_ph.make_future_dataframe(periods=self.period, freq=self.freq)
        forecast_ph = model_ph.predict(future)
        forecast_ph = forecast_ph[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail(self.period)

        return df[self.target_name], forecast_ph, trend_df
