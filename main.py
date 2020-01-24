from etl import etl_job
from model.model_forecast import AlllerProphet


def main():
    df = etl_job.EtlAds().run()
    AlllerProphet().forecast(df)


if __name__ == '__main__':
    main()
