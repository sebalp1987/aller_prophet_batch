import pandas as pd
from resources import STRING
from googleads import ad_manager, errors
import tempfile
import os
import _locale
import gzip
from google.cloud import storage

_locale._getdefaultlocale = (lambda *args: ['en_US', 'UTF-8'])


class EtlAds:
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)

    def __init__(self):
        _storage_client = storage.Client.from_service_account_json(STRING.credentials)
        self.filter_ad_unit = STRING.filter_unit
        self._bucket = _storage_client.get_bucket('aller_prophet')

    def run(self):
        df, df_db = self._extract()
        df = self._transform(df, df_db)
        df = self._load(df)
        return df

    def _extract(self):

        blob = self._bucket.blob(STRING.input_db)
        blob.download_to_filename(STRING.input_db)
        df_db = pd.read_csv(STRING.input_db, sep=',', index_col=0)
        df_db['date'] = pd.to_datetime(df_db['date'], format="%Y-%m-%d")
        ad_manager_client = ad_manager.AdManagerClient.LoadFromStorage(STRING.GOOGLEADS_YAML_FILE)

        # Create statement object to filter for an order.
        statement = (ad_manager.StatementBuilder(version='v201911')
                     .Limit(None)  # No limit or offset for reports
                     .Offset(None))

        # Create report job.
        report_job = {
            'reportQuery': {
                'dimensions': ['MONTH_AND_YEAR', 'AD_UNIT_NAME'],
                'adUnitView': 'TOP_LEVEL',
                'columns': ['TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS', 'TOTAL_LINE_ITEM_LEVEL_CLICKS',
                            'TOTAL_LINE_ITEM_LEVEL_CPM_AND_CPC_REVENUE', 'TOTAL_LINE_ITEM_LEVEL_ALL_REVENUE',
                            'TOTAL_LINE_ITEM_LEVEL_WITHOUT_CPD_AVERAGE_ECPM',
                            'TOTAL_LINE_ITEM_LEVEL_CTR'],
                'dateRangeType': 'LAST_3_MONTHS',
                'statement': statement.ToStatement()
            }
        }

        report_downloader = ad_manager_client.GetDataDownloader(version='v201911')
        try:
            # Run the report and wait for it to finish.
            report_job_id = report_downloader.WaitForReport(report_job)
        except errors.AdManagerReportError as e:
            print('Failed to generate report. Error was: %s' % e)

        report_file = tempfile.NamedTemporaryFile(suffix='.xlsx.gz', delete=False)

        # Download report data.
        report_downloader.DownloadReportToFile(
            report_job_id, 'XLSX', report_file)
        report_file.close()

        # Display results.
        with gzip.open(report_file.name) as f:
            df = pd.read_excel(f)

        os.unlink(report_file.name)

        return df, df_db

    def _transform(self, df, df_db):
        del df['Ad unit ID']

        # Filter Ad Units
        df = df[df['Ad unit'].isin(self.filter_ad_unit)]

        # Change to datetime
        df['Month and year'] = df['Month and year'].str.capitalize()
        df['date'] = pd.to_datetime(df['Month and year'], format='%B %Y')
        del df['Month and year']

        date_rep = df['date'].unique()
        df_db = df_db[-df_db['date'].isin(date_rep)]

        df = pd.concat([df_db, df], axis=0)

        return df

    def _load(self, df):
        self._bucket.blob(STRING.input_db).upload_from_string(df.to_csv(), 'text/csv')
        df = df[df['Ad unit'].isin(self.filter_ad_unit)]
        return df


if __name__ == '__main__':
    EtlAds().run()

