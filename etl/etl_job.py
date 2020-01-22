import pandas as pd
from resources import STRING
from datetime import datetime, timedelta
from googleads import ad_manager, oauth2, errors
from dateutil.relativedelta import relativedelta
import tempfile

class EtlAds:
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)

    def __init__(self, input_file, output_file):
        self.input_file = input_file
        self.output_file = output_file

    def run(self):
        df = self._extract()
        df = self._transform(df)
        self._load(df)

    def _extract(self):
        df = pd.read_csv(self.input_file, sep=';')

        ad_manager_client = ad_manager.AdManagerClient.LoadFromStorage(STRING.GOOGLEADS_YAML_FILE)
        network_service = ad_manager_client.GetService('NetworkService')
        current_network = network_service.getCurrentNetwork()

        root_ad_unit_id = (
            network_service.getCurrentNetwork()['effectiveRootAdUnitId'])
        print(root_ad_unit_id)

        # Create statement object to filter for an order.
        statement = (ad_manager.StatementBuilder(version='v201905')
                     .Limit(None)  # No limit or offset for reports
                     .Offset(None))

        # Set the start and end dates of the report to run
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=365)
        print(end_date)
        print(start_date)
        # Create report job.
        report_job = {
            'reportQuery': {
                'dimensions': ['DATE', 'AD_UNIT_NAME'],
                'adUnitView': 'TOP_LEVEL',
                'columns': ['AD_SERVER_IMPRESSIONS'],
                'dateRangeType': 'LAST_3_MONTHS',
                'statement': statement.ToStatement()
            }
        }

        report_downloader = ad_manager_client.GetDataDownloader(version='v201908')
        try:
            # Run the report and wait for it to finish.
            report_job_id = report_downloader.WaitForReport(report_job)
            print(report_job_id)
        except errors.AdManagerReportError as e:
            print('Failed to generate report. Error was: %s' % e)

        export_format = 'CSV_DUMP'

        report_file = tempfile.NamedTemporaryFile(suffix='.csv.gz', delete=False)

        # Download report data.
        report_downloader.DownloadReportToFile(
            report_job_id, export_format, report_file)

        report_file.close()

        # Display results.
        print('Report job with id "%s" downloaded to:\n%s' % (
            report_job_id, report_file.name))

        return df

    @staticmethod
    def _transform(df):

        del df['Ad unit'], df['Ad unit ID']

        # Change to datetime
        df['Month and year'] = df['Month and year'].str.capitalize()
        df['date'] = pd.to_datetime(df['Month and year'], format='%B %Y')
        del df['Month and year']

        # Remove NOK
        for col in ['Total CPM and CPC revenue (NOK)', 'Total CPM, CPC, CPD, and vCPM revenue (NOK)',
                    'Total average eCPM (NOK)', 'Total CTR', 'AdSense revenue (NOK)', 'Ad Exchange revenue (NOK)']:
            df[col] = df[col].map(str)
            df[col] = df[col].replace('NOK', '', regex=True)
            df[col] = df[col].replace('%', '', regex=True)
            df[col] = df[col].replace(' ', '', regex=True)
            df[col] = df[col].map(float)

        # Group by date (TARGET)
        df_target = df.groupby('date').agg({'Total impressions': 'sum', 'Total clicks': 'sum',
                                            'Total CPM and CPC revenue (NOK)': 'sum',
                                            'Total CPM, CPC, CPD, and vCPM revenue (NOK)': 'sum',
                                            'Total average eCPM (NOK)': 'mean', 'Total CTR': 'mean',
                                            'AdSense revenue (NOK)': 'sum', 'Ad Exchange revenue (NOK)': 'sum'})

        # Convert Panel to time series
        line_list = df['Line item type'].unique()
        for val in line_list:
            print(val)
            df_i = df[df['Line item type'] == val]
            df_i = df_i.drop('Line item type', axis=1)
            df_i = df_i.rename(columns = dict([(x, x + val) for x in df.columns if x not in ['date']]))

            df_target = pd.merge(df_target, df_i, how='left', on='date')

        # Time series variables
        df_target = df_target.sort_values('date', ascending=True).reset_index(drop=True).reset_index(drop=False)
        df_target = df_target.rename(columns={'index': 'TREND'})
        df_target['MONTH'] = df['date'].dt.month
        print(df_target)
        df_target = df_target.fillna(0)

        dummy = pd.get_dummies(df_target['MONTH'], prefix='month')
        df_target = pd.concat([df_target, dummy], axis=1)

        return df_target

    def _load(self, df):
        df.to_csv(self.output_file, sep=';', index=False)


if __name__ == '__main__':
    EtlAds(input_file=STRING.file_dagbladet, output_file=STRING.proc_dagbladet)._extract()