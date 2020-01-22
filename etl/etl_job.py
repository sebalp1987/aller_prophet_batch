import pandas as pd
from resources import STRING
from datetime import datetime, timedelta
from googleads import ad_manager, oauth2, errors
from dateutil.relativedelta import relativedelta
import tempfile
import os

class EtlAds:
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)

    def __init__(self, input_file, output_file):
        self.input_file = input_file
        self.output_file = output_file
        self.filter_ad_unit = ['dagbladet.no', 'Aller_webtv', 'dagbladet_plus_tv',
                               'dinside', 'lommelegen', 'seher', 'sol']

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
        statement = (ad_manager.StatementBuilder(version='v201911')
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
        df = pd.read_csv(report_file.name)
        df.columns = ['date', 'ad_unit_name', 'ad_unit_id', 'Total impressions', 'Total clicks',
                      'Total CPM and CPC revenue (NOK)',
                      'Total CPM, CPC, CPD, and vCPM revenue (NOK)',
                      'Total average eCPM (NOK)', 'Total CTR'
                      ]

        os.unlink(report_file.name)

        return df

    def _transform(self, df):
        del df['ad_unit_id']

        # Filter Ad Units
        print(df)
        df = df[df['ad_unit_name'].isin(self.filter_ad_unit)]

        # Datetime Format
        df['date'] = pd.to_datetime(df['date'], format='%Y-%m')
        print(df)

        # Group by date (TARGET)
        df = df.groupby(['ad_unit_name', 'date']).agg({'Total impressions': 'sum', 'Total clicks': 'sum',
                                                       'Total CPM and CPC revenue (NOK)': 'sum',
                                                       'Total CPM, CPC, CPD, and vCPM revenue (NOK)': 'sum',
                                                       'Total average eCPM (NOK)': 'mean', 'Total CTR': 'mean'})

        return df

    def _load(self, df):
        df.to_csv(self.output_file, sep=';', index=False)


if __name__ == '__main__':
    EtlAds(input_file=STRING.file_dagbladet, output_file=STRING.proc_output).run()
