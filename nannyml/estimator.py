import json
from datetime import datetime, timedelta
from io import StringIO
import os
import nannyml as nml
import pandas as pd
import s3fs
from cloud_watch_client import CloudWatchClient


class Estimator():

    def __init__(self,
                 estimator_root_path,
                 estimator_path,
                 result_cols,
                 cw_namespace,
                 metric_dimensions,
                 capture_path,
                 headers,
                 timestamp_column_name,
                 time_delta: timedelta,
                 ) -> None:
        self.cloud_watch_client = CloudWatchClient()
        self.estimator_root_path = estimator_root_path
        self.estimator_path = estimator_path
        self.result_cols = result_cols
        self.cw_namespace = cw_namespace
        self.metric_dimensions = metric_dimensions
        self.capture_path = capture_path
        self.headers = headers
        self.timestamp_column_name = timestamp_column_name
        self.time_delta = time_delta

    @staticmethod
    def load_model(root_path: str, path: str, as_type):
        store = nml.io.store.FilesystemStore(root_path=root_path)
        return store.load(path=path, as_type=as_type)

    @staticmethod
    def store_model(estimator, root_path: str, path: str):
        store = nml.io.store.FilesystemStore(root_path=root_path)
        store.store(estimator, path=path)

    @staticmethod
    def read_s3file(path: str):
        fs = s3fs.S3FileSystem()
        csv_data = []
        with fs.open(path) as f:
            for line in f:
                json_line = json.loads(line)
                csv_data.append(','.join([
                    json_line['captureData']['endpointInput']['data'],
                    json_line['captureData']['endpointOutput']['data'],
                    json_line['eventMetadata']['inferenceTime'],
                ]))
        print(path, len(csv_data))
        return csv_data

    @staticmethod
    def create_df(csv_data: list, headers: list, timestamp_column_name: str = None,
                  format: str = "%Y-%m-%dT%H:%M:%SZ"):
        csv_strings = '\n'.join(csv_data)
        df = pd.read_csv(StringIO(csv_strings), header=None)
        df.columns = headers
        if timestamp_column_name:
            df[timestamp_column_name] = pd.to_datetime(
                df[timestamp_column_name], format=format)
        return df

    @staticmethod
    def get_date_from_path(path: str):
        path_parts = path.split('/')
        year = int(path_parts[-5])
        month = int(path_parts[-4])
        day = int(path_parts[-3])
        hour = int(path_parts[-2])
        minute = int(path_parts[-1].split('-')[0])
        second = int(path_parts[-1].split('-')[1])
        return datetime(year, month, day, hour, minute, second)

    @staticmethod
    def list_files(path: str):
        fs = s3fs.S3FileSystem()
        return fs.find(path)

    def get_file_list(self, path: str, end_datetime: datetime,
                      time_delta: timedelta):
        files = self.list_files(path)
        start_datetime = end_datetime - time_delta
        return [
            path for path in files
            if start_datetime <= self.get_date_from_path(path) <= end_datetime
        ]

    def get_captured_df(self,
                        path: str,
                        end_datetime: datetime,
                        time_delta: timedelta,
                        headers: list,
                        timestamp_column_name: str,
                        ):
        file_list = self.get_file_list(path=path,
                                       end_datetime=end_datetime,
                                       time_delta=time_delta)
        csv_data = []
        for file in file_list:
            csv_data.extend(self.read_s3file(path=file))
        return self.create_df(csv_data=csv_data,
                              headers=headers,
                              timestamp_column_name=timestamp_column_name)

    @staticmethod
    def estimate(estimator, df):
        results = estimator.estimate(df)
        return results.filter(period='analysis').to_df()

    def store_to_cloudwatch(self, df):
        metrics_columns = [
            col for col in df.columns if (col[1] in self.result_cols)]
        df_metrics = df.dropna(subset=metrics_columns)
        for _, series in df_metrics.iterrows():
            timestamp = series[('chunk', 'start_date')]
            for key in series[metrics_columns].keys():
                name = f'{key[0]}.{key[1]}'
                value = series[key]
                if type(value) == bool:
                    value = int(value)
                print(name, value, timestamp)
                self.cloud_watch_client.put_metric_data(
                    namespace=self.cw_namespace,
                    metric_name=name,
                    dimensions=self.metric_dimensions,
                    timestamp=timestamp,
                    value=value,
                )

    def run(self, end_datetime: datetime):
        estimator = self.load_model(
            root_path=self.estimator_root_path,
            path=self.estimator_path,
            as_type=nml.DLE,
        )
        df = self.get_captured_df(
            path=self.capture_path,
            end_datetime=end_datetime,
            time_delta=self.time_delta,
            headers=self.headers,
            timestamp_column_name=self.timestamp_column_name,
        )
        result_df = self.estimate(estimator=estimator, df=df)
        self.store_to_cloudwatch(df=result_df)


def set_env_test():
    headers = '["amt", "oft", "amount_365_days_lag", "off_365_days_lag", "black_friday", "business_day", "cyber_monday", "day_of_week", "day_of_month", "month", "is_holiday", "list_price", "list_price_7_days_lag", "list_price_30_days_lag", "list_price_365_days_lag", "post_holiday", "pre_holiday", "profit_365_days_lag", "quantity_365_days_lag", "promo_price", "promo_price_7_days_lag", "promo_price_30_days_lag", "promo_price_365_days_lag", "purchase_price", "ratio_list_price_to_purchase_price", "ratio_list_price_to_purchase_price_7_days_lag", "ratio_list_price_to_purchase_price_30_days_lag", "ratio_list_price_to_purchase_price_365_days_lag", "ratio_promo_price_to_list_price", "ratio_promo_price_to_list_price_7_days_lag", "ratio_promo_price_to_list_price_30_days_lag", "ratio_promo_price_to_list_price_365_days_lag", "ratio_promo_price_to_purchase_price", "ratio_promo_price_to_purchase_price_7_days_lag", "ratio_promo_price_to_purchase_price_30_days_lag", "ratio_promo_price_to_purchase_price_365_days_lag", "super_bowl", "season_of_year_one_hot_0", "season_of_year_one_hot_1", "season_of_year_one_hot_2", "season_of_year_one_hot_3", "occasion_one_hot_0", "occasion_one_hot_1", "occasion_one_hot_2", "occasion_one_hot_3", "occasion_one_hot_4", "occasion_one_hot_5", "occasion_one_hot_6", "occasion_one_hot_7", "dress_length_one_hot_0", "dress_length_one_hot_1", "dress_length_one_hot_2", "dress_length_one_hot_3", "dress_types_one_hot_0", "dress_types_one_hot_1", "dress_types_one_hot_2", "dress_types_one_hot_3", "dress_types_one_hot_4", "dress_types_one_hot_5", "dress_types_one_hot_6", "dress_types_one_hot_7", "dress_types_one_hot_8", "dress_types_one_hot_9", "dress_types_one_hot_10", "dress_types_one_hot_11", "material_one_hot_0", "material_one_hot_1", "material_one_hot_2", "material_one_hot_3", "material_one_hot_4", "material_one_hot_5", "material_one_hot_6", "material_one_hot_7", "material_one_hot_8", "material_one_hot_9", "material_one_hot_10", "neckline_style_one_hot_0", "neckline_style_one_hot_1", "neckline_style_one_hot_2", "neckline_style_one_hot_3", "neckline_style_one_hot_4", "neckline_style_one_hot_5", "neckline_style_one_hot_6", "neckline_style_one_hot_7", "color_category_one_hot_0", "color_category_one_hot_1", "color_category_one_hot_2", "color_category_one_hot_3", "color_category_one_hot_4", "color_category_one_hot_5", "color_category_one_hot_6", "color_category_one_hot_7", "color_category_one_hot_8", "color_category_one_hot_9", "color_category_one_hot_10", "color_category_one_hot_11", "color_category_one_hot_12", "color_category_one_hot_13", "color_category_one_hot_14", "color_category_one_hot_15", "size_one_hot_0", "size_one_hot_1", "size_one_hot_2", "size_one_hot_3", "size_one_hot_4", "size_one_hot_5", "size_one_hot_6", "size_one_hot_7", "size_one_hot_8", "size_one_hot_9", "y_pred", "timestamp"]'
    estimator_root_path = 's3://kop-ml-datasets/nannyml/estimators/'
    estimator_path = 'dle.pkl'
    timestamp_column_name = 'timestamp'
    capture_path = 's3://sagemaker-us-east-1-125667932402/sagemaker/nannyml-preformance-monitoring-promo-planning/datacapture/nannyml-preformance-monitoring-promo-planning-2023-02-23-1641'
    delta_minutes = '3'
    os.environ['headers'] = headers
    os.environ['estimator_root_path'] = estimator_root_path
    os.environ['estimator_path'] = estimator_path
    os.environ['timestamp_column_name'] = timestamp_column_name
    os.environ['capture_path'] = capture_path
    os.environ['delta_minutes'] = delta_minutes


if __name__ == '__main__':
    set_env_test()
    result_cols = ['value', 'alert']
    cw_namespace = 'nannyml/ModelMonitoring/test'
    metric_dimensions = [{'Name': 'MonitoringSchedule',
                          'Value': 'schedule_0'}]

    estimator_root_path = os.getenv('estimator_root_path')
    estimator_path = os.getenv('estimator_path')
    capture_path = os.getenv('capture_path')
    headers: list = json.loads(os.getenv('headers'))
    timestamp_column_name = os.getenv('timestamp_column_name')
    delta_minutes = int(os.getenv('delta_minutes'))
    time_delta = timedelta(minutes=delta_minutes)

    estimator = Estimator(
        estimator_root_path=estimator_root_path,
        estimator_path=estimator_path,
        result_cols=result_cols,
        cw_namespace=cw_namespace,
        metric_dimensions=metric_dimensions,
        capture_path=capture_path,
        headers=headers,
        timestamp_column_name=timestamp_column_name,
        time_delta=time_delta
    )
    estimator.run(end_datetime=datetime(2023, 2, 23, 16, 47, 18))
