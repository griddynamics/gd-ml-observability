import json
import logging
import os
from datetime import datetime, timedelta

from estimator import Estimator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


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


def handler(event, context):
    logger.info('Start')
    estimator.run(end_datetime=datetime(2023, 2, 23, 16, 47, 18))
    return "Done"
    
if __name__=='__main__':
    handler('','')