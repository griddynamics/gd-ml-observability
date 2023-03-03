import boto3


class CloudWatchClient():

    def __init__(self) -> None:
        self.client = boto3.client('cloudwatch', region_name='us-east-1')

    def put_metric_data(self, namespace, metric_name, dimensions: list,
                        timestamp, value, unit='None'):
        response = self.client.put_metric_data(
            Namespace=namespace,
            MetricData=[
                {
                    'MetricName': metric_name,
                    'Dimensions': dimensions,
                    'Timestamp': timestamp,
                    'Value': value,
                    'Unit': unit,
                },
            ]
        )
