import boto3
import pandas as pd
from botocore.exceptions import NoCredentialsError, KeyError

class BaseProcessor:
    @staticmethod
    def load_data_from_s3(bucket_name, file_key):
        try:
            s3_resource = boto3.resource('s3')
            obj = s3_resource.Object(bucket_name, file_key)
            data = obj.get()['Body'].read()
            return pd.read_parquet(data)
        except (NoCredentialsError, KeyError) as e:
            print(f"Error loading data from S3: {e}")
            return None

    @staticmethod
    def download_parquet_file(bucket_name, file_key, download_path):
        try:
            s3_client = boto3.client('s3')
            s3_client.download_file(bucket_name, file_key, download_path)
            print(f"Downloaded {file_key} to {download_path}")
        except Exception as e:
            print(f"Error downloading file: {e}")

    @staticmethod
    def extract_datetime_from_keys(keys):
        return [pd.to_datetime(key) for key in keys]

    @staticmethod
    def prepare_features(df):
        # Add logic for preparing features from the dataframe
        return df

    @staticmethod
    def check_model_existence(model_id):
        # Implement logic to check if the model exists
        pass

    # Additional shared methods can be added here
