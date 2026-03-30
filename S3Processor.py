import boto3
import os
import botocore
from botocore.config import Config
#from dotenv import load_dotenv
#load_dotenv()

class S3Processor():
    """
    Klasa do obsługi bucketa S3.
    """

    session = None
    client = None

    def __init__(self, akid="", sak="", endpoint="", region=""):
        """Constructor for S3Processor"""
        try:
            final_akid = ""
            final_sak = ""
            final_endpoint = ""
            final_region = ""

            cfg = self.get_env()
            #print(cfg)
            
            if akid=="":
                final_akid=cfg['AWS_AKID']
            else:
                final_akid = akid

            if sak=="":
                final_sak=cfg['AWS_SAK']
            else:
                final_sak = sak

            if endpoint=="":
                final_endpoint=cfg['S3_ENDPOINT']
            else:
                final_endpoint = endpoint

            if region=="":
                final_region=cfg['AWS_REG']
                if final_region == 'None':
                    final_region=""
            else:
                final_region = region
                

            self.openS3Bucket(final_akid, final_sak, endpoint=final_endpoint, region=final_region)
        except Exception as e:
            print(f"Error: {e}")

    def __del__(self):
        self.closeS3Bucket()

    def get_env(self):
        return {'AWS_AKID':str(os.getenv("AWS_AKID")),'AWS_SAK':str(os.getenv("AWS_SAK")),'AWS_REG':str(os.getenv("AWS_REG")),'S3_ENDPOINT': str(os.getenv("S3_ENDPOINT"))}


    def openS3Bucket(self, akid, sak, endpoint="", region=""):
        try:
            self.session = boto3.session.Session()

            # https://stackoverflow.com/questions/63557500/botocore-exceptions-sslerror-ssl-validation-failed-on-windows
            if region == "":
                self.client = self.session.client(
                    service_name='s3',
                    aws_access_key_id=akid,
                    aws_secret_access_key=sak,
                    endpoint_url=endpoint,
                    # config=botocore.client.Config(proxies={"http": "10.25.33.17:9400"}),
                    verify=False)
            else:
                proxy_vwp = 'http://'+str(os.getenv("PROXY_USERNAME"))+':'+str(os.getenv("PROXY_PASSWORD"))+'@'+str(os.getenv("PROXY_HOST"))+':'+str(os.getenv("PROXY_PORT"))
#                config=botocore.client.Config(proxies={"http": "10.25.33.17:9400"}),
                self.client = self.session.client(
                    service_name='s3',
                    aws_access_key_id=akid,
                    aws_secret_access_key=sak,
                    region_name=region,
                    config=botocore.client.Config(proxies={"https": proxy_vwp}),
                    verify=False)
            return True
        except Exception as e:
            print("Błąd podczas tworzenia połączenia z S3!")
            print(e)
            return False

    def closeS3Bucket(self):
        try:
            self.client.close()
        except:
            return False

    def put_file(self, selected_bucket,local_path,remote_path):
        try:
            self.client.upload_file(local_path, selected_bucket, remote_path)
            return True
        except botocore.exceptions.ClientError as error:
            print(error)
            return False

    def put(self, selected_bucket,object_key,object_body):
        try:
            response = self.client.put_object(Bucket=selected_bucket, Key=object_key,Body=object_body)

            return response
        #['Body'].read().decode('utf-8')
        except botocore.exceptions.ClientError as error:
            print("ERROR:")
            print(error)
            return False

    def get(self, selected_bucket,object_key):
        try:
            response = self.client.get_object(Bucket=selected_bucket, Key=object_key)

            return response['Body'].read().decode('utf-8')
        except botocore.exceptions.ClientError as error:
            print(error)
            return False
        
    def delete(self, selected_bucket,object_key):
        try:
            response = self.client.delete_object(Bucket=selected_bucket, Key=object_key)
            return response
        except botocore.exceptions.ClientError as error:
            print(error)
            return False

    def list(self, selected_bucket,prefix="",fragment="",json=False):
        result = []
        #    print(cnf)
        try:
            #        response = client.list_objects_v2(Bucket=selected_bucket,Prefix = prefix) # , Prefix="testo"
            #        objects = wr.s3.list_objects('s3://myBucket/raw/client/Hist/2017/*/*/Tracking_*.zip')

            paginator = self.client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=selected_bucket,Prefix = prefix)
            total_size = 0
            for page in pages:
              if 'Contents' in page:
                for ob in page['Contents']:
                    if json==True:
                        if fragment=="":
                            result.append(ob["Key"])
                        elif fragment in ob["Key"]:
                            result.append(ob["Key"])
                    else:
                        print(ob["Key"]+", size: " + str(ob["Size"])+", storageClass: " + ob["StorageClass"])

                    total_size += ob['Size']

            # print(f"Total size of a bucket (prefix='{prefix}'): {round(total_size/1024/1024/1024, 2)} GB")

            if json==True:
                return result

            return True
        except botocore.exceptions.ClientError as error:
            print(error)
            return False
        
    def list_buckets(self):
        print(self.client.list_buckets())

    def check_if_exists(self, bucket, key):
        try:
            self.client.head_object(Bucket=bucket, Key=key)
            return True
        except Exception as e:
            return False