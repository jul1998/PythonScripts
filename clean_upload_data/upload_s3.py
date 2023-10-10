import boto3
from dotenv import load_dotenv
import os


load_dotenv() # Load environment variables

aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID") # Get environment variables
aws_secret_access_key= os.getenv("AWS_SECRET_ACCESS_KEY") # Get environment variables



def upload_to_s3(bucket, file_name, key):

    s3 = boto3.resource(
        service_name='s3',
        region_name='us-east-1',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )
    print("Uploading to s3...")
    s3.Bucket(bucket).upload_file(Filename=file_name, Key=key)