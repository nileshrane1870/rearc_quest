import requests
import boto3
import json

# Part 2: APIs

def fetch_population_data():
    """
    Fetches population data from the DataUSA API.
    """
    url = "https://honolulu-api.datausa.io/tesseract/data.jsonrecords?cube=acs_yg_total_population_1&drilldowns=Year%2CNation&locale=en&measures=Population"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def save_to_s3(bucket_name, file_name, data):
    """
    Saves data to a file in an S3 bucket.
    """
    s3 = boto3.client('s3')
    print(f"Uploading {file_name} to S3 bucket {bucket_name}...")
    s3.put_object(
        Bucket=bucket_name,
        Key=file_name,
        Body=json.dumps(data, indent=4)
    )
