import requests
import boto3
from botocore.exceptions import ClientError
import json
import os
import sys

# Part 2: APIs

# Constants
DATA_USA_URL = "https://honolulu-api.datausa.io/tesseract/data.jsonrecords?cube=acs_yg_total_population_1&drilldowns=Year%2CNation&locale=en&measures=Population"
OUTPUT_FILE_NAME = "population_data.json"


def fetch_population_data(url: str):
    """
    Fetches population data from a given URL.
    """
    print(f"Fetching data from {url}...")
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raises an HTTPError for bad responses (4xx or 5xx)
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from API: {e}", file=sys.stderr)
        sys.exit(1)

def save_to_s3(bucket_name: str, file_name: str, data: dict):
    """
    Saves data to a file in an S3 bucket.
    """
    s3_client = boto3.client('s3')
    print(f"Uploading {file_name} to S3 bucket {bucket_name}...")
    try:
        s3_client.put_object(
            Bucket=bucket_name,
            Key=file_name,
            Body=json.dumps(data, indent=4)
        )
        print(f"Successfully uploaded {file_name} to {bucket_name}.")
    except ClientError as e:
        print(f"Error uploading to S3: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    # It's better practice to get the bucket name from an environment variable.
    S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME", "quest-872180502324-ap-south-1-an")
    if not S3_BUCKET_NAME:
        print("Error: S3_BUCKET_NAME environment variable not set.", file=sys.stderr)
        print("Please set it to your S3 bucket name before running.", file=sys.stderr)
        # Example (Linux/macOS): export S3_BUCKET_NAME='your-bucket-name'
        # Example (Windows): set S3_BUCKET_NAME=your-bucket-name
        sys.exit(1)

    print("Part 2: Fetching population data and saving to S3")
    population_data = fetch_population_data(DATA_USA_URL)
    save_to_s3(S3_BUCKET_NAME, OUTPUT_FILE_NAME, population_data)
    print("Part 2 complete.")
