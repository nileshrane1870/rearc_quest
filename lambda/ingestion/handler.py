import requests
from bs4 import BeautifulSoup
import boto3
from botocore.exceptions import ClientError
import os
import hashlib
from urllib.parse import urljoin
import json


# --- Part 1 Logic ---

def get_bls_file_urls():
    """Scrapes the BLS website for time-series data file URLs."""
    base_url = "https://download.bls.gov/pub/time.series/pr/"
    headers = {'User-Agent': 'Rearc Quest (nileshrane1870@gmail.com)'}
    print("Fetching file list from BLS.gov...")
    try:
        response = requests.get(base_url, headers=headers)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error: Could not fetch URL list from BLS.gov. Reason: {e}")
        raise

    soup = BeautifulSoup(response.text, 'html.parser')
    file_urls = [urljoin(base_url, link.get('href')) for link in soup.find_all('a') if
                 link.get('href') and '?' not in link.get('href') and 'Parent Directory' not in link.text]
    print(f"Found {len(file_urls)} files to process.")
    return file_urls


def sync_files_to_s3(s3_client, bucket_name, file_urls):
    """Syncs files from a list of URLs to an S3 bucket."""
    headers = {'User-Agent': 'Rearc Quest (nileshrane1870@gmail.com)'}

    # Get current files in S3 bucket
    s3_files = {}
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket_name):
        if 'Contents' in page:
            for obj in page['Contents']:
                # We only care about files from the 'pr' directory for this sync
                if obj['Key'].startswith('pr.'):
                    s3_files[obj['Key']] = obj['ETag'].strip('"')

    source_files = set()
    for url in file_urls:
        file_name = os.path.basename(url)
        if not file_name or not file_name.startswith('pr.'):
            continue  # Skip directories or irrelevant files

        source_files.add(file_name)
        try:
            response = requests.get(url, headers=headers, stream=True)
            response.raise_for_status()
            content = response.content
            md5_hash = hashlib.md5(content).hexdigest()

            if file_name not in s3_files or s3_files[file_name] != md5_hash:
                print(f"Uploading {file_name} to S3 bucket {bucket_name}...")
                s3_client.put_object(Bucket=bucket_name, Key=file_name, Body=content)
            else:
                print(f"{file_name} is already up to date.")
        except requests.exceptions.RequestException as e:
            print(f"Could not process file {file_name}. Reason: {e}")
            # Decide if you want to continue or fail the whole lambda
            continue

    # Delete files from S3 that are no longer at the source
    for s3_file in s3_files:
        if s3_file not in source_files:
            print(f"Deleting {s3_file} from S3 bucket {bucket_name}...")
            s3_client.delete_object(Bucket=bucket_name, Key=s3_file)


# --- Part 2 Logic ---

DATA_USA_URL = "https://honolulu-api.datausa.io/tesseract/data.jsonrecords?cube=acs_yg_total_population_1&drilldowns=Year%2CNation&locale=en&measures=Population"
OUTPUT_FILE_NAME = "population_data.json"


def fetch_and_save_population_data(s3_client, bucket_name):
    """Fetches population data and saves it to S3."""
    print(f"Fetching data from {DATA_USA_URL}...")
    try:
        response = requests.get(DATA_USA_URL)
        response.raise_for_status()
        data = response.json()

        print(f"Uploading {OUTPUT_FILE_NAME} to S3 bucket {bucket_name}...")
        s3_client.put_object(
            Bucket=bucket_name,
            Key=OUTPUT_FILE_NAME,
            Body=json.dumps(data, indent=4)
        )
        print(f"Successfully uploaded {OUTPUT_FILE_NAME} to {bucket_name}.")
    except requests.exceptions.RequestException as e:
        print(f"Error fetching or saving population data: {e}")
        raise
    except ClientError as e:
        print(f"Error uploading to S3: {e}")
        raise


# --- Lambda Handler ---

def lambda_handler(event, context):
    """
    Main Lambda entry point.
    """
    S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME")
    if not S3_BUCKET_NAME:
        raise ValueError("S3_BUCKET_NAME environment variable not set.")

    s3_client = boto3.client('s3')

    print("--- Starting Part 1: Syncing BLS data ---")
    bls_urls = get_bls_file_urls()
    if bls_urls:
        sync_files_to_s3(s3_client, S3_BUCKET_NAME, bls_urls)
    print("--- Part 1 complete ---")

    print("\n--- Starting Part 2: Fetching population data ---")
    fetch_and_save_population_data(s3_client, S3_BUCKET_NAME)
    print("--- Part 2 complete ---")

    return {
        'statusCode': 200,
        'body': json.dumps('Data ingestion pipeline executed successfully!')
    }