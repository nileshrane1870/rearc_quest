import requests
from bs4 import BeautifulSoup
import boto3
from botocore.exceptions import ClientError
import os
import hashlib
from urllib.parse import urljoin

# Part 1: AWS S3 & Sourcing Datasets

def get_bls_file_urls():
    """
    Scrapes the BLS website for time-series data file URLs.
    """
    base_url = "https://download.bls.gov/pub/time.series/pr/"
    headers = {
        'User-Agent': 'Rearc Quest (nileshrane1870@gmail.com)'  # Complies with BLS policy
    }

    print("Fetching file list from BLS.gov...")
    try:
        response = requests.get(base_url, headers=headers)
        response.raise_for_status()  # Will raise an HTTPError for bad responses (4xx or 5xx)
    except requests.exceptions.RequestException as e:
        print(f"Error: Could not fetch URL list from BLS.gov. Reason: {e}")
        return []

    soup = BeautifulSoup(response.text, 'html.parser')
    file_urls = []

    for link in soup.find_all('a'):
        href = link.get('href')
        # The HTML source shows that all links are absolute paths from the server root.
        # We filter out the link to the parent directory by checking its text.
        if href and link.text.strip() != '[To Parent Directory]' and '?' not in href:
            # Use urljoin to correctly handle joining the base URL with an absolute path.
            full_url = urljoin(base_url, href)
            file_urls.append(full_url)

    if not file_urls:
        print("Warning: No file URLs were found after parsing. The page structure may have changed.")

    print(f"Found {len(file_urls)} files to process.")
    return file_urls

def sync_files_to_s3(bucket_name, file_urls):
    """
    Syncs files from a list of URLs to an S3 bucket.
    """
    s3 = boto3.client('s3')
    headers = {
        'User-Agent': 'Rearc Quest (nileshrane1870@gmail.com)'
    }

    try:
        s3.head_bucket(Bucket=bucket_name)
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            print(f"Bucket {bucket_name} does not exist. Creating it now.")
            s3.create_bucket(Bucket=bucket_name)
        else:
            raise

    # Get current files in S3 bucket
    s3_files = {}
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket_name):
        if 'Contents' in page:
            for obj in page['Contents']:
                s3_files[obj['Key']] = obj['ETag'].strip('"')

    # Sync files
    source_files = set()
    for url in file_urls:
        file_name = os.path.basename(url)
        source_files.add(file_name)
        response = requests.get(url, headers=headers, stream=True)
        response.raise_for_status()
        content = response.content
        md5_hash = hashlib.md5(content).hexdigest()

        if file_name not in s3_files or s3_files[file_name] != md5_hash:
            print(f"Uploading {file_name} to S3 bucket {bucket_name}...")
            s3.put_object(Bucket=bucket_name, Key=file_name, Body=content)
        else:
            print(f"{file_name} is already up to date in S3 bucket {bucket_name}.")

    # Delete files from S3 that are no longer at the source
    for s3_file in s3_files:
        if s3_file not in source_files:
            print(f"Deleting {s3_file} from S3 bucket {bucket_name}...")
            s3.delete_object(Bucket=bucket_name, Key=s3_file)


if __name__ == "__main__":
    # Get bucket name from environment variable, with a fallback for local testing
    S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME", "quest-872180502324-ap-south-1-an")

    if not S3_BUCKET_NAME:
        print("Error: S3 bucket name is not set. Please set the S3_BUCKET_NAME environment variable.")
        exit(1)
    
    print("Part 1: Syncing BLS data to S3")
    urls = get_bls_file_urls()

    if not urls:
        print("No file URLs found. Halting sync process.")
    else:
        sync_files_to_s3(S3_BUCKET_NAME, urls)
        print("Part 1 complete.")
        print(f"Data synced to S3 bucket: {S3_BUCKET_NAME}")
