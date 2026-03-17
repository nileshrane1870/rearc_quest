import os
from part1 import get_bls_file_urls, sync_files_to_s3
from part2 import fetch_population_data, save_to_s3 as save_json_to_s3

def handler(event, context):
    """
    Lambda handler for fetching data from BLS and DataUSA API.
    """
    s3_bucket_name = os.environ['S3_BUCKET_NAME']

    # Part 1
    print("Part 1: Syncing BLS data to S3")
    urls = get_bls_file_urls()
    sync_files_to_s3(s3_bucket_name, urls)
    print("Part 1 complete.")

    # Part 2
    print("Part 2: Fetching population data and saving to S3")
    population_data = fetch_population_data()
    save_json_to_s3(s3_bucket_name, "population_data.json", population_data)
    print("Part 2 complete.")

    return {
        'statusCode': 200,
        'body': 'Data fetching and saving completed successfully!'
    }
