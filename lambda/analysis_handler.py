import os
from part3_analysis import load_data_from_s3, run_analysis

def handler(event, context):
    """
    Lambda handler for running the data analysis.
    """
    s3_bucket_name = os.environ['S3_BUCKET_NAME']

    for record in event['Records']:
        print("Processing SQS message...")
        # In a real-world scenario, you might use the message body
        # to get information about the file that was uploaded.
        # For this quest, we know it's population_data.json.
        
        print("Part 3: Data Analysis")
        df_pr, df_pop = load_data_from_s3(s3_bucket_name)
        run_analysis(df_pr, df_pop)
        print("Part 3 complete.")

    return {
        'statusCode': 200,
        'body': 'Analysis completed successfully!'
    }
