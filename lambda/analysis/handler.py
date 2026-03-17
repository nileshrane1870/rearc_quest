import pandas as pd
import boto3
import json
import os

# Note: The logic from your part3_analysis.py is already very clean and modular.
# We just need to adapt the entry point to be a Lambda handler.

def load_data_from_s3(bucket_name: str, region_name: str):
    """Loads and cleans the data from S3 into pandas DataFrames."""
    storage_options = {'client_kwargs': {'region_name': region_name}}
    ts_data_path = f"s3://{bucket_name}/pr.data.0.Current"
    pop_data_path = f"s3://{bucket_name}/population_data.json"

    try:
        print(f"Loading time-series data from {ts_data_path}...")
        df_pr = pd.read_csv(ts_data_path, sep='\t', storage_options=storage_options)
        df_pr.columns = df_pr.columns.str.strip()
        for col in df_pr.select_dtypes(include=['object', 'string']):
            df_pr[col] = df_pr[col].str.strip()
    except Exception as e:
        print(f"Error loading time-series data from {ts_data_path}: {e}")
        raise

    try:
        print(f"Loading population data from {pop_data_path}...")
        s3_client = boto3.client('s3', region_name=region_name)
        response = s3_client.get_object(Bucket=bucket_name, Key='population_data.json')
        pop_json_data = json.loads(response['Body'].read().decode('utf-8'))
        df_pop = pd.json_normalize(pop_json_data['data'])
        df_pop.rename(columns={'Year': 'year', 'Population': 'population'}, inplace=True)
    except Exception as e:
        print(f"Error loading population data from {pop_data_path}: {e}")
        raise

    df_pr['value'] = pd.to_numeric(df_pr['value'], errors='coerce')
    df_pr.dropna(subset=['value'], inplace=True)
    df_pr['year'] = df_pr['year'].astype(int)
    df_pop['year'] = df_pop['year'].astype(int)
    return df_pr, df_pop

def run_analysis(df_pr: pd.DataFrame, df_pop: pd.DataFrame):
    """Performs the data analysis as described in Part 3."""
    # 1. Mean and standard deviation of US population
    df_pop_filtered = df_pop[(df_pop['year'] >= 2013) & (df_pop['year'] <= 2018)]
    population_mean = df_pop_filtered['population'].mean()
    population_std = df_pop_filtered['population'].std()

    print("\n--- Report 1: US Population Analysis (2013-2018) ---")
    print(f"Mean Population: {population_mean:,.0f}")
    print(f"Standard Deviation of Population: {population_std:,.0f}")

    # 2. Best year for each series_id
    df_yearly_sum = df_pr.groupby(['series_id', 'year'])['value'].sum().reset_index()
    df_best_year = df_yearly_sum.loc[df_yearly_sum.groupby('series_id')['value'].idxmax()]

    print("\n--- Report 2: Best Year per Time Series ---")
    print(df_best_year.to_string(index=False))

    # 3. Joined report
    df_pr_q1 = df_pr[(df_pr['series_id'] == 'PRS30006032') & (df_pr['period'] == 'Q01')].copy()
    df_joined = pd.merge(df_pr_q1, df_pop, on='year', how='left')
    df_joined = df_joined[['series_id', 'year', 'period', 'value', 'population']]

    print("\n--- Report 3: Joined Time Series and Population Data ---")
    print(df_joined.to_string(
        index=False,
        formatters={'population': lambda x: f'{x:,.0f}' if pd.notna(x) else 'NaN'}
    ))

def lambda_handler(event, context):
    """
    Main Lambda entry point, triggered by SQS.
    """
    S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME")
    AWS_REGION = os.environ.get("AWS_REGION", "ap-south-1") # Default to a common region

    if not S3_BUCKET_NAME:
        raise ValueError("S3_BUCKET_NAME environment variable not set.")

    # The SQS message contains the S3 event. We parse it to confirm the file.
    for record in event['Records']:
        body = json.loads(record['body'])
        s3_record = body['Records'][0]
        bucket = s3_record['s3']['bucket']['name']
        key = s3_record['s3']['object']['key']
        print(f"Processing event for s3://{bucket}/{key}")

        # Ensure we only run the analysis for the correct file
        if key == "population_data.json":
            print("--- Starting Part 3: Data Analysis ---")
            df_pr, df_pop = load_data_from_s3(S3_BUCKET_NAME, AWS_REGION)
            run_analysis(df_pr, df_pop)
            print("--- Part 3 complete ---")
        else:
            print(f"Skipping analysis for file {key}, as it is not the trigger file.")

    return {'statusCode': 200, 'body': json.dumps('Analysis complete!')}