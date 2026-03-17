import pandas as pd
import boto3
import json
import os
import sys

# Part 3: Data Analytics

def load_data_from_s3(bucket_name: str, region_name: str):
    """
    Loads and cleans the data from S3 into pandas DataFrames.
    """
    # Create storage options to pass to pandas for S3 access.
    storage_options = {'client_kwargs': {'region_name': region_name}}
    # Construct S3 paths. Using s3fs allows pandas to read directly.
    ts_data_path = f"s3://{bucket_name}/pr.data.0.Current"
    pop_data_path = f"s3://{bucket_name}/population_data.json"

    try:
        print(f"Loading time-series data from {ts_data_path}...")
        # The time-series file is tab-separated. Using read_csv is more robust.
        # The column names and data have extra whitespace that needs to be trimmed.
        # Pass storage_options to specify the AWS region.
        df_pr = pd.read_csv(ts_data_path, sep='\t', storage_options=storage_options)
        df_pr.columns = df_pr.columns.str.strip()
        # Select object and string dtypes to handle text columns and silence Pandas 4 warning.
        for col in df_pr.select_dtypes(include=['object', 'string']):
            df_pr[col] = df_pr[col].str.strip()
    except Exception as e:
        print(f"Error loading time-series data from {ts_data_path}: {e}", file=sys.stderr)
        print("Please check your AWS credentials and ensure the file exists in your bucket.", file=sys.stderr)
        sys.exit(1)

    try:
        print(f"Loading population data from {pop_data_path}...")
        # The JSON file from the API has a nested structure ('data' and 'source' keys).
        # pd.read_json fails if top-level arrays have different lengths.
        # It's more robust to load the file content first and then parse it.
        s3_client = boto3.client('s3', region_name=region_name)
        response = s3_client.get_object(Bucket=bucket_name, Key='population_data.json')
        pop_json_data = json.loads(response['Body'].read().decode('utf-8'))

        # The actual data records are in the 'data' key.
        df_pop = pd.json_normalize(pop_json_data['data'])
        df_pop.rename(columns={'Year': 'year', 'Population': 'population'}, inplace=True)
    except Exception as e:
        print(f"Error loading population data from {pop_data_path}: {e}", file=sys.stderr)
        print("Please check your AWS credentials and ensure the file exists in your bucket.", file=sys.stderr)
        sys.exit(1)

    # --- Perform type conversions and cleaning upfront ---
    # Convert 'value' to numeric, turning errors into Not a Number (NaN)
    df_pr['value'] = pd.to_numeric(df_pr['value'], errors='coerce')
    # Drop rows where 'value' could not be parsed
    df_pr.dropna(subset=['value'], inplace=True)
    # Ensure year columns are the same integer type for merging
    df_pr['year'] = df_pr['year'].astype(int)
    df_pop['year'] = df_pop['year'].astype(int)

    return df_pr, df_pop

def run_analysis(df_pr: pd.DataFrame, df_pop: pd.DataFrame):
    """
    Performs the data analysis as described in Part 3.
    """
    # 1. Mean and standard deviation of US population
    df_pop_filtered = df_pop[(df_pop['year'] >= 2013) & (df_pop['year'] <= 2018)]
    population_mean = df_pop_filtered['population'].mean()
    population_std = df_pop_filtered['population'].std()

    print("--- Report 1: US Population Analysis (2013-2018) ---")
    print(f"Mean Population: {population_mean:,.0f}")
    print(f"Standard Deviation of Population: {population_std:,.0f}")
    print("\n")

    # 2. Best year for each series_id
    df_yearly_sum = df_pr.groupby(['series_id', 'year'])['value'].sum().reset_index()
    # Use idxmax to efficiently find the index of the max value for each group
    df_best_year = df_yearly_sum.loc[df_yearly_sum.groupby('series_id')['value'].idxmax()]

    print("--- Report 2: Best Year per Time Series ---")
    print(df_best_year.to_string(index=False))
    print("\n")

    # 3. Joined report
    # Filter for the specific series and period. Use .copy() to avoid SettingWithCopyWarning.
    df_pr_q1 = df_pr[(df_pr['series_id'] == 'PRS30006032') & (df_pr['period'] == 'Q01')].copy()
    # Use a 'left' merge to keep all time-series records and add population where available.
    df_joined = pd.merge(df_pr_q1, df_pop, on='year', how='left')
    # Select and reorder columns for the final report
    df_joined = df_joined[['series_id', 'year', 'period', 'value', 'population']]

    print("--- Report 3: Joined Time Series and Population Data ---")
    # Use a formatter for better readability of the population number.
    print(df_joined.to_string(
        index=False,
        formatters={'population': lambda x: f'{x:,.0f}' if pd.notna(x) else 'NaN'}
    ))
    print("\n")

if __name__ == "__main__":
    # Get the bucket name from an environment variable. This is a safer pattern.
    S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME", "quest-872180502324-ap-south-1-an")
    # It's good practice to also specify the region.
    AWS_REGION = os.environ.get("AWS_REGION", "ap-south-1")

    if not S3_BUCKET_NAME:
        print("Error: S3_BUCKET_NAME environment variable not set.", file=sys.stderr)
        print("Please set it to your S3 bucket name before running.", file=sys.stderr)
        print("Example (macOS/Linux): export S3_BUCKET_NAME='your-bucket-name'", file=sys.stderr)
        sys.exit(1)
    
    print("Part 3: Data Analysis")
    df_pr, df_pop = load_data_from_s3(S3_BUCKET_NAME, AWS_REGION)
    run_analysis(df_pr, df_pop)
    print("Part 3 complete.")
