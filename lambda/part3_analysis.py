import pandas as pd
import boto3
import json

# Part 3: Data Analytics

def load_data_from_s3(bucket_name):
    """
    Loads the data from S3 into pandas DataFrames.
    """
    s3 = boto3.client('s3')

    # Load pr.data.0.Current from S3
    obj = s3.get_object(Bucket=bucket_name, Key='pr.data.0.Current')
    # Clean up the raw data before creating the dataframe
    lines = obj['Body'].read().decode('utf-8').splitlines()
    data = [line.split() for line in lines]
    df_pr = pd.DataFrame(data, columns=['series_id', 'year', 'period', 'value'])
    df_pr['series_id'] = df_pr['series_id'].str.strip()


    # Load population_data.json from S3
    obj = s3.get_object(Bucket=bucket_name, Key='population_data.json')
    population_data = json.loads(obj['Body'].read().decode('utf-8'))
    df_pop = pd.DataFrame(population_data['data'])

    return df_pr, df_pop

def run_analysis(df_pr, df_pop):
    """
    Performs the data analysis as described in Part 3.
    """
    # 1. Mean and standard deviation of US population
    df_pop['Year'] = pd.to_numeric(df_pop['Year'])
    df_pop_filtered = df_pop[(df_pop['Year'] >= 2013) & (df_pop['Year'] <= 2018)]
    population_mean = df_pop_filtered['Population'].mean()
    population_std = df_pop_filtered['Population'].std()

    print("--- Report 1: US Population Analysis (2013-2018) ---")
    print(f"Mean Population: {population_mean}")
    print(f"Standard Deviation of Population: {population_std}")
    print("\n")

    # 2. Best year for each series_id
    df_pr['value'] = pd.to_numeric(df_pr['value'], errors='coerce')
    df_pr.dropna(subset=['value'], inplace=True)
    df_yearly_sum = df_pr.groupby(['series_id', 'year'])['value'].sum().reset_index()
    df_best_year = df_yearly_sum.loc[df_yearly_sum.groupby('series_id')['value'].idxmax()]

    print("--- Report 2: Best Year per Time Series ---")
    print(df_best_year)
    print("\n")

    # 3. Joined report
    df_pr_q1 = df_pr[(df_pr['series_id'] == 'PRS30006032') & (df_pr['period'] == 'Q01')]
    df_pr_q1['year'] = df_pr_q1['year'].astype(int)
    df_pop['Year'] = df_pop['Year'].astype(int)
    df_joined = pd.merge(df_pr_q1, df_pop, left_on='year', right_on='Year', how='inner')
    df_joined = df_joined[['series_id', 'year', 'period', 'value', 'Population']]


    print("--- Report 3: Joined Time Series and Population Data ---")
    print(df_joined)
    print("\n")
