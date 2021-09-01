import os
import pandas as pd
import logging
from datetime import date
import re

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def extract(s3_input_file_path: str) -> pd.DataFrame:
    """
    Returns a Dataframe.
    :param s3_input_file_path: Full path to S3 object, which will be loaded into Dataframe.
    :return: Pandas Dataframe.
    """
    df = pd.read_csv(s3_input_file_path)
    return df

def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Renames input Dataframe Columns to lowercase, removes leading and trailing
    whitespaces, replaces dot, comma and whitespace characters in middle of column names
    with underscores. And adds a new column called processing_time with value of datetime.now().
    :param df: Input Pandas Dataframe.
    :return: Transformed Pandas Dataframe.
    """
    # Column names to lowercase.
    # Remove leading and traling white spaces.
    # Replace whitespaces with single underscore.
    new_column_names = {}
    for column_name in df.columns:
        #  Strip leading and trailing whitespaces.
        # Replace multiple whitespaces with single whitespace.
        col_name = ' '.join(column_name.strip().split())
        # Replace whitespaces with a single underscore.
        # And convert all characters to lowercase.
        col_name = re.sub('\s+', '_', col_name).lower()

        new_column_names[column_name] = col_name

    df = df.rename(columns=new_column_names)

    df['processing_date'] = date.today()
    return df

def load(df: pd.DataFrame, s3_output_file_path: str) -> str:
    try:
        df.to_parquet(s3_output_file_path)
        return f'ETL Finished.'
    except Exception as e:
        logging.info(f'Exception {e.__class__}, {str(e)} occured during Load function.')




def lambda_handler(event, context):
    logging.info(f'Event: {event}')
    s3_bucket_prefix = 's3://'
    output_file_folder= 'output/'
    output_file_suffix = '_processed.snappy.parquet'

    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        logging.info(f'Bucket: {bucket}')

        input_file_name = record['s3']['object']['key']
        logging.info(f'Key: {input_file_name}')

        path_split = os.path.splitext(os.path.basename(input_file_name))

        if(path_split[1] != '.csv'):
            logging.info(f'Key: {input_file_name}, is not a .csv file. Skipping.')
            continue

        output_file_name = path_split[0] + output_file_suffix
        s3_input_file_path = os.path.join(s3_bucket_prefix, bucket, input_file_name)
        logging.info(f'S3 Input File Path: {s3_input_file_path}')

        s3_output_file_path = os.path.join(s3_bucket_prefix, bucket, output_file_folder, output_file_name)
        logging.info(f'S3 Output File Path: {s3_output_file_path}')

        df = extract(s3_input_file_path)
        df = transform(df)
        result = load(df, s3_output_file_path)
        return result


        
       

