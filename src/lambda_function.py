import os
import datetime
import pdb

from collections import OrderedDict
from StringIO import StringIO

import requests
import pandas as pd
import sqlalchemy
import pymysql
import boto3

from lambda_config import *

S3_CLIENT = boto3.client('s3')
SES_CLIENT = boto3.client('ses')


def upload_rds(df, table_name):    
    rds_engine = sqlalchemy.create_engine(RDS_CONNECTION_STRING)
    
    df_prior = pd.read_sql_table(table_name, rds_engine).assign(Modification='Removed')
    df_modified = pd.concat([df_prior, df.assign(Modification='Added')]).drop_duplicates(subset=df_prior.columns.tolist()[1:-1], keep=False)
    df_modified = df_modified.set_index("Modification").sort_index()
    
    with rds_engine.connect() as con:
        cursor = con.execute("delete from {0}.{1}".format(RDS_DB_NAME, table_name)) 
        
    df.to_sql(name=table_name, con=rds_engine, if_exists='append', index=False)
    return df_modified
    
def upload_s3(df, local_dirpath, file_name, s3_bucket, s3_dirpath):
    local_filepath = os.path.join(local_dirpath, file_name)
    df.to_csv(local_filepath, index=False)   
    s3_filepath = os.path.join(s3_dirpath, file_name)
    S3_CLIENT.upload_file(local_filepath, s3_bucket, s3_filepath)     

def parse_df(response_changes, sort_idx=None, date_columns=None):
    df = pd.read_table(response_changes, sep='|', header=0, dtype=str).iloc[:-1]
    
    for date_column in date_columns:
        df[date_column] = pd.to_datetime(df[date_column])
        
    for col in df.columns.tolist():
        if df[col].dtype == object:
            df[col] = df[col].fillna('').apply(lambda s: "".join(i for i in s if 31 < ord(i) < 127))     
    
    if sort_idx: df = df.set_index(sort_idx).sort_index(ascending=True).reset_index()
    df = df.applymap(lambda x: '' if pd.isnull(x) else x)    
    df.insert(0, 'Lambda_Timestamp', FUNCTION_TIMESTAMP)
    return df
    
def get_file(url):
    s = requests.Session()
    s.headers.update({'Accept-Encoding': 'gzip, deflate, sdch, br',
                      'Accept': '*/*', 
                      'User-Agent': 'python-requests/2.7.11'})
    page = StringIO(s.get(url).text)
    s.close()   
    return page
     
def execute_pipeline():
    summary_stream = []
    summary_stream.append("{0}-Starting Lambda for {1}".format(FUNCTION_TIMESTAMP_STRING, LAMBDA_FUNCTION_NAME))
    
    ##### Request for data files #####
    try:
        response_main = get_file(URL_MAIN)  
    except Exception as error_msg:
        summary_stream.append("ERROR: Requesting File ({0})".format(error_msg))
        return summary_stream
    else:    
        summary_stream.append("Success: Requesting File")
        
        
    ##### Parse and data files #####
    try:
        df_main = parse_df(response_main, sort_idx=SORT_INDEX_MAIN, date_columns=DATE_COLUMNS_MAIN)
    except Exception as error_msg:
        summary_stream.append("ERROR: Parsing Data ({0})".format(error_msg))
        return summary_stream
    else:    
        summary_stream.append("Success: Parsing Data")

    
    ##### Upload dataframes to s3 as CSV files #####
    try:
        upload_s3(df_main, LOCAL_DIRPATH, FILENAME_MAIN, S3_BUCKET_NAME, S3_BUCKET_DIRPATH) 
    except Exception as error_msg:
        summary_stream.append("ERROR: Uploading Raw Data ({0})".format(error_msg))
        return summary_stream
    else:    
        summary_stream.append("Success: Uploading Raw Data")

    
    ##### Perform RDS Insert here #####
    try:
        df_mod_main = upload_rds(df_main, TABLE_MAIN)
    except Exception as error_msg:
        summary_stream.append("ERROR: Updating ({0})".format(error_msg))
        return summary_stream 
    else:
        summary_stream.append("Success: Updating")
        summary_stream.append("Modifications: " + DATA_NAME_MAIN)
        summary_stream.append(df_mod_main)      
    
    return summary_stream
    
    
def handler(lambda_event, lambda_context=None):
    summary_stream = execute_pipeline()
   
    #### Build Email Summary #####
    try:
        summary = []
        for item in summary_stream:
            if isinstance(item, basestring):
                item = '<h2>' + item + '</h2>'
                summary.append(item)
            elif isinstance(item, pd.DataFrame):
                item = item.to_html()
                summary.append(item)
        summary = '<html>' + '<br>'.join(summary) + '</html>'                
    except Exception as error_msg:
        summary = '<html><h2>'+ "ERROR: Creating Report ({0})".format(error_msg) +'</h2></html>'
       
    ##### Send Email Summary #####
    email_subject = '{0} - AWS Lambda Report'.format(LAMBDA_FUNCTION_NAME)
    email_body = summary
    response = SES_CLIENT.send_email(Source=EMAIL_FROM, Destination={'ToAddresses': [EMAIL_TO,]},
                                     Message={'Subject': {'Data': email_subject}, 
                                              'Body': {'Html': {'Data': email_body}}})
                                              