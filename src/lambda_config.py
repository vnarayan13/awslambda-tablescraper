# Lambda Function Configurations
import datetime

##### Define Function Constants #####
LAMBDA_FUNCTION_NAME = ''
FUNCTION_TIMESTAMP = datetime.datetime.now()
FUNCTION_TIMESTAMP_STRING = FUNCTION_TIMESTAMP.strftime("%m-%d-%Y_%H-%M-%S")
LOCAL_DIRPATH = r'/tmp' # do not modify

# Data Constants (Example)
DATA_NAME_MAIN = 'TSPilotSecurities'
URL_MAIN = r'http://tsp.finra.org/finra_org/ticksizepilot/TSPilotSecurities.txt'
FILENAME_MAIN = '{0}_{1}.csv'.format(DATA_NAME_MAIN, FUNCTION_TIMESTAMP_STRING)
SORT_INDEX_MAIN = ['Effective_Date', 'Ticker_Symbol']
DATE_COLUMNS_MAIN = ['Effective_Date']

# S3
S3_BUCKET_NAME = ''
S3_BUCKET_DIRPATH = ''

# RDS
RDS_HOST = ''
RDS_USER = ''
RDS_PASSWORD = ''
RDS_DB_NAME = ''
RDS_PORT = ''
TABLE_MAIN = ''
RDS_CONNECTION_STRING = "mysql+pymysql://{0}:{1}@{2}/{3}?host={2}?port={4}".format(RDS_USER,RDS_PASSWORD,RDS_HOST,RDS_DB_NAME,RDS_PORT)



# SES (need to validate emails)
EMAIL_FROM = ''
EMAIL_TO = ''
