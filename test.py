# Connecting to MySQL in AWS
import mysql.connector
from config import *
import boto3
import os

ENDPOINT = config['host']

PORT = config['port']

USR = 'admin'

DBNAME = config['database']

REGION = 'us-east-1'

os.environ['LIBMYSQL_ENABLE_CLEARTEXT_PLUGIN'] = '1'

#gets the credentials from .aws/credentials
session = boto3.Session(profile_name='default')
client = session.client('rds')

token = client.generate_db_auth_token(DBHostname=ENDPOINT, Port=PORT, DBUsername=USR, Region=REGION)

print(token)

try:
    conn = mysql.connector.connect(host=ENDPOINT, user=USR, passwd=token, port=PORT, database=DBNAME,
                                    ssl_ca='[full path]rds-combined-ca-bundle.pem')
    cur = conn.cursor()
    cur.execute("""SELECT now()""")
    query_results = cur.fetchall()
    print(query_results)
except Exception as e:
    print("Database connection failed due to {}".format(e))