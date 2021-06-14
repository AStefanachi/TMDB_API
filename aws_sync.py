# Connecting to MySQL in AWS
import mysql.connector
from config import *
import os

os.environ['LIBMYSQL_ENABLE_CLEARTEXT_PLUGIN'] = '1'

try:
    conn = mysql.connector.connect(host=ENDPOINT, user=USR, passwd=PASS, port=PORT, database=DBNAME,
                                   ssl_ca=os.path.join(os.getcwd(), 'rds-ca-2019-us-east-1.pem'))

    if conn:
        print("[+] Connected successfully... ")

except Exception as e:
    print("Database connection failed due to {}".format(e))


