# Connecting to MySQL in AWS
# import mysql.connector
# from config import *
from controller import *
import os
import time as t

os.environ['LIBMYSQL_ENABLE_CLEARTEXT_PLUGIN'] = '1'

test_db_conn()

src = "*.csv"

file = get_latest_file(BATCH_DIR, src)

print("[+] Begin processing of " + str(file))

t.sleep(1)

create_temp_table = "CREATE TABLE `tmp_movies` ( " \
    "`idmovies` bigint NOT NULL AUTO_INCREMENT, " \
    "`adult` varchar(200) DEFAULT NULL, " \
    "`backdrop_path` varchar(200) DEFAULT NULL, " \
    "`belongs_to_collection` varchar(200) DEFAULT NULL, " \
    "`budget` decimal(10,2) DEFAULT NULL, " \
    "`genres` varchar(200) DEFAULT NULL, " \
    "`homepage` varchar(200) DEFAULT NULL, " \
    "`id` bigint DEFAULT NULL, " \
    "`imdb_id` varchar(200) DEFAULT NULL, " \
    "`original_language` varchar(200) DEFAULT NULL, " \
    "`original_title` varchar(200) DEFAULT NULL, " \
    "`overview` varchar(200) DEFAULT NULL, " \
    "`popularity` varchar(200) DEFAULT NULL, " \
    "`poster_path` varchar(200) DEFAULT NULL, " \
    "`production_companies` varchar(200) DEFAULT NULL, " \
    "`production_countries` varchar(200) DEFAULT NULL, " \
    "`release_date` date DEFAULT NULL, " \
    "`revenue` decimal(10,2) DEFAULT NULL, " \
    "`runtime` varchar(200) DEFAULT NULL, " \
    "`spoken_languages` varchar(200) DEFAULT NULL, " \
    "`status` varchar(200) DEFAULT NULL, " \
    "`tagline` varchar(200) DEFAULT NULL, " \
    "`title` varchar(200) DEFAULT NULL, " \
    "`video` varchar(200) DEFAULT NULL, " \
    "`vote_average` varchar(200) DEFAULT NULL, " \
    "`vote_count` varchar(200) DEFAULT NULL, " \
    "PRIMARY KEY (`idmovies`) " \
    ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; "

try:
    conn = mysql.connector.connect(host=ENDPOINT, user=USR, passwd=PASS, port=PORT, database=DBNAME,
                                   ssl_ca=os.path.join(os.getcwd(), 'rds-ca-2019-us-east-1.pem'))
    cursor = conn.cursor(buffered=True)
    cursor.execute(create_temp_table)
    conn.commit()
    print("[+] Created temp table")
except Exception as e:
    print("[-] Couldn't do create temp table")
    print("[-] Error: {}".format(str(e)))

merge = "LOAD DATA LOCAL " \
                        "INFILE '" + file + "' " \
                        "INTO TABLE tmp_movies " \
                        "FIELDS TERMINATED BY ';' ENCLOSED BY '' ESCAPED BY '' " \
                        "LINES TERMINATED BY '\r\n' " \
                        "IGNORE 1 LINES " \
                        "(adult, backdrop_path, belongs_to_collection, budget, genres, homepage, id, " \
                        "imdb_id, original_language, original_title, overview, popularity, poster_path, " \
                        "production_companies, production_countries, release_date, revenue, runtime, " \
                        "spoken_languages, status, tagline, title, video, vote_average, vote_count)" \
                        "SET idmovies = NULL;"

try:
    conn = mysql.connector.connect(host=ENDPOINT, user=USR, passwd=PASS, port=PORT, database=DBNAME,
                                   ssl_ca=os.path.join(os.getcwd(), 'rds-ca-2019-us-east-1.pem'))
    cursor = conn.cursor(buffered=True)
    cursor.execute(merge)
    conn.commit()
    print("[+] Load data local infile, performed")
except Exception as e:
    print("[-] Couldn't Load data local infile")
    print("[-] Error: {}".format(str(e)))

