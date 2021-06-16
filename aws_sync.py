# Connecting to MySQL in AWS
from controller import *
import os
import time as t
import pandas as pd

def aws_rds_sync():

    os.environ['LIBMYSQL_ENABLE_CLEARTEXT_PLUGIN'] = '1'

    test_db_conn()

    src = "*.csv"

    file = get_latest_file(BATCH_DIR, src)

    print("[+] Begin processing of " + str(file))

    file = BATCH_DIR + "/" + file

    t.sleep(1)
    # Creating temporary table
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
        "`db_create` varchar(200) DEFAULT NULL, " \
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

    # Bulk upload of data from local .csv obtained in the main process, in the temporary table
    load_sql = "LOAD DATA LOCAL " \
        "INFILE '" + file + "' " \
        "INTO TABLE tmp_movies " \
        "FIELDS TERMINATED BY ';' ENCLOSED BY '' ESCAPED BY '' " \
        "LINES TERMINATED BY '\r\n' " \
        "IGNORE 1 LINES " \
        "(adult, backdrop_path, belongs_to_collection, budget, genres, homepage, id, " \
        "imdb_id, original_language, original_title, overview, popularity, poster_path, " \
        "production_companies, production_countries, release_date, revenue, runtime, " \
        "spoken_languages, status, tagline, title, video, vote_average, vote_count, db_create)" \
        "SET idmovies = NULL;"

    try:
        conn = mysql.connector.connect(host=ENDPOINT, user=USR, passwd=PASS, port=PORT, database=DBNAME,
                                       ssl_ca=os.path.join(os.getcwd(), 'rds-ca-2019-us-east-1.pem'),
                                       allow_local_infile=True)
        cursor = conn.cursor(buffered=True)
        cursor.execute(load_sql)
        conn.commit()
        print("[+] Load data local infile, performed")
    except Exception as e:
        print("[-] Couldn't Load data local infile")
        print("[-] Error: {}".format(str(e)))

    # Merging data from the temporary table with the original table
    merge = "INSERT INTO movies " \
            "SELECT * from tmp_movies " \
            "ON DUPLICATE KEY UPDATE adult = tmp_movies.adult, " \
            "backdrop_path = tmp_movies.backdrop_path, adult = tmp_movies.adult, " \
            "belongs_to_collection = tmp_movies.belongs_to_collection, budget = tmp_movies.budget, " \
            "genres = tmp_movies.genres, homepage = tmp_movies.homepage, " \
            "id = tmp_movies.id, imdb_id = tmp_movies.imdb_id, " \
            "original_language = tmp_movies.original_language, original_title = tmp_movies.original_title, " \
            "overview = tmp_movies.overview, popularity = tmp_movies.popularity, " \
            "poster_path = tmp_movies.poster_path, production_companies = tmp_movies.production_companies, " \
            "production_countries = tmp_movies.production_countries, release_date = tmp_movies.release_date, " \
            "revenue = tmp_movies.revenue, runtime = tmp_movies.runtime, " \
            "spoken_languages = tmp_movies.spoken_languages, adult = tmp_movies.adult, " \
            "status = tmp_movies.status, tagline = tmp_movies.tagline, " \
            "title = tmp_movies.title, video = tmp_movies.video, " \
            "vote_average = tmp_movies.vote_average, vote_count = tmp_movies.vote_count," \
            "db_create = " + pd.Timestamp.now() + ";"

    try:
        conn = mysql.connector.connect(host=ENDPOINT, user=USR, passwd=PASS, port=PORT, database=DBNAME,
                                       ssl_ca=os.path.join(os.getcwd(), 'rds-ca-2019-us-east-1.pem'))
        cursor = conn.cursor(buffered=True)
        cursor.execute(merge)
        conn.commit()
        print("[+] Data merged into original table")
    except Exception as e:
        print("[-] Couldn't do merge into original table")
        print("[-] Error: {}".format(str(e)))

    # Deleting Temporary table
    delete_sql = "DROP TABLE tmp_movies;"

    try:
        conn = mysql.connector.connect(host=ENDPOINT, user=USR, passwd=PASS, port=PORT, database=DBNAME,
                                       ssl_ca=os.path.join(os.getcwd(), 'rds-ca-2019-us-east-1.pem'))
        cursor = conn.cursor(buffered=True)
        cursor.execute(delete_sql)
        conn.commit()
        print("[+] Temporary table deleted")
    except Exception as e:
        print("[-] Couldn't delete temporary table")
        print("[-] Error: {}".format(str(e)))
