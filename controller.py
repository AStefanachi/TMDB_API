import glob
import json
import requests
import random
import numpy as np
from config import *
import mysql.connector


def get_latest_file(path, *paths):
    # Get latest file in a folder using a search parameter *paths

    full_path = os.path.join(path, *paths)
    files = glob.iglob(full_path)

    if not files:
        return None
    latest_file = max(files, key=os.path.getctime)
    _, filename = os.path.split(latest_file)

    return filename


def get_new_movies():
    # GET request to the API using requests
    # Load requests in an array for later processing
    # Random movie id

    request_collection = []
    for i in range(1001):
        movie_id = random.randint(1, 10000)
        endpoint_path = f"/movie/{movie_id}"
        endpoint = f"{api_base_url}{endpoint_path}?api_key={api_key}"
        r = requests.get(endpoint).text
        request_collection.append(r)

    print("[+] Requests loaded in the collection")

    # Load text response as JSON
    # Load text response in an array for later processing
    # Only valid requests will be stored

    json_collection = []
    for i in range(len(request_collection)):
        r_json = json.loads(request_collection[i])
        try:
            if r_json['success'] is False:
                pass
        except KeyError:
            # Make dictionaries usable (hashable) by pandas
            genres = ""
            for j in range(len(r_json['genres'])):
                genres += r_json['genres'][j]['name'] + ","
            r_json['genres'] = genres[:-1]

            production_companies = ""
            for k in range(len(r_json['production_companies'])):
                production_companies += r_json['production_companies'][k]['name'] + ","
            r_json['production_companies'] = production_companies[:-1]

            if r_json['production_companies'] == "":
                r_json['production_companies'] = np.NaN

            production_countries = ""
            for L in range(len(r_json['production_countries'])):
                production_countries += r_json['production_countries'][L]['iso_3166_1'] + ","
            r_json['production_countries'] = production_countries[:-1]

            spoken_languages = ""
            for m in range(len(r_json['spoken_languages'])):
                spoken_languages += r_json['spoken_languages'][m]['name'] + ","
            r_json['spoken_languages'] = spoken_languages[:-1]

            belongs_to_collection = ""
            if r_json['belongs_to_collection'] is not None:
                belongs_to_collection += r_json['belongs_to_collection']['name']
                r_json['belongs_to_collection'] = belongs_to_collection
            else:
                r_json['belongs_to_collection'] = np.NaN

            # Forcing empty values to np.NaN

            if r_json['production_countries'] == "":
                r_json['production_countries'] = np.NaN

            if r_json['homepage'] == "":
                r_json['homepage'] = np.NaN

            if r_json['original_title'] == "":
                r_json['original_title'] = np.NaN

            if r_json['tagline'] == "":
                r_json['tagline'] = np.NaN

            if r_json['genres'] == "":
                r_json['genres'] = np.NaN

            # Parsing overview for ; separator
            overview = str(r_json['overview'])
            overview = overview.replace(';', '')
            overview = overview.replace('"', '')
            r_json['overview'] = overview

            json_collection.append(r_json)

    print("[+] Text responses loaded as json in the json collection")

    return json_collection


def test_db_conn():
    # Test connection to AWS
    try:
        conn = mysql.connector.connect(host=ENDPOINT, user=USR, passwd=PASS, port=PORT, database=DBNAME,
                                       ssl_ca=os.path.join(os.getcwd(), 'rds-ca-2019-us-east-1.pem'))

        if conn:
            print("[+] Connected successfully... ")

    except Exception as e:
        print("Database connection failed due to {}".format(e))
