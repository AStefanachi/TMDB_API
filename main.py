# Build a movie DB using themoviedb.org API
# Andrea Stefanachi, developer

import json
import requests
import pandas as pd
import time as t
import random
import os
from styleframe import StyleFrame

# Setting API Parameters
# https://developers.themoviedb.org/3/getting-started/authentication

api_key = "5c2033380838b0c5c07dd74211dae17d"
api_version = 3
api_base_url = f"https://api.themoviedb.org/{api_version}"


# GET request to the API using requests
# Load requests in an array for later processing
# Random movie id

request_collection = []
for i in range(1000):
    movie_id = random.randint(1, 1000)
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
    except KeyError as e:
        # Make dictionaries usable (hashable) by pandas
        genres = ""
        for j in range(len(r_json['genres'])):
            genres += r_json['genres'][j]['name'] + ","
        r_json['genres'] = genres[:-1]

        if r_json['genres'] == "":
            r_json['genres'] = "No"

        production_companies = ""
        for k in range(len(r_json['production_companies'])):
            production_companies += r_json['production_companies'][k]['name'] + ","
        r_json['production_companies'] = production_companies[:-1]

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
            r_json['belongs_to_collection'] = "No"

        if r_json['homepage'] == "":
            r_json['homepage'] = "No"

        if r_json['tagline'] == "":
            r_json['tagline'] = "No"

        t.sleep(0.2)
        json_collection.append(r_json)

print("[+] Text responses loaded as json in the json collection")

# Setting up column names for the Data Frame
df_columns = ['adult', 'backdrop_path', 'belongs_to_collection', 'budget', 'genres', 'homepage', 'id', 'imdb_id',
              'original_language', 'original_title', 'overview', 'popularity', 'poster_path', 'production_companies',
              'production_countries', 'release_date', 'revenue', 'runtime', 'spoken_languages', 'status', 'tagline',
              'title', 'video', 'vote_average', 'vote_count']

# Loading JSON data in the Data Frame and dropping duplicates (we are using a random movie ID ;) ),
# keeping no occurences

df = pd.DataFrame(json_collection, columns=df_columns)
t.sleep(1)
df = df.drop_duplicates(subset=['id', ], keep=False).reset_index(drop=True)
t.sleep(1)
# Saving output in an excel file with StyleFrame

print("[+] Saving Output")

sf = StyleFrame(df)
ts = t.strftime("%Y-%m-%d-%H%M%S")
file = os.path.join(os.getcwd(), "output" + str(ts) + ".xlsx")
writer = sf.ExcelWriter(file)

sf.to_excel(excel_writer=writer, sheet_name="Movies", index=False, row_to_add_filters=0, best_fit=df_columns)

writer.save()

print("[+] Done.")
