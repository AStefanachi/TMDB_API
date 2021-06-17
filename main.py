# Build a movie DB using themoviedb.org API
# Data is hosted in MySQL RDS AWS
# Andrea Stefanachi, developer

import pandas as pd
from styleframe import StyleFrame
from aws_sync import *

print("[+] Let's go")

json_collection = get_new_movies()

# Setting up column names for the Data Frame
df_columns = ['adult', 'backdrop_path', 'belongs_to_collection', 'budget', 'genres', 'homepage', 'id', 'imdb_id',
              'original_language', 'original_title', 'overview', 'popularity', 'poster_path', 'production_companies',
              'production_countries', 'release_date', 'revenue', 'runtime', 'spoken_languages', 'status', 'tagline',
              'title', 'video', 'vote_average', 'vote_count', 'db_create']

# Loading JSON data in the Data Frame and dropping duplicates (we are using a random movie ID ;) ),
# keeping no occurrences

df = pd.DataFrame(json_collection, columns=df_columns)
df['db_create'] = pd.Timestamp.now()
df = df.drop_duplicates(subset=['id', ], keep=False).reset_index(drop=True)

# Saving output in an excel file with StyleFrame

print("[+] Saving Output")

sf = StyleFrame(df)
ts = t.strftime("%Y-%m-%d-%H%M%S")
file = os.path.join(EXCEL_DIR, "output" + str(ts) + ".xlsx")

writer = sf.ExcelWriter(file)

sf.to_excel(excel_writer=writer, na_rep=np.NaN, sheet_name="Movies", index=False,
            row_to_add_filters=0, best_fit=df_columns)

writer.save()

# Saving output as csv for Load Data Local Infile to AWS

df.to_csv(os.path.join(BATCH_DIR, "csv-output" + str(ts) + ".csv"), sep=";", na_rep=np.NaN, index=False)

print("[+] Done")

t.sleep(1)

print("[+] Beginning synchronization with AWS RDS...")

try:
    aws_rds_sync()
    t.sleep(1)
    print("[+] Synchronization completed")
except Exception as e:
    print("Synchronization failed due to {}".format(e))
