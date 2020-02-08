import os, sys
import glob
import psycopg2
import json
import pandas as pd
import datetime
from sql_queries import *

conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
cur = conn.cursor()

def get_files(filepath):
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    return all_files

filepath = "data/song_data/"

song_files = get_files(filepath)
print(len(song_files))
print(song_files)

i = 0

songs_json_data = {}

for file in song_files:
    songs_json_data[i] = pd.read_json(open(file), lines=True)
    i += 1
print(type(songs_json_data))
#print(songs_json_data)
#print(songs_json_data)

df = pd.DataFrame(songs_json_data)
#df = pd.DataFrame(data=songs_json_data, columns=['song_id', 'title', 'artist_id', 'year', 'duration'])
#df = pd.DataFrame.from_dict(songs_json_data, orient='index')#, columns=['song_id', 'title', 'artist_id', 'year', 'duration'])
print(df.values)

song_data = pd.DataFrame(data = df.values, index = ['song_id', 'title', 'artist_id', 'year', 'duration'])
#song_data = ([df['song_id'],df['title'],df['artist_id'],df['year'],df['duration']])
print(song_data)
#song_data

for s in range(len(song_files)):
    str_song = str(song_data[s]['song_id']), str(song_data[s]['title']), str(song_data[s]['artist_id']), int(song_data[s]['year']), float(song_data[s]['duration'])
    print(song_table_insert)
    print(str_song)
    cur.execute(song_table_insert, str_song)
    conn.commit()


#artist_data =
#artist_data

print(str(song_data[0]))
for a in range(len(song_files)):
    str_artist = str(song_data[a]['artist_id']), str(song_data[a]['artist_name']), str(song_data[a]['artist_location']), str(song_data[a]['artist_latitude']), str(song_data[a]['artist_longitude'])
    print(artist_table_insert)
    print(str_artist)
    cur.execute(artist_table_insert, str_artist)
    conn.commit()

filepath_log = "data/log_data/"

log_files = get_files(filepath_log)
print(len(log_files))
print(log_files)

json_file_input_log = sys.argv[1]

l = 0
lstLog = []

for log_file in log_files:
    for line in open(log_file, 'r'):
        lstLog.append(json.loads(line))

print(len(lstLog))
print(lstLog[0] )

#for log_file in log_files:
#    print(log_file)
#    json_file_input_log = open(log_file)
#    log_data[l] = json.load(json_file_input_log)
#    json_file_input_log.close()
#    l += 1

print("\n" + str(lstLog[0]['artist']) + ", " + str(lstLog[0]['auth']) + ", " + str(lstLog[0]['firstName']) + ", " + str(lstLog[0]['gender']) + ", " + str(lstLog[0]['itemInSession']) +
      str(lstLog[0]['lastName']) + ", " + str(lstLog[0]['length']) + ", " + str(lstLog[0]['level']) + ", " + str(lstLog[0]['location']) + ", " + str(lstLog[0]['method']) +
      str(lstLog[0]['page']) + ", " + str(lstLog[0]['registration']) + ", " + str(lstLog[0]['sessionId']) + ", " + str(lstLog[0]['song']) + ", " + str(lstLog[0]['status']) +
      str(lstLog[0]['ts']) + ", " + str(lstLog[0]['userAgent']) + ", " + str(lstLog[0]['userId']) +
      "\n")

print(datetime.datetime.fromtimestamp( int(lstLog[0]['ts']) /1000.0).strftime('%Y-%m-%d %H:%M:%S'))
print(datetime.datetime.fromtimestamp( int(lstLog[0]['ts']) /1000.0).strftime('%W'))
# df =
# df.head()


for n in range(len(lstLog)):
    str_time = datetime.datetime.fromtimestamp( int(lstLog[n]['ts']) /1000.0).strftime('%Y%m%d%H%M%S'), datetime.datetime.fromtimestamp( int(lstLog[n]['ts']) /1000.0).strftime('%H'), datetime.datetime.fromtimestamp( int(lstLog[n]['ts']) /1000.0).strftime('%d'), datetime.datetime.fromtimestamp( int(lstLog[n]['ts']) /1000.0).strftime('%W'), datetime.datetime.fromtimestamp( int(lstLog[n]['ts']) /1000.0).strftime('%m'), datetime.datetime.fromtimestamp( int(lstLog[n]['ts']) /1000.0).strftime('%Y'), datetime.datetime.fromtimestamp( int(lstLog[n]['ts']) /1000.0).strftime('%w')

   # print(time_table_insert)
    #print(str_time)

    cur.execute(time_table_insert, str_time)
    conn.commit()


#df =
#df.head()

#t =
#t.head()

#time_data = ()
#column_labels = ()

#time_df =
#time_df.head()

#for i, row in time_df.iterrows():
#    cur.execute(time_table_insert, list(row))
#    conn.commit()

#user_df =

for u in range(len(lstLog)):
    str_time = str(lstLog[u]['userId']), str(lstLog[u]['firstName']), str(lstLog[u]['lastName']), str(lstLog[u]['gender']), str(lstLog[u]['level'])
    cur.execute(user_table_insert, str_time)
    conn.commit()

#for i, row in user_df.iterrows():
#    cur.execute(user_table_insert, row)
#    conn.commit()

for index, row in df.iterrows():

    # get songid and artistid from song and artist tables
   # print(song_select)
   # print(row.title)
   # print(row.artist_name)
   # print(row.duration)
    conn.commit()
    cur.execute(song_select, (row.title, row.artist_name, str(row.duration)))
    results = cur.fetchone()

    if results:
        songid, artistid = results
    else:
        songid, artistid = None, None

    # insert songplay record

    print(index.start_time, str(lstLog[row]['userId']), df.level, songId, artistId, df.sessionId, df.location, df.user_agent)
    songplay_data = ()
    cur.execute(songplay_table_insert, songplay_data)
    conn.commit()

conn.close()