import os
import glob
import psycopg2
import pandas as pd
import datetime
from sql_queries import *


def process_song_file(cur, filepath):
    i = 0

    songs_json_data = {}
    songs_json_data = pd.read_json(open(filepath), lines=True)

    # open song file

    df = pd.concat([songs_json_data])

    # insert song record
    song_data = str(df["song_id"]), str(df["title"]), str(df["artist_id"]), str(df["year"]), str(df["duration"])
    cur.execute(song_table_insert, song_data)

    # insert artist record
    artist_data = str(df["artist_id"]), str(df["artist_name"]), str(df["artist_location"]), str(
        df["artist_latitude"]), str(df["artist_longitude"])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    logs_json_data = {}
    logs_json_data = pd.read_json(open(filepath), lines=True)

    # open log file
    df = pd.concat([logs_json_data])

    # filter by NextSong action
    # df =

    # convert timestamp column to datetime
    t = str(df["ts"])

    # insert time data records
    time_data = {}
    time_data = datetime.datetime.fromtimestamp(int(t[0][0]) / 1000.0).strftime('%Y%m%d%H%M%S'), str(
        datetime.datetime.fromtimestamp(int(t[0][0]) / 1000.0).strftime('%H')), str(
        datetime.datetime.fromtimestamp(int(t[0][0]) / 1000.0).strftime('%d')), str(
        datetime.datetime.fromtimestamp(int(t[0][0]) / 1000.0).strftime('%W')), str(
        datetime.datetime.fromtimestamp(int(t[0][0]) / 1000.0).strftime('%m')), str(
        datetime.datetime.fromtimestamp(int(t[0][0]) / 1000.0).strftime('%Y')), str(
        datetime.datetime.fromtimestamp(int(t[0][0]) / 1000.0).strftime('%w'))

    column_labels = ("timestamp", "hour", "day", "week of year", "month", "year", "weekday")
    time_df = pd.DataFrame(data=[time_data], index=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, str(row.length)))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (
        index, row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()