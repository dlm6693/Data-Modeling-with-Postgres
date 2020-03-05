import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

#helper function to chunk data for efficient loading
def chunk_and_execute(cur, query, data, chunksize):
    """
    Helper function that chunks data and pushes each chunk to the database
    Parameters:
        cur (db cursor object): Object that points to the database
        query (str): SQL query that tells the cursor what to do
        data (list): Datapoints to be injected into the database
        chunksize (int): The size of each chunk
    """
    for i in range(0, len(data), chunksize):
        chunk = data[i:i+chunksize]
        cur.executemany(query, chunk)

def process_song_files(cur, filepaths):
    """
    Function that processes song files, sending them to the artists and songs table
    in the database
    Parameters:
        cur (db cursor object): Object that points to the database
        filepaths (list): List of strings indicating each location of JSON files
        to be processed
    """
    
    dfs = []
    #read each filepath into a dataframe then combine them all into one
    for path in filepaths:
        dfa = pd.read_json(path, lines=True)
        dfs.append(dfa)
    df = pd.concat(dfs).reset_index(drop=True)
    
    # grab all artist values while dropping duplicates that arise in the raw data
    artist_data = list(df[[
                        'artist_id', 
                        'artist_name', 
                        'artist_location', 
                        'artist_latitude', 
                        'artist_longitude'
                                    ]].drop_duplicates().values)
    #insert 1000 artist records at once
    chunk_and_execute(cur=cur, query=artist_table_insert, data=artist_data, chunksize=1000)
    
    # insert 1000 song records at once
    song_data = list(df[['song_id', 'title', 'artist_id', 'year', 'duration']].drop_duplicates().values)
    chunk_and_execute(cur=cur, query=song_table_insert, data=song_data, chunksize=1000)
    
    

def process_log_files(cur, filepaths):
    """
    Function that processes log files, sending them to the time, users and songplays
    tables in the database.
    Parameters:
        cur (db cursor object): Object that points to the database
        filepaths (list): List of strings indicating each location of JSON files
        to be processed
    """
    dfs = []
    for path in filepaths:
        dfa = pd.read_json(path, lines=True)
        dfs.append(dfa)
    df = pd.concat(dfs).reset_index(drop=True)

    # filter by NextSong action and dropping duplicates returned from raw data
    df = df.query('page == "NextSong"')
    df = df.drop_duplicates()
    # convert timestamp column to datetime
    df.ts = df.ts.apply(lambda x: pd.Timestamp(x, unit='ms'))
    
    # insert time data records
    column_labels = [
                    'timestamp',
                    'hour_of_day', 
                    'day_of_month', 
                    'week_of_year', 
                    'month_of_year', 
                    'year', 
                    'day_of_week'
                                ]
    #creating the time dataframe
    time_df = pd.concat(
                        [
                        df['ts'], 
                        df['ts'].dt.hour, 
                        df['ts'].dt.day, 
                        df['ts'].dt.week, 
                        df['ts'].dt.month, 
                        df['ts'].dt.year, 
                        df['ts'].dt.weekday
                            ], 
                            axis=1, keys=column_labels)
    time_values = list(time_df.values)
    chunk_and_execute(cur=cur, query=time_table_insert, data=time_values, chunksize=1000)

    # load user table
    user_df = df[[
                'userId', 
                'firstName', 
                'lastName', 
                'gender', 
                'level'
                    ]]
    user_df.userId = pd.to_numeric(user_df.userId)
    user_df = user_df.drop_duplicates()
    # insert user records
    user_values = list(user_df.values)
    chunk_and_execute(cur=cur, query=user_table_insert, data=user_values, chunksize=1000)
    
    # insert songplay records
    #due to the complex nature of the overall query, one record has to be inserted at a time
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = [
                            index, 
                            row['ts'], 
                            row['userId'], 
                            row['level'], 
                            songid, 
                            artistid, 
                            row['sessionId'], 
                            row['location'], 
                            row['userAgent']
                                            ]
        
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Function that grabs filepaths and processes data, utilizing functions above.
    Parameters: 
        cur (db cursor object): Object that points to the database
        conn (db conncection object): Object that establishes connection with database
        filepath (str): Base filepath to grab all JSON files to process
        func (function): One of the functions above to actually process data
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    func(cur=cur, filepaths=all_files)
    conn.commit()
    print('{} files processed.'.format(len(all_files)))


def main():
    """
    Function that establishes connection to database and processes data
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_files)
    process_data(cur, conn, filepath='data/log_data', func=process_log_files)

    conn.close()


if __name__ == "__main__":
    main()