import pandas as pd
from datetime import datetime
import sqlite3 
import json
import glob
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from spotipy.oauth2 import SpotifyOAuth


# Set up the client credentials
client_id = "45619205c59c48e4994e90e4467ba619"
client_secret = "8eb8762881104af3978774e3ce61acaf"
redirect_uri = "http://localhost:8000/callback"
scope = "user-library-read user-top-read"

# Create an instance of the SpotifyOAuth class
sp = spotipy.Spotify(auth_manager=SpotifyOAuth(client_id="45619205c59c48e4994e90e4467ba619",
                                               client_secret="8eb8762881104af3978774e3ce61acaf",
                                               redirect_uri="http://localhost:8000/callback",
                                               scope=scope))

def connect():
    global connection, cursor

    connection = sqlite3.connect('music.db')
    cursor = connection.cursor()

    drop_music = "DROP TABLE IF EXISTS music; "
    cursor.execute(drop_music)

    # find all JSON files in the json_files folder
    file_pattern = os.path.join('json_files', '*.json')
    file_list = glob.glob(file_pattern)

    # loop through the JSON files and load them into the database
    # Create an empty list to store the data from the JSON files
    data_list = []

    # Loop through the JSON files and load them into the data_list
    for file in file_list:
        with open(file, encoding="utf-8") as f:
            data = json.load(f)
        data_list.extend(data)

    # Convert the data_list into a pandas DataFrame
    df = pd.DataFrame(data_list)

    # Add new columns to the DataFrame
    df['track_id'] = ""
    # df['album'] = ""
    df['release_date'] = ""
    df['duration'] = 0
    df['genre'] = ""

    # Retrieve all the song names and artists from the DataFrame
    songs = df[['master_metadata_track_name', 'master_metadata_album_artist_name']].values

    # Loop through the songs and search for track information on the Spotify platform
    # Create a list of search queries
    queries_set = set()

    # Loop through the songs and create search queries
    for song in songs:
        track_name = song[0]
        artist_name = song[1]
        query = f"track:{track_name} artist:{artist_name}"
        queries_set.add(query)
    queries = list(queries_set)
    # Set the batch size for the track search request
    batch_size = 50

    # Make the batch request for track search results in chunks
    results = []
    for i in range(0, len(queries), batch_size):
        chunk = queries[i:i+batch_size]
        result = sp.search(chunk, type='track')
        results.extend(result['tracks']['items'])
        print(i*50)

    # Loop through the rows of the DataFrame
    # album_ids = []
    # added_album_ids = set()
    # batch_size = 20

    for index, row in df.iterrows():
        track_name = row['master_metadata_track_name']
        artist_name = row['master_metadata_album_artist_name']
        # Find the search result for the current track
        result = next((r for r in results if r['name'] == track_name and r['artists'][0]['name'] == artist_name), None)
        if result is not None:
            # Update the row in the DataFrame with the track information
            df.loc[index, 'track_id'] = result['id']
            # df.loc[index, 'album'] = result['album']['name']
            df.loc[index, 'release_date'] = result['album']['release_date']
            df.loc[index, 'duration'] = result['duration_ms']
            # Add the album ID to the list of album IDs, if it has not been added already
            # album_id = result['album']['id']
            # if album_id not in added_album_ids:
            #     album_ids.append(album_id)
            #     added_album_ids.add(album_id)

    # album_results = []
    # for i in range(0, len(album_ids), batch_size):
    #     chunk = album_ids[i:i+batch_size]
    #     result = sp.albums(chunk)
    #     album_results.extend(result['albums'])

    # # Loop through the album results and rows of the DataFrame
    # for index, row in df.iterrows():
    #     # Find the album result for the current album
    #     album_result = next((r for r in album_results if r['id'] == row['album']), None)
    #     if album_result is not None:
    #         # Update the genre in the DataFrame with the album genre
    #         df.loc[index, 'genre'] = album_result['genres'][0]

    df.to_sql("music", connection, if_exists='append')

def getSkippedSongs():
    global connection, cursor
    name = "Bad Bunny"
    start_date = '2022-01-01'
    end_date = '2022-11-31'
    cursor.execute('''Select master_metadata_track_name, count(*) 
        from music 
        where ms_played < 30000 AND date(ts) BETWEEN ? AND ? 
        GROUP BY master_metadata_track_name 
        ORDER BY(count(*)) DESC LIMIT 5''', (start_date,end_date))
    all_songs = cursor.fetchall()
    connection.commit
    print("This are your most skipped songs")
    print(all_songs)
    return

def getTopSongs():
    global connection, cursor
    name = "Bad Bunny"
    start_date = '2022-01-01'
    end_date = '2022-11-31'
    cursor.execute('''Select master_metadata_track_name, genre, count(*) 
        from music 
        where ms_played > 30000 AND date(ts) BETWEEN ? AND ? 
        GROUP BY master_metadata_track_name 
        ORDER BY(count(*)) DESC LIMIT 5''', (start_date,end_date))
    all_songs = cursor.fetchall()
    connection.commit
    print("This are your top 5 songs") 
    print(all_songs)
    return

def getTopArtists():
    global connection, cursor
    name = "Bad Bunny"
    start_date = '2022-01-01'
    end_date = '2022-11-31'
    cursor.execute('''Select master_metadata_album_artist_name, count(*) 
        from music 
        where ms_played > 30000 AND date(ts) BETWEEN ? AND ? 
        GROUP BY master_metadata_album_artist_name 
        ORDER BY(count(*)) DESC LIMIT 5''', (start_date,end_date))
    all_songs = cursor.fetchall()
    connection.commit
    print(all_songs)
    return

def findSongPlays():
    global connection, cursor
    prompt = input("Input the name of the song you want information of: ")
    start_date = '2022-01-01'
    end_date = '2022-11-31'
    cursor.execute('''Select master_metadata_track_name, count(*) 
        from music 
        where ms_played > 30000 AND master_metadata_track_name = ? AND date(ts) BETWEEN ? AND ? 
        GROUP BY master_metadata_track_name 
        ORDER BY(count(*)) DESC LIMIT 5''', (prompt,start_date,end_date))
    all_songs = cursor.fetchall()
    connection.commit
    print(all_songs)
    return

def findArtistPlays():
    global connection, cursor
    prompt = input("Input the name of the artist you want information of: ")
    start_date = '2022-01-01'
    end_date = '2022-11-31'
    cursor.execute('''Select master_metadata_album_artist_name, count(*) 
    from music 
    where ms_played > 30000 AND master_metadata_album_artist_name = ? AND date(ts) BETWEEN ? AND ? 
    GROUP BY master_metadata_album_artist_name 
    ORDER BY(count(*)) DESC LIMIT 5''', (prompt,start_date,end_date))
    all_songs = cursor.fetchall()
    connection.commit

    cursor.execute('''Select min(ts) from music''')
    minn = cursor.fetchall()
    connection.commit
    print(all_songs)
    print(minn)
    return


def main_menu():
    
    Quit = False;
    while(not Quit):
        prompt = input("Select 1 for skipped songs, 2 for top songs, 3 top artists, 4 songs plays, 5 artist plays: ")
        if prompt == '1':

            getSkippedSongs()
        elif prompt == '2':
            getTopSongs()
        elif prompt == '3':
            getTopArtists()
        elif prompt == '4':
            findSongPlays()
        elif prompt == '5':
            findArtistPlays()
        else:
            Quit = True
    return
def main():
    global connection, cursor
    # set up data
    connect()
    main_menu()
    

    connection.commit()
    connection.close()
    return
if __name__ == "__main__":
    main()