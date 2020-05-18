import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import spotipy.util as util

import yaml
import numpy as np
import pandas as pd
import random
import pickle
import os

from concurrent.futures import ThreadPoolExecutor
from concurrent import futures
import multiprocessing
with open ('config.yml') as f:
    config = yaml.load(f,Loader=yaml.FullLoader)
    
username = config['username']
client_id = config['client_id']
client_secret = config['client_secret']
redirect_uri = config['redirect_uri']
scope = 'playlist-modify-private,playlist-modify-public'

client_credentials_manager=SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)

token = util.prompt_for_user_token(username, 
                                   scope=scope, 
                                   client_id=client_id, 
                                   client_secret=client_secret, 
                                   redirect_uri=redirect_uri)

sp = spotipy.Spotify(auth=token,client_credentials_manager=client_credentials_manager)# 


cpus = multiprocessing.cpu_count()
MAX_RESPONSE = 50 #spotipy defned
MAX_OFFSET = 100 #user defined

#General Helpers
def load_pickle(file_path):
    with open(file_path, 'rb') as f:
        result = pickle.load(f)
    return result

def save_pickle(data, file_path):
    with open(file_path, 'wb') as f:
        pickle.dump(data, f)
        
# Query Helpers
def run_single_query(query, sp):
    track_ids = []
    df = pd.DataFrame()
    offset = 0
    while 1:
        response = sp.search(q=query, type ='track', limit=MAX_RESPONSE, offset=offset, market='US')
        num_tracks = len(response['tracks']['items'])
        if num_tracks > 0 and offset < MAX_OFFSET:
            new_track_ids = [response['id'] for response in response['tracks']['items']]
            new_df = pd.DataFrame(sp.audio_features(new_track_ids))
            new_df['popularity'] = [track['popularity'] for track in response['tracks']['items']]
            df = pd.concat((df, new_df))
            offset = offset + 1
        else:
            break
    return df


def query_for_tracks(genre_list, start_year, end_year, track_search_term):
    if track_search_term:
        queries = [f'track:{track_search_term} genre:{genre} year:{start_year}-{end_year}' 
                   for genre in genre_list]
    else:
        queries = [f'genre:{genre} year:{start_year}-{end_year}' 
                   for genre in genre_list]
    
    with ThreadPoolExecutor(max_workers=cpus-1) as executor:
        track_dfs = {executor.submit(run_single_query,query, sp):query for query in queries}

    track_df = pd.concat(([future.result() for future in futures.as_completed(track_dfs)]))

    return track_df


def filter_for_audio_features(df, **audio_features):
    for key in audio_features:
        if audio_features[key]:
            if key == 'mode':
                df = df[(df['mode;'] == audio_features[key])]
            else:
                col_name = key.split('_')[0]
                lower_value, upper_value = audio_features[key]
                df = df[(df[col_name] > lower_value) & (df[col_name] < upper_value)]
    return df


#Other API Helpers
def create_playlist(playlist_name):
    print(f'Creating playlist "{playlist_name}"')
    playlist = sp.user_playlist_create(username, playlist_name, public=True, description='beep boop')
    playlist_id = playlist['id']
    return playlist_id