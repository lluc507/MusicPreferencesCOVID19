# -*- coding: utf-8 -*-
"""
DSCI 510 Homework 5 (Project Milestone 2)
11/27/2020
@author: Leon Luc
"""

#Import packages
import argparse
import pandas as pd
import numpy as np
import re
import time
import requests
from datetime import date, timedelta
from bs4 import BeautifulSoup
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from spotipy.client import SpotifyException
import lyricsgenius as genius
from lyricsgenius import Genius

#1).  Accessing the data
def get_data_by_scraping(grade = False): 
    # Billboard Hot 100 Web Scraping
    start_week = date(2020,3,28) #when most places started shutting down for quarantine
    end_week = date(2020,10,31)
    # this will return all date weeks to be used in the url
    dates = [(start_week + timedelta(days=x)).strftime('%Y-%m-%d') for x in range((end_week-start_week).days + 1) if (start_week + timedelta(days=x)).weekday() == 5]
    
    billboard_data = { #initialize dictionary to store Billboard data
        'song_name':[],
        'artist_name':[],
        'rank':[],
        'rank_change':[],
        'last_rank':[],
        'peak_rank':[],
        'weeks_on_chart':[],
        'chart_week':[]
        }
    
    start = time.time()
    print("Billboard Hot 100 web scraping has started!")
    for chart_week in range(len(dates)):
        if chart_week == 3 and grade == True: #if grading, then do only 3 iterations
            break
        print(dates[chart_week])
        if chart_week in [*range(10,32,10)]:
            time.sleep(10) #take a 10 second break every 10 weeks to slow down requests
        try:
            content = requests.get(f"https://www.billboard.com/charts/hot-100/{dates[chart_week]}", timeout=5)
            content.raise_for_status()
        except requests.exceptions.Timeout as e:
            print ("Timeout Error found. Will continue in 30 seconds:")
            print(e)
            time.sleep(30)
            continue
        except (requests.exceptions.SSLError, requests.exceptions.ConnectionError, requests.exceptions.HTTPError) as e:
            print ("Error found. Will continue in 30 seconds:")
            print(e)
            time.sleep(30)
            continue
        except requests.exceptions.RequestException as e:
            print("Bad error found. Will stop script:")
            raise SystemExit(e) #exit if very bad issue
        else:
            soup = BeautifulSoup(content.content, 'html.parser')
            try:
                top100 = soup.find('ol', {"class":"chart-list__elements"}).find_all('li') #isolate table with the data
            except Exception as e: #if the charts table is not found, then skip to next week
                print("Error found:")
                print(e)
                continue
            else:
                for i in range(len(top100)): #iterating over each song in that week's table
                    song_info = top100[i]
                    #all elements should exist within the source code
                    rank_table = song_info.find('span', {"class":"chart-element__rank"})
                    artist = song_info.find('span', {"class":"chart-element__information__artist"}).text.split('Featuring')[0].strip() 
                    
                    billboard_data['rank'].append(song_info.find('span', {"class":"chart-element__rank__number"}).text)
                    billboard_data['rank_change'].append(rank_table.text.split('\n')[2]) #the class name is not the same for each rank change, so we instead pull the text
                    billboard_data['song_name'].append(song_info.find('span', {"class":"chart-element__information__song"}).text)
                    #want to keep only the first/primary artist... not perfect but will suffice
                    billboard_data['artist_name'].append(artist.split(' X ')[0].split(' x ')[0].split(' With ')[0].split(' Presents ')[0].split('Duet')[0].split(' & ')[0].split(',')[0])
                    billboard_data['last_rank'].append(song_info.find('span', {"class":"chart-element__meta text--center color--secondary text--last"}).text)
                    billboard_data['peak_rank'].append(song_info.find('span', {"class":"chart-element__meta text--center color--secondary text--peak"}).text)
                    billboard_data['weeks_on_chart'].append(song_info.find('span', {"class":"chart-element__meta text--center color--secondary text--week"}).text)
                    billboard_data['chart_week'].append(dates[chart_week])
    print("Billboard Hot 100 web scraping has completed!")
    end = time.time()
    print((end - start)/60) # about 3 minutes

    #convert dictionary to pandas dataframe and create ID columns for track and artist
    billboard_df = pd.DataFrame(billboard_data)
    billboard_df = billboard_df.assign(song_id=billboard_df.groupby(['song_name','artist_name']).ngroup())
    billboard_df = billboard_df.assign(artist_id=billboard_df.groupby(['artist_name']).ngroup())

    #create a dataframe with only the unique tracks, so that the APIs won't duplicate searches
    unique_track = billboard_df.drop_duplicates(subset=['song_id'])
    unique_track = unique_track[['song_name','song_id','artist_name','artist_id']]
    unique_track = unique_track.reset_index(drop=True)
    
    #find songs that have an * in the title and remove them since search issues with Spotify/Genius API
    unique_track[unique_track['song_name'].str.contains("\*")]
    unique_track = unique_track[~unique_track['song_name'].str.contains("\*")]
    unique_track = unique_track.reset_index(drop=True)


    #Spotify API
    cid = 'a5465019d284413898d6128ad598b776'
    secret = 'c53fe03eabf64c8ea31d53c31613df28'
    client_credentials_manager = SpotifyClientCredentials(client_id=cid, client_secret=secret)
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
    
    track_data = { #track features
        'track_id':[], 'track_name':[], #track_id will be the same as song_id from billboard data
        'track_artist_id':[], 'track_artist':[], #track_artist_id will be the same as artist_id from billboard data
        'track_popularity':[], 'track_album':[],
        'track_explicit':[], 'track_duration':[],
        'track_release_date':[],
        #audio features
        'danceability':[], 'energy':[],
        'loudness':[], 'speechiness':[],
        'acousticness':[], 'instrumentalness':[], 'liveness':[],
        'valence':[], 'tempo':[]
        }
    
    artist_data = {
        'artist_id':[], #artist_id will be the same as artist_id from billboard data
        'artist_name':[], 'artist_genre':[],
        'artist_popularity':[], 'artist_followers':[],
        'artist_image':[]
        }
    
    start2 = time.time()
    print("Spotify API scraping has started!")
    artist_list = [] #list to identify unique artists
    for i in range(len(unique_track)):
        if i == 3 and grade == True:
            break
        #print(i)
        #using the track name and artist name from billboard
        track_name = unique_track.iloc[i]['song_name']
        artist_name = unique_track.iloc[i]['artist_name']
        if i in [*range(100,501,100)]:
            time.sleep(10) #10 second break every 100 searches 
        try:
            search_track = sp.search(q=f"artist:{artist_name} track:{track_name}", type="track", limit=1)
        except SpotifyException as e:
            print("Error found. Will continue in 30 seconds:")
            print(e)
            time.sleep(30) #retry after 30 seconds
            continue
        except requests.exceptions.RequestException as e:
            print("Bad error found. Will stop script:")
            raise SystemExit(e)
        else:
            try:
                track_artist_id = search_track['tracks']['items'][0]['artists'][0]['id']
                track_id = search_track['tracks']['items'][0]['id']
                track_data['track_id'].append(unique_track.iloc[i]['song_id'])
                track_data['track_artist_id'].append(unique_track.iloc[i]['artist_id'])
                track_data['track_name'].append(search_track['tracks']['items'][0]['name']) #spotify track name
                track_data['track_artist'].append(search_track['tracks']['items'][0]['artists'][0]['name']) #artist name
                track_data['track_popularity'].append(search_track['tracks']['items'][0]['popularity']) #The popularity is calculated by algorithm and is based, in the most part, on the total number of plays the track has had and how recent those plays are.
                track_data['track_explicit'].append(search_track['tracks']['items'][0]['explicit'])
                track_data['track_album'].append(search_track['tracks']['items'][0]['album']['name'])
                track_data['track_release_date'].append(search_track['tracks']['items'][0]['album']['release_date'])
                time.sleep(0.1)
            except IndexError:
                #possibilities include Spotify failed to search for the song because the title is different or Spotify does not have the song at all in its library
                print(f"{track_name} by {artist_name} was not found in the search.")
                continue
            else:
                try:
                    audio = sp.audio_features(track_id)
                except SpotifyException as e:
                    print("Error found. Will continue in 30 seconds:")
                    print(e)
                    time.sleep(30)
                    continue
                else:
                    try: #all elements should exist if audio features was found
                        track_data['track_duration'].append(audio[0]['duration_ms'])
                        track_data['danceability'].append(audio[0]['danceability'])
                        track_data['energy'].append(audio[0]['energy'])
                        track_data['loudness'].append(audio[0]['loudness'])
                        track_data['speechiness'].append(audio[0]['speechiness'])
                        track_data['acousticness'].append(audio[0]['acousticness'])
                        track_data['instrumentalness'].append(audio[0]['instrumentalness'])
                        track_data['liveness'].append(audio[0]['liveness'])
                        track_data['valence'].append(audio[0]['valence'])
                        track_data['tempo'].append(audio[0]['tempo'])
                        time.sleep(0.1)
                    except Exception:
                        continue
            try:
                artist_info = sp.artist(track_artist_id)
            except SpotifyException as e:
                print("Error found. Will continue in 30 seconds:")
                print(e)
                time.sleep(30)
                continue
            else: #if artist name not in artist list then proceed, dont want to duplicate
                if artist_info['name'] not in artist_list:
                    try:
                        artist_data['artist_id'].append(unique_track.iloc[i]['artist_id'])
                        artist_data['artist_name'].append(artist_info['name'])
                        try:
                            artist_data['artist_genre'].append(artist_info['genres'][0]) #Spotify songs do not have a genre, so we use the first genre that the artist is classified as
                        except IndexError: #some artists have no genre... yet
                            artist_data['artist_genre'].append('')
                        artist_data['artist_popularity'].append(artist_info['popularity']) #The artist’s popularity is calculated from the popularity of all the artist’s tracks.
                        artist_data['artist_followers'].append(artist_info['followers']['total'])
                        artist_data['artist_image'].append(artist_info['images'][0]['url'])
                        artist_list.append(artist_info['name'])
                        time.sleep(0.2)
                    except Exception:
                        continue
    print("Spotify API scraping has completed!")
    end2 = time.time()
    print((end2 - start2)/60) # about 5.5 minutes
    
    #convert dictionary to pandas dataframe for track and artist
    track_df = pd.DataFrame(track_data)
    artist_df = pd.DataFrame(artist_data)

    
    #Genius API
    geniusCreds = 'EnBjxuMP1hxvE9JjAdKA3n2J8maebZpcAzQPnxQygd7dNtgveq1XwziLkNFY8DDU'
    api = genius.Genius(geniusCreds)
    
    lyric_data = {
        'track_id':[], #track_id will be the same as song_id from billboard data
        'track_name':[],
        'track_artist_id':[], #track_artist_id will be the same as artist_id from billboard data
        'track_artist':[],
        'track_lyrics':[]
        }
    
    start3 = time.time()
    print("Genius API scraping has started! This part will take a while if doing all data.")
    for i in range(len(unique_track)): #about 3-6 seconds per song (lyrics make it take long)
        if i == 3 and grade == True:
            break
        #print(i)
        track_name = unique_track.iloc[i]['song_name']
        artist_name = unique_track.iloc[i]['artist_name']
        
        if i in [*range(100,501,50)]:
            time.sleep(15) #15 second break every 50 searches
        try:
            track = api.search_song(title = track_name, artist = artist_name)
        except requests.exceptions.Timeout as e:
            print ("Timeout Error found. Will continue in 30 seconds:")
            print(e)
            time.sleep(30)
            continue
        except requests.exceptions.HTTPError as e:
            print ("Error found. Will continue in 30 seconds:")
            print(e)
            time.sleep(30)
            continue
        except Exception as e:
            print ("Error found. Will continue in 30 seconds:")
            print(e)
            time.sleep(30)
            continue
        except requests.exceptions.RequestException as e:
            print("Bad error found. Will stop script:")
            raise SystemExit(e)
        else:
            try: #meaning, the search found a song
                lyric_data['track_name'].append(track.title)
                lyric_data['track_id'].append(unique_track.iloc[i]['song_id'])
                lyric_data['track_artist_id'].append(unique_track.iloc[i]['artist_id'])
                lyric_data['track_artist'].append(track.artist)
                lyric_data['track_lyrics'].append(track.lyrics)
                time.sleep(0.2)
            except Exception:
                continue
    print("Genius API scraping has completed!")
    end3 = time.time()
    print((end3 - start3)/60)# about 30 minutes
    print((end3 - start)/60) # about 40 minutes for whole process
    
    #convert dictionary to pandas dataframe for lyrics
    lyric_df = pd.DataFrame(lyric_data)
    
    print("The process has finished!")
    return [billboard_df, unique_track, track_df, artist_df, lyric_df]


def get_data_by_loading_files():
    #loading in the "raw" data files
    billboard_df = pd.read_csv(r"data/raw_billboard_data.csv")
    unique_track = pd.read_csv(r"data/raw_unique_track_data.csv")
    track_df = pd.read_csv(r"data/raw_track_data.csv")
    artist_df = pd.read_csv(r"data/raw_artist_data.csv")
    lyric_df = pd.read_csv(r"data/raw_lyric_data.csv")
    return [billboard_df, unique_track, track_df, artist_df, lyric_df]


#2).  Modeling the data
def clean_and_model_data(raw_data):
    #keep the ID creation in the get_data_by_scraping() function for convenience of scraping the API data
    #Billboard data
    billboard_df = raw_data[0]
    #rearrange order of columns
    billboard_df = billboard_df[['song_id','song_name','artist_id','artist_name',
                'rank','rank_change','last_rank','peak_rank','weeks_on_chart','chart_week']]
    unique_track = raw_data[1]    
    
    #Spotify data
    track_df = raw_data[2]
    artist_df = raw_data[3]
    #want to keep standard format for apostrophes
    replacements = {"’": "'"}
    track_df['track_name'].replace(replacements, inplace=True, regex=True)

    #simplify genres to broader groups
    cond = [
        (artist_df['artist_genre'].str.contains("pop", na=False)),
        (artist_df['artist_genre'].str.contains("hip hop|trap", na=False)),
        (artist_df['artist_genre'].str.contains("rap", na=False)),
        (artist_df['artist_genre'].str.contains("r&b", na=False)),
        (artist_df['artist_genre'].str.contains("country", na=False)),
        (artist_df['artist_genre'].str.contains("rock", na=False)),
        (artist_df['artist_genre'].str.contains("edm|house", na=False)),
        (artist_df['artist_genre'] == ""),
        (artist_df['artist_genre'] == artist_df['artist_genre'])
        ]
    genre_list = ["pop","hip hop","rap","r&b", "country", "rock", "edm", "n/a", "other"]
    artist_df['artist_genre_group'] = np.select(cond, genre_list)
    #reorder columns
    artist_df = artist_df[['artist_id','artist_name','artist_genre','artist_genre_group',
                                 'artist_popularity', 'artist_followers', 'artist_image']]
    
    #Genius data
    lyric_df = raw_data[4]
    lyric_df['track_name'].replace(replacements, inplace=True, regex=True)
    #remove [Verse], [Chorus], etc. from lyrics and remove new line
    for i in range(len(lyric_df)):
        lyric_df.loc[i,"track_lyrics"] = re.sub(r'[\(\[].*?[\)\]]', '', lyric_df.loc[i,"track_lyrics"])
        #lyric_df.loc[i,"track_lyrics"]= re.sub(r'\n', ' ', lyric_df.loc[i,"track_lyrics"])

    #create a combined data set that includes all dataframes except for artist_df
    final_df = pd.merge(billboard_df, track_df.iloc[:,[0,*range(5,18)]], how='left', left_on='song_id', right_on='track_id',
         suffixes=('_x', '_y'))
    final_df = pd.merge(final_df, lyric_df.iloc[:,[0,4]], how='left', left_on='song_id', right_on='track_id',
         suffixes=('_x', '_y'))   
    final_df.drop(['track_id_x', 'track_id_y'], axis=1, inplace=True)
    
    return [billboard_df,unique_track,track_df,artist_df,lyric_df, final_df]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", choices=["local", "remote"], help="Get data from local or remote?")
    #can only choose "yes" if want to use grade option
    parser.add_argument("--grade", action='store_true', help="Want to grade? Include this to --source remote (performs 3 iterations only for each data source).")

    args = parser.parse_args()
    location = args.source
    grade = args.grade

    if location == "local" and not grade:
        print('Data is being loaded in from the stored raw data csv files.')
        data = get_data_by_loading_files()
    elif location == "remote" and not grade:
        print('Data is being extracted. Please wait for the process to complete.')
        data = get_data_by_scraping(grade = False)
    elif location == "remote" and grade: #for HW grading
        print('Grading option chosen. Data is being extracted and will go through 3 iterations for each source.')
        data = get_data_by_scraping(grade = True)
    else:
        print("This option does not work. Please try again.")
            
    print("Data is being cleaned and modeled.")
    data = clean_and_model_data(data)
    
    #3).  Store the data: 
    print("Data is being saved to the data model.")
    data[0].to_csv(r"data/billboard_data.csv", index=False,encoding="utf-8-sig")
    data[1].to_csv(r"data/unique_track_data.csv", index=False,encoding="utf-8-sig")
    data[2].to_csv(r"data/track_data.csv", index=False,encoding="utf-8-sig")
    data[3].to_csv(r"data/artist_data.csv", index=False,encoding="utf-8-sig")
    data[4].to_csv(r"data/lyric_data.csv", index=False,encoding="utf-8-sig")
    data[5].to_csv(r"data/combined_data.csv", index=False,encoding="utf-8-sig")
    print('The process has completed. There should be 6 more csv files in the data folder besides the raw data.')

if __name__ == '__main__':
    main()

