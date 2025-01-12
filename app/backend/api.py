from fastapi import FastAPI, HTTPException
import pandas as pd
import requests
import pickle
import io
import functools
from diskcache import Cache
import os
import sqlite3

app = FastAPI()

# Dropbox URL
DROPBOX_URL = "https://www.dropbox.com/scl/fi/kyihmcph1cfk3ynqsr2hn/filtered_df.pkl?rlkey=uzfs7lsir37hqywuxtlq1ko1r&st=hd66boln&dl=1"
CACHE_PATH = "./cache"  # Path to store the cached file
PERSISTENT_CACHE_FILE = "./cache/df_cache.pkl"  # File path to store the cached DataFrame

# Create cache directory if it doesn't exist
if not os.path.exists(CACHE_PATH):
    os.makedirs(CACHE_PATH)

# Load the data and cache it on first request
def fetch_pickle_file():
    cache_key = "pickle_file"
    
    # Check if the data is already in cache
    if os.path.exists(PERSISTENT_CACHE_FILE):
        print("Cache hit - loading from disk.")
        with open(PERSISTENT_CACHE_FILE, "rb") as f:
            return pickle.load(f)

    try:
        # Send a GET request to the Dropbox link
        response = requests.get(DROPBOX_URL, stream=True)
        response.raise_for_status()  # Raise an error for bad responses

        # Load the content of the pickle file directly into memory
        with io.BytesIO(response.content) as file:
            data = pickle.load(file)

        # Save the dataframe to a persistent cache file
        with open(PERSISTENT_CACHE_FILE, "wb") as f:
            pickle.dump(data, f)

        print("Pickle file loaded and cached successfully.")
        return data
    except requests.exceptions.RequestException as e:
        print(f"An error occurred while accessing the file: {e}")
        raise HTTPException(status_code=500, detail="Failed to access the pickle file")
    except Exception as e:
        print(f"An error occurred while loading the pickle file: {e}")
        raise HTTPException(status_code=500, detail="Failed to load the pickle file")


@app.get("/")
def read_root():
    return {"message": "Welcome to the Pickle File Reader API"}

# Endpoint to interact with the cached dataframe using python
@app.get("/return-data")
def return_data():
    try:
        df = fetch_pickle_file()  # Fetch the cached dataframe
        return df
    except Exception as e:
        raise HTTPException(status_code=500, detail="Failed to fetch the DataFrame from cache")
