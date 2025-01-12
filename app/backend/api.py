from fastapi import FastAPI, HTTPException
import pandas as pd
import io
import requests

app = FastAPI()

# Dropbox link
url = 'https://www.dropbox.com/scl/fi/kyihmcph1cfk3ynqsr2hn/filtered_df.pkl?rlkey=uzfs7lsir37hqywuxtlq1ko1r&st=hd66boln&dl=1'

# Load the DataFrame once on app startup
@app.on_event("startup")
async def load_dataframe():
    try:
        response = requests.get(url)
        response.raise_for_status()  # Ensure the request was successful
        global filtered_df
        filtered_df = pd.read_pickle(io.BytesIO(response.content))
    except requests.RequestException as e:
        raise HTTPException(status_code=500, detail="Failed to load data") from e

# Endpoint to get the whole DataFrame
@app.get("/get_whole_dataframe")
async def get_whole_dataframe():
    if 'filtered_df' in globals():
        return filtered_df.to_dict(orient="records")
    else:
        raise HTTPException(status_code=404, detail="Data not loaded")
