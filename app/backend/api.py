from fastapi import FastAPI
import pandas as pd
import io
import requests

# Initialize the FastAPI app
app = FastAPI()

# Load the DataFrame from Dropbox URL
@app.on_event("startup")
async def load_dataframe():
    url = 'https://www.dropbox.com/scl/fi/kyihmcph1cfk3ynqsr2hn/filtered_df.pkl?rlkey=uzfs7lsir37hqywuxtlq1ko1r&st=hd66boln&dl=1'
    response = requests.get(url)
    response.raise_for_status()  # Ensure the request was successful
    global filtered_df
    filtered_df = pd.read_pickle(io.BytesIO(response.content))

# Define an endpoint to display the DataFrame
@app.get("/get_dataframe")
async def get_dataframe():
    return filtered_df.to_dict(orient="records")  # Return DataFrame as JSON records
