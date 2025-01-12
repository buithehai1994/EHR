from fastapi import FastAPI, HTTPException
import requests
import pickle

# Dropbox link for direct access
dropbox_link = "https://www.dropbox.com/scl/fi/kyihmcph1cfk3ynqsr2hn/filtered_df.pkl?rlkey=uzfs7lsir37hqywuxtlq1ko1r&st=hd66boln&dl=1"

app = FastAPI()

def load_pickle_from_dropbox(dropbox_link):
    try:
        # Send a GET request to the Dropbox link
        response = requests.get(dropbox_link, stream=True, timeout=10)
        response.raise_for_status()  # Raise an error for bad responses

        # Load the content of the pickle file directly into memory
        data = pickle.load(response.raw)

        return data
    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=500, detail=f"An error occurred while accessing the file: {e}")
    except pickle.PickleError as e:
        raise HTTPException(status_code=500, detail=f"An error occurred while loading the pickle file: {e}")

@app.get("/load_pickle")
async def load_pickle_data():
    data = load_pickle_from_dropbox(dropbox_link)
    return {"message": "Pickle file loaded successfully", "data": data}

# To run this application, use the command:
# uvicorn app:app --reload
