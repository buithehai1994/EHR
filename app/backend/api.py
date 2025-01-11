from fastapi import FastAPI, HTTPException
import pandas as pd
import requests
import io
import sqlite3

app = FastAPI()

# Dropbox URL
DROPBOX_URL = "https://www.dropbox.com/scl/fi/kyihmcph1cfk3ynqsr2hn/filtered_df.pkl?rlkey=uzfs7lsir37hqywuxtlq1ko1r&st=hd66boln&dl=1"

@app.get("/")
def read_root():
    return {"message": "Welcome to the Pickle File Reader API"}

@app.get("/read-pickle")
def read_pickle():
    """
    Fetch and load the pickle file from Dropbox, and return the content as JSON.
    """
    try:
        # Fetch the file from Dropbox
        response = requests.get(DROPBOX_URL)
        response.raise_for_status()  # Raise an error for unsuccessful requests
        
        # Load the pickle file into DataFrame
        data = pd.read_pickle(io.BytesIO(response.content))
        
        # Convert DataFrame to JSON (optional: serialize with proper settings)
        json_data = data.to_json(orient="records")
        return {"data": json_data}
    
    except requests.RequestException as req_err:
        raise HTTPException(status_code=500, detail=f"Request error: {str(req_err)}")
    except pd.errors.EmptyDataError:
        raise HTTPException(status_code=500, detail="No data found in the pickle file.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/query-sql")
def query_sql(sql_query: str):
    """
    Execute a given SQL query on the loaded DataFrame.
    """
    try:
        # Fetch the file from Dropbox
        response = requests.get(DROPBOX_URL)
        response.raise_for_status()  # Raise an error for unsuccessful requests
        
        # Load the pickle file into DataFrame
        df = pd.read_pickle(io.BytesIO(response.content))
        
        # Convert the DataFrame to an in-memory SQLite database
        conn = sqlite3.connect(":memory:")
        df.to_sql('medical_data', conn, index=False, if_exists='replace')
        
        # Execute the SQL query
        result = pd.read_sql(sql_query, conn)
        
        # Close the connection
        conn.close()
        
        return {"query_result": result.to_json(orient="records")}
    
    except requests.RequestException as req_err:
        raise HTTPException(status_code=500, detail=f"Request error: {str(req_err)}")
    except pd.errors.EmptyDataError:
        raise HTTPException(status_code=500, detail="No data found in the pickle file.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
