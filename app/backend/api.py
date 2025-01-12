from fastapi import FastAPI, HTTPException
import pandas as pd
import os

# Initialize the FastAPI app
app = FastAPI()

# Load all the DataFrames from parquet files on startup and concatenate them into a full DataFrame
@app.on_event("startup")
async def load_dataframe():
    data_folder = 'data'  # Path to your data folder containing parquet files
    parquet_files = [os.path.join(data_folder, f) for f in os.listdir(data_folder) if f.endswith('.parquet')]
    
    if not parquet_files:
        raise HTTPException(status_code=404, detail="No parquet files found in the data folder.")
    
    global full_dataframe
    try:
        full_dataframe = pd.concat(pd.read_parquet(f) for f in sorted(parquet_files), ignore_index=True)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error loading parquet files: {str(e)}")

# Define an endpoint to display the entire reconstructed DataFrame
@app.get("/get_dataframe")
async def get_dataframe():
    if 'full_dataframe' not in globals():
        raise HTTPException(status_code=404, detail="DataFrame not loaded.")
    
    return full_dataframe  # Return the entire DataFrame as a pandas DataFrame
