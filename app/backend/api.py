from fastapi import FastAPI, HTTPException
import pandas as pd
import os

# Initialize the FastAPI app
app = FastAPI()

# Function to load individual parquet files into small DataFrames
def load_parquet_files(data_folder='data'):
    parquet_files = [os.path.join(data_folder, f) for f in os.listdir(data_folder) if f.endswith('.parquet')]
    
    if not parquet_files:
        raise HTTPException(status_code=404, detail="No parquet files found in the data folder.")
    
    dataframes = []
    for f in sorted(parquet_files):
        try:
            df = pd.read_parquet(f)
            dataframes.append(df)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error loading parquet file {f}: {str(e)}")
    
    return dataframes

# Load all the DataFrames from parquet files and concatenate them into a single DataFrame on startup
@app.on_event("startup")
async def load_dataframe():
    global full_dataframe
    dataframe_parts = load_parquet_files()
    
    if dataframe_parts:
        # Load each parquet into separate DataFrame and store in a list
        full_dataframe = pd.concat(dataframe_parts, ignore_index=True)
    else:
        raise HTTPException(status_code=404, detail="No DataFrames were loaded.")

# Define an endpoint to display the entire concatenated DataFrame
@app.get("/get_dataframe")
async def get_dataframe():
    if 'full_dataframe' not in globals():
        raise HTTPException(status_code=404, detail="DataFrame not loaded.")
    
    return full_dataframe  # Return the entire DataFrame as a pandas DataFrame
