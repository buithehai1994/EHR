from fastapi import FastAPI, HTTPException, Query
from pyspark.sql import SparkSession
import os

app = FastAPI()

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Dynamic Parquet Data Loader") \
    .getOrCreate()

# Path to the data folder
DATA_FOLDER = "data"


# Function to load a specific Parquet file into a Spark DataFrame
def load_parquet_file(file_name: str):
    file_path = os.path.join(DATA_FOLDER, file_name)
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail=f"File '{file_name}' not found.")
    
    try:
        return spark.read.parquet(file_path)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error loading Parquet file '{file_name}': {str(e)}")


# Endpoint to list available Parquet files
@app.get("/list_files")
async def list_files():
    if not os.path.exists(DATA_FOLDER):
        raise HTTPException(status_code=404, detail="Data folder not found.")
    
    files = [f for f in os.listdir(DATA_FOLDER) if f.endswith('.parquet')]
    if not files:
        raise HTTPException(status_code=404, detail="No Parquet files found.")
    
    return {"files": sorted(files)}


# Endpoint to fetch data from a specified number of Parquet files
@app.get("/get_data")
async def get_data(
    num_files: int = Query(..., ge=1, description="Number of Parquet files to load (starting from the first one)")
):
    # List all Parquet files
    if not os.path.exists(DATA_FOLDER):
        raise HTTPException(status_code=404, detail="Data folder not found.")

    files = sorted([f for f in os.listdir(DATA_FOLDER) if f.endswith('.parquet')])
    if not files:
        raise HTTPException(status_code=404, detail="No Parquet files found.")
    
    # Validate the number of files to load
    if num_files > len(files):
        raise HTTPException(
            status_code=400,
            detail=f"Requested {num_files} files, but only {len(files)} are available."
        )

    # Load the specified number of Parquet files
    selected_files = files[:num_files]
    dataframes = []
    for file_name in selected_files:
        df = load_parquet_file(file_name)
        dataframes.append(df)

    # Concatenate the DataFrames
    combined_df = dataframes[0]
    for df in dataframes[1:]:
        combined_df = combined_df.union(df)

    # Convert to Pandas DataFrame
    try:
        pandas_df = combined_df.toPandas()
        return pandas_df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error converting DataFrame: {str(e)}")
