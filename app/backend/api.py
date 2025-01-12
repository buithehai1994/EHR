from fastapi import FastAPI, HTTPException
from pyspark.sql import SparkSession
import os

# Initialize the FastAPI app
app = FastAPI()

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Parquet Data Loader") \
    .getOrCreate()

# Function to load individual parquet files into Spark DataFrames
def load_parquet_files(data_folder='data'):
    parquet_files = [os.path.join(data_folder, f) for f in os.listdir(data_folder) if f.endswith('.parquet')]
    
    if not parquet_files:
        raise HTTPException(status_code=404, detail="No parquet files found in the data folder.")
    
    dataframes = []
    for f in sorted(parquet_files):
        try:
            df = spark.read.parquet(f)  # Read each Parquet file using Spark
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
        # Concatenate each Spark DataFrame into a single DataFrame
        full_dataframe = dataframe_parts[0]  # Start with the first one for simplicity
        for df in dataframe_parts[1:]:
            full_dataframe = full_dataframe.union(df)
    else:
        raise HTTPException(status_code=404, detail="No DataFrames were loaded.")

# Define an endpoint to display the entire concatenated DataFrame
@app.get("/get_dataframe")
async def get_dataframe():
    if 'full_dataframe' not in globals():
        raise HTTPException(status_code=404, detail="DataFrame not loaded.")
    
    # Convert Spark DataFrame to Pandas DataFrame and return
    return full_dataframe.toPandas()  # Convert Spark DataFrame into a pandas DataFrame for return
