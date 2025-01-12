from fastapi import FastAPI
import pandas as pd

# Initialize the FastAPI app
app = FastAPI()

# Load the data using pandas
file_path = 'data/filtered_df_part_1.parquet'

@app.get("/")
def read_root():
    return {"message": "Welcome to the FastAPI App for Loading Data"}

# Endpoint to load the dataframe and display the first few rows
@app.get("/load_data/")
def load_and_display_data():
    try:
        # Load the parquet file
        df = pd.read_parquet(file_path, engine='pyarrow')
        return df.to_dict(orient="records")
    except FileNotFoundError:
        return {"error": "The file data/filtered_df_part_1.parquet does not exist."}
    except Exception as e:
        return {"error": f"An error occurred: {str(e)}"}
