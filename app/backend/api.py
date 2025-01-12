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
        data = pd.read_parquet(file_path, engine='pyarrow')
        
        # Ensure the DataFrame is not empty
        if data.empty:
            return {"error": "The parquet file is empty."}
        
        # Convert DataFrame to a serializable format (JSON)
        result = data.to_json(orient='records')
        
        # Return the JSON response
        return {"data": result}
        
    except FileNotFoundError:
        return {"error": f"The file {file_path} does not exist."}
    except pd.errors.EmptyDataError:
        return {"error": "The parquet file is empty."}
    except Exception as e:
        return {"error": f"An error occurred: {str(e)}"}
