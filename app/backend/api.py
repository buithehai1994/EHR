from fastapi import FastAPI
import pandas as pd

# Initialize the FastAPI app
app = FastAPI()

# Define the file path
file_path = 'data/chunk_1.json'

@app.get("/")
def read_root():
    return {"message": "Welcome to the FastAPI App for Loading JSON Data"}

# Endpoint to load the dataframe and display the first few rows
@app.get("/load_data/")
def load_and_display_data():
    try:
        # Load the JSON file
        data = pd.read_json(file_path)
        
        # Ensure the DataFrame is not empty
        if data.empty:
            return {"error": "The JSON file is empty."}
        
        # Convert DataFrame to a serializable format (JSON)
        result = data.to_json(orient='records', indent=4)
        
        # Return the JSON response
        return {"data": result}
        
    except FileNotFoundError:
        return {"error": f"The file {file_path} does not exist."}
    except pd.errors.EmptyDataError:
        return {"error": "The JSON file is empty."}
    except Exception as e:
        return {"error": f"An error occurred: {str(e)}"}
