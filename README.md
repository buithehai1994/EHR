# README

## Overview

This project consists of a frontend and backend system designed for querying historical patient data from an EHR (Electronic Health Record) and displaying answers in a user-friendly interface. The application is split into two main components:

1. **Frontend**: A Streamlit-based application for asking questions.
2. **Backend**: A FastAPI-based service for querying historical data of patients from EHR to serve as a knowledge base.

---

## URLs

- **Frontend (Ask Questions)**:  
  [Streamlit Frontend](https://streamlit-frontend-jkni.onrender.com/)

- **Backend (Query Historical Data)**:  
  [FastAPI Backend](https://ehr-api-nm8l.onrender.com/)

---

## Features

### Backend (FastAPI)
- Provides an API to query historical data from an EHR dataset.
- Processes requests and serves patient data via RESTful endpoints.
- Handles data loading, validation, and transformation for a knowledge base.

### Frontend (Streamlit)
- Offers a user-friendly interface for asking questions.
- Displays answers and visualizations based on backend responses.
- Connects seamlessly to the FastAPI backend for data retrieval.

---

## How to Use

1. **Ask Questions**:  
   Use the Streamlit frontend at [https://streamlit-frontend-jkni.onrender.com/](https://streamlit-frontend-jkni.onrender.com/) to ask questions about patient data.

2. **Query Historical Data**:  
   Access the FastAPI backend at [https://ehr-api-nm8l.onrender.com/](https://ehr-api-nm8l.onrender.com/) to query patient data directly via API endpoints.

---

## Technologies Used

- **Frontend**: Streamlit (Python)
- **Backend**: FastAPI (Python)
- **Deployment**: Render.com for hosting the frontend and backend.

---

## Deployment Instructions

1. **Backend**:
   - Deployed on Render at [https://ehr-api-nm8l.onrender.com/](https://ehr-api-nm8l.onrender.com/).
   - Accepts API requests to query historical EHR data.

2. **Frontend**:
   - Deployed on Render at [https://streamlit-frontend-jkni.onrender.com/](https://streamlit-frontend-jkni.onrender.com/).
   - Sends questions to the backend and displays the answers.

---

## Development Setup

### Prerequisites
- Python 3.9 or above
- Docker (optional for containerized development)

### Backend
1. Navigate to the `backend` directory.
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
3. Run the FastAPI app locally
  uvicorn app.backend.api:app --reload --host 0.0.0.0 --port 8000
### Frontend
  Run the Streamlit app locally: Update the FastAPI backend URL in the frontend code to point to the correct link and run the following command:
  streamlit run app/frontend/app.py
