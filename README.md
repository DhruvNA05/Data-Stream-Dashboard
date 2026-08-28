# Data-Stream-Dashboard
React dashboard with live, updated information and data

Command to run producer: python producer.py
Command to run consumer: uvicorn consumer:app --reload --port 8000
Command to run React Frontend: npm run dev

Run it All: docker compose up --build
Deactivate: docker compose down

http://localhost:5173/ is the link to the frontend react site
http://localhost:8080/ for kafka ui