# Mafia Backend

FastAPI + WebSocket server that powers the Mafia game rooms and quiz logic.

## Requirements
- Python 3.11+
- pip

## Setup
Create and activate a virtual environment (recommended), then install deps:
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run the server
```bash
uvicorn main:app --reload --port 8000
```
Serves HTTP and WebSocket endpoints on http://localhost:8000.

## Static/frontend assets
The app is configured to serve built frontend files from `../frontend/dist` if present. Ensure the frontend is built (`npm run build`) so `/` returns the static client.

## Notes
- Quiz questions live in `quiz_bank.json`.
- Uses in-memory room state; restarting the server clears active games.
