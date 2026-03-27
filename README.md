# Local Google Maps Lead Generator

A local web app that scrapes Google Maps listings and enriches leads by extracting emails and WhatsApp numbers from business websites.

## Features

- Search by keyword and location
- Scrape up to 500 businesses per run
- Extract:
  - Business name
  - Phone number
  - Website URL
  - Email address (from website/contact pages)
  - WhatsApp number (from website links)
- View results in browser table
- Export to CSV
- Stop in-progress scraping
- CAPTCHA detection and graceful stop

## Project Structure

- `backend/app.py` - Flask API + CSV export
- `backend/scraper.py` - Playwright Google Maps scraper
- `backend/email_extractor.py` - Website email/WhatsApp extraction
- `frontend/index.html` - UI
- `frontend/styles.css` - Styling
- `frontend/script.js` - Client logic
- `output/` - Generated CSV files

## Setup

1. Create and activate virtual environment:

```bash
python -m venv .venv
# Windows PowerShell
.\.venv\Scripts\Activate.ps1
```

2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Install Playwright browser binaries:

```bash
python -m playwright install chromium
```

4. Run backend:

```bash
python backend/app.py
```

5. Open in browser:

- http://127.0.0.1:5000

## API Endpoints

- `POST /scrape`
  - Body: `{ "keyword": "real estate agent", "location": "Rawalpindi", "max_results": 50, "only_with_website": false, "headless": false }`
  - Returns scraped lead list JSON
- `GET /download`
  - Downloads latest CSV file
- `GET /status`
  - Returns scrape status
- `POST /stop`
  - Requests stopping an active run

## Notes

- This project is intended for local use only.
- Google Maps UI changes can break selectors; update selectors in `backend/scraper.py` when needed.
- If Google shows anti-bot challenge/CAPTCHA, scraper stops and returns an error.
