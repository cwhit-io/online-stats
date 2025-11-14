# Online Video Statistics Analyzer

A Python application for analyzing video statistics from multiple platforms including YouTube and Vimeo.

## Project Structure

```
online-stats/
├── api.py                 # FastAPI server for programmatic access
├── src/                    # Source code
│   ├── main.py            # Main analytics pipeline
│   ├── vimeo.py           # Vimeo API integration
│   └── youtube.py         # YouTube API integration
├── data/                  # Data files and inputs
├── output/                # Generated reports and outputs
├── Dockerfile             # Docker container definition
├── docker-compose.yml     # Docker Compose configuration
├── requirements.txt       # Python dependencies
├── .env                   # Environment variables (not in git)
├── .gitignore            # Git ignore rules
└── README.md             # This file
```

## Setup

### Prerequisites

- Python 3.11+
- Docker (optional, for containerized deployment)

### Local Development

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd online-stats
   ```

2. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Set up environment variables:
   - Copy `.env.example` to `.env` and fill in your API credentials
   - For YouTube: Set up OAuth credentials and download `client_secret.json`
   - For Vimeo: Get your access token and user ID

### Docker Setup

1. Build and run with Docker Compose:
   ```bash
   docker-compose up --build
   ```

2. Or build and run manually:
   ```bash
   docker build -t online-stats .
   docker run -it --env-file .env -v $(pwd):/app online-stats
   ```

## Usage

### Running the Analytics Pipeline

Process online video statistics for a date range and publish to database:

```bash
# Process analytics for a date range and publish
python src/main.py --start-date 2024-01-01 --end-date 2024-01-31

# Process analytics with dry-run mode (no database changes)
python src/main.py --start-date 2024-01-01 --end-date 2024-01-31 --dry-run

# Process and overwrite existing data
python src/main.py --start-date 2024-01-01 --end-date 2024-01-31 --overwrite
```

### Individual Analytics Scripts

**YouTube Analytics:**

```bash
# Process date range (saves to CSV)
python src/youtube.py --start-date 2024-01-01 --end-date 2024-01-31
```

**Vimeo Analytics:**

```bash
# Process date range (saves to CSV)
python src/vimeo.py --start-date 2024-01-01 --end-date 2024-01-31
```

### Docker Usage

**Run the complete pipeline:**

```bash
docker-compose up --build
```

**Run individual scripts in Docker:**

```bash
# YouTube analytics
docker-compose run --rm online-stats python src/youtube.py

# Vimeo analytics
docker-compose run --rm online-stats python src/vimeo.py

# Publish to database (default)
docker-compose run --rm online-stats python src/main.py

# Process analytics and publish
docker-compose run --rm online-stats python src/main.py --process

# Dry run
docker-compose run --rm online-stats python src/main.py --dry-run

# Overwrite existing data
docker-compose run --rm online-stats python src/main.py --overwrite
```

## API Usage

The application includes a REST API server for programmatic access.

### Starting the API Server

**Local development:**

```bash
# Install API dependencies
pip install fastapi uvicorn

# Start the API server
python api.py
```

**Docker:**

```bash
# Start API server
docker-compose up --build

# API will be available at http://localhost:8000
```

### API Endpoints

**GET /** - API information and links to documentation

**GET /health** - Health check endpoint

**POST /analytics** - Run video analytics

### Running Analytics via API

Send a POST request to `/analytics` with JSON payload:

```bash
curl -X POST "http://localhost:8000/analytics" \
  -H "Content-Type: application/json" \
  -d '{
    "start_date": "2024-01-01",
    "end_date": "2024-01-31",
    "dry_run": false,
    "overwrite": false
  }'
```

**Parameters:**

- `start_date` (required): Start date in YYYY-MM-DD format
- `end_date` (required): End date in YYYY-MM-DD format
- `dry_run` (optional): Set to `true` for dry-run mode (default: `false`)
- `overwrite` (optional): Set to `true` to overwrite existing data (default: `false`)

**Response:**

```json
{
  "task_id": "analytics_2024-01-01_2024-01-31",
  "status": "started",
  "message": "Analytics started for date range 2024-01-01 to 2024-01-31"
}
```

### API Documentation

When the API server is running, visit:

- **Interactive API docs**: `http://localhost:8000/docs`
- **Alternative docs**: `http://localhost:8000/redoc`

## API Requirements

### YouTube

- YouTube Data API v3 enabled
- OAuth 2.0 credentials
- Channel ID

### Vimeo

- Vimeo API access token
- User ID
- Pro/Business account for analytics (Free accounts have limited API access)

## Environment Variables

Create a `.env` file with:

```env
# YouTube API
YOUTUBE_CHANNEL_ID=your_channel_id

# Vimeo API
VIMEO_ACCESS_TOKEN=your_access_token
VIMEO_USER_ID=your_user_id
```

## Development

- Scripts are located in the `src/` directory
- Data files go in `data/`
- Output files are generated in `output/`
- Use Docker for consistent development environment

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test with Docker
5. Submit a pull request

## License

[Add your license here]
