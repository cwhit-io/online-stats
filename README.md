# Online Video Statistics Analyzer

A Python application for analyzing video statistics from multiple platforms including YouTube and Vimeo.

## Project Structure

```
online-stats/
├── src/                    # Source code
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
