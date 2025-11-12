# Premier League ETL Pipeline

An ETL pipeline that ingests Premier League team and standings data from two different football APIs, standardizes it into a unified schema, and loads it into Google BigQuery.

---

## Overview

This pipeline solves a common data engineering challenge: **integrating data from multiple APIs with different structures into a single, standardized format**. It fetches team information and standings from both API-Sports and API-Football, transforms them into a unified 15-field schema, and loads the data into separate BigQuery datasets for comparison and analysis.

---

## Architecture

### High-Level Flow
```
┌─────────────────┐     ┌─────────────────┐
│   API-Sports    │     │  API-Football   │
│  (Source 1)     │     │   (Source 2)    │
└────────┬────────┘     └────────┬────────┘
         │                       │
         │  HTTP GET             │  HTTP GET
         │                       │  
         │                       │
         ▼                       ▼
┌─────────────────┐     ┌─────────────────┐
│  Extract Layer  │     │  Extract Layer  │
│  - Fetch data   │     │  - Fetch data   │
│                 │     │                 │
└────────┬────────┘     └────────┬────────┘
         │                       │
         │  JSON                 │  JSON
         │                       │
         ▼                       ▼
┌─────────────────┐     ┌─────────────────┐
│ Transform Layer │     │ Transform Layer │
│ - Validate      │     │ - Validate      │
│ - Flatten       │     │ - Flatten       │
│ - Standardize   │     │ - Standardize   │
└────────┬────────┘     └────────┬────────┘
         │                       │
         │  DataFrame            │  DataFrame
         │                       │  
         │                       │
         ▼                       ▼
┌─────────────────┐     ┌─────────────────┐
│   Load Layer    │     │   Load Layer    │
│ - BigQuery      │     │ - BigQuery      │
│ - CSV Export    │     │ - CSV Export    │
└─────────────────┘     └─────────────────┘
```

### Design Decisions

**Modular Architecture:** The pipeline is split into five files (config, extract, transform, load, main), each with a single responsibility. This makes testing easier and allows components to evolve independently.

**Independent Data Flows:** API-Sports and API-Football are processed separately. If API-Football fails (which happened during testing), API-Sports data still loads successfully. This is critical for production reliability.

**Separate Storage:** Each API source gets its own BigQuery dataset. This preserves data lineage, enables quality comparison between sources, and makes debugging easier when issues arise.

**Base Classes:** Both extractors and transformers inherit from base classes that handle common logic (retry mechanisms, error tracking, logging). This reduces code duplication and makes adding a third API source straightforward.

---

## Setup & Usage

### Prerequisites

- Python 3.8+
- Google Cloud Platform account with BigQuery enabled
- API keys from [API-Sports](https://api-sports.io/) and [API-Football](https://apifootball.com/)
- Gmail account with app password (for email alerts)

### Installation
```bash
# Clone repository
git clone <repository-url>
cd premier-league-pipeline

# Install dependencies
pip install -r requirements.txt
```

### Configuration

Create a `.env` file in the project root:
```bash
# API Keys
API_SPORTS_KEY=your_api_sports_key_here
API_FOOTBALL_KEY=your_api_football_key_here

# Google Cloud Platform
GCP_PROJECT_ID=your-gcp-project-id
BIGQUERY_DATASET_API_SPORTS=api_sports_data
BIGQUERY_DATASET_API_FOOTBALL=api_football_data
GOOGLE_APPLICATION_CREDENTIALS=./bigquery_credentials.json

# League Configuration
SEASON=2023
API_SPORTS_LEAGUE=39
API_FOOTBALL_LEAGUE=152

# Email Alerts
ALERT_EMAIL=your_email@gmail.com
EMAIL_PASSWORD=your_gmail_app_password
```

### BigQuery Setup

1. Create a GCP project
2. Enable BigQuery API
3. Create a service account with BigQuery Admin role
4. Download the JSON key file and save as `bigquery_credentials.json`

### Running the Pipeline
```bash
python main.py
```

**Expected Output:**
```
Starting ETL Pipeline...
**************************************************

[1/2] Processing API-Sports data...
  Data saved to api_sports_teams.json
  Data saved to api_sports_standings.json
  Transformed 20 teams, skipped 0
  Exported to CSV: api_sports_teams_standings.csv
  Loading 20 rows to BigQuery...
  Successfully loaded to BigQuery
API-Sports pipeline completed successfully!

[2/2] Processing API-Football data...
  Attempt 1/3 for API-Football get_teams...
  Data saved to api_football_teams.json
  Transformed 20 teams, skipped 0
  Exported to CSV: api_football_teams_standings.csv
  Successfully loaded to BigQuery
API-Football pipeline completed successfully!

**************************************************
ETL Pipeline finished!

Generated files:
  - api_sports_teams_standings.csv
  - api_football_teams_standings.csv

==================================================
ERROR SUMMARY
==================================================
No errors encountered!
==================================================

Email notification sent: SUCCESS: ETL Pipeline Completed Successfully
```

---

## Schema Design

The pipeline transforms both APIs into a standardized **15-field schema** that combines team information with performance metrics. See [SCHEMA.md](SCHEMA.md) for complete documentation.

**Handling API differences:**

| Challenge | API-Sports | API-Football | Solution |
|-----------|-----------|--------------|----------|
| Team ID | Nested: `team.id` | Flat: `team_key` | Extract to `team_id` |
| Stadium name | `venue.name` | `venue.venue_name` | Map to `stadium` |
| Goal difference | Provided: `goalsDiff` | Not provided | Calculate: GF - GA |
| Response format | Dict with `response` key | List | Handle in validation |

The transformation layer abstracts these differences, providing a clean interface regardless of source.

---

## Error Handling & Alerting

The pipeline implements a **three-layer error strategy**:

### 1. Prevention (Validation)
Before transforming data, we validate:
- Response structure (is the data shaped correctly?)
- Critical fields (do key fields exist?)
- Required values (team_id, name, rank, points must not be null)

If APIs change their schema, validation catches it immediately and logs specific details about what changed.

### 2. Detection (Logging & Tracking)
Every operation is wrapped in try-except blocks. Errors are:
- Logged to `etl_pipeline.log` with timestamps and severity
- Tracked in memory by category (api_sports, api_football, validation, transformation, load)
- Summarized at the end of each run

### 3. Notification (Email Alerts)
Automated email notifications sent for:
- **Success**: Both APIs processed successfully
- **Partial failure**: One API failed, one succeeded
- **Critical failure**: Both APIs failed

**Retry Logic:**
Both APIs use the same retry strategy: 3 attempts with exponential backoff (5s, 10s, 15s). This ensures transient network issues or temporary API unavailability don't cause pipeline failures.

---

## Technology Stack

**Honest answer:** I chose technologies I know well from building production ETL pipelines at CompiraLabs.

- **Python**: My go-to for data engineering. Rich ecosystem, readable, maintainable.
- **BigQuery**: Used it extensively at CompiraLabs. Serverless, scalable, and I'm comfortable with its SQL dialect and quirks.
- **Pandas**: Standard for data transformation. DataFrame operations are intuitive and efficient for this data size.
- **OOP Design**: Experience building modular pipelines taught me the value of classes and inheritance for maintainability.
- **Gmail SMTP**: Simple alerting without external dependencies. For production scale, I'd use SendGrid.

**The pragmatic truth:** When you know a tool well, you can focus on solving the problem instead of fighting the technology. These choices let me build a robust pipeline quickly while demonstrating production-ready patterns I've used in real systems.

---

## Orchestration (Demonstration)

This section demonstrates how the pipeline would be scheduled for periodic runs in production. **Note:** This is not implemented, just documented to show deployment thinking.

### Kubernetes CronJob Approach

For production deployment, I'd containerize the pipeline and run it as a Kubernetes CronJob (based on my experience orchestrating data pipelines with K8s at CompiraLabs).

**Files:**
- `deployment/Dockerfile`: Containerizes the pipeline
- `deployment/kubernetes-cronjob.yaml`: Schedules daily runs at 2 AM
- `deployment/kubernetes-secret.yaml`: Manages API keys and credentials

**Why Kubernetes?**
- Familiar from previous work
- Integrates with existing infrastructure
- Resource isolation and management
- Easy to monitor and debug

See the `deployment/` folder for complete configuration examples.

---

## Project Structure
```
premier-league-pipeline/
├── config.py                   # Configuration & logging
├── extract.py                  # API data extraction with retry logic
├── transform.py                # Data validation & transformation
├── load.py                     # BigQuery loading & email alerts
├── main.py                     # Pipeline orchestration
├── .env                        # Environment variables (not in repo)
├── requirements.txt            # Python dependencies
├── README.md                   # This file
├── SCHEMA.md                   # Schema documentation
├── deployment/                 # Kubernetes deployment files (demo)
│   ├── Dockerfile
│   ├── kubernetes-cronjob.yaml
│   └── kubernetes-secret.yaml
└── bigquery_credentials.json   # GCP credentials (not in repo)
```

---

## Production Considerations

**Current State (Assignment):**
- Uses `WRITE_TRUNCATE` (replaces all data each run)
- Processes full season data
- Manual execution

**For Production Deployment:**

1. **Data Loading Strategy**
   - API-Sports: Keep `WRITE_TRUNCATE` (fetches full season snapshot)
   - API-Football: Switch to `WRITE_APPEND` with incremental loading (fetch only new matches)
   - Add `date_added` timestamp for historical tracking

2. **Scheduling**
   - Deploy as Kubernetes CronJob (daily at 2 AM)

3. **Scalability**
   - Current implementation handles 20 teams efficiently
   - For multiple leagues, add table partitioning by league_id
   - For historical analysis, implement slowly changing dimensions

---

## Assumptions & Limitations

**Assumptions:**
- Season 2023 data is complete and stable
- Premier League has 20 teams
- Team IDs remain consistent across API calls
- APIs maintain current schema structure
- Gmail SMTP is accessible (not blocked by firewalls)

**Limitations:**
- **API Rate Limits**: Free tier limits (100 requests/day per API)
- **No Real-time Updates**: Batch processing, not streaming
- **Single Season**: Processes one season at a time
- **Manual Execution**: No automatic scheduling (demonstrated in deployment/ folder)
- **Basic Email Alerts**: Gmail SMTP has rate limits and setup complexity

**Known Issues:**
- API-Football is unreliable (frequent timeouts and connection errors)
- No incremental loading (always full refresh)
- No deduplication logic for multiple daily runs

---

## Future Enhancements

If continuing development:
1. Multi-season support for historical trend analysis
2. REST API endpoint for data access (Flask/FastAPI)
3. Automated testing (unit tests for transformers, integration tests for full pipeline)
4. Schema versioning system for API evolution
5. Incremental loading for API-Football daily updates

---

## License

MIT
