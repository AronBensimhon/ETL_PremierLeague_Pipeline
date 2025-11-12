# Schema Definition

## Overview
This schema standardizes Premier League team and performance data from multiple API sources into a unified 15-field structure.

## SQL DDL (BigQuery)
```sql
CREATE TABLE IF NOT EXISTS teams_standings (
    -- Primary Key
    team_id INT64 NOT NULL,
    
    -- Generic Team Information
    name STRING NOT NULL,
    country STRING,
    founded INT64,
    stadium STRING,
    city STRING,
    capacity INT64,
    
    -- Season Performance Information
    rank INT64 NOT NULL,
    points INT64 NOT NULL,
    goal_diff INT64,
    goals_for INT64,
    goals_against INT64,
    win INT64,
    draw INT64,
    lose INT64
)
OPTIONS(
    description="Premier League teams with season standings"
);
```

## Field Definitions

| Field | Type | Category | Description | Source Endpoint |
|-------|------|----------|-------------|-----------------|
| team_id | INT64 | Identifier | Unique team identifier (Primary Key) | teams, standings |
| name | STRING | Team Info | Official team name | teams |
| country | STRING | Team Info | Team's country | teams |
| founded | INT64 | Team Info | Year team was founded | teams |
| stadium | STRING | Team Info | Home stadium name | teams |
| city | STRING | Team Info | City where team is based | teams |
| capacity | INT64 | Team Info | Stadium seating capacity | teams |
| rank | INT64 | Performance | Current league position | standings |
| points | INT64 | Performance | Total points accumulated | standings |
| goal_diff | INT64 | Performance | Goal difference (GF - GA) | standings |
| goals_for | INT64 | Performance | Total goals scored | standings |
| goals_against | INT64 | Performance | Total goals conceded | standings |
| win | INT64 | Performance | Number of wins | standings |
| draw | INT64 | Performance | Number of draws | standings |
| lose | INT64 | Performance | Number of losses | standings |

## Design Decisions
- **1 identifier**: team_id (primary key for uniqueness)
- **6 team info fields**: Core information about the team and stadium
- **8 performance fields**: Season statistics
  
- **Selected fields**: Key features have been selected from both teams and standings endpoints, representing each team with essential components for future analytics


### Field Selection Rationale

**Team Information:**
- `name`: Essential for identification
- `country`: Context about team origin
- `founded`: Historical context
- `stadium`, `city`, `capacity`: Venue information (location and size)

**Performance Metrics:**
- `rank` & `points`: Core standing indicators
- `win/draw/lose`: Match outcomes breakdown
- `goals_for/goals_against/goal_diff`: Offensive and defensive performance
 **Goal difference calculated**: API-Football doesn't provide it, so we compute it from goals_for and goals_against

### Handling API Differences

| Challenge | Solution |
|-----------|----------|
| Different team IDs | Use each API's native ID in separate datasets |
| Different response structures | Transform both to same schema |
| Different field names for capacity | Map `venue.capacity` and `venue.venue_capacity` to single `capacity` field |
| API-Sports nests data deeply | Extract and flatten to schema |
| API-Football calculates goal_diff | Compute from GF and GA fields |

### Storage Strategy
- **Separate datasets**: Keep API-Sports and API-Football data in separate BigQuery datasets
- **Same schema**: Both conform to identical 15-field structure
- **Enables comparison**: Can join/compare data from both sources

## Data Sources

### API-Sports
- Base URL: `https://v3.football.api-sports.io/`
- Endpoints used: `/teams`, `/standings`
- League ID: 39 (Premier League)
- Season: 2023

### API-Football
- Base URL: `https://apiv3.apifootball.com/`
- Actions used: `get_teams`, `get_standings`
- League ID: 152 (Premier League)

## BigQuery Tables

- `premier-league-pipeline.api_sports_data.teams_standings`
- `premier-league-pipeline.api_football_data.teams_standings`
