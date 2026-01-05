# ZTBD - Multi-Database Steam Games Importer

A comprehensive data pipeline for importing and normalizing Steam gaming datasets across multiple database engines. This project enables performance comparison and analysis of PostgreSQL, MySQL, MongoDB, and Neo4j using real-world gaming data.

## 🎮 Overview

ZTBD (Zaawansowane Technologie Baz Danych - Advanced Database Technologies) is designed to:
- Import large-scale gaming datasets from Kaggle
- Transform and normalize data for optimal database performance
- Support 4 different database engines simultaneously
- Enable comprehensive database performance testing and comparison
- Provide reproducible, deterministic data across all database types

## 📊 Datasets

The project uses three primary datasets from Kaggle:

### 1. Steam Games Dataset
**Source**: [artermiloff/steam-games-dataset](https://www.kaggle.com/datasets/artermiloff/steam-games-dataset/data?select=games_march2025_cleaned.csv)
- ~85,000+ games from Steam store
- Comprehensive game metadata, pricing, reviews, platforms
- Release dates, developers, publishers, genres, categories
- User engagement metrics (playtime, ownership estimates)

### 2. Steam Reviews Dataset
**Source**: [najzeko/steam-reviews-2021](https://www.kaggle.com/datasets/najzeko/steam-reviews-2021)
- ~21 million user reviews across 300+ games
- Review text, recommendations, timestamps
- Author statistics (playtime, games owned, review history)
- Voting data (helpful/funny votes)

### 3. How Long to Beat Dataset
**Source**: [zaireali/howlongtobeat-games-scraper-2162025](https://www.kaggle.com/datasets/zaireali/howlongtobeat-games-scraper-2162025)
- Completion time estimates for games
- Main story, completionist, and extras playtime data
- User engagement and completion statistics
- Cross-referenced with Steam games by name matching

## 🗄️ Supported Databases

| Database   | Type          | Version    | Use Case                    |
|------------|---------------|------------|-----------------------------|
| PostgreSQL | Relational    | 18.0       | Complex queries, JSONB      |
| MySQL      | Relational    | 8.x        | Traditional SQL workloads   |
| MongoDB    | Document      | 4.15+      | Flexible schema, embedded   |
| Neo4j      | Graph         | 6.0+       | Relationship traversal      |

## 🚀 Installation

### Prerequisites

- Python 3.13+
- Docker & Docker Compose (for databases)
- Kaggle API credentials

### 1. Clone Repository

```bash
git clone <repository-url>
cd ztbd
```

### 2. Install Python Dependencies

Using Poetry (recommended):
```bash
poetry install
```

Or using pip:
```bash
pip install -r requirements.txt
```

### 3. Configure Kaggle API

Create `~/.kaggle/kaggle.json`:
```json
{
  "username": "your-kaggle-username",
  "key": "your-api-key"
}
```

```bash
chmod 600 ~/.kaggle/kaggle.json
```

### 4. Setup Databases

#### PostgreSQL
```bash
cd postgresql
docker-compose up -d
```

#### MongoDB
```bash
cd mongodb
docker-compose up -d
```

#### Neo4j
```bash
cd neo4j
docker-compose up -d
```

#### MySQL
```bash
cd mysql
docker-compose up -d
```

### 5. Configure Environment

Create `.env` file:
```env
# PostgreSQL
SQLALCHEMY_DATABASE_URL=postgresql://user:password@localhost:5432/steamdb
POSTGRES_DB=steamdb
POSTGRES_USER=user
POSTGRES_PASSWORD=password

# MySQL
MYSQL_DATABASE_URL=mysql+pymysql://user:password@localhost:3306/steamdb

# MongoDB
MONGO_URI=mongodb://user:password@localhost:27017/
DATABASE_NAME=steamdb

# Neo4j
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=password

# Cache
CACHE_DIR=./cache
```

---

## 📖 Usage

### Basic Import

Import to all databases:
```bash
python main.py --databases all --drop-all
```

### Selective Import

Import to specific databases:
```bash
python main.py --databases postgresql mysql
```

### Limit Reviews

Import with limited reviews (for testing):
```bash
python main.py --databases all --reviews-limit 100000
```

### Skip Datasets

Skip specific datasets:
```bash
python main.py --skip-reviews --skip-hltb
```

### Use Cached Data

Reuse previously prepared data:
```bash
python main.py --databases all --use-cache
```

### Complete Example

```bash
# Full import with all features
python main.py \
  --databases postgresql mysql mongodb neo4j \
  --reviews-limit 1000000 \
  --drop-all \
  --use-cache
```

### Command-Line Options

| Option             | Description                              | Default      |
|--------------------|------------------------------------------|--------------|
| `--databases`      | Target databases (or 'all')              | all          |
| `--reviews-limit`  | Maximum reviews to import                | 1000000      |
| `--skip-games`     | Skip games dataset                       | False        |
| `--skip-reviews`   | Skip reviews dataset                     | False        |
| `--skip-hltb`      | Skip HLTB dataset                        | False        |
| `--drop-all`       | Drop existing tables before import       | False        |
| `--use-cache`      | Use cached prepared data                 | False        |

---

## IMPORT SUMMARY

Only datasets from keggle

```
MONGODB:
 Games: 89618
 Reviews: 100000

  Timings:
   Import Time: 56.70826959609985
   Verify Time: 0.18144679069519043
   Drop Time: 0.07973814010620117

POSTGRESQL:
 Games: 89618
 Reviews: 1000000

  Timings:
   Import Time: 90.76917624473572
   Verify Time: N/A
   Drop Time: 0.30413246154785156

NEO4J:
   Games: 89618
   Reviews: 1851779
   Developers: 60096
   Genres: 33
  Timings:
   Import Time: 584.9438633918762
   Verify Time: 71.1950330734253
   Drop Time: 40.20628261566162

MYSQL:
  Games: 89618
  Reviews: 1000000
  HLTBs: 147474
 Timings:
  Import Time: 211.13s
  Verify Time: 0.00s
  Drop Time: 0.62s
```

---
With normalized tables

```
 POSTGRESQL:
   Games: 89618
   Reviews: 1000000
   HLTBs: 147474
  Timings:
   Import Time: 153.59s
   Verify Time: 0.00s
   Drop Time: 0.50s

 MYSQL:
   Games: 89618
   Reviews: 1000000
   HLTBs: 147474
  Timings:
   Import Time: 329.89s
   Verify Time: 0.00s
   Drop Time: 7.65s

 MONGODB:
   Games: 89618
   Reviews: 1000000
   HLTBs: 147474
  Timings:
   Import Time: 82.66s
   Verify Time: 0.36s
   Drop Time: 0.05s

 NEO4J:
   Games: 89618
   Reviews: 1851779
   Developers: 60096
  Timings:
   Import Time: 29211.13s
   Verify Time: 71.1950330734253
   Drop Time: 40.20628261566162

```

## 📁 Project Structure

```
ztbd/
├── src/
│   └── ztbd/
│       ├── __init__.py
│       ├── ztbdf.py              # DataFrame wrapper with data operations
│       ├── normalizer.py         # Data normalization logic (NEW)
│       ├── db_manager.py         # Database import orchestration
│       ├── helper.py             # Utility functions (deprecated)
│       ├── postgresql/
│       │   ├── __init__.py
│       │   ├── database.py       # SQLAlchemy engine setup
│       │   ├── models.py         # ORM models (includes normalized tables)
│       │   └── importer.py       # PostgreSQL import logic
│       ├── mysql/
│       │   ├── __init__.py
│       │   ├── database.py
│       │   ├── models.py         # MySQL-specific models
│       │   └── importer.py
│       ├── mongodb/
│       │   ├── __init__.py
│       │   └── importer.py       # MongoDB import logic
│       └── neo4j/
│           ├── __init__.py
│           └── importer.py       # Neo4j import logic
├── postgresql/
│   └── docker-compose.yaml       # PostgreSQL + pgAdmin
├── mysql/
│   └── docker-compose.yaml       # MySQL + phpMyAdmin
├── mongodb/
│   └── docker-compose.yaml       # MongoDB + Mongo Express
├── neo4j/
│   └── docker-compose.yaml       # Neo4j + Browser
├── cache/                         # Cached prepared DataFrames
├── logs/                          # Application logs
├── main.py                        # Entry point
├── verify_ids.py                  # ID verification script (NEW)
├── pyproject.toml                 # Poetry dependencies
├── .env                           # Environment configuration
└── README.md                      # This file
```

---

## 🧪 Testing & Comparison
#### WIP

### Performance Testing

The normalized schema enables comprehensive database performance comparisons:

## 🏗️ Architecture

### Original Tables (Denormalized)

The project starts with three main tables containing denormalized data:

#### `games` Table
Core game information with embedded JSON arrays for relationships.

| Column                    | Type    | Description                           |
|---------------------------|---------|---------------------------------------|
| appid                     | Integer | Steam App ID (Primary Key)           |
| name                      | String  | Game title                            |
| release_date              | Date    | Release date                          |
| price                     | Float   | Current price in USD                  |
| developers                | JSON    | Array of developer names              |
| publishers                | JSON    | Array of publisher names              |
| genres                    | JSON    | Array of genre names                  |
| categories                | JSON    | Array of category names               |
| tags                      | JSON    | Dictionary of tags with vote counts   |
| metacritic_score          | Integer | Metacritic review score               |
| positive/negative         | Integer | Steam review counts                   |
| average_playtime_forever  | Integer | Average playtime in minutes           |
| supported_languages       | JSON    | Available languages                   |
| screenshots               | JSON    | Screenshot URLs                       |
| movies                    | JSON    | Video/trailer URLs                    |

#### `reviews` Table
User reviews with embedded author statistics.

| Column                        | Type      | Description                        |
|-------------------------------|-----------|-------------------------------------|
| review_id                     | BigInt    | Unique review ID (Primary Key)     |
| app_id                        | Integer   | References game                    |
| app_name                      | String    | Game name                          |
| review                        | Text      | Review text content                |
| recommended                   | Boolean   | Positive/negative recommendation   |
| timestamp_created             | BigInt    | Review creation timestamp          |
| author_steamid                | BigInt    | Author's Steam ID                  |
| author_num_games_owned        | Integer   | Author's game library size         |
| author_playtime_forever       | Float     | Author's total playtime            |
| author_playtime_at_review     | Float     | Playtime when review was written   |
| votes_helpful                 | Integer   | Helpful vote count                 |
| steam_purchase                | Boolean   | Purchased on Steam                 |

#### `hltb` Table
Game completion time estimates from HowLongToBeat.

| Column                | Type    | Description                           |
|-----------------------|---------|---------------------------------------|
| game_game_id          | Integer | HLTB game ID (Primary Key)           |
| game_game_name        | String  | Game title                            |
| game_comp_main        | Integer | Main story completion time (minutes)  |
| game_comp_plus        | Integer | Main + extras time (minutes)          |
| game_comp_100         | Integer | 100% completion time (minutes)        |
| game_comp_all_count   | Integer | Number of completion submissions      |

---

## 🔄 Normalized Schema

To enable comprehensive database performance testing, the project generates **13 additional normalized tables** with **deterministic IDs** that are identical across all database engines.

### Dimension Tables

These tables extract and normalize the embedded JSON data from the `games` table.

#### `developers`
Unique game developers with statistics.

| Column        | Type    | Description                          |
|---------------|---------|--------------------------------------|
| developer_id  | Integer | Primary Key (deterministic)          |
| name          | String  | Developer name (unique)              |
| game_count    | Integer | Number of games developed            |

**Example Data:**
```
developer_id | name              | game_count
-------------|-------------------|------------
1            | 11 bit studios    | 12
2            | 2K                | 45
3            | Activision        | 78
```

#### `publishers`
Unique game publishers with statistics.

| Column        | Type    | Description                          |
|---------------|---------|--------------------------------------|
| publisher_id  | Integer | Primary Key (deterministic)          |
| name          | String  | Publisher name (unique)              |
| game_count    | Integer | Number of games published            |

#### `genres`
Unique game genres.

| Column      | Type    | Description                          |
|-------------|---------|--------------------------------------|
| genre_id    | Integer | Primary Key (deterministic)          |
| name        | String  | Genre name (unique)                  |
| description | String  | Genre description (nullable)         |

**Example Data:**
```
genre_id | name        | description
---------|-------------|------------
1        | Action      | NULL
2        | Adventure   | NULL
3        | Indie       | NULL
```

#### `categories`
Steam categories (multiplayer, achievements, etc.).

| Column       | Type    | Description                          |
|--------------|---------|--------------------------------------|
| category_id  | Integer | Primary Key (deterministic)          |
| name         | String  | Category name (unique)               |
| description  | String  | Category description (nullable)      |

#### `tags`
User-generated tags with aggregated vote counts.

| Column       | Type    | Description                          |
|--------------|---------|--------------------------------------|
| tag_id       | Integer | Primary Key (deterministic)          |
| name         | String  | Tag name (unique)                    |
| total_votes  | Integer | Aggregated votes across all games    |

**Example Data:**
```
tag_id | name           | total_votes
-------|----------------|-------------
1      | Action         | 2547893
2      | Adventure      | 1823456
3      | Atmospheric    | 987234
```

### Association Tables (Many-to-Many)

These junction tables implement many-to-many relationships between games and dimensions.

#### `game_developers`
Links games to their developers.

| Column       | Type    | Description                          |
|--------------|---------|--------------------------------------|
| game_appid   | Integer | References games.appid               |
| developer_id | Integer | References developers.developer_id   |

**Primary Key:** (game_appid, developer_id)

#### `game_publishers`
Links games to their publishers.

| Column       | Type    | Description                          |
|--------------|---------|--------------------------------------|
| game_appid   | Integer | References games.appid               |
| publisher_id | Integer | References publishers.publisher_id   |

**Primary Key:** (game_appid, publisher_id)

#### `game_genres`
Links games to genres.

| Column       | Type    | Description                          |
|--------------|---------|--------------------------------------|
| game_appid   | Integer | References games.appid               |
| genre_id     | Integer | References genres.genre_id           |

**Primary Key:** (game_appid, genre_id)

#### `game_categories`
Links games to categories.

| Column       | Type    | Description                          |
|--------------|---------|--------------------------------------|
| game_appid   | Integer | References games.appid               |
| category_id  | Integer | References categories.category_id    |

**Primary Key:** (game_appid, category_id)

#### `game_tags`
Links games to tags with per-game vote counts.

| Column       | Type    | Description                          |
|--------------|---------|--------------------------------------|
| game_appid   | Integer | References games.appid               |
| tag_id       | Integer | References tags.tag_id               |
| vote_count   | Integer | Votes for this tag on this game      |

**Primary Key:** (game_appid, tag_id)

**Example Data:**
```
game_appid | tag_id | vote_count
-----------|--------|------------
730        | 15     | 12453      # CS:GO has 12,453 votes for "FPS" tag
570        | 8      | 18734      # Dota 2 has 18,734 votes for "MOBA" tag
```

### Analysis Tables (Pre-computed Aggregations)

These tables contain pre-computed statistics for performance comparison testing.

#### `user_profiles`
Aggregated user statistics extracted from reviews.

| Column                  | Type    | Description                          |
|-------------------------|---------|--------------------------------------|
| author_steamid          | BigInt  | Primary Key (Steam user ID)          |
| num_games_owned         | Integer | Total games in library               |
| num_reviews             | Integer | Total reviews written                |
| total_playtime_minutes  | Float   | Lifetime playtime across all games   |
| first_review_date       | Date    | Date of first review                 |
| last_review_date        | Date    | Date of most recent review           |
| positive_review_count   | Integer | Number of positive reviews           |
| negative_review_count   | Integer | Number of negative reviews           |
| avg_review_length       | Integer | Average review text length           |
| helpful_votes_received  | Integer | Total helpful votes received         |

**Use Case:** User behavior analysis, reviewer credibility scoring.

#### `game_review_summary`
Pre-aggregated review statistics per game.

| Column                     | Type    | Description                          |
|----------------------------|---------|--------------------------------------|
| game_appid                 | Integer | Primary Key (references games)       |
| total_reviews              | Integer | Total review count                   |
| positive_reviews           | Integer | Positive recommendation count        |
| negative_reviews           | Integer | Negative recommendation count        |
| avg_playtime_at_review     | Float   | Average playtime when reviewed       |
| median_playtime_at_review  | Float   | Median playtime when reviewed        |
| avg_helpful_votes          | Float   | Average helpful votes per review     |
| most_common_language       | String  | Most common review language          |
| steam_purchase_ratio       | Float   | Ratio of Steam purchases (0-1)       |
| early_access_review_count  | Integer | Reviews written during early access  |

**Use Case:** Compare aggregation performance (pre-computed vs. on-the-fly).

#### `developer_stats`
Pre-aggregated statistics per developer.

| Column                  | Type    | Description                          |
|-------------------------|---------|--------------------------------------|
| developer_id            | Integer | Primary Key (references developers)  |
| total_games             | Integer | Number of games developed            |
| avg_game_price          | Float   | Average game price                   |
| avg_metacritic_score    | Float   | Average Metacritic score             |
| total_positive_reviews  | Integer | Sum of positive reviews              |
| total_negative_reviews  | Integer | Sum of negative reviews              |
| avg_playtime            | Float   | Average playtime across games        |
| most_common_genre       | String  | Most frequent genre                  |

**Use Case:** Developer portfolio analysis, reputation metrics.

### Time-Series Table

#### `game_price_history`
Simulated historical price data for time-series query testing.

| Column          | Type    | Description                          |
|-----------------|---------|--------------------------------------|
| history_id      | Integer | Primary Key (auto-increment)         |
| game_appid      | Integer | References games.appid               |
| price           | Float   | Price at this point in time          |
| discount_percent| Integer | Discount percentage (0-100)          |
| recorded_date   | Date    | Date of price recording              |

**Data Characteristics:**
- 12 months of historical data per game
- Monthly snapshots with realistic price variations
- Includes discount events (sales)
- Simulated but realistic patterns

**Use Case:** Time-series queries, temporal aggregations, price trend analysis.

---


### Sample Queries

#### PostgreSQL/MySQL
```sql
-- Games by genre with developer info
SELECT g.name, ge.name as genre, d.name as developer, g.price
FROM games g
JOIN game_genres gg ON g.appid = gg.game_appid
JOIN genres ge ON gg.genre_id = ge.genre_id
JOIN game_developers gd ON g.appid = gd.game_appid
JOIN developers d ON gd.developer_id = d.developer_id
WHERE ge.name IN ('Action', 'RPG')
LIMIT 100;
```

#### MongoDB
```javascript
// Games with review summaries
db.games.aggregate([
  {$lookup: {
    from: "game_review_summary",
    localField: "appid",
    foreignField: "game_appid",
    as: "summary"
  }},
  {$unwind: "$summary"},
  {$match: {"summary.total_reviews": {$gt: 1000}}},
  {$limit: 100}
])
```

#### Neo4j
```cypher
// Games by genre with relationships
MATCH (g:Game)-[:HAS_GENRE]->(ge:Genre)
MATCH (g)-[:DEVELOPED_BY]->(d:Developer)
WHERE ge.name IN ['Action', 'RPG']
RETURN g.name, ge.name, d.name, g.price
LIMIT 100
```

### Verification Script
#### WIP
Verify data consistency across databases:



```bash
poetry run python ./tests/verify_ids.py 

================================================================================
VERIFYING DETERMINISTIC IDs ACROSS DATABASES
================================================================================

[1/4] Checking PostgreSQL...
  ✓ Found 10 developers
    ID 1: !CyberApex (SkagoGames)
    ID 2: !ReTigma Studio
    ID 3: "Nieko"
    ID 4: #12
    ID 5: #30A6D-S (Kuma)

[2/4] Checking MySQL...
  ✓ Found 10 developers
    ID 1: !CyberApex (SkagoGames)
    ID 2: !ReTigma Studio
    ID 3: "Nieko"
    ID 4: #12
    ID 5: #30A6D-S (Kuma)

[3/4] Checking MongoDB...
  ✓ Found 10 developers
    ID 1: !CyberApex (SkagoGames)
    ID 2: !ReTigma Studio
    ID 3: "Nieko"
    ID 4: #12
    ID 5: #30A6D-S (Kuma)

[4/4] Checking Neo4j...
  ✓ Found 10 developers
    ID 1: !CyberApex (SkagoGames)
    ID 2: !ReTigma Studio
    ID 3: "Nieko"
    ID 4: #12
    ID 5: #30A6D-S (Kuma)

================================================================================
COMPARISON RESULTS
================================================================================
✓ ID 1: !CyberApex (SkagoGames) - MATCH across all databases
✓ ID 2: !ReTigma Studio - MATCH across all databases
✓ ID 3: "Nieko" - MATCH across all databases
✓ ID 4: #12 - MATCH across all databases
✓ ID 5: #30A6D-S (Kuma) - MATCH across all databases
✓ ID 6: #NVJOB - MATCH across all databases
✓ ID 7: #PragmaBreak - MATCH across all databases
✓ ID 8: #workshop - MATCH across all databases
✓ ID 9: $mitE - MATCH across all databases
✓ ID 10: &y - MATCH across all databases

================================================================================
SUCCESS: All IDs are deterministic and identical across databases!
================================================================================

```

This script:
- Queries each database for dimension table IDs
- Compares first 10 entries across all databases
- Reports any mismatches
- Confirms deterministic ID generation worked correctly

---

## 🔍 Key Features

### Data Processing
- Automatic Kaggle dataset download
- JSON field parsing and normalization
- Duplicate detection and removal
- Timestamp conversion
- Data type validation
- NaN/NULL handling per database

### Database Support
- PostgreSQL with JSONB support
- MySQL with JSON columns
- MongoDB with embedded/referenced documents
- Neo4j with graph relationships
- Automatic index creation
- Constraint enforcement

### Normalized Schema
- 13 additional normalized tables
- Deterministic ID generation
- Many-to-many relationship support
- Pre-computed aggregations
- Time-series data simulation
- Cross-database ID consistency

### Performance & Reliability
- Batch importing (configurable sizes)
- DataFrame caching (pickle-based)
- Comprehensive logging
- Error handling and recovery
- Import verification
- Progress tracking

---

## 📊 Data Statistics

After full import, you'll have approximately:

| Table                  | Rows        | Description                    |
|------------------------|-------------|--------------------------------|
| games                  | 89,618     | Base game information          |
| reviews                | 1,000,000 | User reviews (configurable limit from base ~21,000,000)    |
| hltb                   | 147,474    | Completion time data           |
| developers             | 60,096     | Unique developers              |
| publishers             | 49,839     | Unique publishers              |
| genres                 | 33         | Game genres                    |
| categories             | 40         | Steam categories               |
| tags                   | 452        | User-generated tags            |
| game_developers        | 97,905    | Game-developer associations    |
| game_publishers        | 92,494    | Game-publisher associations    |
| game_genres            | 258,024    | Game-genre associations        |
| game_categories        | 376,153    | Game-category associations     |
| game_tags              | 1,008,987  | Game-tag associations          |
| user_profiles          | 784,348 | Unique reviewer profiles       |
| game_review_summary    | 61        | Per-game review aggregations   |
| developer_stats        | 60,096     | Per-developer statistics       |
| game_price_history     | 980,954  | Historical price data (12mo)   |

**Total Records:** ~5 million across all tables

---

## 🚧 Troubleshooting

### Common Issues

**Issue:** Kaggle download fails
```bash
# Solution: Check API credentials
cat ~/.kaggle/kaggle.json
chmod 600 ~/.kaggle/kaggle.json
```

**Issue:** Database connection refused
```bash
# Solution: Verify Docker containers are running
docker ps
docker-compose logs <service-name>
```

**Issue:** Out of memory during import
```bash
# Solution: Reduce reviews limit or increase Docker memory
python main.py --reviews-limit 500000
```

**Issue:** IDs don't match across databases
```bash
# Solution: Drop all data and re-import
python main.py --databases all --drop-all
python verify_ids.py
```

### Logs

Check application logs for detailed error information:
```bash
tail -f logs/$(date +%Y-%m-%d)-ztbd.log
```

---

## 📝 License

This project is for educational purposes. Please respect the licenses of the underlying datasets:
- Steam Games Dataset: Check Kaggle dataset license
- Steam Reviews Dataset: Check Kaggle dataset license
- HowLongToBeat Dataset: Check Kaggle dataset license


---

## 🎯 Quick Start Checklist

- [ ] Python 3.13+ installed
- [ ] Docker & Docker Compose installed
- [ ] Kaggle API credentials configured
- [ ] Clone repository
- [ ] Install dependencies (`poetry install`)
- [ ] Create `.env` file with database credentials
- [ ] Start database containers (`docker-compose up -d`)
- [ ] Run import: `python main.py --databases all --drop-all`
- [ ] Verify IDs: `python verify_ids.py`
- [ ] Start testing and comparing databases!

**Estimated Time:** 30-60 minutes for setup + 1-3 hours for full import (depending on reviews limit)
