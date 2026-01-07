# Cross-Database Testing Framework

## Overview

This testing framework allows you to:
- Run identical queries across PostgreSQL, MySQL, MongoDB, and Neo4j
- Measure execution time for each database
- Compare results to verify data consistency
- Generate detailed reports in JSON and Markdown formats

## Project Structure

```
src/ztbd/tests/
├── __init__.py           # Package initialization
├── base_test.py          # Abstract base class for tests
├── test_queries.py       # Concrete test implementations
└── test_runner.py        # Test execution framework

tests.py                  # Main test execution script
test_results/             # Generated test reports (created automatically)
```

## Running Tests

### Basic Usage

```bash
# Run all tests on all databases
poetry run python run_tests.py

# Run tests on specific databases
poetry run python run_tests.py -d postgresql mysql

# Save results to custom directory
poetry run python run_tests.py -o my_results

# Enable verbose logging
poetry run python run_tests.py --verbose
```

### Command Line Options

- `--databases, -d`: Specify which databases to test (default: all)
- `--output, -o`: Output directory for reports (default: test_results)
- `--verbose, -v`: Enable verbose logging
- `--no-comparison`: Skip result comparison step

## Current Tests

### 1. Simple SELECT (simple_select_expensive_games)
Tests basic SELECT with WHERE clause - finds games with price > 50.

**SQL (PostgreSQL/MySQL):**
```sql
SELECT appid, name, price 
FROM games 
WHERE price > 50 
ORDER BY price DESC, appid
LIMIT 100
```

**MongoDB:**
```javascript
db.games.find(
    {price: {$gt: 50}},
    {appid: 1, name: 1, price: 1}
).sort({price: -1, appid: 1}).limit(100)
```

**Neo4j Cypher:**
```cypher
MATCH (g:Game)
WHERE g.price > 50
RETURN g.appid, g.name, g.price
ORDER BY g.price DESC, g.appid
LIMIT 100
```

### 2. Aggregation (count_games_by_genre)
Tests GROUP BY aggregation - counts games per genre.

### 3. JOIN Query (games_with_developers)
Tests table joins - retrieves games with their developers.

### 4. Complex Aggregation (review_stats_by_game)
Tests multiple aggregations - calculates review statistics per game.

### 5. Multi-table Statistics (developer_statistics)
Tests joins with aggregations - developer statistics from multiple tables.

### 6. Time Series (price_history_analysis)
Tests temporal data queries - analyzes price history over time.

### 7. Complex Multi-JOIN (multi_join_game_details)
Tests multiple joins - combines games with developers, genres, and categories.

## Adding New Tests

### Step 1: Create Test Class

Create a new class in `src/ztbd/tests/test_queries.py`:

```python
class MyNewTest(BaseTest):
    """Test description here"""
    
    def __init__(self):
        super().__init__(
            name="my_test_name",
            description="What this test does"
        )
```

### Step 2: Implement Database Methods

Implement the query for each database:

```python
    def run_postgresql(self, engine) -> QueryResult:
        with engine.connect() as conn:
            result = conn.execute(text("""
                -- Your PostgreSQL query here
            """))
            rows = [dict(row._mapping) for row in result]
        return QueryResult(rows=rows, row_count=0, execution_time=0,
                         database='postgresql', test_name=self.name)
    
    def run_mysql(self, engine) -> QueryResult:
        # Same as PostgreSQL for most queries
        with engine.connect() as conn:
            result = conn.execute(text("""
                -- Your MySQL query here
            """))
            rows = [dict(row._mapping) for row in result]
        return QueryResult(rows=rows, row_count=0, execution_time=0,
                         database='mysql', test_name=self.name)
    
    def run_mongodb(self, db) -> QueryResult:
        # MongoDB aggregation pipeline or find query
        pipeline = [
            {'$match': {}},
            # Your pipeline stages
        ]
        rows = list(db.collection_name.aggregate(pipeline))
        return QueryResult(rows=rows, row_count=0, execution_time=0,
                         database='mongodb', test_name=self.name)
    
    def run_neo4j(self, driver) -> QueryResult:
        with driver.session() as session:
            result = session.run("""
                // Your Cypher query here
            """)
            rows = [dict(record) for record in result]
        return QueryResult(rows=rows, row_count=0, execution_time=0,
                         database='neo4j', test_name=self.name)
```

### Step 3: Register Test

Add your test class to the registry at the bottom of `test_queries.py`:

```python
ALL_TESTS = [
    SimpleSelectTest,
    CountByGenreTest,
    # ... existing tests ...
    MyNewTest,  # Add your test here
]
```

### Step 4: Run and Verify

```bash
python run_tests.py --verbose
```

## Tips for Writing Tests

### 1. Keep Queries Equivalent
Each database implementation should return the same logical result:

```python
# Good - same data, different syntax
PostgreSQL: SELECT appid, name FROM games WHERE price > 50
MongoDB:    db.games.find({price: {$gt: 50}}, {appid: 1, name: 1})
Neo4j:      MATCH (g:Game) WHERE g.price > 50 RETURN g.appid, g.name

# Bad - different filtering logic
PostgreSQL: WHERE price > 50
MongoDB:    {price: {$gte: 50}}  # >= instead of >
```

### 2. Use Consistent Ordering
Always specify ORDER BY to ensure deterministic results:

```python
# Good
ORDER BY appid, name

# Bad - results may vary
# No ORDER BY clause
```

### 3. Limit Result Sets
Use LIMIT to keep test execution fast:

```python
LIMIT 100  # Good for tests
LIMIT 1000000  # Too slow
```

### 4. Handle NULLs Consistently
Different databases handle NULL differently:

```python
# PostgreSQL/MySQL
WHERE column IS NOT NULL

# MongoDB
{'column': {'$ne': None}}

# Neo4j
WHERE column IS NOT NULL
```

### 5. Test Different Query Types
- **Simple SELECT**: Basic filtering
- **JOIN**: Multi-table queries
- **Aggregation**: GROUP BY, COUNT, AVG
- **Subquery**: Nested queries
- **Time series**: Date/time operations
- **Text search**: String matching (where supported)

## Understanding Test Results

### Test Output
Each test produces:
1. **Row count**: Number of rows returned
2. **Execution time**: Time in milliseconds
3. **Status**: Success or error message

### Comparison Results
- **Row count match**: Do all databases return the same number of rows?
- **Data sample match**: Do the first 5 rows contain the same data?

### Report Files

**JSON Report** (`test_results_{timestamp}.json`):
```json
{
  "timestamp": "20250107_143022",
  "databases_tested": ["postgresql", "mysql", "mongodb", "neo4j"],
  "total_tests": 7,
  "tests": [
    {
      "name": "simple_select_expensive_games",
      "results": {
        "postgresql": {
          "row_count": 100,
          "execution_time_ms": 23.45,
          "success": true
        }
      }
    }
  ]
}
```

**Markdown Report** (`test_results_{timestamp}.md`):
- Human-readable summary
- Tables showing performance comparison
- Result validation status

## Troubleshooting

### Connection Errors

**Problem**: "Database connection failed"

**Solution**: Check your `.env` file:
```bash
SQLALCHEMY_DATABASE_URL=postgresql://user:pass@localhost/dbname
MYSQL_DATABASE_URL=mysql+pymysql://user:pass@localhost/dbname
MONGO_URI=mongodb://user:pass@localhost:27017/
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=password
```

### Result Mismatch

**Problem**: Row counts don't match across databases

**Possible causes**:
1. Data wasn't imported to all databases
2. Query logic differs between implementations
3. NULL handling differences
4. Floating point precision differences

**Debug steps**:
```bash
# Run with verbose logging
python run_tests.py --verbose

# Check the log file
cat logs/$(date +%Y-%m-%d)-tests.log

# Run test on single database
python run_tests.py -d postgresql
```

### Performance Issues

**Problem**: Tests take too long

**Solutions**:
1. Add indexes to frequently queried columns
2. Reduce LIMIT values in tests
3. Run tests on subset of databases
4. Use `--no-comparison` to skip comparison step

## Best Practices

1. **Write tests that validate actual use cases** - base tests on your real queries
2. **Start simple** - test basic queries before complex ones
3. **Document assumptions** - note if test requires specific data
4. **Keep tests fast** - aim for < 1 second per test per database
5. **Verify results manually first** - run query in each DB to confirm it works
6. **Use appropriate data types** - ensure consistent typing across databases

## Future Enhancements

Potential improvements:
- [ ] Add support for more complex text search queries
- [ ] Implement statistical comparison (beyond row count)
- [ ] Add performance regression detection
- [ ] Support for custom assertions per test
- [ ] Parallel test execution
- [ ] HTML report generation with charts
- [ ] Integration with CI/CD pipelines

## Example: Adding a Custom Test

Here's a complete example of adding a test for "most reviewed games":

```python
class MostReviewedGamesTest(BaseTest):
    """Find games with the most reviews"""
    
    def __init__(self):
        super().__init__(
            name="most_reviewed_games",
            description="Get top 20 games by review count"
        )
    
    def run_postgresql(self, engine) -> QueryResult:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT g.appid, g.name, COUNT(r.review_id) as review_count
                FROM games g
                LEFT JOIN reviews r ON g.appid = r.app_id
                GROUP BY g.appid, g.name
                ORDER BY review_count DESC
                LIMIT 20
            """))
            rows = [dict(row._mapping) for row in result]
        return QueryResult(rows=rows, row_count=0, execution_time=0,
                         database='postgresql', test_name=self.name)
    
    def run_mysql(self, engine) -> QueryResult:
        # Same as PostgreSQL
        return self.run_postgresql(engine)
    
    def run_mongodb(self, db) -> QueryResult:
        pipeline = [
            {'$lookup': {
                'from': 'reviews',
                'localField': 'appid',
                'foreignField': 'app_id',
                'as': 'reviews'
            }},
            {'$project': {
                '_id': 0,
                'appid': 1,
                'name': 1,
                'review_count': {'$size': '$reviews'}
            }},
            {'$sort': {'review_count': -1}},
            {'$limit': 20}
        ]
        rows = list(db.games.aggregate(pipeline))
        return QueryResult(rows=rows, row_count=0, execution_time=0,
                         database='mongodb', test_name=self.name)
    
    def run_neo4j(self, driver) -> QueryResult:
        with driver.session() as session:
            result = session.run("""
                MATCH (g:Game)
                OPTIONAL MATCH (r:Review {app_id: g.appid})
                WITH g, COUNT(r) as review_count
                ORDER BY review_count DESC
                LIMIT 20
                RETURN g.appid as appid, g.name as name, review_count
            """)
            rows = [dict(record) for record in result]
        return QueryResult(rows=rows, row_count=0, execution_time=0,
                         database='neo4j', test_name=self.name)

# Add to ALL_TESTS list
ALL_TESTS = [
    # ... existing tests ...
    MostReviewedGamesTest,
]
```

Then run:
```bash
python run_tests.py --verbose
```

Check the results in `test_results/test_results_*.md`!
