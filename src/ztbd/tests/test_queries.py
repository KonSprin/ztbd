"""
Concrete test implementations for cross-database queries
"""
from sqlalchemy import text
from .base_test import BaseTest, QueryResult
import logging

logger = logging.getLogger('ztbd.tests')


class SimpleSelectTest(BaseTest):
    """Test 1: Simple SELECT with WHERE clause - Games with price > 50"""
    
    def __init__(self):
        super().__init__(
            name="simple_select_expensive_games",
            description="Select all games with price greater than 50"
        )
    
    def run_postgresql(self, engine) -> QueryResult:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT appid, name, price 
                FROM games 
                WHERE price > 50 
                ORDER BY price DESC, appid
                LIMIT 100
            """))
            rows = [dict(row._mapping) for row in result]
        return QueryResult(rows=rows, row_count=0, execution_time=0, 
                         database='postgresql', test_name=self.name)
    
    def run_mysql(self, engine) -> QueryResult:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT appid, name, price 
                FROM games 
                WHERE price > 50 
                ORDER BY price DESC, appid
                LIMIT 100
            """))
            rows = [dict(row._mapping) for row in result]
        return QueryResult(rows=rows, row_count=0, execution_time=0,
                         database='mysql', test_name=self.name)
    
    def run_mongodb(self, db) -> QueryResult:
        cursor = db.games.find(
            {'price': {'$gt': 50}},
            {'appid': 1, 'name': 1, 'price': 1, '_id': 0}
        ).sort([('price', -1), ('appid', 1)]).limit(100)
        
        rows = list(cursor)
        return QueryResult(rows=rows, row_count=0, execution_time=0,
                         database='mongodb', test_name=self.name)
    
    def run_neo4j(self, driver) -> QueryResult:
        with driver.session() as session:
            result = session.run("""
                MATCH (g:Game)
                WHERE g.price > 50
                RETURN g.appid as appid, g.name as name, g.price as price
                ORDER BY g.price DESC, g.appid
                LIMIT 100
            """)
            rows = [dict(record) for record in result]
        return QueryResult(rows=rows, row_count=0, execution_time=0,
                         database='neo4j', test_name=self.name)


class CountByGenreTest(BaseTest):
    """Test 2: Aggregation - Count games by genre"""
    
    def __init__(self):
        super().__init__(
            name="count_games_by_genre",
            description="Count number of games for each genre"
        )
    
    def run_postgresql(self, engine) -> QueryResult:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT g.name as genre, COUNT(*) as game_count
                FROM game_genres gg
                JOIN genres g ON gg.genre_id = g.genre_id
                GROUP BY g.name
                ORDER BY game_count DESC, g.name
                LIMIT 50
            """))
            rows = [dict(row._mapping) for row in result]
        return QueryResult(rows=rows, row_count=0, execution_time=0,
                         database='postgresql', test_name=self.name)
    
    def run_mysql(self, engine) -> QueryResult:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT g.name as genre, COUNT(*) as game_count
                FROM game_genres gg
                JOIN genres g ON gg.genre_id = g.genre_id
                GROUP BY g.name
                ORDER BY game_count DESC, g.name
                LIMIT 50
            """))
            rows = [dict(row._mapping) for row in result]
        return QueryResult(rows=rows, row_count=0, execution_time=0,
                         database='mysql', test_name=self.name)
    
    def run_mongodb(self, db) -> QueryResult:
        pipeline = [
            {'$lookup': {
                'from': 'genres',
                'localField': 'genre_id',
                'foreignField': 'genre_id',
                'as': 'genre_info'
            }},
            {'$unwind': '$genre_info'},
            {'$group': {
                '_id': '$genre_info.name',
                'game_count': {'$sum': 1}
            }},
            {'$project': {
                '_id': 0,
                'genre': '$_id',
                'game_count': 1
            }},
            {'$sort': {'game_count': -1, 'genre': 1}},
            {'$limit': 50}
        ]
        rows = list(db.game_genres.aggregate(pipeline))
        return QueryResult(rows=rows, row_count=0, execution_time=0,
                         database='mongodb', test_name=self.name)
    
    def run_neo4j(self, driver) -> QueryResult:
        with driver.session() as session:
            result = session.run("""
                MATCH (g:Game)-[:HAS_GENRE_NORM]->(ge:GameGenre)
                RETURN ge.name as genre, COUNT(g) as game_count
                ORDER BY game_count DESC, genre
                LIMIT 50
            """)
            rows = [dict(record) for record in result]
        return QueryResult(rows=rows, row_count=0, execution_time=0,
                         database='neo4j', test_name=self.name)


class GamesWithDevelopersTest(BaseTest):
    """Test 3: JOIN query - Games with their developers"""
    
    def __init__(self):
        super().__init__(
            name="games_with_developers",
            description="Get games with their developer names"
        )
    
    def run_postgresql(self, engine) -> QueryResult:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT g.appid, g.name as game_name, d.name as developer
                FROM games g
                JOIN game_developers gd ON g.appid = gd.game_appid
                JOIN developers d ON gd.developer_id = d.developer_id
                WHERE g.price > 30
                ORDER BY g.appid, d.name
                LIMIT 100
            """))
            rows = [dict(row._mapping) for row in result]
        return QueryResult(rows=rows, row_count=0, execution_time=0,
                         database='postgresql', test_name=self.name)
    
    def run_mysql(self, engine) -> QueryResult:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT g.appid, g.name as game_name, d.name as developer
                FROM games g
                JOIN game_developers gd ON g.appid = gd.game_appid
                JOIN developers d ON gd.developer_id = d.developer_id
                WHERE g.price > 30
                ORDER BY g.appid, d.name
                LIMIT 100
            """))
            rows = [dict(row._mapping) for row in result]
        return QueryResult(rows=rows, row_count=0, execution_time=0,
                         database='mysql', test_name=self.name)
    
    def run_mongodb(self, db) -> QueryResult:
        pipeline = [
            {'$match': {'price': {'$gt': 30}}},
            {'$lookup': {
                'from': 'game_developers',
                'localField': 'appid',
                'foreignField': 'game_appid',
                'as': 'dev_links'
            }},
            {'$unwind': '$dev_links'},
            {'$lookup': {
                'from': 'developers',
                'localField': 'dev_links.developer_id',
                'foreignField': 'developer_id',
                'as': 'dev_info'
            }},
            {'$unwind': '$dev_info'},
            {'$project': {
                '_id': 0,
                'appid': 1,
                'game_name': '$name',
                'developer': '$dev_info.name'
            }},
            {'$sort': {'appid': 1, 'developer': 1}},
            {'$limit': 100}
        ]
        rows = list(db.games.aggregate(pipeline))
        return QueryResult(rows=rows, row_count=0, execution_time=0,
                         database='mongodb', test_name=self.name)
    
    def run_neo4j(self, driver) -> QueryResult:
        with driver.session() as session:
            result = session.run("""
                MATCH (g:Game)-[:DEVELOPED_BY_NORM]->(d:GameDeveloper)
                WHERE g.price > 30
                RETURN g.appid as appid, g.name as game_name, d.name as developer
                ORDER BY g.appid, d.name
                LIMIT 100
            """)
            rows = [dict(record) for record in result]
        return QueryResult(rows=rows, row_count=0, execution_time=0,
                         database='neo4j', test_name=self.name)


class ReviewStatsByGameTest(BaseTest):
    """Test 4: Complex aggregation - Review statistics by game"""
    
    def __init__(self):
        super().__init__(
            name="review_stats_by_game",
            description="Calculate review statistics per game"
        )
    
    def run_postgresql(self, engine) -> QueryResult:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT 
                    r.app_id,
                    COUNT(*) as total_reviews,
                    SUM(CASE WHEN r.recommended THEN 1 ELSE 0 END) as positive_reviews,
                    ROUND(AVG(r.votes_helpful), 2) as avg_helpful_votes
                FROM reviews r
                WHERE r.app_id IN (SELECT appid FROM games WHERE price > 0 LIMIT 20)
                GROUP BY r.app_id
                ORDER BY total_reviews DESC
                LIMIT 20
            """))
            rows = [dict(row._mapping) for row in result]
        return QueryResult(rows=rows, row_count=0, execution_time=0,
                         database='postgresql', test_name=self.name)
    
    def run_mysql(self, engine) -> QueryResult:
        with engine.connect() as conn:
            # result = conn.execute(text("""
            #     SELECT 
            #         r.app_id,
            #         COUNT(*) as total_reviews,
            #         SUM(CASE WHEN r.recommended THEN 1 ELSE 0 END) as positive_reviews,
            #         ROUND(AVG(r.votes_helpful), 2) as avg_helpful_votes
            #     FROM reviews r
            #     WHERE r.app_id IN (SELECT appid FROM games WHERE price > 0 LIMIT 20)
            #     GROUP BY r.app_id
            #     ORDER BY total_reviews DESC
            #     LIMIT 20
            # """)) # This version of MySQL doesn't yet support 'LIMIT & IN/ALL/ANY/SOME subquery'

            result = conn.execute(text("""
                SELECT
                    r.app_id,
                    COUNT(*) as total_reviews,
                    SUM(CASE WHEN r.recommended THEN 1 ELSE 0 END) as positive_reviews,
                    ROUND(AVG(r.votes_helpful), 2) as avg_helpful_votes
                FROM reviews r
                JOIN (
                    SELECT appid
                    FROM games
                    WHERE price > 0
                    LIMIT 20
                ) AS limited_games ON r.app_id = limited_games.appid
                GROUP BY r.app_id
                ORDER BY total_reviews DESC
                LIMIT 20;
            """))

            rows = [dict(row._mapping) for row in result]
        return QueryResult(rows=rows, row_count=0, execution_time=0,
                         database='mysql', test_name=self.name)
    
    def run_mongodb(self, db) -> QueryResult:
        # First get game IDs
        game_ids = [g['appid'] for g in db.games.find(
            {'price': {'$gt': 0}}, {'appid': 1, '_id': 0}
        ).limit(20)]
        
        pipeline = [
            {'$match': {'app_id': {'$in': game_ids}}},
            {'$group': {
                '_id': '$app_id',
                'total_reviews': {'$sum': 1},
                'positive_reviews': {
                    '$sum': {'$cond': ['$recommended', 1, 0]}
                },
                'avg_helpful_votes': {'$avg': '$votes_helpful'}
            }},
            {'$project': {
                '_id': 0,
                'app_id': '$_id',
                'total_reviews': 1,
                'positive_reviews': 1,
                'avg_helpful_votes': {'$round': ['$avg_helpful_votes', 2]}
            }},
            {'$sort': {'total_reviews': -1}},
            {'$limit': 20}
        ]
        rows = list(db.reviews.aggregate(pipeline))
        return QueryResult(rows=rows, row_count=0, execution_time=0,
                         database='mongodb', test_name=self.name)
    
    def run_neo4j(self, driver) -> QueryResult:
        with driver.session() as session:
            result = session.run("""
                MATCH (g:Game)
                WHERE g.price > 0
                WITH g.appid as game_appid
                LIMIT 20
                
                MATCH (r:Review {app_id: game_appid})
                WITH game_appid as app_id,
                     COUNT(r) as total_reviews,
                     SUM(CASE WHEN r.recommended THEN 1 ELSE 0 END) as positive_reviews,
                     ROUND(AVG(r.votes_helpful), 2) as avg_helpful_votes
                ORDER BY total_reviews DESC
                RETURN app_id, total_reviews, positive_reviews, avg_helpful_votes
                LIMIT 20
            """)
            rows = [dict(record) for record in result]
        return QueryResult(rows=rows, row_count=0, execution_time=0,
                         database='neo4j', test_name=self.name)


class DeveloperStatsTest(BaseTest):
    """Test 5: Complex JOIN with aggregation - Developer statistics"""
    
    def __init__(self):
        super().__init__(
            name="developer_statistics",
            description="Get statistics for developers with most games"
        )
    
    def run_postgresql(self, engine) -> QueryResult:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT 
                    ds.developer_id,
                    d.name as developer_name,
                    ds.total_games,
                    ROUND(CAST(ds.avg_game_price as numeric), 2) as avg_price,
                    ds.total_positive_reviews,
                    ds.total_negative_reviews
                FROM developer_stats ds
                JOIN developers d ON ds.developer_id = d.developer_id
                WHERE ds.total_games >= 3
                ORDER BY ds.total_games DESC, d.name
                LIMIT 30
            """))
            rows = [dict(row._mapping) for row in result]
        return QueryResult(rows=rows, row_count=0, execution_time=0,
                         database='postgresql', test_name=self.name)
    
    def run_mysql(self, engine) -> QueryResult:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT 
                    ds.developer_id,
                    d.name as developer_name,
                    ds.total_games,
                    ROUND(ds.avg_game_price, 2) as avg_price,
                    ds.total_positive_reviews,
                    ds.total_negative_reviews
                FROM developer_stats ds
                JOIN developers d ON ds.developer_id = d.developer_id
                WHERE ds.total_games >= 3
                ORDER BY ds.total_games DESC, d.name
                LIMIT 30
            """))
            rows = [dict(row._mapping) for row in result]
        return QueryResult(rows=rows, row_count=0, execution_time=0,
                         database='mysql', test_name=self.name)
    
    def run_mongodb(self, db) -> QueryResult:
        pipeline = [
            {'$match': {'total_games': {'$gte': 3}}},
            {'$lookup': {
                'from': 'developers',
                'localField': 'developer_id',
                'foreignField': 'developer_id',
                'as': 'dev_info'
            }},
            {'$unwind': '$dev_info'},
            {'$project': {
                '_id': 0,
                'developer_id': 1,
                'developer_name': '$dev_info.name',
                'total_games': 1,
                'avg_price': {'$round': ['$avg_game_price', 2]},
                'total_positive_reviews': 1,
                'total_negative_reviews': 1
            }},
            {'$sort': {'total_games': -1, 'developer_name': 1}},
            {'$limit': 30}
        ]
        rows = list(db.developer_stats.aggregate(pipeline))
        return QueryResult(rows=rows, row_count=0, execution_time=0,
                         database='mongodb', test_name=self.name)
    
    def run_neo4j(self, driver) -> QueryResult:
        with driver.session() as session:
            result = session.run("""
                MATCH (ds:DeveloperStats)
                WHERE ds.total_games >= 3
                MATCH (d:GameDeveloper {developer_id: ds.developer_id})
                RETURN ds.developer_id as developer_id,
                       d.name as developer_name,
                       ds.total_games as total_games,
                       ROUND(ds.avg_game_price, 2) as avg_price,
                       ds.total_positive_reviews as total_positive_reviews,
                       ds.total_negative_reviews as total_negative_reviews
                ORDER BY ds.total_games DESC, d.name
                LIMIT 30
            """)
            rows = [dict(record) for record in result]
        return QueryResult(rows=rows, row_count=0, execution_time=0,
                         database='neo4j', test_name=self.name)


class PriceHistoryTest(BaseTest):
    """Test 6: Time series - Price history for specific games"""
    
    def __init__(self):
        super().__init__(
            name="price_history_analysis",
            description="Get price history for games with most price points"
        )
    
    def run_postgresql(self, engine) -> QueryResult:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT 
                    ph.game_appid,
                    COUNT(*) as price_points,
                    MIN(ph.price) as min_price,
                    MAX(ph.price) as max_price,
                    AVG(ph.price) as avg_price
                FROM game_price_history ph
                GROUP BY ph.game_appid
                ORDER BY price_points DESC
                LIMIT 50
            """))
            rows = [dict(row._mapping) for row in result]
        return QueryResult(rows=rows, row_count=0, execution_time=0,
                         database='postgresql', test_name=self.name)
    
    def run_mysql(self, engine) -> QueryResult:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT 
                    ph.game_appid,
                    COUNT(*) as price_points,
                    MIN(ph.price) as min_price,
                    MAX(ph.price) as max_price,
                    AVG(ph.price) as avg_price
                FROM game_price_history ph
                GROUP BY ph.game_appid
                ORDER BY price_points DESC
                LIMIT 50
            """))
            rows = [dict(row._mapping) for row in result]
        return QueryResult(rows=rows, row_count=0, execution_time=0,
                         database='mysql', test_name=self.name)
    
    def run_mongodb(self, db) -> QueryResult:
        pipeline = [
            {'$group': {
                '_id': '$game_appid',
                'price_points': {'$sum': 1},
                'min_price': {'$min': '$price'},
                'max_price': {'$max': '$price'},
                'avg_price': {'$avg': '$price'}
            }},
            {'$project': {
                '_id': 0,
                'game_appid': '$_id',
                'price_points': 1,
                'min_price': 1,
                'max_price': 1,
                'avg_price': 1
            }},
            {'$sort': {'price_points': -1}},
            {'$limit': 50}
        ]
        rows = list(db.game_price_history.aggregate(pipeline))
        return QueryResult(rows=rows, row_count=0, execution_time=0,
                         database='mongodb', test_name=self.name)
    
    def run_neo4j(self, driver) -> QueryResult:
        with driver.session() as session:
            result = session.run("""
                MATCH (ph:GamePriceHistory)
                WITH ph.game_appid as game_appid,
                     COUNT(ph) as price_points,
                     MIN(ph.price) as min_price,
                     MAX(ph.price) as max_price,
                     AVG(ph.price) as avg_price
                ORDER BY price_points DESC
                RETURN game_appid, price_points, min_price, max_price, avg_price
                LIMIT 50
            """)
            rows = [dict(record) for record in result]
        return QueryResult(rows=rows, row_count=0, execution_time=0,
                         database='neo4j', test_name=self.name)


class MultiJoinTest(BaseTest):
    """Test 7: Complex multi-table JOIN - Games with developers, genres, and categories"""
    
    def __init__(self):
        super().__init__(
            name="multi_join_game_details",
            description="Get games with developers, genres, and categories in one query"
        )
    
    def run_postgresql(self, engine) -> QueryResult:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT 
                    g.appid,
                    g.name as game_name,
                    d.name as developer,
                    ge.name as genre,
                    c.name as category
                FROM games g
                LEFT JOIN game_developers gd ON g.appid = gd.game_appid
                LEFT JOIN developers d ON gd.developer_id = d.developer_id
                LEFT JOIN game_genres gg ON g.appid = gg.game_appid
                LEFT JOIN genres ge ON gg.genre_id = ge.genre_id
                LEFT JOIN game_categories gc ON g.appid = gc.game_appid
                LEFT JOIN categories c ON gc.category_id = c.category_id
                WHERE g.price BETWEEN 20 AND 40
                ORDER BY g.appid, d.name, ge.name, c.name
                LIMIT 100
            """))
            rows = [dict(row._mapping) for row in result]
        return QueryResult(rows=rows, row_count=0, execution_time=0,
                         database='postgresql', test_name=self.name)
    
    def run_mysql(self, engine) -> QueryResult:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT 
                    g.appid,
                    g.name as game_name,
                    d.name as developer,
                    ge.name as genre,
                    c.name as category
                FROM games g
                LEFT JOIN game_developers gd ON g.appid = gd.game_appid
                LEFT JOIN developers d ON gd.developer_id = d.developer_id
                LEFT JOIN game_genres gg ON g.appid = gg.game_appid
                LEFT JOIN genres ge ON gg.genre_id = ge.genre_id
                LEFT JOIN game_categories gc ON g.appid = gc.game_appid
                LEFT JOIN categories c ON gc.category_id = c.category_id
                WHERE g.price BETWEEN 20 AND 40
                ORDER BY g.appid, d.name, ge.name, c.name
                LIMIT 100
            """))
            rows = [dict(row._mapping) for row in result]
        return QueryResult(rows=rows, row_count=0, execution_time=0,
                         database='mysql', test_name=self.name)
    
    def run_mongodb(self, db) -> QueryResult:
        pipeline = [
            {'$match': {'price': {'$gte': 20, '$lte': 40}}},
            {'$limit': 20},  # Limit games first to reduce join complexity
            {'$lookup': {
                'from': 'game_developers',
                'localField': 'appid',
                'foreignField': 'game_appid',
                'as': 'dev_links'
            }},
            {'$lookup': {
                'from': 'game_genres',
                'localField': 'appid',
                'foreignField': 'game_appid',
                'as': 'genre_links'
            }},
            {'$lookup': {
                'from': 'game_categories',
                'localField': 'appid',
                'foreignField': 'game_appid',
                'as': 'category_links'
            }},
            {'$unwind': {'path': '$dev_links', 'preserveNullAndEmptyArrays': True}},
            {'$unwind': {'path': '$genre_links', 'preserveNullAndEmptyArrays': True}},
            {'$unwind': {'path': '$category_links', 'preserveNullAndEmptyArrays': True}},
            {'$lookup': {
                'from': 'developers',
                'localField': 'dev_links.developer_id',
                'foreignField': 'developer_id',
                'as': 'dev_info'
            }},
            {'$lookup': {
                'from': 'genres',
                'localField': 'genre_links.genre_id',
                'foreignField': 'genre_id',
                'as': 'genre_info'
            }},
            {'$lookup': {
                'from': 'categories',
                'localField': 'category_links.category_id',
                'foreignField': 'category_id',
                'as': 'category_info'
            }},
            {'$project': {
                '_id': 0,
                'appid': 1,
                'game_name': '$name',
                'developer': {'$arrayElemAt': ['$dev_info.name', 0]},
                'genre': {'$arrayElemAt': ['$genre_info.name', 0]},
                'category': {'$arrayElemAt': ['$category_info.name', 0]}
            }},
            {'$sort': {'appid': 1, 'developer': 1, 'genre': 1, 'category': 1}},
            {'$limit': 100}
        ]
        rows = list(db.games.aggregate(pipeline))
        return QueryResult(rows=rows, row_count=0, execution_time=0,
                         database='mongodb', test_name=self.name)
    
    def run_neo4j(self, driver) -> QueryResult:
        with driver.session() as session:
            result = session.run("""
                MATCH (g:Game)
                WHERE g.price >= 20 AND g.price <= 40
                OPTIONAL MATCH (g)-[:DEVELOPED_BY_NORM]->(d:GameDeveloper)
                OPTIONAL MATCH (g)-[:HAS_GENRE_NORM]->(ge:GameGenre)
                OPTIONAL MATCH (g)-[:HAS_CATEGORY_NORM]->(c:GameCategory)
                RETURN g.appid as appid,
                       g.name as game_name,
                       d.name as developer,
                       ge.name as genre,
                       c.name as category
                ORDER BY g.appid, d.name, ge.name, c.name
                LIMIT 100
            """)
            rows = [dict(record) for record in result]
        return QueryResult(rows=rows, row_count=0, execution_time=0,
                         database='neo4j', test_name=self.name)


# Test registry - add new tests here
ALL_TESTS = [
    SimpleSelectTest,
    CountByGenreTest,
    GamesWithDevelopersTest,
    ReviewStatsByGameTest,
    DeveloperStatsTest,
    PriceHistoryTest,
    MultiJoinTest,
]
