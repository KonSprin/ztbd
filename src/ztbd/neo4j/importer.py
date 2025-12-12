from neo4j import GraphDatabase
import pandas as pd
import time
from typing import Dict, Any, List
from ..ztbdf import ZTBDataFrame

class Neo4jImporter:
    def __init__(self, uri: str, user: str, password: str):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
    
    def close(self):
        self.driver.close()
    
    def clear_database(self):
        """Clear all nodes and relationships"""
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
            print("Database cleared")
    
    def create_constraints(self):
        """Create constraints and indexes"""
        constraints = [
            "CREATE CONSTRAINT game_appid IF NOT EXISTS FOR (g:Game) REQUIRE g.appid IS UNIQUE",
            "CREATE CONSTRAINT review_id IF NOT EXISTS FOR (r:Review) REQUIRE r.review_id IS UNIQUE",
            "CREATE CONSTRAINT developer_name IF NOT EXISTS FOR (d:Developer) REQUIRE d.name IS UNIQUE",
            "CREATE CONSTRAINT publisher_name IF NOT EXISTS FOR (p:Publisher) REQUIRE p.name IS UNIQUE",
            "CREATE CONSTRAINT genre_name IF NOT EXISTS FOR (g:Genre) REQUIRE g.name IS UNIQUE",
            "CREATE CONSTRAINT category_name IF NOT EXISTS FOR (c:Category) REQUIRE c.name IS UNIQUE",
            "CREATE CONSTRAINT language_name IF NOT EXISTS FOR (l:Language) REQUIRE l.name IS UNIQUE",
            "CREATE CONSTRAINT tag_name IF NOT EXISTS FOR (t:Tag) REQUIRE t.name IS UNIQUE"
        ]
        
        indexes = [
            "CREATE INDEX game_name IF NOT EXISTS FOR (g:Game) ON (g.name)",
            "CREATE INDEX review_app_id IF NOT EXISTS FOR (r:Review) ON (r.app_id)"
        ]
        
        with self.driver.session() as session:
            for constraint in constraints:
                session.run(constraint)
            for index in indexes:
                session.run(index)
            
        print("Constraints and indexes created")
    
    def _parse_json_field(self, value):
        """Parse JSON field from CSV"""
        if pd.isna(value) or value == '':
            return None
        try:
            return eval(value) if isinstance(value, str) else value
        except:
            return None
    
    def _prepare_game_data(self, row) -> Dict[str, Any]:
        """Convert DataFrame row to game data dict"""
        return {
            'appid': int(row['appid']) if pd.notna(row['appid']) else None,
            'name': str(row['name']) if pd.notna(row['name']) else None,
            'release_date': str(row['release_date']) if pd.notna(row['release_date']) else None,
            'required_age': int(row['required_age']) if pd.notna(row['required_age']) else None,
            'price': float(row['price']) if pd.notna(row['price']) else None,
            # ... existing fields ...
            'developers': self._parse_json_field(row['developers']),
            'publishers': self._parse_json_field(row['publishers']),
            'genres': self._parse_json_field(row['genres']),
            'categories': self._parse_json_field(row['categories']),
            'supported_languages': self._parse_json_field(row['supported_languages']),
            'tags': self._parse_json_field(row['tags'])
        }
    
    def _prepare_review_data(self, row) -> Dict[str, Any]:
        """Convert DataFrame row to review data dict"""
        return {
            'review_id': int(row['review_id']) if pd.notna(row['review_id']) else None,
            'app_id': int(row['app_id']) if pd.notna(row['app_id']) else None,
            'app_name': str(row['app_name']) if pd.notna(row['app_name']) else None,
            'language': str(row['language']) if pd.notna(row['language']) else None,
            'review_text': str(row['review']) if pd.notna(row['review']) else None,
            'timestamp_created': int(row['timestamp_created']) if pd.notna(row['timestamp_created']) else None,
            'recommended': bool(row['recommended']) if pd.notna(row['recommended']) else None,
            # ... existing fields ...
        }
    
    def import_games(self, ztb_df: ZTBDataFrame, batch_size=500):
        """Import games with relationships in batches"""
        total_games = len(ztb_df.df)
        
        # Check if games already imported
        with self.driver.session() as session:
            result = session.run("MATCH (g:Game) RETURN count(g) as count")
            existing_count = result.single()['count']
        
        if existing_count == total_games:
            print(f"Games already imported: {total_games}")
            return
        
        self.clear_database()
        self.create_constraints()
        
        games_batch = []
        imported = 0
        
        for idx, row in ztb_df.df.iterrows():
            game_data = self._prepare_game_data(row)
            games_batch.append(game_data)
            
            if len(games_batch) >= batch_size:
                self._import_games_batch(games_batch)
                imported += len(games_batch)
                print(f"  Imported {imported} games out of {total_games}")
                games_batch = []
        
        if games_batch:
            self._import_games_batch(games_batch)
        
        print(f"Imported {total_games} games")
    
    def import_reviews(self, ztb_df: ZTBDataFrame, limit=1000000, batch_size=5000):
        """Import reviews with relationships in batches"""
        # Process dataframe
        column_mapping = {
            'author.steamid': 'author_steamid',
            'author.num_games_owned': 'author_num_games_owned',
            # ... existing mappings ...
        }
        
        ztb_df.rename_columns(column_mapping)
        ztb_df.drop_unnamed_columns()
        ztb_df.handle_duplicates()
        ztb_df.sort_by_column('author_steamid')
        ztb_df.limit_records(limit)
        
        reviews_batch = []
        imported = 0
        total_reviews = len(ztb_df.df)
        
        for idx, row in ztb_df.df.iterrows():
            review_data = self._prepare_review_data(row)
            reviews_batch.append(review_data)
            
            if len(reviews_batch) >= batch_size:
                self._import_reviews_batch(reviews_batch)
                imported += len(reviews_batch)
                print(f"  Imported {imported} reviews out of {total_reviews}")
                reviews_batch = []
        
        if reviews_batch:
            self._import_reviews_batch(reviews_batch)
        
        print(f"Imported {total_reviews} reviews")
    
    # TODO: batch import methods
