from pymongo import MongoClient, ASCENDING
from ..ztbdf import ZTBDataFrame

class MongoDBImporter:
    def __init__(self, uri, database_name):
        self.client = MongoClient(uri)
        self.db = self.client[database_name]
    
    def close(self):
        self.client.close()
    
    def import_games(self, ztb_df: ZTBDataFrame):
        """Import pre-cleaned games to MongoDB"""
        print("Importing games to MongoDB...")
        
        # MongoDB-specific data cleaning (NaN handling)
        games_records = ztb_df.clean_nan_values()
        
        if games_records:
            result = self.db.games.insert_many(games_records)
            print(f"✓ Imported {len(result.inserted_ids)} games")
            
            # Create indexes
            print("Creating indexes on games collection...")
            self.db.games.create_index([("appid", ASCENDING)], unique=True)
            self.db.games.create_index([("name", ASCENDING)])
            self.db.games.create_index([("release_date", ASCENDING)])
    
    def import_reviews(self, ztb_df: ZTBDataFrame):
        """Import pre-cleaned reviews to MongoDB"""
        print("Importing reviews to MongoDB...")
        
        # MongoDB-specific data cleaning (NaN handling)
        reviews_records = ztb_df.clean_nan_values()
        
        # Insert in batches for better performance
        batch_size = 5000
        total_inserted = 0
        
        for i in range(0, len(reviews_records), batch_size):
            batch = reviews_records[i:i + batch_size]
            result = self.db.reviews.insert_many(batch)
            total_inserted += len(result.inserted_ids)
            if total_inserted % 25000 == 0:  # Less frequent logging
                print(f"  Inserted {total_inserted}/{len(reviews_records)} reviews...")
        
        print(f"✓ Imported {total_inserted} reviews")
        
        # Create indexes
        print("Creating indexes on reviews collection...")
        self.db.reviews.create_index([("app_id", ASCENDING)])
        self.db.reviews.create_index([("review_id", ASCENDING)], unique=True)
        self.db.reviews.create_index([("recommended", ASCENDING)])
        self.db.reviews.create_index([("timestamp_created", ASCENDING)])
from neo4j import GraphDatabase
import pandas as pd
from typing import Dict, Any
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
    
    def import_games(self, ztb_df: ZTBDataFrame, batch_size=500):
        """Import pre-cleaned games with relationships"""
        total_games = len(ztb_df.df)
        
        # Check if games already exist
        with self.driver.session() as session:
            result = session.run("MATCH (g:Game) RETURN count(g) as count")
            existing_count = result.single()['count']
        
        if existing_count == total_games:
            print(f"Games already imported: {total_games}")
            return
        
        print("Setting up Neo4j database...")
        self.clear_database()
        self.create_constraints()
        
        print(f"Importing {total_games} games in batches of {batch_size}...")
        
        games_batch = []
        imported = 0
        
        for idx, row in ztb_df.df.iterrows():
            game_data = self._prepare_game_data(row)
            games_batch.append(game_data)
            
            if len(games_batch) >= batch_size:
                self._import_games_batch(games_batch)
                imported += len(games_batch)
                print(f"  Imported {imported}/{total_games} games")
                games_batch = []
        
        if games_batch:
            self._import_games_batch(games_batch)
            imported += len(games_batch)
        
        print(f"✓ Imported {imported} games")
    
    def import_reviews(self, ztb_df: ZTBDataFrame, batch_size=5000):
        """Import pre-cleaned reviews with relationships"""
        total_reviews = len(ztb_df.df)
        print(f"Importing {total_reviews} reviews in batches of {batch_size}...")
        
        reviews_batch = []
        imported = 0
        
        for idx, row in ztb_df.df.iterrows():
            review_data = self._prepare_review_data(row)
            reviews_batch.append(review_data)
            
            if len(reviews_batch) >= batch_size:
                self._import_reviews_batch(reviews_batch)
                imported += len(reviews_batch)
                if imported % 25000 == 0:  # Less frequent logging
                    print(f"  Imported {imported}/{total_reviews} reviews")
                reviews_batch = []
        
        if reviews_batch:
            self._import_reviews_batch(reviews_batch)
            imported += len(reviews_batch)
        
        print(f"✓ Imported {imported} reviews")
    
    # ... existing helper methods (_prepare_game_data, _prepare_review_data, etc.)