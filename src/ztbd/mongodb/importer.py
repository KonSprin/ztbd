from pymongo import MongoClient, ASCENDING
from ..ztbdf import ZTBDataFrame

class MongoDBImporter:
    def __init__(self, uri, database_name):
        self.client = MongoClient(uri)
        self.db = self.client[database_name]
    
    def close(self):
        self.client.close()
    
    def clean_database(self, collections=None):
        """
        Clean (drop) collections from MongoDB database
        
        Args:
            collections: List of collection names to drop. If None, drops all collections.
        """
        if collections is None:
            collections = self.db.list_collection_names()
        
        print(f"Cleaning MongoDB database '{self.db.name}'...")
        for collection_name in collections:
            if collection_name in self.db.list_collection_names():
                self.db[collection_name].drop()
                print(f"  Dropped collection: {collection_name}")
        
        print("MongoDB cleanup complete")
    
    def clean_games(self):
        """Drop only the games collection"""
        self.clean_database(['games'])
    
    def clean_reviews(self):
        """Drop only the reviews collection"""
        self.clean_database(['reviews'])
    
    def import_games(self, ztb_df: ZTBDataFrame):
        """Import pre-cleaned games to MongoDB"""
        print("Importing games to MongoDB...")
        
        # MongoDB-specific data cleaning (NaN handling)
        games_records = ztb_df.clean_nan_values()
        
        if games_records:
            result = self.db.games.insert_many(games_records)
            print(f"Imported {len(result.inserted_ids)} games")
            
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
        
        print(f"Imported {total_inserted} reviews")
        
        # Create indexes
        print("Creating indexes on reviews collection...")
        self.db.reviews.create_index([("app_id", ASCENDING)])
        self.db.reviews.create_index([("review_id", ASCENDING)], unique=True)
        self.db.reviews.create_index([("recommended", ASCENDING)])
        self.db.reviews.create_index([("timestamp_created", ASCENDING)])
