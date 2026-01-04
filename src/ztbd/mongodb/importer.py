from pymongo import MongoClient, ASCENDING
from ..ztbdf import ZTBDataFrame
import logging
import pandas as pd

logger = logging.getLogger('ztbd')


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
        
        logger.info(f"Cleaning MongoDB database '{self.db.name}'...")
        for collection_name in collections:
            if collection_name in self.db.list_collection_names():
                self.db[collection_name].drop()
                logger.info(f"  Dropped collection: {collection_name}")
        
        logger.info("MongoDB cleanup complete")
    
    def clean_games(self):
        """Drop only the games collection"""
        self.clean_database(['games'])
    
    def clean_reviews(self):
        """Drop only the reviews collection"""
        self.clean_database(['reviews'])
    
    def import_games(self, ztb_df: ZTBDataFrame):
        logger.warning("USING DEPRECATED FUNCTION: use import_df() instead")
        """Import pre-cleaned games to MongoDB"""
        logger.info("Importing games to MongoDB...")
        
        # MongoDB-specific data cleaning (NaN handling)
        games_records = ztb_df.clean_nan_values()
        
        if games_records:
            result = self.db.games.insert_many(games_records)
            logger.info(f"Imported {len(result.inserted_ids)} games")
            
            # Create indexes
            logger.info("Creating indexes on games collection...")
            self.db.games.create_index([("appid", ASCENDING)], unique=True)
            self.db.games.create_index([("name", ASCENDING)])
            self.db.games.create_index([("release_date", ASCENDING)])
    
    def import_reviews(self, ztb_df: ZTBDataFrame):
        """Import pre-cleaned reviews to MongoDB"""
        logger.warning("USING DEPRECATED FUNCTION: use import_df() instead")
        logger.info("Importing reviews to MongoDB...")
        
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
                logger.info(f"  Inserted {total_inserted}/{len(reviews_records)} reviews...")
        
        logger.info(f"Imported {total_inserted} reviews")
        
        # Create indexes
        logger.info("Creating indexes on reviews collection...")
        self.db.reviews.create_index([("app_id", ASCENDING)])
        self.db.reviews.create_index([("review_id", ASCENDING)], unique=True)
        self.db.reviews.create_index([("recommended", ASCENDING)])
        self.db.reviews.create_index([("timestamp_created", ASCENDING)])

    def import_df(self, ztb_df: ZTBDataFrame, indexes: list, primary_key = "", batch_size = 0):
        """Import pre-cleaned dataframe to MongoDB"""
        logger.info("Importing data to MongoDB")

        # MongoDB-specific data cleaning (NaN handling)
        df_records = ztb_df.clean_nan_values()

        collection_name = ztb_df.name

        if batch_size > 0:
            total_inserted = 0
            for i in range(0, len(df_records), batch_size):
                batch = df_records[i:i + batch_size]
                result = self.db[collection_name].insert_many(batch)
                total_inserted += len(result.inserted_ids)
                if total_inserted % (batch_size * 5) == 0:  # Less frequent logging
                    logger.info(f"  Inserted {total_inserted}/{len(df_records)} records...")
        else:
            result = self.db[collection_name].insert_many(df_records)
            logger.info(f"Imported {len(result.inserted_ids)} records")

        if not primary_key:
            primary_key = indexes[0]

        # Create indexes
        logger.info(f"Creating indexes on {collection_name} collection...")
        for index in indexes:
            if index == primary_key:
                self.db[collection_name].create_index([(index, ASCENDING)], unique=True)
            else:
                self.db[collection_name].create_index([(index, ASCENDING)])

    def import_dataframe(self, df, collection_name, indexes=None, batch_size=0):
        """
        Import a regular pandas DataFrame to MongoDB
        
        Args:
            df: pandas DataFrame to import
            collection_name: Name of the collection
            indexes: List of fields to index
            batch_size: Batch size for insertion (0 = all at once)
        """
        try:
            logger.info(f"Importing {len(df)} records to {collection_name}...")
            
            records = df.to_dict('records')
            
            # Clean NaN values
            for record in records:
                for key, value in list(record.items()):
                    if isinstance(value, float) and pd.isna(value):
                        record[key] = None
            
            if batch_size > 0:
                total_inserted = 0
                for i in range(0, len(records), batch_size):
                    batch = records[i:i + batch_size]
                    result = self.db[collection_name].insert_many(batch)
                    total_inserted += len(result.inserted_ids)
                    if total_inserted % (batch_size * 5) == 0:
                        logger.info(f"  Inserted {total_inserted}/{len(records)} records...")
            else:
                result = self.db[collection_name].insert_many(records)
                logger.info(f"  Imported {len(result.inserted_ids)} records")
            
            if indexes:
                logger.info(f"Creating indexes on {collection_name}...")
                for index in indexes:
                    self.db[collection_name].create_index([(index, ASCENDING)])
            
            logger.info(f"Completed import to {collection_name}")
            
        except Exception as e:
            logger.error(f"XX Error importing to MongoDB {collection_name}: {e}")
            raise

    def verify_empty(self, collections=None):
        """
        Verify that collections are empty or don't exist
        
        Args:
            collections: List of collection names to verify. If None, checks common collections.
        
        Returns:
            bool: True if all collections are empty/don't exist, False otherwise
        """
        if collections is None:
            collections = ['games', 'reviews', 'hltb']
        
        logger.info(f"Verifying MongoDB collections are dropped...")
        all_empty = True
        existing_collections = self.db.list_collection_names()
        
        for collection_name in collections:
            if collection_name in existing_collections:
                count = self.db[collection_name].count_documents({})
                if count > 0:
                    logger.error(f"{collection_name} still has {count} documents")
                    all_empty = False
                else:
                    logger.info(f"  OK: {collection_name} exists but is empty")
            else:
                logger.info(f"  OK: {collection_name} does not exist")
        
        if all_empty:
            logger.info("MongoDB verification: All collections dropped successfully")
        else:
            logger.error("MongoDB verification: FAILED - some collections still have data")
        
        return all_empty
