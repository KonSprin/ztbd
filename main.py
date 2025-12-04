import os
import argparse
from dotenv import load_dotenv
from src.ztbd.ztbdf import create_games_dataframe, create_reviews_dataframe
from src.ztbd.mongodb.importer import MongoDBImporter
from src.ztbd.neo4j.importer import Neo4jImporter
from src.ztbd.postgresql.importer import PostgreSQLImporter

load_dotenv()

class DataProcessor:
    """Centralized data processing for all datasets"""
    
    @staticmethod
    def prepare_games_dataframe():
        """Prepare and clean games dataframe"""
        print("\n=== Processing Games Dataset ===")
        games_df = create_games_dataframe()
        games_df.log_shape()
        
        # Parse JSON columns
        json_columns = ['supported_languages', 'full_audio_languages', 'packages', 
                       'developers', 'publishers', 'categories', 'genres', 
                       'screenshots', 'movies', 'tags']
        games_df.parse_json_columns(json_columns)
        
        # Convert date column
        games_df.convert_datetime_column('release_date', unit=None)
        
        # Handle duplicates
        games_df.handle_duplicates()
        
        print(f"Games dataset prepared: {len(games_df.df)} records")
        return games_df
    
    @staticmethod
    def prepare_reviews_dataframe(limit=1000000):
        """Prepare and clean reviews dataframe"""
        print("\n=== Processing Reviews Dataset ===")
        reviews_df = create_reviews_dataframe()
        reviews_df.log_shape()
        
        # Rename columns
        column_mapping = {
            'author.steamid': 'author_steamid',
            'author.num_games_owned': 'author_num_games_owned',
            'author.num_reviews': 'author_num_reviews',
            'author.playtime_forever': 'author_playtime_forever',
            'author.playtime_last_two_weeks': 'author_playtime_last_two_weeks',
            'author.playtime_at_review': 'author_playtime_at_review',
            'author.last_played': 'author_last_played'
        }
        reviews_df.rename_columns(column_mapping)
        
        # Clean up dataframe
        reviews_df.drop_unnamed_columns()
        
        # Convert timestamps
        timestamp_columns = ['timestamp_created', 'timestamp_updated', 'author_last_played']
        for col in timestamp_columns:
            reviews_df.convert_datetime_column(col, unit='s')
        
        # Handle duplicates
        reviews_df.handle_duplicates()
        
        # Sort and limit records
        reviews_df.sort_by_column('author_steamid')
        reviews_df.limit_records(limit)
        
        print(f" Reviews dataset prepared: {len(reviews_df.df)} records")
        return reviews_df

class DatabaseManager:
    """Manages database imports"""
    
    def __init__(self):
        self.importers = {}
        self.results = {}
    
    def init_mongodb(self):
        """Initialize MongoDB importer"""
        try:
            self.importers['mongodb'] = MongoDBImporter(
                uri=os.getenv('MONGO_URI'),
                database_name=os.getenv('DATABASE_NAME')
            )
            print(" MongoDB importer initialized")
            return True
        except Exception as e:
            print(f"✗ MongoDB initialization failed: {e}")
            return False
    
    def init_neo4j(self):
        """Initialize Neo4j importer"""
        try:
            self.importers['neo4j'] = Neo4jImporter(
                uri=os.getenv('NEO4J_URI'),
                user=os.getenv('NEO4J_USER'),
                password=os.getenv('NEO4J_PASSWORD')
            )
            print(" Neo4j importer initialized")
            return True
        except Exception as e:
            print(f"✗ Neo4j initialization failed: {e}")
            return False
    
    def init_postgresql(self):
        """Initialize PostgreSQL importer"""
        try:
            self.importers['postgresql'] = PostgreSQLImporter()
            print(" PostgreSQL importer initialized")
            return True
        except Exception as e:
            print(f"✗ PostgreSQL initialization failed: {e}")
            return False
    
    def import_to_mongodb(self, games_df, reviews_df):
        """Import data to MongoDB"""
        if 'mongodb' not in self.importers:
            print("MongoDB importer not initialized")
            return False
        
        try:
            print("\n=== Importing to MongoDB ===")
            importer = self.importers['mongodb']
            
            # Import games (importer handles MongoDB-specific data cleaning)
            importer.import_games(games_df)
            
            # Import reviews
            importer.import_reviews(reviews_df)
            
            # Verify imports
            games_count = importer.db.games.count_documents({})
            reviews_count = importer.db.reviews.count_documents({})
            
            self.results['mongodb'] = {
                'games': games_count,
                'reviews': reviews_count,
                'status': 'success'
            }
            
            print(f" MongoDB import completed - Games: {games_count}, Reviews: {reviews_count}")
            return True
            
        except Exception as e:
            print(f"✗ MongoDB import failed: {e}")
            self.results['mongodb'] = {'status': 'failed', 'error': str(e)}
            return False
    
    def import_to_neo4j(self, games_df, reviews_df):
        """Import data to Neo4j"""
        if 'neo4j' not in self.importers:
            print("Neo4j importer not initialized")
            return False
        
        try:
            print("\n=== Importing to Neo4j ===")
            importer = self.importers['neo4j']
            
            # Import games
            importer.import_games(games_df)
            
            # Import reviews
            importer.import_reviews(reviews_df)
            
            # Verify imports
            with importer.driver.session() as session:
                games_count = session.run("MATCH (g:Game) RETURN count(g) as count").single()['count']
                reviews_count = session.run("MATCH (r:Review) RETURN count(r) as count").single()['count']
                devs_count = session.run("MATCH (d:Developer) RETURN count(d) as count").single()['count']
                genres_count = session.run("MATCH (g:Genre) RETURN count(g) as count").single()['count']
            
            self.results['neo4j'] = {
                'games': games_count,
                'reviews': reviews_count,
                'developers': devs_count,
                'genres': genres_count,
                'status': 'success'
            }
            
            print(f" Neo4j import completed - Games: {games_count}, Reviews: {reviews_count}")
            print(f"  Additional nodes - Developers: {devs_count}, Genres: {genres_count}")
            return True
            
        except Exception as e:
            print(f"✗ Neo4j import failed: {e}")
            self.results['neo4j'] = {'status': 'failed', 'error': str(e)}
            return False
    
    def import_to_postgresql(self, games_df, reviews_df):
        """Import data to PostgreSQL"""
        if 'postgresql' not in self.importers:
            print("PostgreSQL importer not initialized")
            return False
        
        try:
            print("\n=== Importing to PostgreSQL ===")
            importer = self.importers['postgresql']
            
            # Import games with JSON columns
            games_json_cols = ['supported_languages', 'full_audio_languages', 'packages', 
                              'developers', 'publishers', 'categories', 'genres', 
                              'screenshots', 'movies', 'tags']
            importer.import_dataset('games', games_df, json_columns=games_json_cols)
            
            # Import reviews
            importer.import_dataset('reviews', reviews_df)
            
            self.results['postgresql'] = {
                'games': len(games_df.df),
                'reviews': len(reviews_df.df),
                'status': 'success'
            }
            
            print(f" PostgreSQL import completed - Games: {len(games_df.df)}, Reviews: {len(reviews_df.df)}")
            return True
            
        except Exception as e:
            print(f"✗ PostgreSQL import failed: {e}")
            self.results['postgresql'] = {'status': 'failed', 'error': str(e)}
            return False
    
    def close_connections(self):
        """Close all database connections"""
        for db_name, importer in self.importers.items():
            try:
                if hasattr(importer, 'close'):
                    importer.close()
                print(f" {db_name.title()} connection closed")
            except Exception as e:
                print(f"✗ Error closing {db_name} connection: {e}")
    
    def print_summary(self):
        """Print import summary"""
        print("\n" + "="*60)
        print("IMPORT SUMMARY")
        print("="*60)
        
        for db_name, result in self.results.items():
            status_symbol = "" if result['status'] == 'success' else "✗"
            print(f"{status_symbol} {db_name.upper()}:")
            
            if result['status'] == 'success':
                print(f"  Games: {result.get('games', 'N/A')}")
                print(f"  Reviews: {result.get('reviews', 'N/A')}")
                if 'developers' in result:
                    print(f"  Developers: {result['developers']}")
                if 'genres' in result:
                    print(f"  Genres: {result['genres']}")
            else:
                print(f"  Error: {result.get('error', 'Unknown error')}")
            print()

def main():
    parser = argparse.ArgumentParser(description='Import Steam datasets to multiple databases')
    parser.add_argument('--databases', '-d', nargs='+', 
                       choices=['mongodb', 'neo4j', 'postgresql', 'all'],
                       default=['all'],
                       help='Databases to import to (default: all)')
    parser.add_argument('--reviews-limit', '-r', type=int, default=1000000,
                       help='Limit number of reviews to import (default: 1000000)')
    parser.add_argument('--skip-games', action='store_true',
                       help='Skip games import')
    parser.add_argument('--skip-reviews', action='store_true',
                       help='Skip reviews import')
    
    args = parser.parse_args()
    
    # Expand 'all' option
    if 'all' in args.databases:
        args.databases = ['mongodb', 'neo4j', 'postgresql']
    
    print("Steam Dataset Multi-Database Importer")
    print("====================================")
    print(f"Target databases: {', '.join(args.databases)}")
    print(f"Reviews limit: {args.reviews_limit}")
    
    try:
        # Process datasets once
        processor = DataProcessor()
        
        games_df = None
        reviews_df = None
        
        if not args.skip_games:
            games_df = processor.prepare_games_dataframe()
        
        if not args.skip_reviews:
            reviews_df = processor.prepare_reviews_dataframe(limit=args.reviews_limit)
        
        # Initialize database manager
        db_manager = DatabaseManager()
        
        # Initialize requested databases
        initialized_dbs = []
        for db_name in args.databases:
            if db_name == 'mongodb':
                if db_manager.init_mongodb():
                    initialized_dbs.append(db_name)
            elif db_name == 'neo4j':
                if db_manager.init_neo4j():
                    initialized_dbs.append(db_name)
            elif db_name == 'postgresql':
                if db_manager.init_postgresql():
                    initialized_dbs.append(db_name)
        
        if not initialized_dbs:
            print("✗ No databases successfully initialized. Exiting.")
            return
        
        print(f"\n Successfully initialized: {', '.join(initialized_dbs)}")
        
        # Import to each database
        for db_name in initialized_dbs:
            try:
                if db_name == 'mongodb':
                    db_manager.import_to_mongodb(games_df, reviews_df)
                elif db_name == 'neo4j':
                    db_manager.import_to_neo4j(games_df, reviews_df)
                elif db_name == 'postgresql':
                    db_manager.import_to_postgresql(games_df, reviews_df)
            except Exception as e:
                print(f"✗ Critical error during {db_name} import: {e}")
                import traceback
                traceback.print_exc()
        
        # Print summary
        db_manager.print_summary()
        
    except Exception as e:
        print(f"✗ Critical error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Always close connections
        if 'db_manager' in locals():
            db_manager.close_connections()

if __name__ == "__main__":
    main()