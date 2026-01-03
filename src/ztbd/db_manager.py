import os
import time
import pickle
from pathlib import Path

from .ztbdf import create_games_dataframe, create_reviews_dataframe, create_hltb_dataframe
from .mongodb.importer import MongoDBImporter
from .neo4j.importer import Neo4jImporter
from .postgresql.importer import PostgreSQLImporter

import logging

logger = logging.getLogger('ztbd')

class DataProcessor:
    """Centralized data processing for all datasets"""
    
    CACHE_DIR = Path(os.getenv('CACHE_DIR', "cache"))

    @staticmethod
    def prepare_games_dataframe(use_cache=False):
        """Prepare and clean games dataframe"""
        if use_cache:
            cached = DataProcessor.load_dataframe('games')
            if cached:
                return cached

        logger.info("\n=== Processing Games Dataset ===")
        games_df = create_games_dataframe()
        games_df.log_shape()
        
        # Parse JSON columns
        json_columns = ['supported_languages', 'full_audio_languages', 'packages', 
                       'developers', 'publishers', 'categories', 'genres', 
                       'screenshots', 'movies', 'tags']
        games_df.parse_json_columns(json_columns)
        
        # Convert date column
        # games_df.convert_datetime_column('release_date')
        
        # Handle duplicates
        games_df.handle_duplicates()
        
        logger.info(f"Games dataset prepared: {len(games_df.df)} records")
        DataProcessor.save_dataframe(games_df, 'games')
        return games_df
    
    @staticmethod
    def prepare_reviews_dataframe(limit=1000000, use_cache=False):
        """Prepare and clean reviews dataframe"""
        cache_name = f'reviews_{limit}'
        
        if use_cache:
            cached = DataProcessor.load_dataframe(cache_name)
            if cached:
                return cached
        
        logger.info("\n=== Processing Reviews Dataset ===")
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
        reviews_df.sort_by_column(reviews_df.primary_key)
        reviews_df.limit_records(limit)
        
        logger.info(f" Reviews dataset prepared: {len(reviews_df.df)} records")
        DataProcessor.save_dataframe(reviews_df, cache_name)
        return reviews_df

    @staticmethod
    def prepare_hltb_dataframe(use_cache=False):
        """Prepare and clean How Long to Beat dataframe"""
        if use_cache:
            cached = DataProcessor.load_dataframe('hltb')
            if cached:
                return cached

        logger.info("\n=== Processing Games How Long to Beat Dataset ===")
        hltb_df = create_hltb_dataframe()
        hltb_df.log_shape()
        hltb_df.handle_duplicates()
        
        logger.info(f"How Long to Beat dataset prepared: {len(hltb_df.df)} records")
        DataProcessor.save_dataframe(hltb_df, 'hltb')
        return hltb_df

    @staticmethod
    def _ensure_cache_dir():
        """Create cache directory if it doesn't exist"""
        DataProcessor.CACHE_DIR.mkdir(exist_ok=True)
    
    @staticmethod
    def _get_cache_path(dataset_name):
        """Get cache file path for a dataset"""
        return DataProcessor.CACHE_DIR / f"{dataset_name}_prepared.pkl"
    
    @staticmethod
    def save_dataframe(ztb_df, dataset_name):
        """Save prepared dataframe to cache"""
        DataProcessor._ensure_cache_dir()
        cache_path = DataProcessor._get_cache_path(dataset_name)
        
        with open(cache_path, 'wb') as f:
            pickle.dump(ztb_df, f)
        
        logger.info(f"Saved {dataset_name} to cache: {cache_path}")


    @staticmethod
    def load_dataframe(dataset_name):
        """Load prepared dataframe from cache"""
        cache_path = DataProcessor._get_cache_path(dataset_name)
        
        if not cache_path.exists():
            logger.warning(f"Cache not found for {dataset_name}")
            return None
        
        with open(cache_path, 'rb') as f:
            ztb_df = pickle.load(f)
        
        logger.info(f"Loaded {dataset_name} from cache: {cache_path}")
        return ztb_df

class DatabaseManager:
    """Manages database imports"""
    
    def __init__(self):
        self.importers = {}
        self.results = {}

    def init_db(self, name):
        """Initialize Any DB importer"""
        funcs = {'neo4j': self.init_neo4j,
                 'mongodb': self.init_mongodb,
                 'postgresql': self.init_postgresql}
        return funcs[name]()
    
    def init_mongodb(self):
        """Initialize MongoDB importer"""
        try:
            self.importers['mongodb'] = MongoDBImporter(
                uri=os.getenv('MONGO_URI', "mongodb://user:password@localhost:27017/"),
                database_name=os.getenv('DATABASE_NAME', "mongodb")
            )
            logger.info(" MongoDB importer initialized")
            return True
        except Exception as e:
            logging.error(f"XX MongoDB initialization failed: {e}")
            return False
    
    def init_neo4j(self):
        """Initialize Neo4j importer"""
        try:
            self.importers['neo4j'] = Neo4jImporter(
                uri=os.getenv('NEO4J_URI', "bolt://localhost:7687"),
                user=os.getenv('NEO4J_USER', "user"),
                password=os.getenv('NEO4J_PASSWORD', "password")
            )
            logger.info(" Neo4j importer initialized")
            return True
        except Exception as e:
            logging.error(f"XX Neo4j initialization failed: {e}")
            return False
    
    def init_postgresql(self):
        """Initialize PostgreSQL importer"""
        try:
            self.importers['postgresql'] = PostgreSQLImporter()
            logger.info(" PostgreSQL importer initialized")
            return True
        except Exception as e:
            logging.error(f"XX PostgreSQL initialization failed: {e}")
            return False
    
    def import_to_mongodb(self, games_df, reviews_df, hltb_df, drop=False):
        """Import data to MongoDB"""
        if 'mongodb' not in self.importers:
            logger.info("MongoDB importer not initialized")
            return False
        
        try:
            logger.info("\n=== Importing to MongoDB ===")
            start_time = time.time()
            importer = self.importers['mongodb']

            drop_time = start_time
            if drop:
                importer.clean_database(['reviews', 'games', 'hltb'])
                importer.verify_empty(['reviews', 'games', 'hltb'])
                drop_time = time.time()
            
            # Import games (importer handles MongoDB-specific data cleaning)
            importer.import_df(games_df, indexes = ["appid", "name", "release_date"])

            # Import reviews
            importer.import_df(reviews_df, indexes = ["review_id", "app_id", "recommended", "timestamp_created"], batch_size = 5000)

            # Import hltb
            importer.import_df(hltb_df, indexes = ["game_game_id", "game_game_name", "game_comp_all_count"])
            import_time = time.time()
            
            # Verify imports
            games_count = importer.db.games.count_documents({})
            reviews_count = importer.db.reviews.count_documents({})
            hltb_count = importer.db.hltb.count_documents({})
            verify_time = time.time()
            
            self.results['mongodb'] = {
                'games': games_count,
                'reviews': reviews_count,
                'hltbs': hltb_count,
                'status': 'success',
                'import_time': import_time - start_time,
                'verify_time': verify_time - import_time,
                'drop_time': drop_time - start_time,
            }
            
            logger.info(f" MongoDB import completed - Games: {games_count}, Reviews: {reviews_count}, HLTBs: {hltb_count}")
            return True
            
        except Exception as e:
            logging.error(f"XX MongoDB import failed: {e}")
            self.results['mongodb'] = {'status': 'failed', 'error': str(e)}
            return False
    
    def import_to_neo4j(self, games_df, reviews_df, hltb_df, drop=False):
        """Import data to Neo4j"""
        if 'neo4j' not in self.importers:
            logger.info("Neo4j importer not initialized")
            return False
        
        try:
            logger.info("\n=== Importing to Neo4j ===")
            start_time = time.time()
            importer = self.importers['neo4j']

            drop_time = start_time
            if drop:
                importer.clean_database()  # Clean everything
                importer.verify_empty()
                drop_time = time.time()
            
            # Import games with relationships
            relationship_configs = [
                {'type': 'DEVELOPED_BY', 'target_label': 'Developer', 'source_key': 'developers'},
                {'type': 'PUBLISHED_BY', 'target_label': 'Publisher', 'source_key': 'publishers'},
                {'type': 'HAS_GENRE', 'target_label': 'Genre', 'source_key': 'genres'},
                {'type': 'HAS_CATEGORY', 'target_label': 'Category', 'source_key': 'categories'}
            ]
            
            importer.import_df(
                games_df, 
                node_label='Game',
                indexes=['name', 'release_date', 'price'],
                relationship_configs=relationship_configs
            )
            
            # Import reviews
            importer.import_df(
                reviews_df,
                node_label='Review',
                indexes=['app_id', 'recommended', 'timestamp_created'],
                batch_size=5000
            )

            # Import HLTB data
            importer.import_df(
                hltb_df,
                node_label='HLTB',
                indexes=['game_game_name', 'game_comp_main', 'game_comp_all_count'],
                batch_size=1000
            )

            import_time = time.time()
            
            # Create REVIEWED relationships
            with importer.driver.session() as session:
                session.run("""
                    MATCH (r:Review)
                    MATCH (g:Game {appid: r.app_id})
                    MERGE (r)-[:REVIEWED]->(g)
                """)


            # Create HAS_PLAYTIME_DATA relationships
            # Match by game name (case-insensitive)
            result = session.run("""
                MATCH (h:HLTB)
                MATCH (g:Game)
                WHERE toLower(h.game_game_name) = toLower(g.name)
                MERGE (g)-[:HAS_PLAYTIME_DATA]->(h)
                RETURN count(*) as linked
            """)
            linked_count = result.single()['linked']
            logger.info(f"  Linked {linked_count} HLTB records to games")
        
            relationships_time = time.time()
            
            
            # Verify imports
            with importer.driver.session() as session:
                games_count = session.run("MATCH (g:Game) RETURN count(g) as count").single()['count']
                reviews_count = session.run("MATCH (r:Review) RETURN count(r) as count").single()['count']
                devs_count = session.run("MATCH (d:Developer) RETURN count(d) as count").single()['count']
                genres_count = session.run("MATCH (g:Genre) RETURN count(g) as count").single()['count']
                hltb_count = session.run("MATCH (h:HLTB) RETURN count(h) as count").single()['count']
            verify_time = time.time()
            
            self.results['neo4j'] = {
                'games': games_count,
                'reviews': reviews_count,
                'developers': devs_count,
                'genres': genres_count,
                'hltbs': hltb_count,
                'status': 'success',
                'import_time': import_time - start_time,
                'verify_time': verify_time - import_time,
                'drop_time': drop_time - start_time,
                'relationships_time': relationships_time - import_time,
            }
            

            logger.info(f" Neo4j import completed - Games: {games_count}, Reviews: {reviews_count}, HLTB: {hltb_count}")
            logger.info(f"  Additional nodes - Developers: {devs_count}, Genres: {genres_count}")
            return True
            
        except Exception as e:
            logger.error(f"XX Neo4j import failed: {e}")
            self.results['neo4j'] = {'status': 'failed', 'error': str(e)}
            return False
    
    def import_to_postgresql(self, games_df, reviews_df, hltb_df, drop = False):
        """Import data to PostgreSQL"""
        if 'postgresql' not in self.importers:
            logger.warning("PostgreSQL importer not initialized")
            return False
        
        try:
            logger.info("\n=== Importing to PostgreSQL ===")
            start_time = time.time()
            importer = self.importers['postgresql']
            
            drop_time = start_time
            if drop:
                importer.clean_database(['reviews', 'games', 'hltb'])
                importer.verify_empty(['reviews', 'games', 'hltb'])
                drop_time = time.time()

            # Import games with JSON columns
            games_json_cols = ['supported_languages', 'full_audio_languages', 'packages', 
                              'developers', 'publishers', 'categories', 'genres', 
                              'screenshots', 'movies', 'tags']
            importer.import_df(games_df, json_columns=games_json_cols)
            
            # Import reviews
            importer.import_df(reviews_df)
            
            importer.import_df(hltb_df)

            import_time = time.time()

            # TODO: Verify postgre import
            verify_time = time.time()
            
            self.results['postgresql'] = {
                'games': len(games_df.df),
                'reviews': len(reviews_df.df),
                'hltbs': len(hltb_df.df),
                'status': 'success',
                'import_time': import_time - start_time,
                'verify_time': verify_time - import_time,
                'drop_time': drop_time - start_time,
            }
            
            logger.info(f" PostgreSQL import completed - Games: {len(games_df.df)}, Reviews: {len(reviews_df.df)}, HLTBs: {len(hltb_df.df)}")
            return True
            
        except Exception as e:
            logger.error(f"XX PostgreSQL import failed: {e}")
            self.results['postgresql'] = {'status': 'failed', 'error': str(e)}
            return False
    
    def close_connections(self):
        """Close all database connections"""
        for db_name, importer in self.importers.items():
            try:
                if hasattr(importer, 'close'):
                    importer.close()
                logger.info(f" {db_name.title()} connection closed")
            except Exception as e:
                logger.error(f"XX Error closing {db_name} connection: {e}")
    
    def print_summary(self):
        """Print import summary"""
        print("\n" + "="*60)
        print("IMPORT SUMMARY")
        print("="*60)
        
        for db_name, result in self.results.items():
            status_symbol = "" if result['status'] == 'success' else "X"
            print(f"{status_symbol} {db_name.upper()}:")
            
            if result['status'] == 'success':
                print(f"   Games: {result.get('games', 'N/A')}")
                print(f"   Reviews: {result.get('reviews', 'N/A')}")
                print(f"   HLTBs: {result.get('hltbs', 'N/A')}")
                if 'developers' in result:
                    print(f"   Developers: {result['developers']}")
                if 'genres' in result:
                    print(f"   Genres: {result['genres']}")
                print("  Timings:")
                print(f"   Import Time: {result.get('import_time', 'N/A'):.2f}s")
                print(f"   Verify Time: {result.get('verify_time', 'N/A'):.2f}s")
                if result.get('drop_time') != 0:
                    print(f"   Drop Time: {result.get('drop_time', 'N/A'):.2f}s")
                if 'relationships_time' in result:
                    print(f"   Relationships Time: {result.get('relationships_time', 'N/A'):.2f}s")
            else:
                print(f"  Error: {result.get('error', 'Unknown error')}")
            print()

    def print_summary(self):
        """Print import summary"""
        logger.info("\n" + "="*60)
        logger.info("IMPORT SUMMARY")
        logger.info("="*60)
        
        for db_name, result in self.results.items():
            status_symbol = "" if result['status'] == 'success' else "X"
            logger.info(f"{status_symbol} {db_name.upper()}:")
            
            if result['status'] == 'success':
                logger.info(f"   Games: {result.get('games', 'N/A')}")
                logger.info(f"   Reviews: {result.get('reviews', 'N/A')}")
                logger.info(f"   HLTBs: {result.get('hltbs', 'N/A')}")
                if 'developers' in result:
                    logger.info(f"   Developers: {result['developers']}")
                if 'genres' in result:
                    logger.info(f"   Genres: {result['genres']}")
                logger.info("  Timings:")
                logger.info(f"   Import Time: {result.get('import_time', 'N/A'):.2f}s")
                logger.info(f"   Verify Time: {result.get('verify_time', 'N/A'):.2f}s")
                if result.get('drop_time') != 0:
                    logger.info(f"   Drop Time: {result.get('drop_time', 'N/A'):.2f}s")
                if 'relationships_time' in result:
                    logger.info(f"   Relationships Time: {result.get('relationships_time', 'N/A'):.2f}s")
            else:
                logger.error(f"  Error: {result.get('error', 'Unknown error')}")
