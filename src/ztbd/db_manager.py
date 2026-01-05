import os
import time
import pickle
from pathlib import Path

from .ztbdf import create_games_dataframe, create_reviews_dataframe, create_hltb_dataframe
from .mongodb.importer import MongoDBImporter
from .neo4j.importer import Neo4jImporter
from .postgresql.importer import PostgreSQLImporter
from .mysql.importer import MySQLImporter
from .normalizer import DataNormalizer

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

        logger.info("== Processing Games Dataset ===")
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
        
        logger.info("== Processing Reviews Dataset ===")
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

        logger.info("== Processing Games How Long to Beat Dataset ===")
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
        self.normalized_data = {}

    def init_db(self, name):
        """Initialize Any DB importer"""
        funcs = {'neo4j': self.init_neo4j,
                 'mongodb': self.init_mongodb,
                 'postgresql': self.init_postgresql,
                 'mysql': self.init_mysql}
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
    
    def init_mysql(self):
        """Initialize MySQL importer"""
        try:
            self.importers['mysql'] = MySQLImporter()
            logger.info(" MySQL importer initialized")
            return True
        except Exception as e:
            logging.error(f" MySQL initialization failed: {e}")
            return False

    def prepare_normalized_data(self, games_df, reviews_df):
        """
        Prepare all normalized tables from the main dataframes
        Call this once before importing to any database
        """
        logger.info("=== PREPARING NORMALIZED DATA ===")
        
        # Extract dimension tables
        self.normalized_data['developers'] = DataNormalizer.extract_developers(games_df)
        self.normalized_data['publishers'] = DataNormalizer.extract_publishers(games_df)
        self.normalized_data['genres'] = DataNormalizer.extract_genres(games_df)
        self.normalized_data['categories'] = DataNormalizer.extract_categories(games_df)
        self.normalized_data['tags'] = DataNormalizer.extract_tags(games_df)
        
        # Create association tables
        self.normalized_data['game_developers'] = DataNormalizer.create_game_developer_associations(
            games_df, self.normalized_data['developers']
        )
        self.normalized_data['game_publishers'] = DataNormalizer.create_game_publisher_associations(
            games_df, self.normalized_data['publishers']
        )
        self.normalized_data['game_genres'] = DataNormalizer.create_game_genre_associations(
            games_df, self.normalized_data['genres']
        )
        self.normalized_data['game_categories'] = DataNormalizer.create_game_category_associations(
            games_df, self.normalized_data['categories']
        )
        self.normalized_data['game_tags'] = DataNormalizer.create_game_tag_associations(
            games_df, self.normalized_data['tags']
        )
        
        # Extract user profiles
        self.normalized_data['user_profiles'] = DataNormalizer.extract_user_profiles(reviews_df)
        
        # Create aggregation tables
        self.normalized_data['game_review_summary'] = DataNormalizer.create_game_review_summary(
            games_df, reviews_df
        )
        self.normalized_data['developer_stats'] = DataNormalizer.create_developer_stats(
            games_df, self.normalized_data['developers'], self.normalized_data['game_developers']
        )
        
        # Simulate price history
        self.normalized_data['game_price_history'] = DataNormalizer.simulate_price_history(
            games_df, months_back=12
        )
        
        logger.info("=== NORMALIZED DATA PREPARED ===\n")
        return self.normalized_data
    
    def import_to_mongodb(self, games_df, reviews_df, hltb_df, drop=False):
        """Import data to MongoDB"""
        if 'mongodb' not in self.importers:
            logger.info("MongoDB importer not initialized")
            return False
        
        try:
            logger.info("== Importing to MongoDB ===")
            start_time = time.time()
            importer = self.importers['mongodb']

            drop_time = start_time
            if drop:
                tables_to_drop = [
                    'reviews', 'games', 'hltb',
                    'game_developers', 'game_publishers', 'game_genres', 
                    'game_categories', 'game_tags',
                    'developers', 'publishers', 'genres', 'categories', 'tags',
                    'user_profiles', 'game_review_summary', 'developer_stats',
                    'game_price_history'
                ]
                importer.clean_database(tables_to_drop)
                importer.verify_empty(tables_to_drop)
                drop_time = time.time()
            
            # Import main tables
            importer.import_df(games_df, indexes=["appid", "name", "release_date"])
            importer.import_df(reviews_df, indexes=["review_id", "app_id", "recommended", "timestamp_created"], batch_size=5000)
            importer.import_df(hltb_df, indexes=["game_game_id", "game_game_name", "game_comp_all_count"])
            
            # Import normalized tables if available
            if self.normalized_data:
                normalized_time = time.time()
                logger.info("Importing normalized tables to MongoDB...")
                
                # Import dimension tables
                importer.import_dataframe(
                    self.normalized_data['developers'], 
                    collection_name='developers',
                    indexes=['developer_id', 'name']
                )
                importer.import_dataframe(
                    self.normalized_data['publishers'], 
                    collection_name='publishers',
                    indexes=['publisher_id', 'name']
                )
                importer.import_dataframe(
                    self.normalized_data['genres'], 
                    collection_name='genres',
                    indexes=['genre_id', 'name']
                )
                importer.import_dataframe(
                    self.normalized_data['categories'], 
                    collection_name='categories',
                    indexes=['category_id', 'name']
                )
                importer.import_dataframe(
                    self.normalized_data['tags'], 
                    collection_name='tags',
                    indexes=['tag_id', 'name']
                )
                
                # Import association tables
                importer.import_dataframe(
                    self.normalized_data['game_developers'], 
                    collection_name='game_developers',
                    indexes=['game_appid', 'developer_id']
                )
                importer.import_dataframe(
                    self.normalized_data['game_publishers'], 
                    collection_name='game_publishers',
                    indexes=['game_appid', 'publisher_id']
                )
                importer.import_dataframe(
                    self.normalized_data['game_genres'], 
                    collection_name='game_genres',
                    indexes=['game_appid', 'genre_id']
                )
                importer.import_dataframe(
                    self.normalized_data['game_categories'], 
                    collection_name='game_categories',
                    indexes=['game_appid', 'category_id']
                )
                importer.import_dataframe(
                    self.normalized_data['game_tags'], 
                    collection_name='game_tags',
                    indexes=['game_appid', 'tag_id'],
                    batch_size=5000
                )
                
                # Import aggregation tables
                importer.import_dataframe(
                    self.normalized_data['user_profiles'], 
                    collection_name='user_profiles',
                    indexes=['author_steamid']
                )
                importer.import_dataframe(
                    self.normalized_data['game_review_summary'], 
                    collection_name='game_review_summary',
                    indexes=['game_appid']
                )
                importer.import_dataframe(
                    self.normalized_data['developer_stats'], 
                    collection_name='developer_stats',
                    indexes=['developer_id']
                )
                importer.import_dataframe(
                    self.normalized_data['game_price_history'], 
                    collection_name='game_price_history',
                    indexes=['game_appid', 'recorded_date'],
                    batch_size=5000
                )
            
            import_time = time.time()
            
            # Verify imports
            games_count = importer.db.games.count_documents({})
            reviews_count = importer.db.reviews.count_documents({})
            hltb_count = importer.db.hltb.count_documents({})
            
            normalized_counts = {}
            if self.normalized_data:
                normalized_counts = {
                    'developers': importer.db.developers.count_documents({}),
                    'publishers': importer.db.publishers.count_documents({}),
                    'genres': importer.db.genres.count_documents({}),
                    'categories': importer.db.categories.count_documents({}),
                    'tags': importer.db.tags.count_documents({}),
                }
            
            verify_time = time.time()
            
            self.results['mongodb'] = {
                'games': games_count,
                'reviews': reviews_count,
                'hltbs': hltb_count,
                'normalized_tables': len(self.normalized_data) if self.normalized_data else 0,
                'status': 'success',
                'import_time': import_time - start_time,
                'verify_time': verify_time - import_time,
                'drop_time': drop_time - start_time,
                'normalized_time': normalized_time - import_time if self.normalized_data else 0,
            }
            
            logger.info(f" MongoDB import completed - Games: {games_count}, Reviews: {reviews_count}, HLTBs: {hltb_count}")
            if normalized_counts:
                logger.info(f"  Normalized - Developers: {normalized_counts['developers']}, Publishers: {normalized_counts['publishers']}")
            return True
            
        except Exception as e:
            logger.error(f"XX MongoDB import failed: {e}")
            self.results['mongodb'] = {'status': 'failed', 'error': str(e)}
            return False


    def import_to_neo4j(self, games_df, reviews_df, hltb_df, drop=False):
        """Import data to Neo4j"""
        if 'neo4j' not in self.importers:
            logger.info("Neo4j importer not initialized")
            return False
        
        try:
            logger.info("== Importing to Neo4j ===")
            start_time = time.time()
            importer = self.importers['neo4j']

            drop_time = start_time
            if drop:
                # Don't drop - takes to much time to reimport
                # importer.clean_database()
                # importer.verify_empty()
                drop_time = time.time()
            
            # Import games with relationships
            relationship_configs = [
                {'type': 'DEVELOPED_BY', 'target_label': 'Developer', 'source_key': 'developers'},
                {'type': 'PUBLISHED_BY', 'target_label': 'Publisher', 'source_key': 'publishers'},
                {'type': 'HAS_GENRE', 'target_label': 'Genre', 'source_key': 'genres'},
                {'type': 'HAS_CATEGORY', 'target_label': 'Category', 'source_key': 'categories'}
            ]
            
            if games_df:
                importer.import_df(
                    games_df, 
                    node_label='Game',
                    indexes=['name', 'release_date', 'price'],
                    relationship_configs=relationship_configs
                )
            
            # Import reviews
            if reviews_df:
                importer.import_df(
                    reviews_df,
                    node_label='Review',
                    indexes=['app_id', 'recommended', 'timestamp_created'],
                    batch_size=5000
                )

            # Import HLTB data
            if hltb_df:
                importer.import_df(
                    hltb_df,
                    node_label='HLTB',
                    indexes=['game_game_name', 'game_comp_main', 'game_comp_all_count'],
                    batch_size=1000
                )

            # Import normalized tables
            if self.normalized_data:
                normalized_time = time.time()
                logger.info("\nImporting normalized dimension tables to Neo4j...")
                
                # Primary key mapping for each table
                pk_mapping = {
                    'developers': 'developer_id',
                    'publishers': 'publisher_id',
                    'genres': 'genre_id',
                    'categories': 'category_id',
                    'tags': 'tag_id',
                    'user_profiles': 'author_steamid',
                    'game_review_summary': 'game_appid',
                    'developer_stats': 'developer_id',
                    'game_price_history': 'history_id'
                }
                
                # Node label mapping (capitalize first letter)
                label_mapping = {
                    'developers': 'GameDeveloper',
                    'publishers': 'GamePublisher',
                    'genres': 'GameGenre',
                    'categories': 'GameCategory',
                    'tags': 'GameTag',
                    'user_profiles': 'UserProfile',
                    'game_review_summary': 'GameReviewSummary',
                    'developer_stats': 'DeveloperStats',
                    'game_price_history': 'GamePriceHistory'
                }
                
                # Import dimension tables
                # for table in ['developers', 'publishers', 'genres', 'categories', 'tags']:
                #     importer.import_dataframe(
                #         self.normalized_data[table],
                #         node_label=label_mapping[table],
                #         primary_key=pk_mapping[table],
                #         indexes=['name']
                #     )
                
                # Import aggregation tables
                # Imported only 305000/784348 nodes - it was taking too long
                # importer.import_dataframe(
                #     self.normalized_data['user_profiles'],
                #     node_label='UserProfile',
                #     primary_key='author_steamid',
                #     indexes=['num_games_owned', 'positive_review_count']
                # )
                
                importer.import_dataframe(
                    self.normalized_data['game_review_summary'],
                    node_label='GameReviewSummary',
                    primary_key='game_appid',
                    indexes=['total_reviews', 'positive_reviews']
                )
                
                importer.import_dataframe(
                    self.normalized_data['developer_stats'],
                    node_label='DeveloperStats',
                    primary_key='developer_id',
                    indexes=['total_games', 'avg_game_price']
                )
                
                # Imported only 420000/980954 nodes - it was taking too long
                # importer.import_dataframe(
                #     self.normalized_data['game_price_history'],
                #     node_label='GamePriceHistory',
                #     primary_key='history_id',
                #     indexes=['game_appid', 'recorded_date'],
                #     batch_size=5000
                # )
                
                # Create relationships for normalized tables
                logger.info("\nCreating normalized table relationships...")
                
                with importer.driver.session() as session:
                    # Link developers to games via game_developers
                    if 'game_developers' in self.normalized_data:
                        for _, row in self.normalized_data['game_developers'].iterrows():
                            session.run("""
                                MATCH (g:Game {appid: $game_appid})
                                MATCH (d:GameDeveloper {developer_id: $developer_id})
                                MERGE (g)-[:DEVELOPED_BY_NORM]->(d)
                            """, game_appid=int(row['game_appid']), developer_id=int(row['developer_id']))
                        logger.info("  Created DEVELOPED_BY_NORM relationships")
                    
                    # Link publishers to games
                    if 'game_publishers' in self.normalized_data:
                        for _, row in self.normalized_data['game_publishers'].iterrows():
                            session.run("""
                                MATCH (g:Game {appid: $game_appid})
                                MATCH (p:GamePublisher {publisher_id: $publisher_id})
                                MERGE (g)-[:PUBLISHED_BY_NORM]->(p)
                            """, game_appid=int(row['game_appid']), publisher_id=int(row['publisher_id']))
                        logger.info("  Created PUBLISHED_BY_NORM relationships")
                    
                    # Link genres to games
                    if 'game_genres' in self.normalized_data:
                        for _, row in self.normalized_data['game_genres'].iterrows():
                            session.run("""
                                MATCH (g:Game {appid: $game_appid})
                                MATCH (ge:GameGenre {genre_id: $genre_id})
                                MERGE (g)-[:HAS_GENRE_NORM]->(ge)
                            """, game_appid=int(row['game_appid']), genre_id=int(row['genre_id']))
                        logger.info("  Created HAS_GENRE_NORM relationships")
                    
                    # Link categories to games
                    if 'game_categories' in self.normalized_data:
                        for _, row in self.normalized_data['game_categories'].iterrows():
                            session.run("""
                                MATCH (g:Game {appid: $game_appid})
                                MATCH (c:GameCategory {category_id: $category_id})
                                MERGE (g)-[:HAS_CATEGORY_NORM]->(c)
                            """, game_appid=int(row['game_appid']), category_id=int(row['category_id']))
                        logger.info("  Created HAS_CATEGORY_NORM relationships")
                    
                    # Link tags to games with vote counts
                    if 'game_tags' in self.normalized_data:
                        # Process in batches for performance
                        batch_size = 1000
                        tags_data = self.normalized_data['game_tags']
                        for i in range(0, len(tags_data), batch_size):
                            batch = tags_data.iloc[i:i+batch_size]
                            for _, row in batch.iterrows():
                                session.run("""
                                    MATCH (g:Game {appid: $game_appid})
                                    MATCH (t:GameTagTag {tag_id: $tag_id})
                                    MERGE (g)-[r:HAS_TAG_NORM]->(t)
                                    SET r.vote_count = $vote_count
                                """, game_appid=int(row['game_appid']), 
                                    tag_id=int(row['tag_id']),
                                    vote_count=int(row['vote_count']))
                            if (i + batch_size) % 5000 == 0:
                                logger.info(f"    Created {min(i + batch_size, len(tags_data))}/{len(tags_data)} tag relationships")
                        logger.info("  Created HAS_TAG_NORM relationships")
                    
                    # Link game review summaries to games
                    session.run("""
                        MATCH (s:GameReviewSummary)
                        MATCH (g:Game {appid: s.game_appid})
                        MERGE (g)-[:HAS_REVIEW_SUMMARY]->(s)
                    """)
                    logger.info("  Created HAS_REVIEW_SUMMARY relationships")
                    
                    # Link developer stats to developers
                    session.run("""
                        MATCH (s:DeveloperStats)
                        MATCH (d:Developer {developer_id: s.developer_id})
                        MERGE (d)-[:HAS_STATS]->(s)
                    """)
                    logger.info("  Created HAS_STATS relationships")
                    
                    # Link price history to games
                    session.run("""
                        MATCH (h:GamePriceHistory)
                        MATCH (g:Game {appid: h.game_appid})
                        MERGE (g)-[:HAS_PRICE_HISTORY]->(h)
                    """)
                    logger.info("  Created HAS_PRICE_HISTORY relationships")
            
            import_time = time.time()
            
            # # Create REVIEWED relationships
            # with importer.driver.session() as session:
            #     session.run("""
            #         MATCH (r:Review)
            #         MATCH (g:Game {appid: r.app_id})
            #         MERGE (r)-[:REVIEWED]->(g)
            #     """)

                # # Create HAS_PLAYTIME_DATA relationships
                # result = session.run("""
                #     MATCH (h:HLTB)
                #     MATCH (g:Game)
                #     WHERE toLower(h.game_game_name) = toLower(g.name)
                #     MERGE (g)-[:HAS_PLAYTIME_DATA]->(h)
                #     RETURN count(*) as linked
                # """)
                # linked_count = result.single()['linked']
                # logger.info(f"  Linked {linked_count} HLTB records to games")
            
            relationships_time = time.time()
            
            
            # Verify imports
            with importer.driver.session() as session:
                games_count = session.run("MATCH (g:Game) RETURN count(g) as count").single()['count']
                reviews_count = session.run("MATCH (r:Review) RETURN count(r) as count").single()['count']
                devs_count = session.run("MATCH (d:Developer) RETURN count(d) as count").single()['count']
                genres_count = session.run("MATCH (g:Genre) RETURN count(g) as count").single()['count']
                hltb_count = session.run("MATCH (h:HLTB) RETURN count(h) as count").single()['count']
                
                # Count normalized tables
                norm_devs_count = 0
                norm_pubs_count = 0
                if self.normalized_data:
                    norm_devs_count = session.run("MATCH (d:Developer) RETURN count(d) as count").single()['count']
                    norm_pubs_count = session.run("MATCH (p:Publisher) RETURN count(p) as count").single()['count']
            
            verify_time = time.time()
            
            self.results['neo4j'] = {
                'games': games_count,
                'reviews': reviews_count,
                'developers': devs_count,
                'genres': genres_count,
                'hltbs': hltb_count,
                'normalized_devs': norm_devs_count,
                'normalized_pubs': norm_pubs_count,
                'status': 'success',
                'import_time': import_time - start_time,
                'verify_time': verify_time - import_time,
                'drop_time': drop_time - start_time,
                'relationships_time': relationships_time - import_time,
                'normalized_time': normalized_time - import_time if self.normalized_data else 0,
            }
            
            logger.info(f" Neo4j import completed - Games: {games_count}, Reviews: {reviews_count}, HLTB: {hltb_count}")
            logger.info(f"  Additional nodes - Developers: {devs_count}, Genres: {genres_count}")
            if self.normalized_data:
                logger.info(f"  Normalized - Developers: {norm_devs_count}, Publishers: {norm_pubs_count}")
            return True
            
        except Exception as e:
            logger.error(f"XX Neo4j import failed: {e}")
            self.results['neo4j'] = {'status': 'failed', 'error': str(e)}
            return False
    
    def import_to_postgresql(self, games_df, reviews_df, hltb_df, drop=False):
        """Import data to PostgreSQL"""
        if 'postgresql' not in self.importers:
            logger.warning("PostgreSQL importer not initialized")
            return False
        
        try:
            logger.info("== Importing to PostgreSQL ===")
            start_time = time.time()
            importer = self.importers['postgresql']
            
            drop_time = start_time
            if drop:
                tables_to_drop = [
                    'reviews', 'games', 'hltb',
                    'game_developers', 'game_publishers', 'game_genres', 
                    'game_categories', 'game_tags',
                    'developers', 'publishers', 'genres', 'categories', 'tags',
                    'user_profiles', 'game_review_summary', 'developer_stats',
                    'game_price_history'
                ]
                importer.clean_database(tables_to_drop)
                importer.verify_empty(tables_to_drop)
                drop_time = time.time()

            # Import main tables
            games_json_cols = ['supported_languages', 'full_audio_languages', 'packages', 
                              'developers', 'publishers', 'categories', 'genres', 
                              'screenshots', 'movies', 'tags']
            importer.import_df(games_df, json_columns=games_json_cols)
            
            # Import reviews
            importer.import_df(reviews_df)

            importer.import_df(hltb_df)

            # Import normalized tables
            if self.normalized_data:
                normalized_time = time.time()
                logger.info("\nImporting normalized tables...")
                
                # Import dimension tables first
                for table in ['developers', 'publishers', 'genres', 'categories', 'tags']:
                    importer.import_dataframe(
                        self.normalized_data[table], 
                        table_name=table
                    )
                
                # Import association tables
                for table in ['game_developers', 'game_publishers', 'game_genres', 
                            'game_categories', 'game_tags']:
                    importer.import_dataframe(
                        self.normalized_data[table], 
                        table_name=table
                    )
                
                # Import aggregation tables
                importer.import_dataframe(
                    self.normalized_data['user_profiles'], 
                    table_name='user_profiles'
                )
                importer.import_dataframe(
                    self.normalized_data['game_review_summary'], 
                    table_name='game_review_summary'
                )
                importer.import_dataframe(
                    self.normalized_data['developer_stats'], 
                    table_name='developer_stats'
                )
                importer.import_dataframe(
                    self.normalized_data['game_price_history'], 
                    table_name='game_price_history'
                )

            import_time = time.time()

            # TODO: Verify postgre import
            verify_time = time.time()
            
            self.results['postgresql'] = {
                'games': len(games_df.df),
                'reviews': len(reviews_df.df),
                'hltbs': len(hltb_df.df),
                'normalized_tables': len(self.normalized_data) if self.normalized_data else 0,
                'status': 'success',
                'import_time': import_time - start_time,
                'verify_time': verify_time - import_time,
                'drop_time': drop_time - start_time,
                'normalized_time': normalized_time - import_time if self.normalized_data else 0,
            }
            
            logger.info(f" PostgreSQL import completed - Games: {len(games_df.df)}, Reviews: {len(reviews_df.df)}, HLTBs: {len(hltb_df.df)}")
            return True
            
        except Exception as e:
            logger.error(f"XX PostgreSQL import failed: {e}")
            self.results['postgresql'] = {'status': 'failed', 'error': str(e)}
            return False
    
    def import_to_mysql(self, games_df, reviews_df, hltb_df, drop=False):
        """Import data to MySQL"""
        if 'mysql' not in self.importers:
            logger.warning("MySQL importer not initialized")
            return False
        
        try:
            logger.info("== Importing to MySQL ===")
            start_time = time.time()
            importer = self.importers['mysql']
            
            drop_time = start_time
            if drop:
                tables_to_drop = [
                    'reviews', 'games', 'hltb',
                    'game_developers', 'game_publishers', 'game_genres', 
                    'game_categories', 'game_tags',
                    'developers', 'publishers', 'genres', 'categories', 'tags',
                    'user_profiles', 'game_review_summary', 'developer_stats',
                    'game_price_history'
                ]
                importer.clean_database(tables_to_drop)
                importer.verify_empty(tables_to_drop)
                drop_time = time.time()

            # Import main tables
            games_json_cols = ['supported_languages', 'full_audio_languages', 'packages', 
                            'developers', 'publishers', 'categories', 'genres', 
                            'screenshots', 'movies', 'tags']
            importer.import_df(games_df, json_columns=games_json_cols)
            importer.import_df(reviews_df)
            importer.import_df(hltb_df)

            # Import normalized tables if available
            if self.normalized_data:
                normalized_time = time.time()
                logger.info("\nImporting normalized tables to MySQL...")
                
                # Import dimension tables
                for table in ['developers', 'publishers', 'genres', 'categories', 'tags']:
                    importer.import_dataframe(
                        self.normalized_data[table], 
                        table_name=table
                    )
                
                # Import association tables
                for table in ['game_developers', 'game_publishers', 'game_genres', 
                            'game_categories', 'game_tags']:
                    importer.import_dataframe(
                        self.normalized_data[table], 
                        table_name=table
                    )
                
                # Import aggregation tables
                importer.import_dataframe(
                    self.normalized_data['user_profiles'], 
                    table_name='user_profiles'
                )
                importer.import_dataframe(
                    self.normalized_data['game_review_summary'], 
                    table_name='game_review_summary'
                )
                importer.import_dataframe(
                    self.normalized_data['developer_stats'], 
                    table_name='developer_stats'
                )
                importer.import_dataframe(
                    self.normalized_data['game_price_history'], 
                    table_name='game_price_history'
                )

            import_time = time.time()
            verify_time = time.time()
            
            self.results['mysql'] = {
                'games': len(games_df.df),
                'reviews': len(reviews_df.df),
                'hltbs': len(hltb_df.df),
                'normalized_tables': len(self.normalized_data) if self.normalized_data else 0,
                'status': 'success',
                'import_time': import_time - start_time,
                'verify_time': verify_time - import_time,
                'drop_time': drop_time - start_time,
                'normalized_time': normalized_time - import_time if self.normalized_data else 0,
            }
            
            logger.info(f" MySQL import completed - Games: {len(games_df.df)}, Reviews: {len(reviews_df.df)}, HLTBs: {len(hltb_df.df)}")
            if self.normalized_data:
                logger.info(f"  Normalized tables: {len(self.normalized_data)}")
            return True
            
        except Exception as e:
            logger.error(f"XX MySQL import failed: {e}")
            self.results['mysql'] = {'status': 'failed', 'error': str(e)}
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

    def log_summary(self):
        """Log import summary"""
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
