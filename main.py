import argparse
from dotenv import load_dotenv
from src.ztbd.db_manager import DatabaseManager, DataProcessor
import logging
from datetime import datetime

load_dotenv()

logger = logging.getLogger('ztbd')
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s | %(levelname)-8s | %(lineno)04d | %(message)s')

fh = logging.FileHandler('logs/{:%Y-%m-%d}-ztbd.log'.format(datetime.now()))
formatter = logging.Formatter('%(asctime)s | %(levelname)-8s | %(lineno)04d | %(message)s')
fh.setFormatter(formatter)

logger.addHandler(fh)

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
    parser.add_argument('--skip-hltb', action='store_true',
                       help='Skip how long to beat import')
    
    parser.add_argument('--drop-all', action='store_true',
                       help='Drop all databases before import')
    
    parser.add_argument('--use-cache', action='store_true',
                       help='Use cached prepared data if available')

    args = parser.parse_args()
    
    # Expand 'all' option
    if 'all' in args.databases:
        args.databases = ['mongodb', 'postgresql', 'neo4j']
        # args.databases = ['mongodb', 'postgresql']
    
    print("Steam Dataset Multi-Database Importer")
    print("====================================")
    print(f"Target databases: {', '.join(args.databases)}")
    print(f"Reviews limit: {args.reviews_limit}")
    print(f"Use cache: {args.use_cache}")
    
    try:
        # Process datasets once
        processor = DataProcessor()
        
        games_df = None
        reviews_df = None
        hltb_df = None
        
        if not args.skip_games:
            games_df = processor.prepare_games_dataframe(use_cache=args.use_cache)
        
        if not args.skip_reviews:
            reviews_df = processor.prepare_reviews_dataframe(
                limit=args.reviews_limit, 
                use_cache=args.use_cache
            )
        
        if not args.skip_hltb:
            hltb_df = processor.prepare_hltb_dataframe(use_cache=args.use_cache)
        
        # Initialize database manager
        db_manager = DatabaseManager()
        
        # Initialize requested databases
        initialized_dbs = []
        for db_name in args.databases:
            if db_manager.init_db(db_name):
                initialized_dbs.append(db_name)

            # if db_name == 'mongodb':
            #     if db_manager.init_mongodb():
            #         initialized_dbs.append(db_name)
            # elif db_name == 'neo4j':
            #     if db_manager.init_neo4j():
            #         initialized_dbs.append(db_name)
            # elif db_name == 'postgresql':
            #     if db_manager.init_postgresql():
            #         initialized_dbs.append(db_name)
        
        if not initialized_dbs:
            print("XX No databases successfully initialized. Exiting.")
            return
        
        print(f"\n Successfully initialized: {', '.join(initialized_dbs)}")
        
        import_func = {'mongodb': db_manager.import_to_mongodb,
                       'neo4j': db_manager.import_to_neo4j,
                       'postgresql': db_manager.import_to_postgresql}

        # Import to each database
        for db_name in initialized_dbs:
            try:
                import_func[db_name](games_df, reviews_df, hltb_df, args.drop_all)
                # if db_name == 'mongodb':
                #     db_manager.import_to_mongodb(games_df, reviews_df)
                # elif db_name == 'neo4j':
                #     db_manager.import_to_neo4j(games_df, reviews_df)
                # elif db_name == 'postgresql':
                #     db_manager.import_to_postgresql(games_df, reviews_df)
            except Exception as e:
                print(f"XX Critical error during {db_name} import: {e}")
                import traceback
                traceback.print_exc()
        
        # Print summary
        db_manager.print_summary()
        
    except Exception as e:
        print(f"XX Critical error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Always close connections
        if 'db_manager' in locals():
            db_manager.close_connections()

if __name__ == "__main__":
    main()