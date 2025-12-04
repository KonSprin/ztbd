import os
from dotenv import load_dotenv
from src.ztbd.ztbdf import create_games_dataframe, create_reviews_dataframe
from src.ztbd.mongodb.importer import MongoDBImporter

load_dotenv()

def main():
    try:
        # Initialize MongoDB importer
        importer = MongoDBImporter(
            uri=os.getenv('MONGO_URI'),
            database_name=os.getenv('DATABASE_NAME')
        )
        
        # Import games
        games_df = create_games_dataframe()
        importer.import_games(games_df)
        
        # Import reviews
        reviews_df = create_reviews_dataframe()
        importer.import_reviews(reviews_df)
        
        # Verify imports
        games_count = importer.db.games.count_documents({})
        reviews_count = importer.db.reviews.count_documents({})
        print(f"Total games: {games_count}")
        print(f"Total reviews: {reviews_count}")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if 'importer' in locals():
            importer.close()

if __name__ == "__main__":
    main()
