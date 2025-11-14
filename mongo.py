import pandas as pd
import kagglehub
from pymongo import MongoClient, ASCENDING
import os
from dotenv import load_dotenv
import os
from src.ztbd import helper

load_dotenv()

MONGO_URI = os.getenv('MONGO_URI')
DATABASE_NAME = os.getenv('DATABASE_NAME')

# Kaggle dataset identifier

GAMES_DATASET = "artermiloff/steam-games-dataset"
GAMES_FILENAME = "games_march2025_cleaned.csv"

REVIEW_DATASET = "najzeko/steam-reviews-2021"
REVIEW_FILENAME = "steam_reviews.csv"

def import_games_to_mongodb(db):
    df_games = helper.downlaod_keggle_ds(dataset=GAMES_DATASET, csv_filename=GAMES_FILENAME)
    
    # # Drop existing collections (optional - comment out to append instead)
    # print("Dropping existing collections...")
    # db.games.drop()
    # db.reviews.drop()
    
        
    # Convert date column to datetime
    if 'release_date' in df_games.columns:
        df_games['release_date'] = pd.to_datetime(df_games['release_date'], errors='coerce')
    
    # Parse JSON columns (stored as strings in CSV)
    json_columns = ['supported_languages', 'full_audio_languages', 'packages', 
                    'developers', 'publishers', 'categories', 'genres', 
                    'screenshots', 'movies', 'tags']
    for col in json_columns:
        if col in df_games.columns:
            df_games[col] = df_games[col].apply(
                lambda x: eval(x) if pd.notna(x) and x != '' else None
            )
    
    print(f"Games shape: {df_games.shape}")
    print(f"Games columns: {df_games.columns.tolist()}")
    
    # Convert DataFrame to list of dictionaries
    games_records = df_games.to_dict('records')
    

    # Clean up NaN values (MongoDB doesn't like NaN)
    for record in games_records:
        for key, value in list(record.items()):
            # Check if it's a scalar value and is NaN
            if isinstance(value, float) and pd.isna(value):
                record[key] = None
            # Handle NaT (Not a Time) for datetime
            elif pd.isna(value) if not isinstance(value, (list, dict)) else False:
                record[key] = None
    
    # Insert into MongoDB
    if games_records:
        result = db.games.insert_many(games_records)
        print(f"✓ Imported {len(result.inserted_ids)} games")
        
        # Create indexes
        print("Creating indexes on games collection...")
        db.games.create_index([("appid", ASCENDING)], unique=True)
        db.games.create_index([("name", ASCENDING)])
        db.games.create_index([("release_date", ASCENDING)])

def import_reviews_to_mongodb(db):
    df_reviews = helper.downlaod_keggle_ds(dataset=GAMES_DATASET, csv_filename=GAMES_FILENAME)
        
    # Rename columns (handle 'author.' prefix)
    column_mapping = {
        'author.steamid': 'author_steamid',
        'author.num_games_owned': 'author_num_games_owned',
        'author.num_reviews': 'author_num_reviews',
        'author.playtime_forever': 'author_playtime_forever',
        'author.playtime_last_two_weeks': 'author_playtime_last_two_weeks',
        'author.playtime_at_review': 'author_playtime_at_review',
        'author.last_played': 'author_last_played'
    }
    df_reviews.rename(columns=column_mapping, inplace=True)
    
    # Drop unnamed index column
    if 'Unnamed: 0' in df_reviews.columns:
        df_reviews.drop('Unnamed: 0', axis=1, inplace=True)
    
    # Convert timestamps to datetime
    timestamp_columns = ['timestamp_created', 'timestamp_updated', 'author_last_played']
    for col in timestamp_columns:
        if col in df_reviews.columns:
            df_reviews[col] = pd.to_datetime(df_reviews[col], unit='s', errors='coerce')

    # remove duplicates
    df = helper.handle_duplicates(df_reviews, 'review_id')

    # get only milion records
    df = df.sort_values('author_steamid').head(1000000)

    # Convert to dictionaries
    reviews_records = df.to_dict('records')
    
    # Clean up NaN values
    for record in reviews_records:
        for key, value in list(record.items()):
            if pd.isna(value):
                record[key] = None
    
    # Insert in batches for better performance
    batch_size = 5000
    total_inserted = 0
    
    for i in range(0, len(reviews_records), batch_size):
        batch = reviews_records[i:i + batch_size]
        result = db.reviews.insert_many(batch)
        total_inserted += len(result.inserted_ids)
        print(f"  Inserted {total_inserted}/{len(reviews_records)} reviews...")
    
    print(f"✓ Imported {total_inserted} reviews")
    
    # Create indexes
    print("Creating indexes on reviews collection...")
    db.reviews.create_index([("app_id", ASCENDING)])
    db.reviews.create_index([("review_id", ASCENDING)], unique=True)
    db.reviews.create_index([("recommended", ASCENDING)])
    db.reviews.create_index([("timestamp_created", ASCENDING)])
    
    print("\n=== Import completed successfully! ===")

if __name__ == "__main__":
    try:
    # Connect to MongoDB
        print(f"\nConnecting to MongoDB...")
        client = MongoClient(MONGO_URI)
        db = client[DATABASE_NAME] # pyright: ignore[reportArgumentType]

        import_games_to_mongodb(db)
        import_reviews_to_mongodb(db)
        
        # Verify imports
        games_count = db.games.count_documents({})
        reviews_count = db.reviews.count_documents({})
        print(f"Total games in database: {games_count}")
        print(f"Total reviews in database: {reviews_count}")
        
        # Sample query examples
        print("\n=== Sample Queries ===")
        sample_game = db.games.find_one({"name": {"$exists": True}})
        if sample_game:
            print(f"Sample game: {sample_game.get('name')}")
        
        positive_reviews = db.reviews.count_documents({"recommended": True})
        print(f"Positive reviews: {positive_reviews}")

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if 'client' in locals():
            client.close()
            print("\nMongoDB connection closed.")
