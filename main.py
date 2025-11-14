# Install dependencies as needed:
# pip install kagglehub[pandas-datasets]
import kagglehub
# from kagglehub import KaggleDatasetAdapter
import pandas as pd
import sqlalchemy
import os

from src.ztbd import models, database
models.Base.metadata.create_all(bind=database.engine)

def handle_duplicates(df, key_column, output_file='duplicates.csv'):
    """
    Identify duplicates, save them to file, and return deduplicated dataframe
    """
    print(f"\n=== HANDLING DUPLICATES ===")
    print(f"Original rows: {len(df)}")
    
    # Find duplicates based on key column
    duplicates_mask = df.duplicated(subset=[key_column], keep='first')
    duplicates = df[duplicates_mask]
    
    print(f"Duplicate rows found: {len(duplicates)}")
    
    if len(duplicates) > 0:
        # Save duplicates to file
        duplicates.to_csv(output_file, index=False)
        print(f"✓ Duplicates saved to: {output_file}")
        
        # Show some stats about duplicates
        duplicate_ids = duplicates[key_column].value_counts().head(10)
        print(f"\nTop 10 most duplicated {key_column}s:")
        print(duplicate_ids)
    
    # Remove duplicates, keeping first occurrence
    df_clean = df.drop_duplicates(subset=[key_column], keep='first')
    print(f"\nRows after deduplication: {len(df_clean)}")
    print(f"Removed: {len(df) - len(df_clean)} rows")
    
    return df_clean

def diagnose_columns(df, table_name='reviews'):
    """Test each column's data range to identify issues"""
    print(f"\n=== DIAGNOSING {table_name.upper()} COLUMNS ===\n")
    
    for col in df.columns:
        if col == 'Unnamed: 0':
            continue
            
        print(f"\nColumn: {col}")
        print(f"  Type: {df[col].dtype}")
        
        # Check for numeric columns
        if pd.api.types.is_numeric_dtype(df[col]):
            non_null = df[col].dropna()
            if len(non_null) > 0:
                min_val = non_null.min()
                max_val = non_null.max()
                print(f"  Min: {min_val}")
                print(f"  Max: {max_val}")
                
                # Check if values fit in different integer types
                if df[col].dtype in ['int64', 'float64']:
                    # PostgreSQL Integer range: -2147483648 to 2147483647
                    int_min, int_max = -2147483648, 2147483647
                    # PostgreSQL BigInteger range: -9223372036854775808 to 9223372036854775807
                    bigint_min, bigint_max = -9223372036854775808, 9223372036854775807
                    
                    if min_val < int_min or max_val > int_max:
                        print(f"  ⚠️  EXCEEDS INTEGER - needs BigInteger")
                    else:
                        print(f"  ✓ Fits in Integer")
                        
                    if min_val < bigint_min or max_val > bigint_max:
                        print(f"  ❌ EXCEEDS BIGINTEGER!")
        
        # Check string lengths
        elif pd.api.types.is_string_dtype(df[col]) or df[col].dtype == 'object':
            non_null = df[col].dropna().astype(str)
            if len(non_null) > 0:
                max_len = non_null.str.len().max()
                print(f"  Max length: {max_len}")

def import_dataset(name, dataset, csv_filename, json_columns = [], column_mapping = {}):
    try:
        # Download dataset from Kaggle
        print(f"Downloading dataset from Kaggle: {dataset}")
        dataset_path = kagglehub.dataset_download(dataset)
        print(f"Dataset downloaded to: {dataset_path}")

        print(f"\nAvailable files in dataset:")
        files = os.listdir(dataset_path)
        for file in files:
            print(f"  - {file}")

        csv_path = os.path.join(dataset_path, csv_filename)

        if os.path.exists(csv_path):
            print(f"\n=== Importing {csv_filename} ===")
            df = pd.read_csv(csv_path)
                       
            print(f"\n=== Dataset shape: {df.shape}")
            print(f"\n=== GamDatasetes columns: {df.columns.tolist()}")

            if json_columns:
                # Parse JSON columns (they're stored as strings in CSV)
                for col in json_columns:
                    if col in df.columns:
                        df[col] = df[col].apply(lambda x: eval(x) if pd.notna(x) and x != '' else None)
            
            if column_mapping:
                df.rename(columns=column_mapping, inplace=True)

            # Drop the unnamed index column if it exists
            if 'Unnamed: 0' in df.columns:
                df.drop('Unnamed: 0', axis=1, inplace=True)

            # diagnose_columns(df)
            df = handle_duplicates(df, 'review_id')

            # Sort by author_steamid and limit to 1 million records
            df = df.sort_values('author_steamid')
            df_milion = df.head(1000000)
            

            df_milion.to_sql(
                name=name,
                con=database.engine,
                if_exists='append',
                index=False,
                chunksize=1000,
                dtype={key: sqlalchemy.types.JSON for key in json_columns},
            )
            print(f"\n=== Imported {len(df_milion)} items ====")

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if 'engine' in locals():
            database.engine.dispose()


if __name__ == "__main__":
    GAMES_DATASET = "artermiloff/steam-games-dataset"
    GAMES_FILENAME = "games_march2025_cleaned.csv"
    GAMES_JSON_COLS = [
        'supported_languages', 'full_audio_languages', 'packages', 
        'developers', 'publishers', 'categories', 'genres', 
        'screenshots', 'movies', 'tags']
    # import_dataset('games', GAMES_DATASET, GAMES_FILENAME, json_columns = GAMES_JSON_COLS)

    REVIEW_DATASET = "najzeko/steam-reviews-2021"
    REVIEW_FILENAME = "steam_reviews.csv"
    REVIEW_COL_MAPPING = {
        'author.steamid': 'author_steamid',
        'author.num_games_owned': 'author_num_games_owned',
        'author.num_reviews': 'author_num_reviews',
        'author.playtime_forever': 'author_playtime_forever',
        'author.playtime_last_two_weeks': 'author_playtime_last_two_weeks',
        'author.playtime_at_review': 'author_playtime_at_review',
        'author.last_played': 'author_last_played'
    }
    import_dataset('reviews', REVIEW_DATASET, REVIEW_FILENAME, column_mapping=REVIEW_COL_MAPPING)
