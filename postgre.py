import pandas as pd
import sqlalchemy
from src.ztbd import helper
from src.ztbd.postgresql import models, database

models.Base.metadata.create_all(bind=database.engine)


def import_dataset(name, dataset, csv_filename, json_columns = [], column_mapping = {}, head = 0):
    try:
        df = helper.downlaod_keggle_ds(dataset=dataset, csv_filename=csv_filename)

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

        # helper.diagnose_columns(df)
        df = helper.handle_duplicates(df, 'review_id')

        # Sort by author_steamid and limit to 1 million records
        df = df.sort_values('author_steamid')
        if head:
            df = df.head(head)
        

        df.to_sql(
            name=name,
            con=database.engine,
            if_exists='append',
            index=False,
            chunksize=1000,
            dtype={key: sqlalchemy.types.JSON for key in json_columns},
        )
        print(f"\n=== Imported {len(df)} items ====")

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
    # import_dataset('reviews', REVIEW_DATASET, REVIEW_FILENAME, column_mapping=REVIEW_COL_MAPPING)
