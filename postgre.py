from src.ztbd.ztbdf import create_games_dataframe, create_reviews_dataframe
from src.ztbd.postgresql.importer import PostgreSQLImporter

def main():
    importer = PostgreSQLImporter()
    
    # Import games
    games_df = create_games_dataframe()
    games_json_cols = ['supported_languages', 'full_audio_languages', 'packages', 
                      'developers', 'publishers', 'categories', 'genres', 
                      'screenshots', 'movies', 'tags']
    importer.import_dataset('games', games_df, json_columns=games_json_cols)
    
    # Import reviews
    reviews_df = create_reviews_dataframe()
    review_col_mapping = {
        'author.steamid': 'author_steamid',
        'author.num_games_owned': 'author_num_games_owned',
        'author.num_reviews': 'author_num_reviews',
        'author.playtime_forever': 'author_playtime_forever',
        'author.playtime_last_two_weeks': 'author_playtime_last_two_weeks',
        'author.playtime_at_review': 'author_playtime_at_review',
        'author.last_played': 'author_last_played'
    }
    reviews_df.rename_columns(review_col_mapping)
    importer.import_dataset('reviews', reviews_df, limit=1000000)

if __name__ == "__main__":
    main()
