# src/ztbd/normalizer.py
"""
Data normalizer - extracts and transforms denormalized data into normalized tables
"""
import pandas as pd
import logging
from datetime import datetime, timedelta
import random

logger = logging.getLogger('ztbd')


class DataNormalizer:
    """Extract normalized data from existing dataframes"""
    
    @staticmethod
    def extract_developers(games_df):
        """Extract unique developers from games dataframe"""
        logger.info("Extracting developers...")
        developers = set()
        
        for _, row in games_df.df.iterrows():
            if pd.notna(row['developers']).any() and row['developers']:
                if isinstance(row['developers'], list):
                    developers.update(row['developers'])
                elif isinstance(row['developers'], str):
                    developers.add(row['developers'])
        
        df = pd.DataFrame([
            {'name': dev} for dev in sorted(developers) if dev
        ])
        logger.info(f"Extracted {len(df)} unique developers")
        return df
    
    @staticmethod
    def extract_publishers(games_df):
        """Extract unique publishers from games dataframe"""
        logger.info("Extracting publishers...")
        publishers = set()
        
        for _, row in games_df.df.iterrows():
            if pd.notna(row['publishers']).any() and row['publishers']:
                if isinstance(row['publishers'], list):
                    publishers.update(row['publishers'])
                elif isinstance(row['publishers'], str):
                    publishers.add(row['publishers'])
        
        df = pd.DataFrame([
            {'name': pub} for pub in sorted(publishers) if pub
        ])
        logger.info(f"Extracted {len(df)} unique publishers")
        return df
    
    @staticmethod
    def extract_genres(games_df):
        """Extract unique genres from games dataframe"""
        logger.info("Extracting genres...")
        genres = set()
        
        for _, row in games_df.df.iterrows():
            if pd.notna(row['genres']).any() and row['genres']:
                if isinstance(row['genres'], list):
                    genres.update(row['genres'])
                elif isinstance(row['genres'], str):
                    genres.add(row['genres'])
        
        df = pd.DataFrame([
            {'name': genre} for genre in sorted(genres) if genre
        ])
        logger.info(f"Extracted {len(df)} unique genres")
        return df
    
    @staticmethod
    def extract_categories(games_df):
        """Extract unique categories from games dataframe"""
        logger.info("Extracting categories...")
        categories = set()
        
        for _, row in games_df.df.iterrows():
            if pd.notna(row['categories']).any() and row['categories']:
                if isinstance(row['categories'], list):
                    categories.update(row['categories'])
                elif isinstance(row['categories'], str):
                    categories.add(row['categories'])
        
        df = pd.DataFrame([
            {'name': cat} for cat in sorted(categories) if cat
        ])
        logger.info(f"Extracted {len(df)} unique categories")
        return df
    
    @staticmethod
    def extract_tags(games_df):
        """Extract unique tags from games dataframe"""
        logger.info("Extracting tags...")
        tags_dict = {}
        
        for _, row in games_df.df.iterrows():
            # if not pd.isna(row['tags']).any() and row['tags']:
            if row['tags']:
                if isinstance(row['tags'], dict):
                    for tag, votes in row['tags'].items():
                        if tag not in tags_dict:
                            tags_dict[tag] = 0
                        tags_dict[tag] += int(votes) if votes else 0
        
        df = pd.DataFrame([
            {'name': tag, 'total_votes': votes} 
            for tag, votes in sorted(tags_dict.items())
        ])
        logger.info(f"Extracted {len(df)} unique tags")
        return df
    
    @staticmethod
    def create_game_developer_associations(games_df, developers_df):
        """Create game-developer associations"""
        logger.info("Creating game-developer associations...")
        
        dev_lookup = {row['name']: idx + 1 for idx, row in developers_df.iterrows()}
        associations = []
        
        for _, row in games_df.df.iterrows():
            if pd.notna(row['developers']).any() and row['developers']:
                devs = row['developers'] if isinstance(row['developers'], list) else [row['developers']]
                for dev in devs:
                    if dev in dev_lookup:
                        associations.append({
                            'game_appid': row['appid'],
                            'developer_id': dev_lookup[dev]
                        })
        
        df = pd.DataFrame(associations)
        logger.info(f"Created {len(df)} game-developer associations")
        return df
    
    @staticmethod
    def create_game_publisher_associations(games_df, publishers_df):
        """Create game-publisher associations"""
        logger.info("Creating game-publisher associations...")
        
        pub_lookup = {row['name']: idx + 1 for idx, row in publishers_df.iterrows()}
        associations = []
        
        for _, row in games_df.df.iterrows():
            if pd.notna(row['publishers']).any() and row['publishers']:
                pubs = row['publishers'] if isinstance(row['publishers'], list) else [row['publishers']]
                for pub in pubs:
                    if pub in pub_lookup:
                        associations.append({
                            'game_appid': row['appid'],
                            'publisher_id': pub_lookup[pub]
                        })
        
        df = pd.DataFrame(associations)
        logger.info(f"Created {len(df)} game-publisher associations")
        return df
    
    @staticmethod
    def create_game_genre_associations(games_df, genres_df):
        """Create game-genre associations"""
        logger.info("Creating game-genre associations...")
        
        genre_lookup = {row['name']: idx + 1 for idx, row in genres_df.iterrows()}
        associations = []
        
        for _, row in games_df.df.iterrows():
            if pd.notna(row['genres']).any() and row['genres']:
                genres = row['genres'] if isinstance(row['genres'], list) else [row['genres']]
                for genre in genres:
                    if genre in genre_lookup:
                        associations.append({
                            'game_appid': row['appid'],
                            'genre_id': genre_lookup[genre]
                        })
        
        df = pd.DataFrame(associations)
        logger.info(f"Created {len(df)} game-genre associations")
        return df
    
    @staticmethod
    def create_game_category_associations(games_df, categories_df):
        """Create game-category associations"""
        logger.info("Creating game-category associations...")
        
        cat_lookup = {row['name']: idx + 1 for idx, row in categories_df.iterrows()}
        associations = []
        
        for _, row in games_df.df.iterrows():
            if pd.notna(row['categories']).any() and row['categories']:
                cats = row['categories'] if isinstance(row['categories'], list) else [row['categories']]
                for cat in cats:
                    if cat in cat_lookup:
                        associations.append({
                            'game_appid': row['appid'],
                            'category_id': cat_lookup[cat]
                        })
        
        df = pd.DataFrame(associations)
        logger.info(f"Created {len(df)} game-category associations")
        return df
    
    @staticmethod
    def create_game_tag_associations(games_df, tags_df):
        """Create game-tag associations with vote counts"""
        logger.info("Creating game-tag associations...")
        
        tag_lookup = {row['name']: idx + 1 for idx, row in tags_df.iterrows()}
        associations = []
        
        for _, row in games_df.df.iterrows():
            # if pd.notna(row['tags']) and row['tags']:
            if row['tags']:
                if isinstance(row['tags'], dict):
                    for tag, votes in row['tags'].items():
                        if tag in tag_lookup:
                            associations.append({
                                'game_appid': row['appid'],
                                'tag_id': tag_lookup[tag],
                                'vote_count': int(votes) if votes else 0
                            })
        
        df = pd.DataFrame(associations)
        logger.info(f"Created {len(df)} game-tag associations")
        return df
    
    @staticmethod
    def extract_user_profiles(reviews_df):
        """Extract user profiles from reviews"""
        logger.info("Extracting user profiles from reviews...")
        
        df_reviews = reviews_df.df
        
        user_stats = df_reviews.groupby('author_steamid').agg({
            'author_num_games_owned': 'first',
            'author_num_reviews': 'first',
            'author_playtime_forever': 'first',
            'timestamp_created': ['min', 'max'],
            'recommended': ['sum', 'count'],
            'review': lambda x: x.str.len().mean() if x.dtype == 'object' else 0,
            'votes_helpful': 'sum'
        }).reset_index()
        
        user_stats.columns = [
            'author_steamid', 'num_games_owned', 'num_reviews', 
            'total_playtime_minutes', 'first_review_date', 'last_review_date',
            'positive_review_count', 'total_review_count', 'avg_review_length',
            'helpful_votes_received'
        ]
        
        user_stats['negative_review_count'] = (
            user_stats['total_review_count'] - user_stats['positive_review_count']
        )
        user_stats = user_stats.drop('total_review_count', axis=1)
        
        logger.info(f"Extracted {len(user_stats)} user profiles")
        return user_stats
    
    @staticmethod
    def create_game_review_summary(games_df, reviews_df):
        """Create aggregated review summary per game"""
        logger.info("Creating game review summaries...")
        
        df_reviews = reviews_df.df
        
        summary = df_reviews.groupby('app_id').agg({
            'review_id': 'count',
            'recommended': ['sum', lambda x: (~x).sum()],
            'author_playtime_at_review': ['mean', 'median'],
            'votes_helpful': 'mean',
            'language': lambda x: x.mode()[0] if len(x.mode()) > 0 else None,
            'steam_purchase': lambda x: x.sum() / len(x) if len(x) > 0 else 0,
            'written_during_early_access': 'sum'
        }).reset_index()
        
        summary.columns = [
            'game_appid', 'total_reviews', 'positive_reviews', 'negative_reviews',
            'avg_playtime_at_review', 'median_playtime_at_review', 'avg_helpful_votes',
            'most_common_language', 'steam_purchase_ratio', 'early_access_review_count'
        ]
        
        logger.info(f"Created summaries for {len(summary)} games")
        return summary
    
    @staticmethod
    def create_developer_stats(games_df, developers_df, game_developers_df):
        """Create aggregated statistics per developer"""
        logger.info("Creating developer statistics...")
        
        game_dev_merged = game_developers_df.merge(
            games_df.df[['appid', 'price', 'metacritic_score', 'positive', 'negative', 
                        'average_playtime_forever', 'genres']],
            left_on='game_appid',
            right_on='appid',
            how='left'
        )
        
        dev_stats = game_dev_merged.groupby('developer_id').agg({
            'game_appid': 'count',
            'price': 'mean',
            'metacritic_score': 'mean',
            'positive': 'sum',
            'negative': 'sum',
            'average_playtime_forever': 'mean',
            'genres': lambda x: x.mode()[0] if len(x.mode()) > 0 else None
        }).reset_index()
        
        dev_stats.columns = [
            'developer_id', 'total_games', 'avg_game_price', 'avg_metacritic_score',
            'total_positive_reviews', 'total_negative_reviews', 'avg_playtime',
            'most_common_genre'
        ]
        
        dev_stats['most_common_genre'] = dev_stats['most_common_genre'].apply(
            lambda x: x[0] if isinstance(x, list) and len(x) > 0 else None
        )
        
        logger.info(f"Created statistics for {len(dev_stats)} developers")
        return dev_stats
    
    @staticmethod
    def simulate_price_history(games_df, months_back=12):
        """Simulate historical price data for time-series testing"""
        logger.info(f"Simulating {months_back} months of price history...")
        
        price_history = []
        base_date = datetime.now().date()
        
        for _, game in games_df.df.iterrows():
            if pd.notna(game['price']) and game['price'] > 0:
                current_price = game['price']
                discount = game['discount'] if pd.notna(game['discount']) else 0
                
                for month in range(months_back, -1, -1):
                    date = base_date - timedelta(days=month * 30)
                    
                    price_variation = random.uniform(0.9, 1.1)
                    historical_price = current_price * price_variation
                    
                    historical_discount = 0
                    if random.random() < 0.3:
                        historical_discount = random.choice([10, 15, 20, 25, 33, 50, 75])
                    
                    price_history.append({
                        'game_appid': game['appid'],
                        'price': round(historical_price, 2),
                        'discount_percent': historical_discount,
                        'recorded_date': date
                    })
        
        df = pd.DataFrame(price_history)
        logger.info(f"Simulated {len(df)} price history records")
        return df
