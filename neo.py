import pandas as pd
import kagglehub
from neo4j import GraphDatabase
import os
import json
from typing import Optional, Dict, Any
from datetime import datetime
from dotenv import load_dotenv
from src.ztbd import helper
import time

class Neo4jImporter:
    def __init__(self, uri: str, user: str, password: str):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
    
    def close(self):
        self.driver.close()
    
    def clear_database(self):
        """Clear all nodes and relationships"""
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
            print("Database cleared")
    
    def create_constraints(self):
        """Create constraints and indexes"""
        with self.driver.session() as session:
            # Constraints for unique identifiers
            session.run("CREATE CONSTRAINT game_appid IF NOT EXISTS FOR (g:Game) REQUIRE g.appid IS UNIQUE")
            session.run("CREATE CONSTRAINT review_id IF NOT EXISTS FOR (r:Review) REQUIRE r.review_id IS UNIQUE")
            session.run("CREATE CONSTRAINT developer_name IF NOT EXISTS FOR (d:Developer) REQUIRE d.name IS UNIQUE")
            session.run("CREATE CONSTRAINT publisher_name IF NOT EXISTS FOR (p:Publisher) REQUIRE p.name IS UNIQUE")
            session.run("CREATE CONSTRAINT genre_name IF NOT EXISTS FOR (g:Genre) REQUIRE g.name IS UNIQUE")
            session.run("CREATE CONSTRAINT category_name IF NOT EXISTS FOR (c:Category) REQUIRE c.name IS UNIQUE")
            session.run("CREATE CONSTRAINT language_name IF NOT EXISTS FOR (l:Language) REQUIRE l.name IS UNIQUE")
            session.run("CREATE CONSTRAINT tag_name IF NOT EXISTS FOR (t:Tag) REQUIRE t.name IS UNIQUE")
            
            # Indexes for performance
            session.run("CREATE INDEX game_name IF NOT EXISTS FOR (g:Game) ON (g.name)")
            session.run("CREATE INDEX review_app_id IF NOT EXISTS FOR (r:Review) ON (r.app_id)")
            
            print("Constraints and indexes created")
    
    def import_game(self, game_data: Dict[str, Any]):
        """Import a single game node with relationships"""
        with self.driver.session() as session:
            # Create Game node
            query = """
            CREATE (g:Game {
                appid: $appid,
                name: $name,
                release_date: $release_date,
                required_age: $required_age,
                price: $price,
                dlc_count: $dlc_count,
                detailed_description: $detailed_description,
                about_the_game: $about_the_game,
                short_description: $short_description,
                reviews_text: $reviews_text,
                header_image: $header_image,
                website: $website,
                support_url: $support_url,
                support_email: $support_email,
                windows: $windows,
                mac: $mac,
                linux: $linux,
                metacritic_score: $metacritic_score,
                metacritic_url: $metacritic_url,
                achievements: $achievements,
                recommendations: $recommendations,
                notes: $notes,
                user_score: $user_score,
                score_rank: $score_rank,
                positive: $positive,
                negative: $negative,
                estimated_owners: $estimated_owners,
                average_playtime_forever: $average_playtime_forever,
                average_playtime_2weeks: $average_playtime_2weeks,
                median_playtime_forever: $median_playtime_forever,
                median_playtime_2weeks: $median_playtime_2weeks,
                discount: $discount,
                peak_ccu: $peak_ccu,
                pct_pos_total: $pct_pos_total,
                num_reviews_total: $num_reviews_total,
                pct_pos_recent: $pct_pos_recent,
                num_reviews_recent: $num_reviews_recent
            })
            RETURN g
            """
            
            session.run(query, **game_data)
            
            # Create relationships for developers
            if game_data.get('developers'):
                for dev in game_data['developers']:
                    session.run("""
                        MATCH (g:Game {appid: $appid})
                        MERGE (d:Developer {name: $dev_name})
                        MERGE (g)-[:DEVELOPED_BY]->(d)
                    """, appid=game_data['appid'], dev_name=dev)
            
            # Create relationships for publishers
            if game_data.get('publishers'):
                for pub in game_data['publishers']:
                    session.run("""
                        MATCH (g:Game {appid: $appid})
                        MERGE (p:Publisher {name: $pub_name})
                        MERGE (g)-[:PUBLISHED_BY]->(p)
                    """, appid=game_data['appid'], pub_name=pub)
            
            # Create relationships for genres
            if game_data.get('genres'):
                for genre in game_data['genres']:
                    session.run("""
                        MATCH (g:Game {appid: $appid})
                        MERGE (gen:Genre {name: $genre_name})
                        MERGE (g)-[:HAS_GENRE]->(gen)
                    """, appid=game_data['appid'], genre_name=genre)
            
            # Create relationships for categories
            if game_data.get('categories'):
                for cat in game_data['categories']:
                    session.run("""
                        MATCH (g:Game {appid: $appid})
                        MERGE (c:Category {name: $cat_name})
                        MERGE (g)-[:HAS_CATEGORY]->(c)
                    """, appid=game_data['appid'], cat_name=cat)
            
            # Create relationships for languages
            if game_data.get('supported_languages'):
                for lang in game_data['supported_languages']:
                    session.run("""
                        MATCH (g:Game {appid: $appid})
                        MERGE (l:Language {name: $lang_name})
                        MERGE (g)-[:SUPPORTS_LANGUAGE]->(l)
                    """, appid=game_data['appid'], lang_name=lang)
            
            # Create relationships for tags with scores
            if game_data.get('tags'):
                for tag_name, score in game_data['tags'].items():
                    session.run("""
                        MATCH (g:Game {appid: $appid})
                        MERGE (t:Tag {name: $tag_name})
                        MERGE (g)-[rel:HAS_TAG]->(t)
                        SET rel.score = $score
                    """, appid=game_data['appid'], tag_name=tag_name, score=score)
    
    def import_review(self, review_data: Dict[str, Any]):
        """Import a single review node with relationships"""
        with self.driver.session() as session:
            # Create Review node
            query = """
            MATCH (g:Game {appid: $app_id})
            CREATE (r:Review {
                review_id: $review_id,
                app_id: $app_id,
                app_name: $app_name,
                language: $language,
                review_text: $review_text,
                timestamp_created: $timestamp_created,
                timestamp_updated: $timestamp_updated,
                recommended: $recommended,
                votes_helpful: $votes_helpful,
                votes_funny: $votes_funny,
                weighted_vote_score: $weighted_vote_score,
                comment_count: $comment_count,
                steam_purchase: $steam_purchase,
                received_for_free: $received_for_free,
                written_during_early_access: $written_during_early_access,
                author_steamid: $author_steamid,
                author_num_games_owned: $author_num_games_owned,
                author_num_reviews: $author_num_reviews,
                author_playtime_forever: $author_playtime_forever,
                author_playtime_last_two_weeks: $author_playtime_last_two_weeks,
                author_playtime_at_review: $author_playtime_at_review,
                author_last_played: $author_last_played
            })
            CREATE (r)-[:REVIEWS]->(g)
            RETURN r
            """
            
            session.run(query, **review_data)

    def import_games_batch(self, games_batch: list):
        """Import a batch of games with relationships"""
        with self.driver.session() as session:
            # Create Game nodes in batch
            query = """
            UNWIND $games AS game
            CREATE (g:Game {
                appid: game.appid,
                name: game.name,
                release_date: game.release_date,
                required_age: game.required_age,
                price: game.price,
                dlc_count: game.dlc_count,
                detailed_description: game.detailed_description,
                about_the_game: game.about_the_game,
                short_description: game.short_description,
                reviews_text: game.reviews_text,
                header_image: game.header_image,
                website: game.website,
                support_url: game.support_url,
                support_email: game.support_email,
                windows: game.windows,
                mac: game.mac,
                linux: game.linux,
                metacritic_score: game.metacritic_score,
                metacritic_url: game.metacritic_url,
                achievements: game.achievements,
                recommendations: game.recommendations,
                notes: game.notes,
                user_score: game.user_score,
                score_rank: game.score_rank,
                positive: game.positive,
                negative: game.negative,
                estimated_owners: game.estimated_owners,
                average_playtime_forever: game.average_playtime_forever,
                average_playtime_2weeks: game.average_playtime_2weeks,
                median_playtime_forever: game.median_playtime_forever,
                median_playtime_2weeks: game.median_playtime_2weeks,
                discount: game.discount,
                peak_ccu: game.peak_ccu,
                pct_pos_total: game.pct_pos_total,
                num_reviews_total: game.num_reviews_total,
                pct_pos_recent: game.pct_pos_recent,
                num_reviews_recent: game.num_reviews_recent
            })
            """
            session.run(query, games=games_batch)
            
            # Create all entity relationships in batches
            all_devs = [(g['appid'], dev) for g in games_batch if g.get('developers') for dev in g['developers']]
            if all_devs:
                session.run("""
                    UNWIND $data AS item
                    MATCH (g:Game {appid: item[0]})
                    MERGE (d:Developer {name: item[1]})
                    MERGE (g)-[:DEVELOPED_BY]->(d)
                """, data=all_devs)
            
            all_pubs = [(g['appid'], pub) for g in games_batch if g.get('publishers') for pub in g['publishers']]
            if all_pubs:
                session.run("""
                    UNWIND $data AS item
                    MATCH (g:Game {appid: item[0]})
                    MERGE (p:Publisher {name: item[1]})
                    MERGE (g)-[:PUBLISHED_BY]->(p)
                """, data=all_pubs)
            
            all_genres = [(g['appid'], genre) for g in games_batch if g.get('genres') for genre in g['genres']]
            if all_genres:
                session.run("""
                    UNWIND $data AS item
                    MATCH (g:Game {appid: item[0]})
                    MERGE (gen:Genre {name: item[1]})
                    MERGE (g)-[:HAS_GENRE]->(gen)
                """, data=all_genres)
            
            all_cats = [(g['appid'], cat) for g in games_batch if g.get('categories') for cat in g['categories']]
            if all_cats:
                session.run("""
                    UNWIND $data AS item
                    MATCH (g:Game {appid: item[0]})
                    MERGE (c:Category {name: item[1]})
                    MERGE (g)-[:HAS_CATEGORY]->(c)
                """, data=all_cats)
            
            all_langs = [(g['appid'], lang) for g in games_batch if g.get('supported_languages') for lang in g['supported_languages']]
            if all_langs:
                session.run("""
                    UNWIND $data AS item
                    MATCH (g:Game {appid: item[0]})
                    MERGE (l:Language {name: item[1]})
                    MERGE (g)-[:SUPPORTS_LANGUAGE]->(l)
                """, data=all_langs)
            
            all_tags = [(g['appid'], tag_name, score) for g in games_batch if g.get('tags') 
                       for tag_name, score in g['tags'].items()]
            if all_tags:
                session.run("""
                    UNWIND $data AS item
                    MATCH (g:Game {appid: item[0]})
                    MERGE (t:Tag {name: item[1]})
                    MERGE (g)-[rel:HAS_TAG]->(t)
                    SET rel.score = item[2]
                """, data=all_tags)
    
    def import_reviews_batch(self, reviews_batch: list):
        """Import a batch of reviews with relationships"""
        with self.driver.session() as session:
            query = """
            UNWIND $reviews AS review
            MATCH (g:Game {appid: review.app_id})
            CREATE (r:Review {
                review_id: review.review_id,
                app_id: review.app_id,
                app_name: review.app_name,
                language: review.language,
                review_text: review.review_text,
                timestamp_created: review.timestamp_created,
                timestamp_updated: review.timestamp_updated,
                recommended: review.recommended,
                votes_helpful: review.votes_helpful,
                votes_funny: review.votes_funny,
                weighted_vote_score: review.weighted_vote_score,
                comment_count: review.comment_count,
                steam_purchase: review.steam_purchase,
                received_for_free: review.received_for_free,
                written_during_early_access: review.written_during_early_access,
                author_steamid: review.author_steamid,
                author_num_games_owned: review.author_num_games_owned,
                author_num_reviews: review.author_num_reviews,
                author_playtime_forever: review.author_playtime_forever,
                author_playtime_last_two_weeks: review.author_playtime_last_two_weeks,
                author_playtime_at_review: review.author_playtime_at_review,
                author_last_played: review.author_last_played
            })
            CREATE (r)-[:REVIEWS]->(g)
            """
            session.run(query, reviews=reviews_batch)


def parse_json_field(value):
    """Parse JSON field from CSV"""
    if pd.isna(value) or value == '':
        return None
    try:
        return eval(value) if isinstance(value, str) else value
    except:
        return None

def prepare_game_data(row):
    """Convert DataFrame row to game data dict"""
    data = {
        'appid': int(row['appid']) if pd.notna(row['appid']) else None,
        'name': str(row['name']) if pd.notna(row['name']) else None,
        'release_date': str(row['release_date']) if pd.notna(row['release_date']) else None,
        'required_age': int(row['required_age']) if pd.notna(row['required_age']) else None,
        'price': float(row['price']) if pd.notna(row['price']) else None,
        'dlc_count': int(row['dlc_count']) if pd.notna(row['dlc_count']) else None,
        'detailed_description': str(row['detailed_description']) if pd.notna(row['detailed_description']) else None,
        'about_the_game': str(row['about_the_game']) if pd.notna(row['about_the_game']) else None,
        'short_description': str(row['short_description']) if pd.notna(row['short_description']) else None,
        'reviews_text': str(row['reviews']) if pd.notna(row['reviews']) else None,
        'header_image': str(row['header_image']) if pd.notna(row['header_image']) else None,
        'website': str(row['website']) if pd.notna(row['website']) else None,
        'support_url': str(row['support_url']) if pd.notna(row['support_url']) else None,
        'support_email': str(row['support_email']) if pd.notna(row['support_email']) else None,
        'windows': bool(row['windows']) if pd.notna(row['windows']) else None,
        'mac': bool(row['mac']) if pd.notna(row['mac']) else None,
        'linux': bool(row['linux']) if pd.notna(row['linux']) else None,
        'metacritic_score': int(row['metacritic_score']) if pd.notna(row['metacritic_score']) else None,
        'metacritic_url': str(row['metacritic_url']) if pd.notna(row['metacritic_url']) else None,
        'achievements': int(row['achievements']) if pd.notna(row['achievements']) else None,
        'recommendations': int(row['recommendations']) if pd.notna(row['recommendations']) else None,
        'notes': str(row['notes']) if pd.notna(row['notes']) else None,
        'user_score': int(row['user_score']) if pd.notna(row['user_score']) else None,
        'score_rank': str(row['score_rank']) if pd.notna(row['score_rank']) else None,
        'positive': int(row['positive']) if pd.notna(row['positive']) else None,
        'negative': int(row['negative']) if pd.notna(row['negative']) else None,
        'estimated_owners': str(row['estimated_owners']) if pd.notna(row['estimated_owners']) else None,
        'average_playtime_forever': int(row['average_playtime_forever']) if pd.notna(row['average_playtime_forever']) else None,
        'average_playtime_2weeks': int(row['average_playtime_2weeks']) if pd.notna(row['average_playtime_2weeks']) else None,
        'median_playtime_forever': int(row['median_playtime_forever']) if pd.notna(row['median_playtime_forever']) else None,
        'median_playtime_2weeks': int(row['median_playtime_2weeks']) if pd.notna(row['median_playtime_2weeks']) else None,
        'discount': int(row['discount']) if pd.notna(row['discount']) else None,
        'peak_ccu': int(row['peak_ccu']) if pd.notna(row['peak_ccu']) else None,
        'pct_pos_total': int(row['pct_pos_total']) if pd.notna(row['pct_pos_total']) else None,
        'num_reviews_total': int(row['num_reviews_total']) if pd.notna(row['num_reviews_total']) else None,
        'pct_pos_recent': int(row['pct_pos_recent']) if pd.notna(row['pct_pos_recent']) else None,
        'num_reviews_recent': int(row['num_reviews_recent']) if pd.notna(row['num_reviews_recent']) else None,
        'developers': parse_json_field(row['developers']),
        'publishers': parse_json_field(row['publishers']),
        'genres': parse_json_field(row['genres']),
        'categories': parse_json_field(row['categories']),
        'supported_languages': parse_json_field(row['supported_languages']),
        'tags': parse_json_field(row['tags'])
    }
    return data

def prepare_review_data(row):
    """Convert DataFrame row to review data dict"""
    return {
        'review_id': int(row['review_id']) if pd.notna(row['review_id']) else None,
        'app_id': int(row['app_id']) if pd.notna(row['app_id']) else None,
        'app_name': str(row['app_name']) if pd.notna(row['app_name']) else None,
        'language': str(row['language']) if pd.notna(row['language']) else None,
        'review_text': str(row['review']) if pd.notna(row['review']) else None,
        'timestamp_created': int(row['timestamp_created']) if pd.notna(row['timestamp_created']) else None,
        'timestamp_updated': int(row['timestamp_updated']) if pd.notna(row['timestamp_updated']) else None,
        'recommended': bool(row['recommended']) if pd.notna(row['recommended']) else None,
        'votes_helpful': int(row['votes_helpful']) if pd.notna(row['votes_helpful']) else None,
        'votes_funny': int(row['votes_funny']) if pd.notna(row['votes_funny']) else None,
        'weighted_vote_score': float(row['weighted_vote_score']) if pd.notna(row['weighted_vote_score']) else None,
        'comment_count': int(row['comment_count']) if pd.notna(row['comment_count']) else None,
        'steam_purchase': bool(row['steam_purchase']) if pd.notna(row['steam_purchase']) else None,
        'received_for_free': bool(row['received_for_free']) if pd.notna(row['received_for_free']) else None,
        'written_during_early_access': bool(row['written_during_early_access']) if pd.notna(row['written_during_early_access']) else None,
        'author_steamid': int(row['author_steamid']) if pd.notna(row['author_steamid']) else None,
        'author_num_games_owned': int(row['author_num_games_owned']) if pd.notna(row['author_num_games_owned']) else None,
        'author_num_reviews': int(row['author_num_reviews']) if pd.notna(row['author_num_reviews']) else None,
        'author_playtime_forever': float(row['author_playtime_forever']) if pd.notna(row['author_playtime_forever']) else None,
        'author_playtime_last_two_weeks': float(row['author_playtime_last_two_weeks']) if pd.notna(row['author_playtime_last_two_weeks']) else None,
        'author_playtime_at_review': float(row['author_playtime_at_review']) if pd.notna(row['author_playtime_at_review']) else None,
        'author_last_played': float(row['author_last_played']) if pd.notna(row['author_last_played']) else None
    }

load_dotenv()

NEO4J_URI = os.getenv('NEO4J_URI')
NEO4J_USER = os.getenv('NEO4J_USER')
NEO4J_PASSWORD = os.getenv('NEO4J_PASSWORD')

# Kaggle dataset identifier

GAMES_DATASET = "artermiloff/steam-games-dataset"
GAMES_FILENAME = "games_march2025_cleaned.csv"

REVIEW_DATASET = "najzeko/steam-reviews-2021"
REVIEW_FILENAME = "steam_reviews.csv"

def main():
    try:

        # Initialize Neo4j connection
        importer = Neo4jImporter(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)

        count_games = 0
        with importer.driver.session() as session:
            result_games = session.run("MATCH (g:Game) RETURN count(g) as count")
            count_games = result_games.single()['count']
            
        df_games = helper.downlaod_keggle_ds(dataset=GAMES_DATASET, csv_filename=GAMES_FILENAME)

        if count_games != len(df_games):
            # Clear database and create constraints
            importer.clear_database()
            importer.create_constraints()

            batch_size = 500
            games_batch = []
            imported = 0
                
            for idx, row in df_games.iterrows():
                start = time.time()
                game_data = prepare_game_data(row)
                games_batch.append(game_data)
                
                # prepare_time = time.time()
                # print(f"Elapsed time to prepare batch: {prepare_time - start}")

                if len(games_batch) >= batch_size:
                    imported += batch_size
                    importer.import_games_batch(games_batch)
                    print(f"  Imported {imported} games out of {len(df_games)}. Left: {len(df_games) - imported}")
                    games_batch = []

                    import_time = time.time()
                    print(f"Elapsed time to import batch: {import_time - start}")
                # importer.import_game(game_data)
            
            if games_batch:
                importer.import_games_batch(games_batch)
                
                # if (idx + 1) % 100 == 0:
                #     print(f"  Imported {idx + 1} games...")
            
            print(f"Imported {len(df_games)} games")
        else:
            print(f"Games already imported: {len(df_games)}")
        
        # Import reviews
        df_reviews = helper.downlaod_keggle_ds(dataset=REVIEW_DATASET, csv_filename=REVIEW_FILENAME)
            
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
        df_reviews.rename(columns=column_mapping, inplace=True)
        
        if 'Unnamed: 0' in df_reviews.columns:
            df_reviews.drop('Unnamed: 0', axis=1, inplace=True)
        
        print(f"Reviews shape: {df_reviews.shape}")
        
        # get only milion records
        df_reviews = df_reviews.sort_values('author_steamid').head(1000000)

        print(f"Reviews shape after cutting: {df_reviews.shape}")
        
        df_reviews = helper.handle_duplicates(df_reviews, 'review_id')

        batch_size = 5000
        reviews_batch = []
        imported = 0

        for idx, row in df_reviews.iterrows():
            start = time.time()
            review_data = prepare_review_data(row)
            reviews_batch.append(review_data)

            # prepare_time = time.time()
            # print(f"Elapsed time to prepare batch: {prepare_time - start}")

            if len(reviews_batch) >= batch_size:
                imported += batch_size
                print(f"Importing batch no {int(imported/batch_size)}")
                importer.import_reviews_batch(reviews_batch)
                print(f"  Imported {imported} reviews out of {len(df_reviews)}. Left: {len(df_reviews) - imported}")
                reviews_batch = []
                
                import_time = time.time()
                print(f"Elapsed time to import batch: {import_time - start}")

            # importer.import_review(review_data)
            
            # if (idx + 1) % 1000 == 0:
            #     print(f"  Imported {idx + 1} reviews...")

        if reviews_batch:
            importer.import_reviews_batch(reviews_batch)
        
        print(f"Imported {len(df_reviews)} reviews")
        
        print("\n=== Import completed successfully! ===")
        
        # Verify import
        with importer.driver.session() as session:
            result_games = session.run("MATCH (g:Game) RETURN count(g) as count")
            count_games = result_games.single()['count']
            print(f"Total games in database: {count_games}")
            
            result_reviews = session.run("MATCH (r:Review) RETURN count(r) as count")
            count_reviews = result_reviews.single()['count']
            print(f"Total reviews in database: {count_reviews}")
            
            result_devs = session.run("MATCH (d:Developer) RETURN count(d) as count")
            count_devs = result_devs.single()['count']
            print(f"Total developers in database: {count_devs}")
            
            result_genres = session.run("MATCH (g:Genre) RETURN count(g) as count")
            count_genres = result_genres.single()['count']
            print(f"Total genres in database: {count_genres}")
        
        importer.close()
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

main()