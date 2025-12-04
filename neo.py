import os
from dotenv import load_dotenv
from src.ztbd.ztbdf import create_games_dataframe, create_reviews_dataframe
from src.ztbd.neo4j.importer import Neo4jImporter

load_dotenv()

def main():
    try:
        # Initialize Neo4j importer
        importer = Neo4jImporter(
            uri=os.getenv('NEO4J_URI'),
            user=os.getenv('NEO4J_USER'),
            password=os.getenv('NEO4J_PASSWORD')
        )
        
        # Import games
        games_df = create_games_dataframe()
        importer.import_games(games_df)
        
        # Import reviews  
        reviews_df = create_reviews_dataframe()
        importer.import_reviews(reviews_df)
        
        # Verify import
        with importer.driver.session() as session:
            games_count = session.run("MATCH (g:Game) RETURN count(g) as count").single()['count']
            reviews_count = session.run("MATCH (r:Review) RETURN count(r) as count").single()['count']
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
