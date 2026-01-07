# verify_ids.py
"""
Verify that dimension table IDs are identical across all databases
"""
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from pymongo import MongoClient
from neo4j import GraphDatabase

load_dotenv()


def verify_postgresql_ids():
    """Get developer IDs from PostgreSQL"""
    engine = create_engine(os.getenv('SQLALCHEMY_DATABASE_URL', ""))
    with engine.connect() as conn:
        result = conn.execute(text(
            "SELECT developer_id, name FROM developers ORDER BY developer_id LIMIT 10"
        ))
        return list(result)


def verify_mysql_ids():
    """Get developer IDs from MySQL"""
    engine = create_engine(os.getenv('MYSQL_DATABASE_URL', ""))
    with engine.connect() as conn:
        result = conn.execute(text(
            "SELECT developer_id, name FROM developers ORDER BY developer_id LIMIT 10"
        ))
        return list(result)


def verify_mongodb_ids():
    """Get developer IDs from MongoDB"""
    client = MongoClient(os.getenv('MONGO_URI'))
    db = client[os.getenv('DATABASE_NAME', 'mongodb')]
    
    # MongoDB doesn't have auto-increment IDs, but we stored developer_id field
    developers = list(db.developers.find(
        {}, 
        {'developer_id': 1, 'name': 1, '_id': 0}
    ).sort('developer_id', 1).limit(10))
    
    return [(d['developer_id'], d['name']) for d in developers]


def verify_neo4j_ids():
    """Get developer IDs from Neo4j"""
    driver = GraphDatabase.driver(
        os.getenv('NEO4J_URI', ""),
        auth=(os.getenv('NEO4J_USER', ""), os.getenv('NEO4J_PASSWORD', ""))
    )
    
    with driver.session() as session:
        result = session.run("""
            MATCH (d:GameDeveloper)
            WHERE d.developer_id IS NOT NULL
            RETURN d.developer_id as developer_id, d.name as name
            ORDER BY d.developer_id
            LIMIT 10
        """)
        return [(r['developer_id'], r['name']) for r in result]


def main():
    print("=" * 80)
    print("VERIFYING DETERMINISTIC IDs ACROSS DATABASES")
    print("=" * 80)
    
    try:
        print("\n[1/4] Checking PostgreSQL...")
        pg_devs = verify_postgresql_ids()
        print(f"  ✓ Found {len(pg_devs)} developers")
        for dev_id, name in pg_devs[:5]:
            print(f"    ID {dev_id}: {name}")
        
        print("\n[2/4] Checking MySQL...")
        mysql_devs = verify_mysql_ids()
        print(f"  ✓ Found {len(mysql_devs)} developers")
        for dev_id, name in mysql_devs[:5]:
            print(f"    ID {dev_id}: {name}")
        
        print("\n[3/4] Checking MongoDB...")
        mongo_devs = verify_mongodb_ids()
        print(f"  ✓ Found {len(mongo_devs)} developers")
        for dev_id, name in mongo_devs[:5]:
            print(f"    ID {dev_id}: {name}")
        
        print("\n[4/4] Checking Neo4j...")
        neo4j_devs = verify_neo4j_ids()
        print(f"  ✓ Found {len(neo4j_devs)} developers")
        for dev_id, name in neo4j_devs[:5]:
            print(f"    ID {dev_id}: {name}")
        
        # Compare all databases
        print("\n" + "=" * 80)
        print("COMPARISON RESULTS")
        print("=" * 80)
        
        all_match = True
        for i in range(min(len(pg_devs), len(mysql_devs), len(mongo_devs), len(neo4j_devs))):
            pg = pg_devs[i]
            mysql = mysql_devs[i]
            mongo = mongo_devs[i]
            neo4j = neo4j_devs[i]
            
            if pg == mysql == mongo == neo4j:
                print(f"✓ ID {pg[0]}: {pg[1]} - MATCH across all databases")
            else:
                print(f"✗ ID {i+1}: MISMATCH!")
                print(f"  PostgreSQL: {pg}")
                print(f"  MySQL:      {mysql}")
                print(f"  MongoDB:    {mongo}")
                print(f"  Neo4j:      {neo4j}")
                all_match = False
        
        print("\n" + "=" * 80)
        if all_match:
            print("SUCCESS: All IDs are deterministic and identical across databases!")
        else:
            print("FAILURE: ID mismatches detected. Check import process.")
        print("=" * 80)
        
    except Exception as e:
        print(f"\n✗ Error during verification: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
