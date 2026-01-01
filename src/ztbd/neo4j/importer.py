from neo4j import GraphDatabase
from ..ztbdf import ZTBDataFrame
import typing
import logging

logger = logging.getLogger('ztbd')


class Neo4jImporter:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self._verify_connectivity()
    
    def _verify_connectivity(self):
        """Verify database connection"""
        try:
            with self.driver.session() as session:
                session.run("RETURN 1")
            logger.info("Neo4j connection verified")
        except Exception as e:
            logger.error(f"Neo4j connection failed: {e}")
            raise
    
    def close(self):
        """Close database connection"""
        self.driver.close()
    
    def clean_database(self, node_types=None):
        """
        Clean (delete) nodes and relationships from Neo4j database
        
        Args:
            node_types: List of node labels to delete. If None, deletes everything.
        """
        print(f"Cleaning Neo4j database...")
        
        with self.driver.session() as session:
            if node_types is None:
                # Delete all nodes and relationships
                session.run("MATCH (n) DETACH DELETE n")
                print("  Deleted all nodes and relationships")
            else:
                # Delete specific node types
                for node_type in node_types:
                    query = typing.cast(typing.LiteralString, f"MATCH (n:{node_type}) DETACH DELETE n")
                    result = session.run(query)
                    print(f"  Deleted {node_type} nodes")
        
        print("Neo4j cleanup complete")
    
    def _create_constraints(self):
        """Create uniqueness constraints for primary keys"""
        constraints = [
            "CREATE CONSTRAINT game_appid IF NOT EXISTS FOR (g:Game) REQUIRE g.appid IS UNIQUE",
            "CREATE CONSTRAINT review_id IF NOT EXISTS FOR (r:Review) REQUIRE r.review_id IS UNIQUE",
            "CREATE CONSTRAINT developer_name IF NOT EXISTS FOR (d:Developer) REQUIRE d.name IS UNIQUE",
            "CREATE CONSTRAINT publisher_name IF NOT EXISTS FOR (p:Publisher) REQUIRE p.name IS UNIQUE",
            "CREATE CONSTRAINT genre_name IF NOT EXISTS FOR (g:Genre) REQUIRE g.name IS UNIQUE",
            "CREATE CONSTRAINT category_name IF NOT EXISTS FOR (c:Category) REQUIRE c.name IS UNIQUE"
        ]
        
        with self.driver.session() as session:
            for constraint in constraints:
                try:
                    constraint = typing.cast(typing.LiteralString, constraint)
                    session.run(constraint)
                except Exception as e:
                    # Constraint might already exist
                    logger.debug(f"Constraint creation note: {e}")
    
    def _create_indexes(self, node_label, properties):
        """Create indexes on specified properties"""
        with self.driver.session() as session:
            for prop in properties:
                try:
                    query = f"CREATE INDEX {node_label.lower()}_{prop} IF NOT EXISTS FOR (n:{node_label}) ON (n.{prop})"
                    query = typing.cast(typing.LiteralString, query)
                    session.run(query)
                    logger.info(f"Created index on {node_label}.{prop}")
                except Exception as e:
                    logger.debug(f"Index creation note: {e}")
    
    def import_df(self, ztb_df: ZTBDataFrame, node_label, indexes=None, relationship_configs=None, batch_size=1000):
        """
        Generic import method for any dataframe to Neo4j
        
        Args:
            ztb_df: ZTBDataFrame to import
            node_label: Label for the nodes (e.g., 'Game', 'Review')
            indexes: List of properties to index
            relationship_configs: List of dicts defining relationships to create
                Example: [{'type': 'DEVELOPED_BY', 'target_label': 'Developer', 
                          'source_key': 'developers', 'target_key': 'name'}]
            batch_size: Number of records per batch
        """
        logger.info(f"Importing {node_label} nodes to Neo4j")
        
        # Create constraints first
        self._create_constraints()
        
        records = ztb_df.clean_nan_values()

        # Prepare records for Neo4j (convert nested structures to JSON strings)
        records = self._prepare_records_for_neo4j(records)
    
        total_records = len(records)
        
        # Import main nodes in batches
        with self.driver.session() as session:
            for i in range(0, total_records, batch_size):
                batch = records[i:i + batch_size]
                
                # Build the query dynamically
                query = f"""
                UNWIND $batch AS record
                MERGE (n:{node_label} {{`{ztb_df.primary_key}`: record.`{ztb_df.primary_key}`}})
                SET n += record
                """
                query = typing.cast(typing.LiteralString, query)
                
                session.run(query, batch=batch)
                
                if (i + batch_size) % (batch_size * 5) == 0:
                    logger.info(f"  Imported {min(i + batch_size, total_records)}/{total_records} {node_label} nodes")
        
        logger.info(f"Completed importing {total_records} {node_label} nodes")
        
        # Create indexes
        if indexes:
            self._create_indexes(node_label, indexes)
        
        # Create relationships if configured
        if relationship_configs:
            self._create_relationships(ztb_df, node_label, relationship_configs, batch_size)
    
    def _create_relationships(self, ztb_df, source_label, relationship_configs, batch_size):
        """Create relationships based on configuration"""
        for config in relationship_configs:
            rel_type = config['type']
            target_label = config['target_label']
            source_key = config['source_key']
            target_key = config.get('target_key', 'name')
            
            logger.info(f"Creating {rel_type} relationships")
            
            # Get records that have the source key
            records = []
            for record in ztb_df.clean_nan_values():
                if source_key in record and record[source_key]:
                    # Handle both single values and lists
                    values = record[source_key] if isinstance(record[source_key], list) else [record[source_key]]
                    for value in values:
                        if value:
                            records.append({
                                'source_id': record[ztb_df.primary_key],
                                'target_value': value
                            })
            
            # Create relationships in batches
            with self.driver.session() as session:
                for i in range(0, len(records), batch_size):
                    batch = records[i:i + batch_size]
                    
                    query = f"""
                    UNWIND $batch AS rel
                    MATCH (source:{source_label} {{`{ztb_df.primary_key}`: rel.source_id}})
                    MERGE (target:{target_label} {{`{target_key}`: rel.target_value}})
                    MERGE (source)-[:{rel_type}]->(target)
                    """
                    query = typing.cast(typing.LiteralString, query)
                    
                    session.run(query, batch=batch)
                    
                    if (i + batch_size) % (batch_size * 10) == 0:
                        logger.info(f"  Created {min(i + batch_size, len(records))}/{len(records)} {rel_type} relationships")
            
            logger.info(f"Completed creating {len(records)} {rel_type} relationships")

    def _prepare_records_for_neo4j(self, records):
        """Convert nested structures to JSON strings for Neo4j compatibility"""
        import json
        
        json_fields = [
            'packages', 'screenshots', 'movies', 'supported_languages',
            'full_audio_languages', 'categories', 'genres', 'tags'
        ]
        
        prepared_records = []
        for record in records:
            clean_record = record.copy()
            
            for field in json_fields:
                if field in clean_record and clean_record[field] is not None:
                    # Convert to JSON string if it's a complex structure
                    value = clean_record[field]
                    if isinstance(value, (dict, list)):
                        # Check if it contains nested structures
                        try:
                            # Simple lists of primitives are OK, but check first item
                            if isinstance(value, list) and len(value) > 0:
                                if isinstance(value[0], (dict, list)):
                                    clean_record[field] = json.dumps(value)
                            elif isinstance(value, dict):
                                clean_record[field] = json.dumps(value)
                        except:
                            clean_record[field] = json.dumps(value)
            
            prepared_records.append(clean_record)
        
        return prepared_records


    # Deprecated methods for backward compatibility
    def import_games(self, ztb_df: ZTBDataFrame):
        """Import games - DEPRECATED: use import_df() instead"""
        logger.warning("USING DEPRECATED FUNCTION: use import_df() instead")
        
        relationship_configs = [
            {'type': 'DEVELOPED_BY', 'target_label': 'Developer', 'source_key': 'developers'},
            {'type': 'PUBLISHED_BY', 'target_label': 'Publisher', 'source_key': 'publishers'},
            {'type': 'HAS_GENRE', 'target_label': 'Genre', 'source_key': 'genres'},
            {'type': 'HAS_CATEGORY', 'target_label': 'Category', 'source_key': 'categories'}
        ]
        
        self.import_df(
            ztb_df=ztb_df,
            node_label='Game',
            indexes=['name', 'release_date', 'price'],
            relationship_configs=relationship_configs
        )
    
    def import_reviews(self, ztb_df: ZTBDataFrame):
        """Import reviews - DEPRECATED: use import_df() instead"""
        logger.warning("USING DEPRECATED FUNCTION: use import_df() instead")
        
        # First import review nodes
        self.import_df(
            ztb_df=ztb_df,
            node_label='Review',
            indexes=['app_id', 'recommended', 'timestamp_created'],
            batch_size=5000
        )
        
        # Then create REVIEWED relationships to games
        logger.info("Creating REVIEWED relationships")
        with self.driver.session() as session:
            query = """
            MATCH (r:Review)
            MATCH (g:Game {appid: r.app_id})
            MERGE (r)-[:REVIEWED]->(g)
            """
            session.run(query)
        
        logger.info("Completed creating REVIEWED relationships")
