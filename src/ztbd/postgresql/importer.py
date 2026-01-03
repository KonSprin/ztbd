import sqlalchemy
from sqlalchemy import text
from .database import engine
from .models import Base
from ..ztbdf import ZTBDataFrame

import logging

logger = logging.getLogger('ztbd')


class PostgreSQLImporter:
    def __init__(self):
        Base.metadata.create_all(bind=engine)
    
    def truncate_database(self, tables=[]):
        """
        Clean (truncate) tables from PostgreSQL database
        
        Args:
            tables: List of table names to truncate. If None, truncates all tables.
        """
        try:
            logger.info(f"Cleaning PostgreSQL database...")
            
            with engine.connect() as conn:
                # if tables is None:
                #     # Get all table names from metadata
                #     tables = [table.name for table in Base.metadata.sorted_tables]
                
                for table_name in tables:
                    # Use TRUNCATE with CASCADE to handle foreign key constraints
                    conn.execute(text(f'TRUNCATE TABLE "{table_name}" CASCADE'))
                    conn.commit()
                    logger.info(f"  Truncated table: {table_name}")
            
            logger.info("PostgreSQL cleanup complete")
            
        except Exception as e:
            logger.error(f"XX Error cleaning PostgreSQL database: {e}")
            raise
    
    def clean_database(self, tables=[]):
        """
        Drop tables from PostgreSQL database (more aggressive than truncate)
        
        Args:
            tables: List of table names to drop. If None, drops all tables.
        """
        try:
            logger.info(f"Dropping PostgreSQL tables...")
            
            # if tables is None:
            #     # Drop all tables using metadata
            #     Base.metadata.drop_all(bind=engine)
            #     logger.info("  Dropped all tables")
            # else:
            with engine.connect() as conn:
                for table_name in tables:
                    conn.execute(text(f'DROP TABLE IF EXISTS "{table_name}" CASCADE'))
                    conn.commit()
                    logger.info(f"  Dropped table: {table_name}")
            
            logger.info("PostgreSQL drop complete")
            
        except Exception as e:
            logger.error(f"XX Error dropping PostgreSQL tables: {e}")
            raise
    
    def import_df(self, ztb_df: ZTBDataFrame, json_columns=None):
        """Import pre-cleaned dataset to PostgreSQL"""
        try:
            table_name = ztb_df._name

            logger.info(f"Importing {len(ztb_df.df)} records to {table_name}...")

            # PostgreSQL-specific JSON column handling
            dtype_mapping = {}
            if json_columns:
                dtype_mapping = {key: sqlalchemy.types.JSON for key in json_columns}
            ztb_df.df.to_sql(
                name=table_name,
                con=engine,
                if_exists='append',
                index=False,
                chunksize=1000,
                dtype=dtype_mapping,
            )
            
            logger.info(f"Imported {len(ztb_df.df)} records to {table_name}")
            
        except Exception as e:
            logger.error(f"XX Error importing to PostgreSQL {table_name}: {e}")
            raise
        finally:
            engine.dispose()

    def verify_empty(self, tables=None):
        """
        Verify that tables are empty or don't exist
        
        Args:
            tables: List of table names to verify. If None, checks common tables.
        
        Returns:
            bool: True if all tables are empty/don't exist, False otherwise
        """
        if tables is None:
            tables = ['games', 'reviews', 'hltb']
        
        logger.info(f"Verifying PostgreSQL tables are dropped...")
        all_empty = True
        
        try:
            with engine.connect() as conn:
                for table_name in tables:
                    # Check if table exists
                    result = conn.execute(text("""
                        SELECT EXISTS (
                            SELECT FROM information_schema.tables 
                            WHERE table_schema = 'public' 
                            AND table_name = :table_name
                        )
                    """), {"table_name": table_name})
                    
                    exists = result.scalar()
                    
                    if exists:
                        # Table exists, check if it has data
                        result = conn.execute(text(f'SELECT COUNT(*) FROM "{table_name}"'))
                        count = result.scalar()
                        
                        if count > 0:
                            logger.error(f"{table_name} still has {count} rows")
                            all_empty = False
                        else:
                            logger.info(f"  OK: {table_name} exists but is empty")
                    else:
                        logger.info(f"  OK: {table_name} does not exist")
            
            if all_empty:
                logger.info("PostgreSQL verification: All tables dropped successfully")
            else:
                logger.error("PostgreSQL verification: FAILED - some tables still have data")
            
            return all_empty
            
        except Exception as e:
            logger.error(f"XX Error verifying PostgreSQL tables: {e}")
            return False
