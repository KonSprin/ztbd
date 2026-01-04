import sqlalchemy
from sqlalchemy import text
from .database import engine
from .models import Base
from ..ztbdf import ZTBDataFrame
import logging

logger = logging.getLogger('ztbd')


class MySQLImporter:
    def __init__(self):
        Base.metadata.create_all(bind=engine)
    
    def truncate_database(self, tables=[]):
        """
        Clean (truncate) tables from MySQL database
        
        Args:
            tables: List of table names to truncate.
        """
        try:
            logger.info(f"Cleaning MySQL database...")
            
            with engine.connect() as conn:
                conn.execute(text('SET FOREIGN_KEY_CHECKS = 0'))
                
                for table_name in tables:
                    conn.execute(text(f'TRUNCATE TABLE `{table_name}`'))
                    conn.commit()
                    logger.info(f"  Truncated table: {table_name}")
                
                conn.execute(text('SET FOREIGN_KEY_CHECKS = 1'))
                conn.commit()
            
            logger.info("MySQL cleanup complete")
            
        except Exception as e:
            logger.error(f"XX Error cleaning MySQL database: {e}")
            raise
    
    def clean_database(self, tables=[]):
        """
        Drop tables from MySQL database
        
        Args:
            tables: List of table names to drop.
        """
        try:
            logger.info(f"Dropping MySQL tables...")
            
            with engine.connect() as conn:
                conn.execute(text('SET FOREIGN_KEY_CHECKS = 0'))
                
                for table_name in tables:
                    conn.execute(text(f'DROP TABLE IF EXISTS `{table_name}`'))
                    conn.commit()
                    logger.info(f"  Dropped table: {table_name}")
                
                conn.execute(text('SET FOREIGN_KEY_CHECKS = 1'))
                conn.commit()
            
            logger.info("MySQL drop complete")
            
        except Exception as e:
            logger.error(f"XX Error dropping MySQL tables: {e}")
            raise
    
    def import_df(self, ztb_df: ZTBDataFrame, json_columns=None):
        """Import pre-cleaned dataset to MySQL"""
        try:
            table_name = ztb_df._name

            logger.info(f"Importing {len(ztb_df.df)} records to {table_name}...")

            dtype_mapping = {}
            if json_columns:
                dtype_mapping = {key: sqlalchemy.types.JSON for key in json_columns}
            
            # Add MEDIUMTEXT for large text columns
            from sqlalchemy.dialects.mysql import MEDIUMTEXT
            text_columns = ['detailed_description', 'about_the_game', 'short_description', 
                          'reviews', 'notes', 'review']
            for col in text_columns:
                if col in ztb_df.df.columns:
                    dtype_mapping[col] = MEDIUMTEXT
            
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
            logger.error(f"XX Error importing to MySQL {table_name}: {e}")
            raise
        finally:
            engine.dispose()

    def import_dataframe(self, df, table_name, json_columns=None):
        """
        Import a regular pandas DataFrame (for normalized tables)
        
        Args:
            df: pandas DataFrame to import
            table_name: Name of the table
            json_columns: List of column names that contain JSON data
        """
        try:
            logger.info(f"Importing {len(df)} records to {table_name}...")

            dtype_mapping = {}
            if json_columns:
                dtype_mapping = {key: sqlalchemy.types.JSON for key in json_columns}
            
            df.to_sql(
                name=table_name,
                con=engine,
                if_exists='append',
                index=False,
                chunksize=1000,
                dtype=dtype_mapping,
            )
            
            logger.info(f"  Imported {len(df)} records to {table_name}")
            
        except Exception as e:
            logger.error(f"XX Error importing to {table_name}: {e}")
            raise

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
        
        logger.info(f"Verifying MySQL tables are dropped...")
        all_empty = True
        
        try:
            with engine.connect() as conn:
                for table_name in tables:
                    result = conn.execute(text("""
                        SELECT EXISTS (
                            SELECT 1 FROM information_schema.tables 
                            WHERE table_schema = DATABASE()
                            AND table_name = :table_name
                        )
                    """), {"table_name": table_name})
                    
                    exists = result.scalar()
                    
                    if exists:
                        result = conn.execute(text(f'SELECT COUNT(*) FROM `{table_name}`'))
                        count = result.scalar()
                        
                        if count > 0:
                            logger.error(f"{table_name} still has {count} rows")
                            all_empty = False
                        else:
                            logger.info(f"  OK: {table_name} exists but is empty")
                    else:
                        logger.info(f"  OK: {table_name} does not exist")
            
            if all_empty:
                logger.info("MySQL verification: All tables dropped successfully")
            else:
                logger.error("MySQL verification: FAILED - some tables still have data")
            
            return all_empty
            
        except Exception as e:
            logger.error(f"XX Error verifying MySQL tables: {e}")
            return False
