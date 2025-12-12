import sqlalchemy
from sqlalchemy import text
from .database import engine
from .models import Base
from ..ztbdf import ZTBDataFrame

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
            print(f"Cleaning PostgreSQL database...")
            
            with engine.connect() as conn:
                # if tables is None:
                #     # Get all table names from metadata
                #     tables = [table.name for table in Base.metadata.sorted_tables]
                
                for table_name in tables:
                    # Use TRUNCATE with CASCADE to handle foreign key constraints
                    conn.execute(text(f'TRUNCATE TABLE "{table_name}" CASCADE'))
                    conn.commit()
                    print(f"  Truncated table: {table_name}")
            
            print("PostgreSQL cleanup complete")
            
        except Exception as e:
            print(f"XX Error cleaning PostgreSQL database: {e}")
            raise
    
    def clean_database(self, tables=[]):
        """
        Drop tables from PostgreSQL database (more aggressive than truncate)
        
        Args:
            tables: List of table names to drop. If None, drops all tables.
        """
        try:
            print(f"Dropping PostgreSQL tables...")
            
            # if tables is None:
            #     # Drop all tables using metadata
            #     Base.metadata.drop_all(bind=engine)
            #     print("  Dropped all tables")
            # else:
            with engine.connect() as conn:
                for table_name in tables:
                    conn.execute(text(f'DROP TABLE IF EXISTS "{table_name}" CASCADE'))
                    conn.commit()
                    print(f"  Dropped table: {table_name}")
            
            print("PostgreSQL drop complete")
            
        except Exception as e:
            print(f"XX Error dropping PostgreSQL tables: {e}")
            raise
    
    def import_dataset(self, table_name: str, ztb_df: ZTBDataFrame, json_columns=None):
        """Import pre-cleaned dataset to PostgreSQL"""
        try:
            print(f"Importing {len(ztb_df.df)} records to {table_name}...")

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
            
            print(f"Imported {len(ztb_df.df)} records to {table_name}")
            
        except Exception as e:
            print(f"XX Error importing to PostgreSQL {table_name}: {e}")
            raise
        finally:
            engine.dispose()
