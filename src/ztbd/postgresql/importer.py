import sqlalchemy
from .database import engine
from .models import Base
from ..ztbdf import ZTBDataFrame

class PostgreSQLImporter:
    def __init__(self):
        Base.metadata.create_all(bind=engine)
    
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
            
            print(f"✓ Imported {len(ztb_df.df)} records to {table_name}")
            
        except Exception as e:
            print(f"✗ Error importing to PostgreSQL {table_name}: {e}")
            raise
        finally:
            engine.dispose()
