import kagglehub
import pandas as pd
import os
import logging

logger = logging.getLogger('logger')

class ZTBDataFrame():
    _df = pd.DataFrame()
    _primary_key = str

    def __init__(self, dataset, csv_filename, primary_key):
        logging.basicConfig(filename='ztb.log', level=logging.INFO)

        # print(f"Downloading dataset from Kaggle: {dataset}")
        dataset_path = kagglehub.dataset_download(dataset)
        logger.info(f"Dataset downloaded to: {dataset_path}")

        logger.info(f"\nAvailable files in dataset:")
        files = os.listdir(dataset_path)
        for file in files:
            logger.info(f"  - {file}")

        csv_path = os.path.join(dataset_path, csv_filename)

        logger.info(f"\n=== Importing {csv_filename} ===")
        self._df = pd.read_csv(csv_path)

        if primary_key in self._df.columns:
            logger.info(f"Set primary key: {primary_key}")
            self._primary_key = primary_key
        else: 
            logger.warning(f"WARNING: Primary key {primary_key} not found")
    
    def log_shape(self):
        logger.info(f"\n=== Dataset shape: {self._df.shape}")
        logger.info(f"\n=== Dataset columns: {self._df.columns.tolist()}")

    def handle_duplicates(self, key_column = "", output_file='duplicates.csv'):
        """
        Identify duplicates, save them to file, and return deduplicated dataframe
        """
        logger.info(f"Checking for duplicates. Original rows: {len(self._df)}")

        if not key_column:
            key_column = self._primary_key

        logger.info(f"Key column: {key_column}")
        
        # Find duplicates based on key column
        duplicates_mask = self._df.duplicated(subset=[key_column], keep='first')
        duplicates = self._df[duplicates_mask]
        
        if len(duplicates) > 0:
            logger.info(f"Duplicate rows found: {len(duplicates)}")
        
            duplicates.to_csv(output_file, index=False)
            logger.info(f"Duplicates saved to: {output_file}")
            
            duplicate_ids = duplicates[key_column].value_counts().head(10)
            logger.info(f"\nTop 10 most duplicated {key_column}s:")
            logger.info(duplicate_ids)
        
            # Remove duplicates, keeping first occurrence
            self._df = self._df.drop_duplicates(subset=[key_column], keep='first')
            logger.info(f"\nRows after deduplication: {len(self._df)}")
            logger.info(f"Removed: {len(duplicates)} rows")
        else:
            logger.info("Found no duplicates")
    
    def check_columns(self):
        """Test each column's data range to identify issues"""

        for col in self._df.columns:
            if col == 'Unnamed: 0':
                continue
                
            logger.info(f"\nColumn: {col}")
            logger.info(f"  Type: {self._df[col].dtype}")

            non_null = self._df[col].dropna()
            
            # Check for numeric columns
            if pd.api.types.is_numeric_dtype(self._df[col]) and len(non_null) > 0:
                min_val = non_null.min()
                max_val = non_null.max()
                logger.info(f"  Min: {min_val}")
                logger.info(f"  Max: {max_val}")
                
                # Check if values fit in different integer types
                if self._df[col].dtype in ['int64', 'float64']:
                    # PostgreSQL Integer range: -2147483648 to 2147483647
                    int_min, int_max = -2147483648, 2147483647
                    # PostgreSQL BigInteger range: -9223372036854775808 to 9223372036854775807
                    bigint_min, bigint_max = -9223372036854775808, 9223372036854775807
                    
                    if min_val < int_min or max_val > int_max:
                        logger.warning(f"EXCEEDS INTEGER - needs BigInteger")
                    else:
                        logger.info(f"Fits in Integer")
                        
                    if min_val < bigint_min or max_val > bigint_max:
                        logger.warning(f"EXCEEDS BIGINTEGER!")
            
            # Check string lengths
            elif pd.api.types.is_string_dtype(self._df[col]) or self._df[col].dtype == 'object':
                non_null = self._df[col].dropna().astype(str)
                if len(non_null) > 0:
                    max_len = non_null.str.len().max()
                    logger.info(f"Max length: {max_len}")
