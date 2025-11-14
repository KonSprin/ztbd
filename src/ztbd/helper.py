import kagglehub
import pandas as pd
import os

def downlaod_keggle_ds(dataset, csv_filename):
    # Download dataset from Kaggle
    print(f"Downloading dataset from Kaggle: {dataset}")
    dataset_path = kagglehub.dataset_download(dataset)
    print(f"Dataset downloaded to: {dataset_path}")

    print(f"\nAvailable files in dataset:")
    files = os.listdir(dataset_path)
    for file in files:
        print(f"  - {file}")

    csv_path = os.path.join(dataset_path, csv_filename)

    if os.path.exists(csv_path):
        print(f"\n=== Importing {csv_filename} ===")
        df = pd.read_csv(csv_path)
                    
        print(f"\n=== Dataset shape: {df.shape}")
        print(f"\n=== GamDatasetes columns: {df.columns.tolist()}")
        
        return df
    return pd.DataFrame()

def handle_duplicates(df: pd.DataFrame, key_column: str, output_file='duplicates.csv'):
    """
    Identify duplicates, save them to file, and return deduplicated dataframe
    """
    print(f"\n=== HANDLING DUPLICATES ===")
    print(f"Original rows: {len(df)}")
    
    # Find duplicates based on key column
    duplicates_mask = df.duplicated(subset=[key_column], keep='first')
    duplicates = df[duplicates_mask]
    
    print(f"Duplicate rows found: {len(duplicates)}")
    
    if len(duplicates) > 0:
        # Save duplicates to file
        duplicates.to_csv(output_file, index=False)
        print(f"✓ Duplicates saved to: {output_file}")
        
        # Show some stats about duplicates
        duplicate_ids = duplicates[key_column].value_counts().head(10)
        print(f"\nTop 10 most duplicated {key_column}s:")
        print(duplicate_ids)
    
    # Remove duplicates, keeping first occurrence
    df_clean = df.drop_duplicates(subset=[key_column], keep='first')
    print(f"\nRows after deduplication: {len(df_clean)}")
    print(f"Removed: {len(df) - len(df_clean)} rows")
    
    return df_clean

def diagnose_columns(df: pd.DataFrame, table_name='reviews'):
    """Test each column's data range to identify issues"""
    print(f"\n=== DIAGNOSING {table_name.upper()} COLUMNS ===\n")
    
    for col in df.columns:
        if col == 'Unnamed: 0':
            continue
            
        print(f"\nColumn: {col}")
        print(f"  Type: {df[col].dtype}")
        
        # Check for numeric columns
        if pd.api.types.is_numeric_dtype(df[col]):
            non_null = df[col].dropna()
            if len(non_null) > 0:
                min_val = non_null.min()
                max_val = non_null.max()
                print(f"  Min: {min_val}")
                print(f"  Max: {max_val}")
                
                # Check if values fit in different integer types
                if df[col].dtype in ['int64', 'float64']:
                    # PostgreSQL Integer range: -2147483648 to 2147483647
                    int_min, int_max = -2147483648, 2147483647
                    # PostgreSQL BigInteger range: -9223372036854775808 to 9223372036854775807
                    bigint_min, bigint_max = -9223372036854775808, 9223372036854775807
                    
                    if min_val < int_min or max_val > int_max:
                        print(f"  ⚠️  EXCEEDS INTEGER - needs BigInteger")
                    else:
                        print(f"  ✓ Fits in Integer")
                        
                    if min_val < bigint_min or max_val > bigint_max:
                        print(f"  ❌ EXCEEDS BIGINTEGER!")
        
        # Check string lengths
        elif pd.api.types.is_string_dtype(df[col]) or df[col].dtype == 'object':
            non_null = df[col].dropna().astype(str)
            if len(non_null) > 0:
                max_len = non_null.str.len().max()
                print(f"  Max length: {max_len}")
