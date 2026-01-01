import kagglehub
import pandas as pd
import os
import logging

logger = logging.getLogger('ztbd')

class ZTBDataFrame:
    def __init__(self, dataset, csv_filename, primary_key, name):
        logging.basicConfig(filename='ztb.log', level=logging.INFO)
        
        self._df = self._download_dataset(dataset, csv_filename)
        self._primary_key = primary_key
        self._name = name
        
        if primary_key in self._df.columns:
            logger.info(f"Set primary key: {primary_key}")
        else: 
            logger.warning(f"WARNING: Primary key {primary_key} not found")
    
    def _download_dataset(self, dataset, csv_filename):
        """Download and load dataset from Kaggle"""
        logger.info(f"Downloading dataset from Kaggle: {dataset}")
        dataset_path = kagglehub.dataset_download(dataset)
        logger.info(f"Dataset downloaded to: {dataset_path}")
        
        csv_path = os.path.join(dataset_path, csv_filename)
        logger.info(f"Importing {csv_filename}")
        
        return pd.read_csv(csv_path)
    
    @property
    def df(self):
        return self._df
    
    @property 
    def primary_key(self):
        return self._primary_key
    
    @property
    def name(self):
        return self._name
    
    def log_shape(self):
        logger.info(f"Dataset shape: {self._df.shape}")
        logger.info(f"Dataset columns: {self._df.columns.tolist()}")
    
    def rename_columns(self, column_mapping):
        """Rename columns using mapping dictionary"""
        if column_mapping:
            self._df.rename(columns=column_mapping, inplace=True)
            logger.info(f"Renamed columns: {list(column_mapping.keys())}")
    
    def drop_unnamed_columns(self):
        """Drop unnamed index columns"""
        if 'Unnamed: 0' in self._df.columns:
            self._df.drop('Unnamed: 0', axis=1, inplace=True)
            logger.info("Dropped unnamed index column")
    
    def parse_json_columns(self, json_columns):
        """Parse JSON columns stored as strings"""
        for col in json_columns:
            if col in self._df.columns:
                self._df[col] = self._df[col].apply(
                    lambda x: eval(x) if pd.notna(x) and x != '' else None
                )
        logger.info(f"Parsed JSON columns: {json_columns}")
    
    def convert_datetime_column(self, column, unit='s', errors='coerce'):
        """Convert column to datetime"""
        if column in self._df.columns:
            if unit:
                # self._df[column] = pd.to_datetime(self._df[column], unit=unit, errors=errors)
                self._df[column] = pd.to_datetime(self._df[column], unit=unit)
            else:
                # self._df[column] = pd.to_datetime(self._df[column], errors=errors)
                self._df[column] = pd.to_datetime(self._df[column])
            logger.info(f"Converted {column} to datetime")
    
    def limit_records(self, head_count):
        """Limit dataframe to specified number of records"""
        if head_count > 0:
            self._df = self._df.head(head_count)
            logger.info(f"Limited to {head_count} records")
    
    def sort_by_column(self, column):
        """Sort dataframe by specified column"""
        if column in self._df.columns:
            self._df = self._df.sort_values(column)
            logger.info(f"Sorted by {column}")
    
    def handle_duplicates(self, key_column="", output_file='duplicates.csv'):
        """Identify duplicates, save them to file, and return deduplicated dataframe"""
        logger.info(f"Checking for duplicates. Original rows: {len(self._df)}")
        
        if not key_column:
            key_column = self._primary_key
        
        logger.info(f"Key column: {key_column}")
        
        duplicates_mask = self._df.duplicated(subset=[key_column], keep='first')
        duplicates = self._df[duplicates_mask]
        
        if len(duplicates) > 0:
            logger.info(f"Duplicate rows found: {len(duplicates)}")
            duplicates.to_csv(output_file, index=False)
            logger.info(f"Duplicates saved to: {output_file}")
            
            duplicate_ids = duplicates[key_column].value_counts().head(10)
            logger.info(f"Top 10 most duplicated {key_column}s: {duplicate_ids}")
            
            self._df = self._df.drop_duplicates(subset=[key_column], keep='first')
            logger.info(f"Rows after deduplication: {len(self._df)}")
        else:
            logger.info("Found no duplicates")
    
    def clean_nan_values(self):
        """Clean NaN values for MongoDB compatibility"""
        records = self._df.to_dict('records')
        for record in records:
            for key, value in list(record.items()):
                if isinstance(value, float) and pd.isna(value):
                    record[key] = None
                elif pd.isna(value) if not isinstance(value, (list, dict)) else False:
                    record[key] = None
        return records
    
    def check_columns(self):
        """Test each column's data range to identify issues"""
        for col in self._df.columns:
            if col == 'Unnamed: 0':
                continue
                
            logger.info(f"Column: {col}, Type: {self._df[col].dtype}")
            non_null = self._df[col].dropna()
            
            if pd.api.types.is_numeric_dtype(self._df[col]) and len(non_null) > 0:
                min_val, max_val = non_null.min(), non_null.max()
                logger.info(f"  Min: {min_val}, Max: {max_val}")
                
                if self._df[col].dtype in ['int64', 'float64']:
                    int_min, int_max = -2147483648, 2147483647
                    if min_val < int_min or max_val > int_max:
                        logger.warning(f"  EXCEEDS INTEGER - needs BigInteger")
                    else:
                        logger.info(f"  Fits in Integer")
            
            elif pd.api.types.is_string_dtype(self._df[col]) or self._df[col].dtype == 'object':
                non_null_str = self._df[col].dropna().astype(str)
                if len(non_null_str) > 0:
                    max_len = non_null_str.str.len().max()
                    logger.info(f"  Max length: {max_len}")

# Factory function for common dataset configurations
def create_games_dataframe():
    """
    This dataset has been created with Steam store page scraping and Steam Games Scraper repository, which gathers data using the Steam API and Steam Spy.
    """
    return ZTBDataFrame(
        dataset="artermiloff/steam-games-dataset",
        csv_filename="games_march2025_cleaned.csv", 
        primary_key="appid",
        name="games"
    )

def create_reviews_dataframe():
    """
    Dataset of around 21 million user reviews of around 300 different games on Steam. Obtained using Steam's provided API outlined in the Steamworks documentation: https://partner.steamgames.com/doc/store/getreviews
    """
    return ZTBDataFrame(
        dataset="najzeko/steam-reviews-2021",
        csv_filename="steam_reviews.csv",
        primary_key="review_id",
        name="reviews"
    )

def create_top100_dataframe():
    """
    This dataset contains the average players by month for the current top 100 games. It was scraped off https://steamcharts.com/top and converted into this easier to analyze format. 
    """
    return ZTBDataFrame(
        dataset="jackogozaly/steam-player-data",
        csv_filename="Valve_Player_Data.csv",
        primary_key="",
        name="top100"
    )

def create_hltb_dataframe():
    """
    The dataset contains structured data on video games scraped from HowLongToBeat as of February 16, 2025. This dataset includes various attributes such as playtime estimates, game metadata, platform-specific details, and completion statistics. Each row represents a unique game, with columns capturing its title, developer, publisher, platform, genre, release dates, and user engagement metrics. Playtime estimates are provided for Main Story, Main + Extras, and Completionist runs, expressed in minutes. Additionally, the dataset includes review scores, the number of players who have completed or are currently playing a game, and platform-specific completion time data. The file also contains user review distributions, showing how many players rated a game at different score levels. Missing values may appear in some columns, particularly for games with limited player engagement. This dataset is suitable for research, data visualization, and machine learning applications, offering insights into game lengths, popularity, and player behavior trends.
    """
    return ZTBDataFrame(
        dataset="zaireali/howlongtobeat-games-scraper-2162025",
        csv_filename="hltb_data.csv",
        primary_key="game_game_id",
        name="hltb"
    )
