import logging
from src.anomaly_detection.entity.config_entity import DataIngestionConfig
from pathlib import Path    
import os 
import pandas as pd    
from .data_parser import TransactionCleaner
from typing import Optional

class DataIngestion:
    def __init__(self, config: DataIngestionConfig):
        self.config = config  

    def load_file(self):
        """A function to load the origin uncleaned log file"""
        file_path = Path(self.config.file_path)
        output_path = Path(self.config.local_data_dir) / "anomaly_detection.csv"
        
        os.makedirs(output_path.parent, exist_ok=True)
        print(file_path)
        
        try:
            df = pd.read_csv(f"c:\\Users\\user\\Desktop\\{file_path}")
            if df.empty:
                logging.warning(f"The file {file_path} is empty")
            else:
                logging.info(f"The file {file_path} loaded successfully with {len(df)} rows")
                print(df.head())

                # Save a local copy
                df.to_csv(output_path, index=False)
                logging.info(f"Data saved to {output_path}")

        except FileNotFoundError:
            logging.error(f"The file {file_path} was not found")
        except Exception as e:
            logging.error(f"An error occurred while loading the file: {e}")
            
    def clean_data(self):
        """A function that cleans the data and save it after cleaning"""
        file_path = Path(self.config.file_path)
        clean_output_path = Path(self.config.clean_path)
        os.makedirs(clean_output_path.parent, exist_ok=True)
        try:
            df = TransactionCleaner().clean_transaction_logs(csv_file_path = f"c:\\Users\\user\\Desktop\\{file_path}")
            if df.empty:
                logging.warning(f"The file {file_path} is empty")
            else:
                logging.info(f"The file {file_path} loaded successfully with {len(df)} rows")
                print(df.head())
                # Save a local copy
                df.to_csv(clean_output_path, index=False)
                logging.info(f"Data saved to {clean_output_path}")
        except FileNotFoundError:
            logging.error(f"The file {file_path} was not found")
        except Exception as e:
            logging.error(f"An error occurred while loading the file: {e}")
            
    def get_data(self) -> Optional[pd.DataFrame]:
        """A simple function to return Dataframe object"""
        file_path = Path(self.config.file_path)
        try:
            df = TransactionCleaner().clean_transaction_logs(csv_file_path = f"c:\\Users\\user\\Desktop\\{file_path}")
            if df.empty:
                logging.warning(f"The file {file_path} is empty")
            else:
                logging.info(f"The file {file_path} loaded successfully with {len(df)} rows")
                return df
        except FileNotFoundError:
            logging.error(f"The file {file_path} was not found")
        except Exception as e:
            logging.error(f"An error occurred while loading the file: {e}")
        return None
        