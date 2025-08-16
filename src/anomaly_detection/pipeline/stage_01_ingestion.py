from src.anomaly_detection.config.configuration import ConfigurationManager
from src.anomaly_detection.components.ingest_pipeline import DataIngestion
import pandas as pd
from typing import Optional

class DataIngestionTrainingPipeline:
    """A pipeline class responsible for orchestrating the data ingestion and cleaning process.
    
    This pipeline manages the complete data ingestion flow by: 
    1. Loading the raw log and saving it into artifacts
    2. Take the saved data from artifacts and uses a parser module for extracting structured features
    3. Allows the loading of the structured features 
    
    It uses configuration details provided by the ConfigurationManager and handles 
    any errors that occur during the ingestion process.
    """
    def __init__(self):
        """
        Initializes the DataIngestionTrainingPipeline instance.

        Currently, no attributes are initialized, as all operations are managed within the `main` method.
        """
        pass 

    def main(self) -> Optional[pd.DataFrame]:
        """
        Executes the data ingestion pipeline.

        Steps:
            1. Loads the data ingestion configuration using ConfigurationManager.
            2. Instantiates the DataIngestion component with the given configuration.
            3. This allowing data Loading, cleaning, and also returning dataframe of the cleaned data
            
        """
        try:
            config = ConfigurationManager()
            data_ingestion_config = config.get_data_ingestion_config()
            data_ingestion = DataIngestion(config=data_ingestion_config)
            data_ingestion.load_file()
            data_ingestion.clean_data()
            return data_ingestion.get_data()
            
        except Exception as e:
            raise e