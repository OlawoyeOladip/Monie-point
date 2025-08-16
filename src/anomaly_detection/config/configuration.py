from src.anomaly_detection.constant import *
from src.anomaly_detection.utils.common import load_yaml, create_directories
from src.anomaly_detection.entity.config_entity import DataIngestionConfig, DataValidationConfig


class ConfigurationManager:
    """
    A class responsible for managing configurations across various pipeline stages.
    It loads configuration files (YAML) and returns stage-specific configuration 
    objects with all necessary parameters.

    Attributes:
        config (dict): Loaded configuration from the main config YAML file.
        params (dict): Loaded parameters from the params YAML file.
        schema (dict): Loaded schema definitions from the schema YAML file.
    """
    def __init__(self, config_filepath = CONFIG_FILE_PATH,
                schema_filepath = SCHEMA_FILE_PATH, 
                params_filepath = PARAMS_FILE_PATH):
        """
        Initializes the ConfigurationManager by loading the configuration, parameters, 
        and schema files. Also ensures that the main artifacts directory exists.

        Args:
            config_filepath (str): Path to the main configuration YAML file.
            params_filepath (str): Path to the parameters YAML file.
            schema_filepath (str): Path to the schema YAML file.
        """
        self.config = load_yaml(config_filepath)
        self.params = load_yaml(params_filepath)
        self.schema = load_yaml(schema_filepath)
        
        create_directories([self.config.artifacts_root])
        
    # def clean_data
        
    def get_data_ingestion_config(self) -> DataIngestionConfig:
        config = self.config.data_ingestion
        create_directories([config.root_dir])
        data_ingestion_config = DataIngestionConfig(
            root_dir = config.root_dir,
            file_path = config.file_path,
            local_data_dir = config.local_data_dir,
            clean_path = config.clean_data_path
        )
        
        return data_ingestion_config
    
    def get_data_validation_config(self, table_name='transaction_logs') -> DataValidationConfig:
        config = self.config.data_validation
        
        # Get all columns for the specified table
        table_schema = None
        for table in self.schema['tables']:
            if table['name'] == table_name:
                table_schema = table
                break
        
        if table_schema is None:
            raise ValueError(f"Table '{table_name}' not found in schema")
        
        # Extract column information
        columns_info = {}
        for column in table_schema['columns']:
            columns_info[column['name']] = {
                'type': column['type'],
                'description': column.get('description', ''),
                'constraints': column.get('constraints', column.get('constraints', {}))  
            }
        
        create_directories([config.root_dir])
        
        data_validation_config = DataValidationConfig(
            root_dir = config.root_dir,
            STATUS_FILE= config.STATUS_FILE,
            data_path = config.data_path,
            data_path2 = config.data_path2,
            all_schema = columns_info,  
            table_info = table_schema   
        )
        return data_validation_config
    
    
    