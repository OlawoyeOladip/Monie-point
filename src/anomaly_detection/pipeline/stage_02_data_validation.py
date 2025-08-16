from src.anomaly_detection.config.configuration import ConfigurationManager
from src.anomaly_detection.components.data_validation import DataValidation

class DataValidationTrainingPipeline:
    """
    A pipeline class responsible for orchestrating the data validation process.

    This class ties together the configuration management and the actual validation logic.
    It ensures that the data is validated against the expected schema before being passed 
    to subsequent stages in the pipeline.
    """
    def __init__(self):
        """
        Initializes the DataValidationTrainingPipeline instance.

        Currently, there are no attributes set during initialization as all operations 
        are handled in the `main` method.
        """
        pass   

    def main(self):
        """
        Executes the data validation pipeline.

        Steps:
            1. Initializes the ConfigurationManager to load configurations.
            2. Retrieves the data validation configuration.
            3. Instantiates the DataValidation component with the configuration.
            4. Calls the `validate_data` method to perform validation.
        
        Raises:
            Exception: If any error occurs during the validation process, it is raised for handling upstream.
        """
        try:
            config = ConfigurationManager()
            data_validation_config = config.get_data_validation_config(table_name="processed_transaction_logs")
            data_validation = DataValidation(config=data_validation_config)
            data_validation.validate_data()
        except Exception as e:
            raise e 
    
    