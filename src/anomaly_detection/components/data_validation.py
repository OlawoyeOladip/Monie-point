import pandas as pd 
import logging
from src.anomaly_detection.entity.config_entity import DataValidationConfig


class DataValidation:
    def __init__(self, config: DataValidationConfig):
        self.config = config

    def validate_data(self):
        try:
            data = pd.read_csv(self.config.data_path2)
            data['datetime'] = pd.to_datetime(data['datetime'])
            all_columns = [col for col in list(data.columns) if col != "row_id"] 
            dtypes_list_str = data.drop(['row_id'], axis="columns").dtypes.astype(str).tolist()
            print(f"Data columns: {all_columns}")
            print(f"Data dtypes: {dtypes_list_str}")

            schema_columns = list(self.config.all_schema.keys())
            print(f"Schema columns: {schema_columns}")
            
            # Fixed: Use self.config instead of data_validation_config
            schema_dtypes = [col['type'] for col in self.config.all_schema.values()]
            print(f"Schema dtypes: {schema_dtypes}")

            # Check for exact column match (order + length)
            column_name_match = all_columns == schema_columns
            # Check if all dtypes match exactly
            dtype_match = dtypes_list_str == schema_dtypes

            validation_status = column_name_match and dtype_match

            if not column_name_match:
                logging.warning(f"Column mismatch:\nExpected: {schema_columns}\nFound: {all_columns}")
                print(f"Column mismatch - Expected: {schema_columns}, Found: {all_columns}")
                
            if not dtype_match:
                logging.warning(f"Dtype mismatch:\nExpected: {schema_dtypes}\nFound: {dtypes_list_str}")
                print(f"Dtype mismatch - Expected: {schema_dtypes}, Found: {dtypes_list_str}")

            if validation_status:
                print("All columns and dtypes match successfully.")
                logging.info("Data validation passed successfully")
            else:
                print("Data validation failed")
                logging.error("Data validation failed")

            # Write validation status once
            with open(self.config.STATUS_FILE, "w") as f:
                f.write(f"Validation status: {validation_status}")

            return validation_status

        except Exception as e:
            logging.error(f"Exception during data validation: {str(e)}")
            print(f"Exception occurred: {str(e)}")
            raise e