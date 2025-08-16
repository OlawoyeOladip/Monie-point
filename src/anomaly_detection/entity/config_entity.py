from pathlib import Path  
from dataclasses import dataclass 
from typing import Optional

@dataclass(frozen=True)
class DataIngestionConfig:
    root_dir: Path
    file_path: str   
    local_data_dir: Path  
    clean_path: str 
    
    
@dataclass(frozen=True)
class DataValidationConfig:
    """
    Configuration class for data validation settings.

    Attributes:
        root_dir (Path): Root directory for storing data validation artifacts.
        STATUS_FILE (str): Path to the file where the validation status will be recorded.
        unzip_data_dir (Path): Directory containing the extracted data for validation.
        all_schema (dict): Dictionary defining the expected schema (column names and data types).

    Notes:
        - The class is frozen, ensuring immutability after instantiation.
        - The schema dictionary plays a critical role in ensuring data integrity before further processing.
    """
    root_dir: Path
    STATUS_FILE: str
    data_path: Path
    data_path2: Path  
    all_schema: dict
    table_info: Optional[str]