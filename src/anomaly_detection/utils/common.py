import os
import sys
import yaml
import json
import joblib
from typing import List, Dict, Any, Union
from ensure import ensure_annotations
from box import Box as ConfigBox
from pathlib import Path
from box.exceptions import BoxValueError, BoxKeyError
from jsonschema import validate, ValidationError
import logging
import pandas as pd
from sklearn.preprocessing import LabelEncoder

def label_encode_columns(df, columns):
    """
    Label encodes the specified categorical columns in a dataframe.

    Parameters:
        df (pd.DataFrame): The input dataframe.
        columns (list): List of column names to encode.

    Returns:
        pd.DataFrame: DataFrame with encoded columns.
        dict: Dictionary of fitted LabelEncoders for inverse transformation later.
    """
    encoders = {}
    df_encoded = df.copy()

    for col in columns:
        le = LabelEncoder()
        df_encoded[col] = le.fit_transform(df_encoded[col])
        encoders[col] = le

    return df_encoded, encoders

def convert_to_usd(df, currency_col="currency", amount_col="amount"):
    """
    Convert all amounts in a DataFrame to USD.
    
    Parameters:
        df (pd.DataFrame): Input DataFrame.
        currency_col (str): Name of the column containing currency codes.
        amount_col (str): Name of the column containing amounts.
        
    Returns:
        pd.DataFrame: DataFrame with a new column 'amount_usd' containing converted values.
    """
    # Example currency rates relative to USD
    rates = {
        "USD": 1.0,
        "EUR": 1.1,  # 1 EUR = 1.1 USD
        "GBP": 1.25  # 1 GBP = 1.25 USD
    }
    
    df = df.copy()  
    
    # Standardize currency codes to uppercase
    df[currency_col] = df[currency_col].str.upper()
    
    # Check for unknown currencies
    unknown_currencies = set(df[currency_col]) - set(rates.keys())
    if unknown_currencies:
        raise ValueError(f"Unknown currencies found: {unknown_currencies}")
    
    # Apply conversion
    df["amount"] = df.apply(
        lambda row: row[amount_col] * rates[row[currency_col]], axis=1
    )
    
    return df


@ensure_annotations
def load_yaml(path_to_yaml: Path) -> ConfigBox:
    """
    Load a YAML file into a ConfigBox object.

    Args:
        path_to_yaml (Path): The path to the YAML file.

    Raises:
        ValueError: If the YAML file is not found, empty, or has an invalid structure.

    Returns:
        ConfigBox: The loaded YAML data as a ConfigBox object.
    """
    try:
        with open(path_to_yaml, "r", encoding='utf-8') as file:
            yaml_data = yaml.safe_load(file)
            if yaml_data is None:
                logging.error(f"YAML file {path_to_yaml} is empty.")
                raise ValueError(f"YAML file {path_to_yaml} is empty.")
            logging.info(f"YAML file {path_to_yaml} loaded successfully.")
        return ConfigBox(yaml_data)
    
    except yaml.YAMLError as e:
        logging.exception(f"Error parsing YAML: {e}")

    except FileNotFoundError:
        logging.exception(f"YAML file {path_to_yaml} not found.")
        raise ValueError(f"YAML file {path_to_yaml} not found.")

    except BoxKeyError as e:
        logging.exception(f"Invalid YAML structure: {e}")
        raise ValueError(f"Invalid YAML structure: {e}")

    except Exception as e:
        logging.exception(f"An unexpected error occurred: {e}")
        raise ValueError(f"An unexpected error occurred: {e}")
    
    
@ensure_annotations
def save_joblib(path: Path, data: Any):
    """Save data using joblib
    
    Args:
        data (Any): data to be saved as binary
        path (Path): path to binary file
    """
    joblib.dump(value=data, filename=path)
    logging.info(f"Binary file saved at: {path}")

@ensure_annotations
def load_joblib(path: Path) -> Any:
    """load binary data

    Args:
        path (Path): path to binary file

    Returns:
        Any: object stored in the file
    """
    data = joblib.load(path)
    logging.info(f"binary file loaded from: {path}")
    return data

@ensure_annotations
def create_directories(paths: list, verbose: bool = True):
    """Create directories from a list of paths.

    Args:
        paths (list): List of directory paths to create.
        verbose (bool, optional): Whether to log directory creation. Defaults to True.
    """
    for path in paths:
        os.makedirs(path, exist_ok=True)
        verbose and logging.info(f"Created directory at: {path}")


@ensure_annotations
def get_size(path: Path) -> str:
    """get size in KB

    Args:
        path (Path): path of the file

    Returns:
        str: size in KB
    """
    size_in_kb = round(os.path.getsize(path)/1024)
    return f"~ {size_in_kb} KB"