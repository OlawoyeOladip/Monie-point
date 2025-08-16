# # import os
# # import sys
# # import pandas as pd
# # from pathlib import Path

# # # Add repo root to sys.path
# # sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# # from data_ingestions import parser


# # def test_parse_csv():
# #     # Find repo root (tests/ is one level under repo root)
# #     repo_root = Path(__file__).resolve().parent.parent
# #     file_path = repo_root / "monie_point_data" / "synthetic_dirty_transaction_logs.csv"

# #     assert file_path.exists(), f"Test CSV file not found at {file_path}"

# #     # Parse CSV with your parser
# #     df = parser.parse_csv(file_path)

# #     # Basic checks
# #     assert not df.empty, "Parsed DataFrame is empty"

# #     # Required columns
# #     expected_columns = {
# #         "timestamp",
# #         "user_id",
# #         "transaction_type",
# #         "amount",
# #         "currency",
# #         "device",
# #         "latitude",
# #         "longitude",
# #     }
# #     assert expected_columns.issubset(df.columns), f"Missing columns: {expected_columns - set(df.columns)}"

# #     # Data type checks
# #     assert pd.api.types.is_integer_dtype(df["user_id"]), "user_id should be integer"
# #     assert pd.api.types.is_numeric_dtype(df["amount"]), "amount should be numeric"

# #     # No nulls in key columns
# #     for col in ["timestamp", "latitude", "longitude"]:
# #         assert df[col].notnull().all(), f"Column '{col}' contains null values"


# import os
# import sys
# import pandas as pd
# from pathlib import Path

# # Add repo root to sys.path so imports work
# sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# from data_ingestions import parser


# def test_parse_csv():
#     """
#     Unit test for parser.parse_csv function.

#     Validates:
#     - CSV exists and loads correctly
#     - Data is cleaned (no nulls, no MALFORMED_LOG rows)
#     - Expected columns are present
#     - Data types are correct
#     """

#     # Locate CSV file
#     repo_root = Path(__file__).resolve().parent.parent
#     file_path = repo_root / "monie_point_data" / "synthetic_dirty_transaction_logs.csv"

#     assert file_path.exists(), f"Test CSV file not found at {file_path}"

#     # Parse CSV
#     df = parser.parse_csv(file_path)

#     # Check DataFrame is not empty
#     assert not df.empty, "Parsed DataFrame is empty"

#     # Ensure no nulls in raw_log before parsing
#     assert df.notnull().all().all(), "DataFrame contains null values after parsing"

#     # Ensure no 'MALFORMED_LOG' remains
#     for col in df.columns:
#         assert not df[col].astype(str).str.contains("MALFORMED_LOG").any(), \
#             f"MALFORMED_LOG still present in column {col}"

#     # Required parsed columns
#     expected_columns = {
#         "timestamp",
#         "user_id",
#         "transaction_type",
#         "amount",
#         "currency",
#         "device",
#         "latitude",
#         "longitude",
#     }
#     assert expected_columns.issubset(df.columns), \
#         f"Missing columns: {expected_columns - set(df.columns)}"

#     # Data type checks
#     assert pd.api.types.is_integer_dtype(df["user_id"]), "user_id should be integer"
#     assert pd.api.types.is_numeric_dtype(df["amount"]), "amount should be numeric"

#     # No nulls in key parsed columns
#     for col in ["timestamp", "latitude", "longitude"]:
#         assert df[col].notnull().all(), f"Column '{col}' contains null values"


import os
import sys
from pathlib import Path
import pandas as pd

# Add repo root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from anomaly_detection.components import data_validation

def test_parse_csv():
    repo_root = Path(__file__).resolve().parent.parent
    file_path = repo_root / "monie_point_data" / "synthetic_dirty_transaction_logs.csv"

    assert file_path.exists(), f"CSV file not found at {file_path}"

    df = data_validation.parse_csv(file_path)

    # Ensure dataframe is not empty
    assert not df.empty, "Parsed DataFrame is empty"

    # Ensure 'raw_log' column exists
    assert "raw_log" in df.columns, "'raw_log' column missing"

    # Ensure no null values
    assert df["raw_log"].notnull().all(), "Null values remain in 'raw_log'"
    
    assert len(df.columns) == 1, "Unnecessary columns must have found way in"

    # Ensure no 'MALFORMED_LOG' rows
    assert not (df["raw_log"] == "MALFORMED_LOG").any(), "'MALFORMED_LOG' rows remain"
    

def test_schema_01():
    ...
