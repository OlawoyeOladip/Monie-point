# Transaction Log Parser

## Overview
This Python package provides a robust transaction log parser that can handle multiple log formats commonly found in financial systems. The parser is designed to extract structured data from semi-structured transaction logs with various formats, date patterns, and currency representations.

## Key Features
- **Multiple Format Support**: Handles 9 different transaction log formats with varying delimiters and structures.
- **Robust Parsing**: Cleans and standardizes messy input data including malformed Unicode characters.
- **Flexible Input**: Accepts both file paths and raw string data for processing.
- **Comprehensive Field Extraction**: Parses all key transaction fields including:
  - Timestamps (supports multiple date formats)
  - User IDs
  - Transaction types
  - Amounts and currencies (supports €, £, $ symbols)
  - Location information
  - Device information


#### Usage
```python
from transaction_parser import TransactionCleaner

# Initialize the cleaner
cleaner = TransactionCleaner()

# Process a log file
df = cleaner.clean_transaction_logs(csv_file_path="transactions.csv")

# Or process raw text directly
raw_data = """2023-05-14 14:05:31::user123::top-up::500::ATM Location::Device
usr:user456|payment|£250|Store|2023-05-14 15:30:00|POS"""
df = cleaner.clean_transaction_logs(raw_data=raw_data)
```

##### Output Format
The parser returns a pandas DataFrame with the following columns:
- `row_id`: Unique identifier for each parsed row
- `original_log`: The original log line
- `datetime`: Parsed datetime object
- `user_id`: Extracted user identifier
- `transaction_type`: Type of transaction (e.g., "top-up", "payment")
- `amount`: Numeric amount
- `currency`: 3-letter currency code (EUR, GBP, USD)
- `location`: Transaction location
- `device`: Device used for transaction

### Supported Log Formats
The parser recognizes the following patterns (examples shown):

1. Double colon format
`2023-05-14 14:05:31::user123::top-up::500::ATM Location::Device`

2. Pipe separated format
`usr:user123|top-up|£500|Location|2023-05-14 14:05:31|Device`

3. Arrow format with brackets
`2023-05-14 14:05:31 >> [user123] did top-up - amt=£500 - Location // dev:Device`

4. Pipe format with labels
`2023-05-14 14:05:31 | user: user123 | txn: top-up of £500 from Location | device: Device`

5. Dash separated with key=value
`2023-05-14 14:05:31 - user=user123 - action=top-up £500 - ATM: Location - device=Device`

6. Triple colon with asterisks (DD/MM/YYYY)
`14/05/2023 14:05:31 ::: user123 *** TOP-UP ::: amt:£500 @ Location <Device>`

7. Simple space-separated
`user123 2023-05-14 14:05:31 top-up 500 Location Device`

8. Alternative triple colon (currency after amount)
`14/05/2023 14:05:31 ::: user123 *** TOP-UP ::: amt:500£ @ Location <Device>`

9. Triple colon with Unicode currencies
`04/07/2025 00:41:51 ::: user1044 *** REFUND ::: amt:3491.94â‚¬ @ Manchester <Huawei P30>`

## Data Cleaning Features
### Datetime Parsing
- Supports both `YYYY-MM-DD` and `DD/MM/YYYY` formats.

### Currency Handling
- Normalizes `€` (Euro), `£` (Pound), and `$` (Dollar) symbols.
- Handles common Unicode issues (e.g., `"â‚¬"` → `"€"`).
- Defaults to GBP when currency isn't specified.

### Text Cleaning
- Removes extraneous whitespace.
- Replaces `"None"`, `"null"`, and empty strings with actual nulls.
- Fixes common Unicode artifacts.

## Error Handling
The parser includes comprehensive error handling:
- Skips malformed lines (logged to console).
- Provides detailed warnings when patterns don't match.
- Maintains original log lines for debugging.
- Uses pandas' built-in error handling for type conversion.

## Extensibility
The parser is built on an abstract base class (`BaseTransactionParser`) that can be extended to support additional formats.

To add support for new formats:
1. Create a subclass of `BaseTransactionParser`.
2. Implement the `parse_transaction_line` method with your new patterns.
3. Optionally override helper methods if you need different cleaning logic.

## Performance
- Optimized for medium-sized log files (up to hundreds of thousands of lines).
- For very large files, consider processing in chunks.

## Testing
The parser includes extensive pattern matching tests. To verify new patterns:
```python
test_cases = [
    ("14/05/2023 14:05:31 ::: user123 *** TOP-UP ::: amt:£500 @ Location <Device>", 
     {'user_id': 'user123', 'amount': 500, 'currency': 'GBP'}),
    # Add more test cases
]

cleaner = TransactionCleaner()
for test_input, expected in test_cases:
    result = cleaner.parse_transaction_line(test_input, 0)
    assert result['user_id'] == expected['user_id']
    # Additional assertions
