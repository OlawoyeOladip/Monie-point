from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional
import pandas as pd 
import re
from pathlib import Path


class BaseTransactionParser(ABC):
    """Abstract base class for transaction log parsers."""

    @abstractmethod
    def clean_transaction_logs(self, csv_file_path=None, raw_data=None) -> pd.DataFrame:
        """Clean and parse transaction logs. raw_data is for testing with raw text."""
        pass

    @abstractmethod
    def parse_transaction_line(self, line: str, row_id: int) -> Optional[dict]:
        """Parse a single transaction log line into structured data."""
        pass

    @staticmethod
    def parse_datetime(date_str: str):
        """Parse datetime string in supported formats."""
        if not date_str or date_str.lower() == 'none':
            return None

        try:
            if re.match(r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$', date_str):
                return datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
            if re.match(r'^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$', date_str):
                return datetime.strptime(date_str, '%d/%m/%Y %H:%M:%S')
        except ValueError:
            print(f"Could not parse datetime: {date_str}")

        return None

    @staticmethod
    def extract_amount(amount_str: str):
        """Extract amount and currency from an amount string."""
        if not amount_str:
            return {'amount': None, 'currency': 'GBP'}  

        # Keep only valid number and currency characters
        amount_clean = re.sub(r'[^\d.,€£$]', '', str(amount_str))

        number_match = re.search(r'([\d,]+\.?\d*)', amount_clean)
        if number_match:
            amount = float(number_match.group(1).replace(',', ''))
            currency = 'GBP'  # default
            if '€' in amount_str or 'â‚¬' in amount_str:
                currency = 'EUR'
            elif '£' in amount_str or 'Â£' in amount_str:
                currency = 'GBP'
            elif '$' in amount_str:
                currency = 'USD'
            return {'amount': amount, 'currency': currency}

        return {'amount': None, 'currency': 'GBP'}

    @staticmethod
    def get_currency_code(symbol: Optional[str]):
        """Convert currency symbol or ISO code to standard currency code."""
        mapping = {
            '€': 'EUR', 'â‚¬': 'EUR', 'EUR': 'EUR',
            '£': 'GBP', 'Â£': 'GBP', 'GBP': 'GBP',
            '$': 'USD', 'USD': 'USD'
        }
        # Default to GBP if not found or missing
        if not symbol:
            return 'GBP'
        return mapping.get(symbol.strip(), 'GBP')

    @staticmethod
    def clean_field(field: str):
        """Clean and standardize field values, including removing known bad unicode."""
        if not field or field.lower() in ['none', 'null', '']:
            return None

        replacements = {
            "â‚¬": "€",
            "Â£": "£",
            "Â": "",
            "\u201a": "",  # weird comma-like character
        }
        for bad, good in replacements.items():
            field = field.replace(bad, good)

        # Collapse multiple spaces and trim
        return re.sub(r"\s+", " ", field).strip()


class TransactionCleaner(BaseTransactionParser):
    """
    Concrete transaction parser with multiple log format patterns.
    
    This class handles 9 different transaction log formats commonly found in 
    financial systems, including various date formats, currency symbols, and 
    delimiters. 
    """
    def clean_transaction_logs(self, csv_file_path=None, raw_data=None) -> pd.DataFrame:
        """
        Clean and parse transaction logs from file or raw data.
        
        Args:
            csv_file_path: Path to file containing transaction logs
            raw_data: Raw string data for testing purposes
            
        Returns:
            pandas.DataFrame with structured transaction data
            
        Raises:
            ValueError: If neither csv_file_path nor raw_data is provided
        """
        if csv_file_path:
            raw_content = Path(csv_file_path).read_text(encoding='utf-8')
        elif raw_data:
            raw_content = raw_data
        else:
            raise ValueError("Either csv_file_path or raw_data must be provided")

        # Filter valid lines - exclude malformed logs and headers
        lines = raw_content.split('\n')
        valid_lines = [
            line.strip()
            for line in lines
            if line.strip()
            and line.strip() not in ['""', 'MALFORMED_LOG', 'raw_log']
            and not line.strip().startswith('raw_log')
        ]

        cleaned_records = []
        for idx, line in enumerate(valid_lines):
            record = self.parse_transaction_line(line, idx + 1)
            if record:
                cleaned_records.append(record)

        df = pd.DataFrame(cleaned_records)

        # Post-processing: ensure proper data types and handle missing values
        if not df.empty:
            df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce')
            df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
            df['row_id'] = df['row_id'].astype(int)
            df['location'] = df['location'].fillna('Unknown')
            df['device'] = df['device'].fillna('Unknown')
            df['currency'] = df['currency'].fillna('USD')

        return df

    def parse_transaction_line(self, line: str, row_id: int) -> Optional[dict]:
        """
        Parse a single transaction log line into structured data.
        
        This method attempts to match the input line against 9 different patterns
        in order of specificity, returning structured data if a match is found.
        
        Args:
            line: Raw transaction log line
            row_id: Row identifier for tracking
            
        Returns:
            Dictionary with parsed transaction data or None if no pattern matches
        """
        if not line or line in ['""', 'MALFORMED_LOG']:
            return None

        # Initialize record structure
        record = {
            'row_id': row_id,
            'original_log': line,
            'datetime': None,
            'user_id': None,
            'transaction_type': None,
            'amount': None,
            'currency': None,
            'location': None,
            'device': None
        }

        try:
            # Pattern 1: Double colon format
            # Example: 2023-05-14 14:05:31::user123::top-up::500::ATM Location::Device
            pattern1 = r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})::(\w+)::([\w-]+)::([\d,.]+)::([^:]+)::(.+)$'
            match = re.match(pattern1, line)
            if match:
                record.update({
                    'datetime': self.parse_datetime(match.group(1)),
                    'user_id': match.group(2),
                    'transaction_type': match.group(3),
                    'amount': self.extract_amount(match.group(4))['amount'],
                    'currency': self.extract_amount(match.group(4))['currency'],
                    'location': self.clean_field(match.group(5)),
                    'device': self.clean_field(match.group(6))
                })
                return record

            # Pattern 2: Pipe separated format
            # Example: usr:user123|top-up|£500|Location|2023-05-14 14:05:31|Device
            pattern2 = r'^usr:(\w+)\|([\w-]+)\|([€£$]?[\d,.]+)\|([^|]+)\|(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\|(.+)$'
            match = re.match(pattern2, line)
            if match:
                amount_info = self.extract_amount(match.group(3))
                record.update({
                    'user_id': match.group(1),
                    'transaction_type': match.group(2),
                    'amount': amount_info['amount'],
                    'currency': amount_info['currency'],
                    'location': self.clean_field(match.group(4)),
                    'datetime': self.parse_datetime(match.group(5)),
                    'device': self.clean_field(match.group(6))
                })
                return record

            # Pattern 3: Arrow format with brackets
            # Example: 2023-05-14 14:05:31 >> [user123] did top-up - amt=£500 - Location // dev:Device
            pattern3 = r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) >> \[(\w+)\] did ([\w-]+) - amt=([€£$]?[\d,.]+) - ([^/]+) // dev:(.+)$'
            match = re.match(pattern3, line)
            if match:
                amount_info = self.extract_amount(match.group(4))
                record.update({
                    'datetime': self.parse_datetime(match.group(1)),
                    'user_id': match.group(2),
                    'transaction_type': match.group(3).replace('-', '_'),
                    'amount': amount_info['amount'],
                    'currency': amount_info['currency'],
                    'location': self.clean_field(match.group(5)),
                    'device': self.clean_field(match.group(6))
                })
                return record

            # Pattern 4: Pipe format with user/txn/device labels
            # Example: 2023-05-14 14:05:31 | user: user123 | txn: top-up of £500 from Location | device: Device
            pattern4 = r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) \| user: (\w+) \| txn: ([\w-]+) of ([€£$]?[\d,.]+) from ([^|]+) \| device: (.+)$'
            match = re.match(pattern4, line)
            if match:
                amount_info = self.extract_amount(match.group(4))
                record.update({
                    'datetime': self.parse_datetime(match.group(1)),
                    'user_id': match.group(2),
                    'transaction_type': match.group(3),
                    'amount': amount_info['amount'],
                    'currency': amount_info['currency'],
                    'location': self.clean_field(match.group(5)),
                    'device': self.clean_field(match.group(6))
                })
                return record

            # Pattern 5: Dash separated with user/action/ATM/device
            # Example: 2023-05-14 14:05:31 - user=user123 - action=top-up £500 - ATM: Location - device=Device
            pattern5 = r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) - user=(\w+) - action=([\w-]+) ([€£$]?[\d,.]+) - ATM: ([^-]+) - device=(.+)$'
            match = re.match(pattern5, line)
            if match:
                amount_info = self.extract_amount(match.group(4))
                record.update({
                    'datetime': self.parse_datetime(match.group(1)),
                    'user_id': match.group(2),
                    'transaction_type': match.group(3),
                    'amount': amount_info['amount'],
                    'currency': amount_info['currency'],
                    'location': self.clean_field(match.group(5)),
                    'device': self.clean_field(match.group(6))
                })
                return record

            # Pattern 6: Triple colon with asterisks (DD/MM/YYYY format)
            # Example: 14/05/2023 14:05:31 ::: user123 *** TOP-UP ::: amt:£500 @ Location <Device>
            pattern6 = r'^(\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}) ::: (\w+) \*\*\* ([\w-]+) ::: amt:([€£$]?[\d,.]+) @ ([^<]+) <([^>]+)>$'
            match = re.match(pattern6, line)
            if match:
                amount_info = self.extract_amount(match.group(4))
                record.update({
                    'datetime': self.parse_datetime(match.group(1)),
                    'user_id': match.group(2),
                    'transaction_type': match.group(3).lower(),
                    'amount': amount_info['amount'],
                    'currency': amount_info['currency'],
                    'location': self.clean_field(match.group(5)),
                    'device': self.clean_field(match.group(6))
                })
                return record

            # Pattern 7: Simple space-separated format
            # Example: user123 2023-05-14 14:05:31 top-up 500 Location Device
            pattern7 = r'^(\w+) (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) ([\w-]+) ([\d,.]+) (\S+) (.+)$'
            match = re.match(pattern7, line)
            if match:
                record.update({
                    'user_id': match.group(1),
                    'datetime': self.parse_datetime(match.group(2)),
                    'transaction_type': match.group(3),
                    'amount': float(match.group(4).replace(',', '')),
                    'currency': 'GBP',  # Default for this pattern
                    'location': self.clean_field(match.group(5)),
                    'device': self.clean_field(match.group(6))
                })
                return record

            # Pattern 8: Alternative triple colon format (DD/MM/YYYY)
            # Example: 14/05/2023 14:05:31 ::: user123 *** TOP-UP ::: amt:500£ @ Location <Device>
            pattern8 = r'^(\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}) ::: (\w+) \*\*\* ([\w-]+) ::: amt:([\d,.]+)([€£$]) @ ([^<]+) <([^>]+)>$'
            match = re.match(pattern8, line)
            if match:
                amount = float(match.group(4).replace(',', ''))
                currency_symbol = match.group(5)
                currency = self.get_currency_code(currency_symbol)
                record.update({
                    'datetime': self.parse_datetime(match.group(1)),
                    'user_id': match.group(2),
                    'transaction_type': match.group(3).lower(),
                    'amount': amount,
                    'currency': currency,
                    'location': self.clean_field(match.group(6)),
                    'device': self.clean_field(match.group(7))
                })
                return record

            # Pattern 9: Triple colon format with unicode currency (DD/MM/YYYY)
            # Example: 04/07/2025 00:41:51 ::: user1044 *** REFUND ::: amt:3491.94â‚¬ @ Manchester <Huawei P30>
            # This pattern specifically handles malformed unicode currency symbols
            pattern9 = r'^(\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}) ::: (\w+) \*\*\* ([\w-]+) ::: amt:([\d,.]+)(â‚¬|€|£|Â£|\$) @ ([^<]+) <([^>]+)>$'
            match = re.match(pattern9, line)
            if match:
                amount = float(match.group(4).replace(',', ''))
                currency_symbol = match.group(5)
                # Use the clean_field method to handle unicode issues first
                cleaned_currency = self.clean_field(currency_symbol)
                currency = self.get_currency_code(cleaned_currency)
                record.update({
                    'datetime': self.parse_datetime(match.group(1)),
                    'user_id': match.group(2),
                    'transaction_type': match.group(3).lower(),
                    'amount': amount,
                    'currency': currency,
                    'location': self.clean_field(match.group(6)),
                    'device': self.clean_field(match.group(7))
                })
                return record

        except Exception as e:
            print(f"Error parsing line {row_id}: {line[:50]}... - {str(e)}")
            return None

        # No pattern matched - this will help debug missing patterns
        print(f"No pattern matched for line {row_id}: {line[:100]}...")
        return None