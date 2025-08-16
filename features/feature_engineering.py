import pandas as pd
import numpy as np
from abc import ABC, abstractmethod
import logging


class FeatureEngineer:
    def __init__(self, df: pd.DataFrame):
        """Initialize with a dataset for batch feature engineering.
        
        A class to engineer features for stream or batch samples
        Args:
        df: pd.DataFrame
        
        Example
        >> df = FeatureEngineer(df).engineer_batch() # Engineer features if the data is the whole Dataframe(or batch of data)
        >> transaction = {"datetime": "2025-08-14 10:00:00", "amount": 150,
                                "transaction_type": "purchase", "currency": "USD",
                                "location": "NY", "device": "mobile"}
        >> df = FeatureEngineer(df).engineer_single(user_id="user1000", transaction=transaction) # Engineer feature even if its a single sample
        """
        self.df = df.copy()
        self.df["datetime"] = pd.to_datetime(self.df["datetime"])

    def _basic_time_features(self, df):
        """Generate time-based features."""
        df = df.copy()  # 
        df["day_of_week"] = df["datetime"].dt.dayofweek
        df["hour_of_day"] = df["datetime"].dt.hour
        df["month"] = df["datetime"].dt.month
        df["quarter"] = df["datetime"].dt.quarter
        df["is_weekend"] = df["day_of_week"] >= 5
        df["day_of_month"] = df["datetime"].dt.day
        df["is_business_hours"] = df["hour_of_day"].between(9, 17)
        return df

    def _user_behavior_features(self, df):
        """Generate rolling and user-specific behavior features."""
        df = df.copy() 
        df = df.sort_values(["user_id", "datetime"]).reset_index(drop=True)
        
        # Set datetime as index temporarily for rolling calculations
        original_index = df.index
        df = df.set_index('datetime')
        
        # Transaction count last 7 days
        df['transaction_count_last_7_days'] = (
            df.groupby('user_id')['amount']
            .rolling('7D', closed='right')
            .count()
            .reset_index(level=0, drop=True)
        )
        
        # Average transaction amount last 10 days        
        df["average_transaction_amount_last_10_days"] = (
            df.groupby('user_id')['amount']
            .rolling('10D', closed='right')
            .mean()
            .reset_index(level=0, drop=True)
        )
        
        # Reset index to bring datetime back as a column
        df = df.reset_index()
        df.index = original_index

        # Days since last transaction
        df['days_since_last_transaction'] = (
            df.groupby('user_id')['datetime'].diff().dt.days
        ).fillna(0)

        df["location"] = df["location"].replace("Unknown", np.nan)
        df["device"] = df["device"].replace("Unknown", np.nan)
        df["location"] = df.groupby("user_id")["location"].ffill()
        df["device"] = df.groupby("user_id")["device"].ffill()
        
        # Unique locations used so far
        df["unique_locations_used"] = (
            df.groupby("user_id")["location"]
            .transform(lambda x: [len(set(x.iloc[:i+1])) for i in range(len(x))])
        )
        
        # For each user, find the first occurrence of each device
        df["new_device"] = (
            df.groupby("user_id")["device"]
            .transform(lambda x: ~x.duplicated())
            .astype(int)
        )

        return df

    def _statistical_features(self, df):
        """Generate statistical anomaly-related features."""
        df = df.copy()  # Prevent modifying original
        df["amount_z_score_user"] = df.groupby("user_id")["amount"].transform(
            lambda x: (x - x.mean()) / (x.std() + 1e-9)
        )
        df["hours_since_last_transaction_user"] = df.groupby("user_id")["datetime"].diff().dt.total_seconds().fillna(0) / 3600
        df["transaction_count_today_user"] = df.groupby(["user_id", df["datetime"].dt.date])["amount"].transform("count")
        df["amount_percentile_user"] = df.groupby("user_id")["amount"].transform(
            lambda x: x.rank(pct=True)
        )
        return df

    def engineer_batch(self):
        """Process the whole DataFrame."""
        df = self._basic_time_features(self.df)
        df = self._user_behavior_features(df)
        df = self._statistical_features(df)
        return df

    def engineer_single(self, user_id: str, transaction: dict):
        """
        Process a single transaction for a given user.
        transaction: dict like 
        
        {"datetime": "2025-08-14 10:00:00", "amount": 150,
                                "transaction_type": "purchase", "currency": "USD",
                                "location": "NY", "device": "mobile"}
        """
        transaction_df = pd.DataFrame([transaction])
        transaction_df["datetime"] = pd.to_datetime(transaction_df["datetime"])
        transaction_df["user_id"] = user_id  

        # Check if user exists and meets activity threshold
        user_data = self.df[self.df["user_id"] == user_id]
        if len(user_data) >= 10 and (user_data["datetime"].max() - user_data["datetime"].min()).days >= 5:
            # Append the new transaction for feature calculation
            combined = pd.concat([user_data, transaction_df], ignore_index=True)
            combined = self._basic_time_features(combined)
            combined = self._user_behavior_features(combined)
            combined = self._statistical_features(combined)

            # Return only the last row (the new transaction features)
            return combined.iloc[[-1]].reset_index(drop=True)
        else:
            # Return only basic time + raw features (rule-based)
            transaction_df = self._basic_time_features(transaction_df)
            return transaction_df.reset_index(drop=True)