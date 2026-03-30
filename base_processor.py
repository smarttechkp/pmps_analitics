"""
base_processor.py
    - Abstract base class containing shared functionality
      used by both ACT and AIR processors.
    - Consolidates common S3 operations, datetime extraction,
      file loading logic, and feature preparation.
"""

import os
import re
import tempfile
import datetime
import pandas as pd
from typing import List, Optional


class BaseProcessor:
    """
    Base class providing common S3 and data processing utilities
    shared between ACT and AIR processors.

    Subclasses must set:
        self.data_prefix  - S3 prefix for the processor type (e.g. 'ACT' or 'AIR')
        self.features     - list of feature column names used in the model
    """

    def __init__(self, s3, sql_processor):
        # Injected dependencies
        self.s3 = s3
        self.sql = sql_processor

        # S3 bucket name
        self.bucket = "pmps-raw-data"

    # ---------------------------
    # S3: Download parquet file
    # ---------------------------
    def _download_parquet(self, key: str) -> Optional[str]:
        """
        Downloads .parquet file from S3 to a temp file and returns local path.
        Returns None on failure.
        """
        try:
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet")
            tmp.close()
            self.s3.client.download_file(self.bucket, key, tmp.name)
            return tmp.name
        except Exception as e:
            print(f"Download error for {key}: {e}")
            return None

    # ---------------------------
    # S3: Extract datetime from key
    # ---------------------------
    @staticmethod
    def _extract_datetime_from_key(key: str) -> Optional[datetime.datetime]:
        """
        Extracts datetime from S3 key of format '.../date=YYYY-MM-DD/HH.parquet'
        Returns None if the key does not match the expected pattern.
        """
        match = re.search(r"date=(\d{4}-\d{2}-\d{2})/(\d{2})\.parquet$", key)
        if not match:
            return None
        try:
            d_str, h_str = match.groups()
            return datetime.datetime.fromisoformat(f"{d_str}T{h_str}:00:00")
        except ValueError:
            return None

    # ---------------------------
    # S3: List device files
    # ---------------------------
    def _list_device_files(self, device: str) -> List[str]:
        """
        Returns a sorted list of .parquet keys for a given device.
        Uses self.data_prefix (e.g. 'ACT' or 'AIR') to build the S3 prefix.
        Sort works thanks to lexicographic date format 'date=YYYY-MM-DD/HH.parquet'.
        """
        prefix = f"{self.data_prefix}/{device}/"
        keys = self.s3.list(self.bucket, prefix=prefix, json=True) or []
        keys = [k for k in keys if k.endswith(".parquet") and "/date=" in k]
        keys.sort()
        return keys

    # ---------------------------
    # S3: Load recent device DataFrame
    # ---------------------------
    def _load_recent_device_df(self, device: str, max_datetime: datetime.datetime, min_rows: int, window_rows: int):
        """
        Collects Parquet files for a given device up to and including 'max_datetime'.
        Ignores newer files. Stops after collecting at least 'min_rows' rows.
        Returns last 'window_rows' rows sorted by timestamp ascending,
        alongside the newest and oldest timestamp from all collected data.

        Returns: (pd.DataFrame, newest_timestamp, oldest_timestamp) or None.
        """
        keys = self._list_device_files(device)
        if not keys:
            return None

        # Filter only those with datetime <= max_datetime
        dated_keys = []
        for k in keys:
            dt = self._extract_datetime_from_key(k)
            if dt and dt <= max_datetime:
                dated_keys.append((dt, k))

        if not dated_keys:
            print(f"[{device}] No files on or before {max_datetime}.")
            return None

        # Sort ascending by datetime so we can iterate newest-to-oldest
        dated_keys.sort()
        total = 0
        parts: List[pd.DataFrame] = []

        for _, key in reversed(dated_keys):  # reversed = newest first within allowed time range
            local_path = self._download_parquet(key)
            if not local_path:
                continue
            try:
                df = pd.read_parquet(local_path)
                parts.append(df)
                total += len(df)
                if total >= min_rows:
                    break
            finally:
                try:
                    os.unlink(local_path)
                except Exception:
                    pass

        if not parts:
            return None

        df_all = pd.concat(parts, ignore_index=True)
        if "timestamp" not in df_all.columns:
            print(f"[{device}] 'timestamp' column missing.")
            return None

        # Sort by timestamp and extract range
        df_all = df_all.sort_values("timestamp", ascending=True)
        oldest_timestamp = df_all["timestamp"].iloc[0]
        newest_timestamp = df_all["timestamp"].iloc[-1]

        final_df = df_all.tail(window_rows).reset_index(drop=True)
        return final_df, newest_timestamp, oldest_timestamp

    # ---------------------------
    # S3: Check if model exists
    # ---------------------------
    def _model_exists(self, model_key: str) -> bool:
        """Checks if model file exists in S3."""
        try:
            self.s3.client.head_object(Bucket=self.bucket, Key=model_key)
            return True
        except Exception:
            keys = self.s3.list(self.bucket, prefix=model_key, json=True) or []
            return any(k == model_key for k in keys)

    # ---------------------------
    # Feature preparation
    # ---------------------------
    def _prepare_features(self, df: pd.DataFrame) -> Optional[pd.DataFrame]:
        """Selects feature columns, converts to numeric, drops NaNs."""
        missing = [f for f in self.features if f not in df.columns]
        if missing:
            print(f"Missing feature columns: {missing}")
            return None
        X = df[self.features].apply(pd.to_numeric, errors="coerce").dropna().reset_index(drop=True)
        return X if not X.empty else None
