import pandas as pd
import numpy as np
import tempfile
import os, json
import datetime
import re
from typing import List, Tuple, Optional
from kmeans import PneumaticAnomalyDetector

class AirProcessor:
    """
    Class responsible for:
    - loading data from S3
    - feature preparation
    - training KMeans-based anomaly detection model
    - running predictions
    - saving results to SQL
    """

    def __init__(self, s3, sql_processor):
        # S3 client (likely a wrapper around boto3)
        self.s3 = s3

        # SQL handler for saving results
        self.sql = sql_processor

        # S3 bucket name
        self.bucket = "pmps-raw-data"

        # Features used for the model
        self.features = ["flow", "cycle"]

        # Minimum number of samples required for processing
        self.required_samples = 500

        # Number of clusters for KMeans
        self.model_clusters = 500

        # Parameters for converting distance -> health score
        self.sensitivity_min = 5
        self.sensitivity_max = 200
        self.warning = 70


    # --------------------------------------------------
    # MAIN
    # --------------------------------------------------

    def process_s3_data(self, device, timestamp):
        """
        Main pipeline:
        - loads recent data from S3
        - groups by 'sort'
        - trains or loads model per group
        - runs prediction and stores results
        """

        print("\n==============================")
        print("PROCESS START")
        print(f"device={device}")
        print(f"timestamp={timestamp}")
        print("==============================")
        
        try:
            # Load recent data window
            df_window, newest, oldest = self._load_recent_device_df(
                device,
                timestamp,
                min_rows=self.required_samples * 10,
                window_rows=self.required_samples * 10
            )

            if df_window is None:
                print("No dataframe loaded from S3")
                return

            print(f"Dataframe loaded rows={len(df_window)}")
            print(f"time range: {oldest} -> {newest}")
            print(f"columns={list(df_window.columns)}")

            # Required grouping column
            if "sort" not in df_window.columns:
                print("Missing column: sort")
                return

            print(f"Unique sorts: {df_window['sort'].unique()}")

            # Group data by 'sort'
            groups = df_window.groupby("sort")

            for sort, df_group in groups:

                print("\n--------------------------------")
                print(f"Processing group")
                print(f"device={device}")
                print(f"sort={sort}")
                print(f"group rows={len(df_group)}")
                print("--------------------------------")

                # Prepare feature matrix
                X = self._prepare_features(df_group)

                if X is None or len(X) < self.required_samples:
                    print("Not enough samples after preprocessing")
                    continue

                print(f"Feature matrix rows={len(X)}")

                # Generate S3 key for model
                model_key = self._model_key(device, sort)

                print(f"Model key={model_key}")

                # Check if model already exists
                has_model = self._model_exists(model_key)

                print(f"Model exists={has_model}")

                if not has_model:
                    print("Training new model")

                    ok = self._train_and_upload_model(device, None, sort, X, model_key)

                    if ok:
                        self._predict_and_store(device, None, sort, df_group, X, model_key, newest, oldest)
                else:
                    print("Using existing model for prediction")

                    self._predict_and_store(device, None, sort, df_group, X, model_key, newest, oldest)
            
        except Exception as e:
            # Any error for a device, set status=0 (no guarantee of ready model)
            print(f"[{device}] Error: {e}")


    # --------------------------------------------------
    # FEATURES
    # --------------------------------------------------

    def _prepare_features(self, df):
        """
        Prepares feature matrix:
        - converts to numeric
        - removes NaNs
        - removes outliers using IQR (Q1–Q3 range)
        """

        print("\nPreparing features")
        print(f"input rows={len(df)}")

        # Convert selected columns to numeric and drop invalid rows
        X = df[self.features].apply(pd.to_numeric, errors="coerce").dropna()

        print(f"rows after numeric conversion={len(X)}")

        if len(X) < 10:
            print("Too few rows after numeric conversion")
            return None

        # Compute quartiles
        q1 = X.quantile(0.25)
        q3 = X.quantile(0.75)

        print(f"Q1:\n{q1}")
        print(f"Q3:\n{q3}")

        # Keep only rows within IQR range
        mask = ((X >= q1) & (X <= q3)).all(axis=1)
        X = X[mask]

        print(f"rows after outlier removal={len(X)}")

        return X.reset_index(drop=True)

    def _load_recent_device_df(self, device: str, max_datetime: datetime, min_rows: int, window_rows: int):
        """
        Collects Parquet files for a given device up to and including 'max_datetime'.
        Returns (DataFrame, newest_timestamp, oldest_timestamp) or None.
        """

        print("\n========== LOAD RECENT DEVICE DF ==========")
        print(f"device={device}")
        print(f"max_datetime={max_datetime}")
        print(f"min_rows={min_rows}, window_rows={window_rows}")

        keys = self._list_device_files(device)
        print(f"Total keys found={len(keys) if keys else 0}")

        if not keys:
            print("No keys returned from S3")
            return None

        # filter only those with datetime <= max_datetime
        dated_keys = []
        for k in keys:
            dt = self._extract_datetime_from_key(k)
            print(f"Key={k}, extracted_dt={dt}")

            if dt and dt <= max_datetime:
                dated_keys.append((dt, k))

        print(f"Keys after datetime filter={len(dated_keys)}")

        if not dated_keys:
            print(f"[{device}] No files on or before {max_datetime}")
            return None

        # sort ascending
        dated_keys.sort()
        print("Sorted keys (ascending):")
        for dt, k in dated_keys:
            print(f"  {dt} -> {k}")

        total = 0
        parts = []

        # iterate newest -> oldest
        for dt, key in reversed(dated_keys):
            print("\n--- Processing file ---")
            print(f"dt={dt}, key={key}")

            local_path = self._download_parquet(key)

            if not local_path:
                print("Download failed, skipping file")
                continue

            try:
                df = pd.read_parquet(local_path)
                print(f"Loaded parquet rows={len(df)}")
                print(f"Columns={list(df.columns)}")

                parts.append(df)
                total += len(df)

                print(f"Accumulated rows={total}")

                if total >= min_rows:
                    print("Reached minimum required rows, stopping load loop")
                    break

            except Exception as e:
                print(f"Error reading parquet: {e}")

            finally:
                try:
                    os.unlink(local_path)
                    print(f"Deleted temp file {local_path}")
                except Exception as e:
                    print(f"Failed to delete temp file: {e}")

        if not parts:
            print("No dataframes collected")
            return None

        print(f"Number of dataframes collected={len(parts)}")

        try:
            df_all = pd.concat(parts, ignore_index=True)
        except Exception as e:
            print(f"Concat failed: {e}")
            return None

        print(f"Total concatenated rows={len(df_all)}")

        if "timestamp" not in df_all.columns:
            print(f"[{device}] 'timestamp' column missing")
            print(f"Available columns={list(df_all.columns)}")
            return None

        # sort by timestamp
        df_all = df_all.sort_values("timestamp", ascending=True)

        oldest_timestamp = df_all["timestamp"].iloc[0]
        newest_timestamp = df_all["timestamp"].iloc[-1]

        print(f"Oldest timestamp={oldest_timestamp}")
        print(f"Newest timestamp={newest_timestamp}")

        final_df = df_all.tail(window_rows).reset_index(drop=True)

        print(f"Final dataframe rows={len(final_df)}")
        print("========== LOAD END ==========\n")

        return final_df, newest_timestamp, oldest_timestamp

    # --------------------------------------------------
    # MODEL PATH
    # --------------------------------------------------

    def _model_key(self, device: str, sort: str):
        """
        Builds S3 key for model storage
        """

        # Replace problematic characters in 'sort'
        sort = str(sort).replace("/", "_")

        key = f"AIR/model/{device}/{sort}.pkl"

        print(f"Generated model key={key}")

        return key


    # --------------------------------------------------
    # MODEL EXISTS
    # --------------------------------------------------

    def _model_exists(self, key):
        """
        Checks if model file exists in S3
        """

        print(f"Checking if model exists in S3: {key}")

        try:
            self.s3.client.head_object(
                Bucket=self.bucket,
                Key=key
            )

            print("Model exists in S3")
            return True

        except Exception as e:
            print(f"Model not found: {e}")
            return False


    # --------------------------------------------------
    # TRAIN
    # --------------------------------------------------

    def _train_and_upload_model(self, device, station, sort, X, key):
        """
        Trains anomaly detection model and uploads it to S3
        """

        print("\nMODEL TRAINING START")
        print(f"device={device}")
        print(f"sort={sort}")
        print(f"samples={len(X)}")
        print(f"clusters={self.model_clusters}")

        try:
            # Initialize detector
            det = PneumaticAnomalyDetector(
                n_clusters=self.model_clusters
            )

            # Train model (with internal outlier handling)
            det.fit(X, outlier_percent=50)

            print("Model trained")

            # Save model temporarily
            with tempfile.NamedTemporaryFile(delete=False, suffix=".pkl") as tmp:
                tmp_path = tmp.name

            print(f"Temporary model path={tmp_path}")

            det.save_model(tmp_path)

            # Upload model to S3
            print("Uploading model to S3")
            self.s3.put_file(self.bucket, tmp_path, key)

            # Remove local temp file
            os.unlink(tmp_path)

            #Update sql status
            self.sql.upsert_model_status(device, status=1, health=100, sort=sort, config=True, typ="air")
            print("Model uploaded successfully")

            return True

        except Exception as e:
            print(f"Model training failed: {e}")
            return False


    # --------------------------------------------------
    # PREDICT
    # --------------------------------------------------

    def _predict_and_store(self, device, station, sort, df, X, key, newest, oldest):
        """
        Loads model, runs prediction, calculates health score,
        and stores results in SQL
        """

        print("\nPREDICTION START")
        print(f"device={device}")
        print(f"sort={sort}")
        print(f"rows for prediction={len(X)}")
        print(f"model key={key}")

        try:
            # Download model from S3
            with tempfile.NamedTemporaryFile(delete=False, suffix=".pkl") as tmp:
                tmp_path = tmp.name

            print(f"Downloading model from S3 -> {tmp_path}")

            self.s3.client.download_file(self.bucket, key, tmp_path)

            # Load model
            det = PneumaticAnomalyDetector.load_model(tmp_path)

            os.unlink(tmp_path)

            print("Model loaded")

            # Run prediction (distance to cluster center)
            dists = det.predict(X)

            print(f"Prediction distances sample={dists[:5]}")

            # Average anomaly score
            avg_dist = float(pd.Series(dists).mean())

            print(f"Average distance={avg_dist}")

            # Convert distance to health score (0–100)
            if avg_dist <= self.sensitivity_min:
                condition = 100
            elif avg_dist >= self.sensitivity_max:
                condition = 0
            else:
                condition = int(
                    round(
                        100 - (
                            (avg_dist - self.sensitivity_min) /
                            (self.sensitivity_max - self.sensitivity_min)
                        ) * 100
                    )
                )

            print(f"Calculated condition={condition}")

            self.sql.upsert_model_status(device, status=1, health=condition, sort=sort, config=False, typ="air")
            # Median values used as representative metrics
            med = X.median().round(2)

            print("Median values:")
            print(med)

            # Save results to database
            print("Saving results to SQL")

            self.sql.insert_flow_data(
                device,
                sort,
                flow=int(med["flow"]),
                cycle=int(med["cycle"]),
                health=condition
            )

            print("SQL insert done")

        except Exception as e:
            print(f"Prediction failed: {e}")


    # --------------------------------------------------
    # S3 / FILE HANDLING
    # --------------------------------------------------
    def _list_device_files(self, device: str) -> List[str]:
        """
        Returns a sorted list of .parquet keys for a given device.
        Sort works thanks to lexicographic date format 'date=YYYY-MM-DD/HH.parquet'.
        """
        prefix = f"AIR/{device}/"
        keys = self.s3.list(self.bucket, prefix=prefix, json=True) or []
        keys = [k for k in keys if k.endswith(".parquet") and "/date=" in k]
        keys.sort()
        return keys

    def _download_parquet(self, key: str) -> Optional[str]:
        """
        Downloads .parquet file from S3 to a temp file and returns local path.
        Why temp file? pandas.read_parquet() works conveniently with a file path.
        """
        try:
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet")
            tmp.close()
            self.s3.client.download_file(self.bucket, key, tmp.name)
            return tmp.name
        except Exception as e:
            print(f"Download error for {key}: {e}")
            return None
            
    def _extract_datetime_from_key(self, key: str):
        """
        Extracts datetime from S3 key using pattern:
        date=YYYY-MM-DD/HH.parquet
        """

        print(f"Extracting datetime from key={key}")

        match = re.search(r"date=(\d{4}-\d{2}-\d{2})/(\d{2})\.parquet$", key)

        if not match:
            print("Datetime pattern not found")
            return None

        d, h = match.groups()

        try:
            dt = datetime.datetime.fromisoformat(f"{d}T{h}:00:00")
            print(f"Parsed datetime={dt}")
            return dt
        except Exception as e:
            print(f"Datetime parse error: {e}")
            return None


    # ---------------------------
    # SQL helper: ensure device exists
    # ---------------------------
    def _ensure_device_in_sql(self, device: str, isort: str) -> Tuple[int, Optional[datetime.datetime]]:
        """If device missing in status table, add it with status 0. Otherwise returns status and fixed_at."""
        
        cur = self.sql.get_model_status(device, typ="air")

        if cur is None:
            self.sql.upsert_model_status(device, status=0, sort=isort, config=True, typ="air")
            return 0, None

        status, fixed_at = cur
        return status, fixed_at