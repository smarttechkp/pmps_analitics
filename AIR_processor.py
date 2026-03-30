"""
AIR_processor.py
    - Class responsible for:
      - loading data from S3
      - feature preparation (with IQR outlier removal)
      - training KMeans-based anomaly detection model per sort group
      - running predictions
      - saving results to SQL
"""

import os
import tempfile
import datetime
from typing import Tuple, Optional

import pandas as pd

from kmeans import PneumaticAnomalyDetector
from base_processor import BaseProcessor


class AirProcessor(BaseProcessor):
    """
    Processor for AIR (pneumatic flow) data.
    Groups data by 'sort', trains or loads a model per group,
    and runs predictions to calculate health scores.
    """

    def __init__(self, s3, sql_processor):
        super().__init__(s3, sql_processor)

        # S3 prefix for AIR data
        self.data_prefix = "AIR"

        # Features used for the model
        self.features = ["flow", "cycle"]

        # Minimum number of samples required for processing
        self.required_samples = 500

        # Number of clusters for KMeans
        self.model_clusters = 500

        # Parameters for converting distance to health score
        self.sensitivity_min = 5
        self.sensitivity_max = 200
        self.warning = 70

    # --------------------------------------------------
    # MAIN PIPELINE
    # --------------------------------------------------

    def process_s3_data(self, device: str, timestamp: datetime.datetime) -> None:
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
            result = self._load_recent_device_df(
                device,
                timestamp,
                min_rows=self.required_samples * 10,
                window_rows=self.required_samples * 10
            )

            if result is None:
                print("No dataframe loaded from S3")
                return

            df_window, newest, oldest = result

            print(f"Dataframe loaded rows={len(df_window)}")
            print(f"time range: {oldest} -> {newest}")
            print(f"columns={list(df_window.columns)}")

            # Required grouping column
            if "sort" not in df_window.columns:
                print("Missing column: sort")
                return

            print(f"Unique sorts: {df_window['sort'].unique()}")

            # Group data by 'sort' and process each group
            for sort, df_group in df_window.groupby("sort"):
                self._process_group(device, sort, df_group, newest, oldest)

        except Exception as e:
            print(f"[{device}] Error: {e}")

    def _process_group(self, device: str, sort, df_group: pd.DataFrame, newest, oldest) -> None:
        """Processes a single sort group: prepares features, trains or predicts."""
        print("\n--------------------------------")
        print(f"Processing group")
        print(f"device={device}")
        print(f"sort={sort}")
        print(f"group rows={len(df_group)}")
        print("--------------------------------")

        # Prepare feature matrix (with IQR outlier removal)
        X = self._prepare_features(df_group)

        if X is None or len(X) < self.required_samples:
            print("Not enough samples after preprocessing")
            return

        print(f"Feature matrix rows={len(X)}")

        # Generate S3 key for model
        model_key = self._model_key(device, sort)
        print(f"Model key={model_key}")

        # Check if model already exists
        has_model = self._model_exists(model_key)
        print(f"Model exists={has_model}")

        if not has_model:
            print("Training new model")
            model_uploaded = self._train_and_upload_model(device, sort, X, model_key)
            if model_uploaded:
                self._predict_and_store(device, sort, df_group, X, model_key, newest, oldest)
        else:
            print("Using existing model for prediction")
            self._predict_and_store(device, sort, df_group, X, model_key, newest, oldest)

    # --------------------------------------------------
    # FEATURE PREPARATION (overrides base: adds IQR outlier removal)
    # --------------------------------------------------

    def _prepare_features(self, df: pd.DataFrame):
        """
        Prepares feature matrix:
        - converts to numeric
        - removes NaNs
        - removes outliers using IQR (Q1-Q3 range)
        """
        print("\nPreparing features")
        print(f"input rows={len(df)}")

        # Convert selected columns to numeric and drop invalid rows
        X = df[self.features].apply(pd.to_numeric, errors="coerce").dropna()

        print(f"rows after numeric conversion={len(X)}")

        if len(X) < 10:
            print("Too few rows after numeric conversion")
            return None

        # Compute quartiles and keep only rows within IQR range
        q1 = X.quantile(0.25)
        q3 = X.quantile(0.75)
        mask = ((X >= q1) & (X <= q3)).all(axis=1)
        X = X[mask]

        print(f"rows after outlier removal={len(X)}")

        return X.reset_index(drop=True)

    # --------------------------------------------------
    # MODEL KEY
    # --------------------------------------------------

    def _model_key(self, device: str, sort: str) -> str:
        """Builds S3 key for model storage."""
        sort_safe = str(sort).replace("/", "_")
        return f"AIR/model/{device}/{sort_safe}.pkl"

    # --------------------------------------------------
    # TRAIN
    # --------------------------------------------------

    def _train_and_upload_model(self, device: str, sort, X: pd.DataFrame, key: str) -> bool:
        """Trains anomaly detection model and uploads it to S3. Returns True on success."""
        print("\nMODEL TRAINING START")
        print(f"device={device}")
        print(f"sort={sort}")
        print(f"samples={len(X)}")
        print(f"clusters={self.model_clusters}")

        try:
            det = PneumaticAnomalyDetector(n_clusters=self.model_clusters)
            det.fit(X, outlier_percent=50)
            print("Model trained")

            with tempfile.NamedTemporaryFile(delete=False, suffix=".pkl") as tmp:
                tmp_path = tmp.name

            try:
                det.save_model(tmp_path)
                self.s3.put_file(self.bucket, tmp_path, key)
                self.sql.upsert_model_status(device, status=1, health=100, sort=sort, config=True, typ="air")
                print("Model uploaded successfully")
                return True
            finally:
                try:
                    os.unlink(tmp_path)
                except Exception:
                    pass

        except Exception as e:
            print(f"Model training failed: {e}")
            return False

    # --------------------------------------------------
    # PREDICT
    # --------------------------------------------------

    def _predict_and_store(self, device: str, sort, df: pd.DataFrame, X: pd.DataFrame, key: str, newest, oldest) -> None:
        """
        Loads model, runs prediction, calculates health score,
        and stores results in SQL.
        """
        print("\nPREDICTION START")
        print(f"device={device}")
        print(f"sort={sort}")
        print(f"rows for prediction={len(X)}")
        print(f"model key={key}")

        try:
            with tempfile.NamedTemporaryFile(delete=False, suffix=".pkl") as tmp:
                tmp_path = tmp.name

            try:
                self.s3.client.download_file(self.bucket, key, tmp_path)
                det = PneumaticAnomalyDetector.load_model(tmp_path)
            finally:
                try:
                    os.unlink(tmp_path)
                except Exception:
                    pass

            print("Model loaded")

            # Run prediction (distance to cluster center)
            dists = det.predict(X)
            avg_dist = float(pd.Series(dists).mean())
            print(f"Average distance={avg_dist}")

            # Convert distance to health score (0-100)
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
