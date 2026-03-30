"""
ACT_processor.py
    - Class responsible for the analytical part of ACT (pneumatic actuator) data.
    - Loads the last N records for a given device from S3, prepares features,
      then decides whether to train a new model or run a prediction.

Processing rules:
    - Training ONLY when we have >= required_samples valid samples.
    - Status=0 means no ready model; Status=1 only after a SUCCESSFUL model
      upload to S3.
    - Prediction only when model exists in S3.

S3 access exclusively through S3Processor.
"""

import os
import re
import json
import tempfile
import datetime
from typing import List, Tuple, Optional

import pandas as pd
from kafka import KafkaProducer

from kmeans import PneumaticAnomalyDetector
from base_processor import BaseProcessor


class ActProcessor(BaseProcessor):
    def __init__(self, s3, sql_processor, model_clusters: int = 1, required_samples: int = 100) -> None:
        super().__init__(s3, sql_processor)

        # S3 prefix for ACT data
        self.data_prefix = "ACT"

        # Feature columns used in the K-Means model
        self.features = ["yr", "vr", "yv", "rv"]

        # Model parameters
        self.model_clusters = int(model_clusters)
        self.sensitivity_min = 10
        self.sensitivity_max = 200
        self.warning = 70

        # Minimum number of samples required for training
        self.required_samples = int(required_samples)

    def process_s3_data(self, device: str, _timestamp: datetime.datetime) -> None:
        """
        Main procedure:
        - collects and cleans data,
        - decides whether to train or predict based on model availability,
        - stores the results.
        """
        try:
            # 1) Load last 'required_samples' records (at least)
            result = self._load_recent_device_df(device, _timestamp, min_rows=self.required_samples, window_rows=self.required_samples)
            if result is None:
                print(f"[{device}] Less than {self.required_samples} records")
                return
            df_window, newest_timestamp, oldest_timestamp = result

            # 2) Prepare features and check if we still have >= required_samples after cleaning
            X = self._prepare_features(df_window)
            if X is None or len(X) < self.required_samples:
                print(f"[{device}] Less than {self.required_samples} valid samples after cleaning")
                return

            # 3) Check if model exists in S3
            model_key = self._model_key(device)
            has_model = self._model_exists(model_key)
            status, fixed_at = self._ensure_device_in_sql(device)

            # 4) Decision: train or predict
            if (not has_model) or (status == 0):
                if fixed_at is None or oldest_timestamp > (fixed_at + datetime.timedelta(minutes=60)):
                    print(f"[{device}] Training has_model={has_model}, status={status}")
                    if self._train_and_upload_model(device, X, model_key):
                        # Set status=1 only after successful upload to S3
                        self._predict_and_store(device, df_window, X, model_key, _timestamp, newest_timestamp, oldest_timestamp)
                    else:
                        print(f"[{device}] Model upload failed. Status remains 0.")
                else:
                    print(f"[{device}] Fixed recently. Not enough new data yet.")
            else:
                # Model exists, perform prediction
                print(f"[{device}] Prediction (model OK, status=1).")
                self._predict_and_store(device, df_window, X, model_key, _timestamp, newest_timestamp, oldest_timestamp)

        except Exception as e:
            print(f"[{device}] Error: {e}")

    # ---------------------------
    # S3 Section: list devices - for tests/utilities
    # ---------------------------
    def _list_devices(self) -> List[str]:
        """
        Finds unique devices 'dn=*' under prefix 'ACT/' based on path structure.
        Assumes keys like: ACT/dn=<device>/date=YYYY-MM-DD/HH.parquet
        """
        keys = self.s3.list(self.bucket, prefix="ACT/", json=True) or []
        devices = set()
        pat = re.compile(r"^ACT/(dn=[^/]+)/date=\d{4}-\d{2}-\d{2}/")
        for k in keys:
            m = pat.match(k)
            if m:
                devices.add(m.group(1))
        return sorted(devices)

    # ---------------------------
    # Model handling: key, training, prediction
    # ---------------------------
    def _model_key(self, device: str) -> str:
        """Builds model path in S3, e.g., ACT/model/dn=R1.pkl"""
        return f"ACT/model/{device}.pkl"

    def _train_and_upload_model(self, device: str, X: pd.DataFrame, model_key: str) -> bool:
        """Trains K-Means model and uploads to S3. Returns True on success."""
        try:
            config = self.sql.get_device_config(device, typ="act")
            if config is None:
                print(f"No configuration found for device: {device}")
        except Exception as e:
            print(f"Error fetching configuration for device {device}: {e}")
            config = None

        _n_clusters = config["n_clusters"] if config is not None else self.model_clusters

        try:
            det = PneumaticAnomalyDetector(n_clusters=_n_clusters)
            det.fit(X)

            with tempfile.NamedTemporaryFile(delete=False, suffix=".pkl") as tmp:
                tmp_path = tmp.name
            try:
                det.save_model(tmp_path)
                self.s3.put_file(self.bucket, tmp_path, model_key)
                print(f"[{device}] Model uploaded: s3://{self.bucket}/{model_key}")
                self.sql.upsert_model_status(device, status=1, health=100, config=False, typ="act")
                return True
            finally:
                try:
                    os.unlink(tmp_path)
                except Exception:
                    pass
        except Exception as e:
            print(f"[{device}] Training/upload error: {e}")
            return False

    # ---------------------------
    # Notification: send via Kafka
    # ---------------------------
    @staticmethod
    def send_pmps_notification(timestamp, mail, mdp, type, device, value, time_from, time_to, priority):
        """Sends a PMPS notification message to the Kafka notifications topic."""
        KAFKA_SERVERS = os.getenv("KAFKA_SERVER").split(",")
        KAFKA_NOTIFICATIONS_TOPIC = os.getenv("KAFKA_NOTIFICATIONS_TOPIC")
        print(f"KAFKA_NOTIFICATIONS_TOPIC:{KAFKA_NOTIFICATIONS_TOPIC}")
        producer = KafkaProducer(bootstrap_servers=KAFKA_SERVERS)

        payload = {
            "timestamp": timestamp,
            "mail": mail,
            "mdp": mdp,
            "type": type,
            "device": device,
            "value": value,
            "time_from": time_from,
            "time_to": time_to,
            "priority": priority
        }

        payload_bytes = json.dumps(payload).encode()
        print(KAFKA_NOTIFICATIONS_TOPIC, "<-", payload_bytes)
        producer.send(KAFKA_NOTIFICATIONS_TOPIC, value=payload_bytes)
        producer.flush()

    def _predict_and_store(self, device: str, df_window: pd.DataFrame, X: pd.DataFrame, model_key: str, _timestamp: datetime.datetime, _newest: datetime.datetime, _oldest: datetime.datetime) -> None:
        """Loads model from S3, predicts distances, and stores results."""
        try:
            config = self.sql.get_device_config(device, typ="act")
            if config is None:
                print(f"No configuration found for device: {device}")
        except Exception as e:
            print(f"Error fetching configuration for device {device}: {e}")
            config = None

        _n_clusters = config.get("n_clusters", self.model_clusters) if config is not None else self.model_clusters
        _sensitivity_min = config.get("sensitivity_min", self.sensitivity_min) if config is not None else self.sensitivity_min
        _sensitivity_max = config.get("sensitivity_max", self.sensitivity_max) if config is not None else self.sensitivity_max
        _warning = config.get("warning", self.warning) if config is not None else self.warning

        try:
            with tempfile.NamedTemporaryFile(delete=False, suffix=".pkl") as tmp:
                tmp_model = tmp.name
            try:
                self.s3.client.download_file(self.bucket, model_key, tmp_model)
                det = PneumaticAnomalyDetector.load_model(tmp_model)
            finally:
                try:
                    os.unlink(tmp_model)
                except Exception:
                    pass

            dists = det.predict(X)
            avg_dist = float(pd.Series(dists).mean())

            if avg_dist <= _sensitivity_min:
                condition = 100
            elif avg_dist >= _sensitivity_max or _sensitivity_max == _sensitivity_min:
                condition = 0
            else:
                condition = int(round(100 - ((avg_dist - _sensitivity_min) / (_sensitivity_max - _sensitivity_min)) * 100))

            out = df_window.copy()
            out["dist"] = dists
            out["condition"] = condition

            self.sql.upsert_model_status(device, status=1, health=condition, config=False, typ="act")
            print(f"[{device}] Prediction stored avg_dist={avg_dist:.6f}, condition={condition}%")

            medians_int = X.median().round().astype(int).to_dict()
            self.sql.insert_device_data(device, yr=medians_int["yr"], vr=medians_int["vr"], yv=medians_int["yv"], rv=medians_int["rv"], health=condition, _timestamp=_timestamp)

            if condition < _warning:
                format_string = "%Y-%m-%dT%H:%M:%S"
                self.send_pmps_notification(
                    timestamp=f"{_timestamp.strftime(format_string)}.000Z",
                    mail="true",
                    mdp="true",
                    type="ACT",
                    device=device[3:],
                    value=int(round(condition)),
                    time_from=f"{_oldest.strftime(format_string)}.000Z",
                    time_to=f"{_newest.strftime(format_string)}.000Z",
                    priority="warning")

        except Exception as e:
            print(f"[{device}] Prediction/store error: {e}")

    # ---------------------------
    # SQL helper: ensure device exists
    # ---------------------------
    def _ensure_device_in_sql(self, device: str) -> Tuple[int, Optional[datetime.datetime]]:
        """If device missing in status table, add it with status 0. Otherwise returns status and fixed_at."""
        cur = self.sql.get_model_status(device, typ="act")

        if cur is None:
            self.sql.upsert_model_status(device, status=0, serial="FESTO", config=True, typ="act")
            return 0, None

        status, fixed_at = cur
        return status, fixed_at
