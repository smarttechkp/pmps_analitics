"""
ACT_Processor.py
    - klasa do obsługi części analitycznej
    - ładuje 100 ostatnich danych dla danego urządzenia
        S3 (ACT/dn=*/date=.../*.parquet) -> wybór ostatnich N=100 rekordów ->
        przygotowanie cech -> (trening lub predykcja)

Zasady sterujące:
 - Trening TYLKO, gdy mamy ≥ required_samples "ważnych" próbek
 - Status=0 oznacza brak gotowego modelu; Status=1 dopiero po UDANYM zapisie
   wytrenowanego modelu do S3.
 - Predykcja wyłącznie, gdy model istnieje w S3.

Dostęp do S3 wyłącznie przez S3Processor
"""

import os,json
import re
import tempfile
import pandas as pd
import datetime
from typing import List, Tuple, Optional
from kmeans import PneumaticAnomalyDetector
from kafka import KafkaProducer

class ActProcessor:
    def __init__(self, s3, sql_processor, model_clusters: int = 1, required_samples: int = 100,) -> None:
        # Injected dependencies
        self.s3 = s3
        self.sql = sql_processor

        # Bucket name (from ENV or default)
        self.bucket = "pmps-raw-data"

        # List of feature columns used in K-Means model
        self.features = ["yr", "vr", "yv", "rv"]

        # Model parameter
        self.model_clusters = int(model_clusters)
        self.sensitivity_min = 10
        self.sensitivity_max = 200
        self.warning = 70

        # Minimum number of samples required for training
        self.required_samples = int(required_samples)

    def process_s3_data(self, device: str, _timestamp: datetime) -> None:
        """
        Main procedure:
        - collects and cleans data,
        - decides whether to train or predict based on model availability,
        - stores the results
        """

        try:
            # 1) Load last 'required_samples' records (at least)
            df_window, newest_timestamp, oldest_timestamp = self._load_recent_device_df(device, _timestamp, min_rows=self.required_samples, window_rows=self.required_samples)
            if df_window is None:
                print(f"[{device}] Less than {self.required_samples} records")
                return
                
            # 2) Prepare features and check if we still have ≥ required_samples after cleaning
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
                if fixed_at is None or  oldest_timestamp > (fixed_at + datetime.timedelta(minutes=60)):
                    print(f"[{device}] Training has_model={has_model}, status={status}")
                    if self._train_and_upload_model(device, X, model_key):
                        # Set status=1 only after successful upload to S3
                        # Optionally predict immediately on fresh data
                        self._predict_and_store(device, df_window, X, model_key, _timestamp, newest_timestamp, oldest_timestamp)
                    else:
                        print(f"[{device}] Model upload failed. Status remains 0.")
                else:
                    print(f"[{device}] fixed recently. Not enough new data yet.")
            else:
                # Model exists, perform prediction
                print(f"[{device}] Prediction (model OK, status=1).")
                self._predict_and_store(device, df_window, X, model_key, _timestamp, newest_timestamp, oldest_timestamp)
                
        except Exception as e:
            # Any error for a device, set status=0 (no guarantee of ready model)
            print(f"[{device}] Error: {e}")

    # ---------------------------
    # S3 Section: listing and download - currently unused, for tests
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

    @staticmethod
    def _extract_datetime_from_key(key: str) -> Optional[datetime.datetime]:
        """
        Extracts datetime from S3 key of format '.../date=YYYY-MM-DD/HH.parquet'
        """
        match = re.search(r"date=(\d{4}-\d{2}-\d{2})/(\d{2})\.parquet$", key)
        if not match:
            return None
        try:
            d_str, h_str = match.groups()
            return datetime.datetime.fromisoformat(f"{d_str}T{h_str}:00:00")
        except ValueError:
            return None

    def _list_device_files(self, device: str) -> List[str]:
        """
        Returns a sorted list of .parquet keys for a given device.
        Sort works thanks to lexicographic date format 'date=YYYY-MM-DD/HH.parquet'.
        """
        prefix = f"ACT/{device}/"
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

    def _load_recent_device_df(self, device: str, max_datetime: datetime, min_rows: int, window_rows: int):
        """
        Collects Parquet files for a given device up to and including 'max_datetime'.
        Ignores newer files. Stops after collecting at least 'min_rows' rows.
        Returns last 'window_rows' rows sorted by timestamp ascending, 
        ALONGSIDE the newest and oldest timestamp from all collected data (df_all).
        
        Zwracane wartości: (pd.DataFrame, datetime, datetime) lub None.
        """
        keys = self._list_device_files(device)
        if not keys:
            return None

        # filter only those with datetime <= max_datetime
        dated_keys = []
        for k in keys:
            dt = self._extract_datetime_from_key(k)
            if dt and dt <= max_datetime:
                dated_keys.append((dt, k))

        if not dated_keys:
            print(f"[{device}] No files on or before {max_datetime}.")
            return None

        # sort ascending by datetime so we can iterate newest-to-oldest
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

        # Sortowanie i ekstrakcja timestampów
        df_all = df_all.sort_values("timestamp", ascending=True)

        # Wydobycie najstarszego i najnowszego timestampu
        oldest_timestamp = df_all["timestamp"].iloc[0]
        newest_timestamp = df_all["timestamp"].iloc[-1]

        # Przygotowanie ostatecznej ramki danych
        final_df = df_all.tail(window_rows).reset_index(drop=True)

        # Zwracamy trzy zmienne BEZ JAWNEGO TUPLE w instrukcji return
        # W Pythonie jest to równoważne zwróceniu tupli (final_df, newest_timestamp, oldest_timestamp),
        # ale spełnia warunek zapytania o 'zwracanie trzech zmiennych'
        return final_df, newest_timestamp, oldest_timestamp

    # ---------------------------
    # Model handling: key, existence, training, prediction
    # ---------------------------
    def _model_key(self, device: str) -> str:
        """Builds model path in S3, e.g., ACT/dn=R1/model_R1.pkl"""
        return f"ACT/model/{device}.pkl"


    def _model_exists(self, model_key: str) -> bool:
        """Checks if model file exists in S3."""
        try:
            self.s3.client.head_object(Bucket=self.bucket, Key=model_key)
            return True
        except Exception:
            keys = self.s3.list(self.bucket, prefix=model_key, json=True) or []
            return any(k == model_key for k in keys)

    def _train_and_upload_model(self, device: str, X: pd.DataFrame, model_key: str) -> bool:
        """Trains K-Means model and uploads to S3. Returns True on success."""   
        try:
            config = self.sql.get_device_config(device, typ="act")
            if config is None:
                print(f"No configuration found for device: {device}")
        except Exception as e:
            print(f"Error fetching configuration for device {device}: {e}")
            config = None  # ustawiamy zmienną na None w razie błędu
        
        _n_clusters = config["n_clusters"] if config is not None else self.n_clusters

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
    # Notification sendfunction via Kafka
    # ---------------------------
    @staticmethod
    def send_pmps_notification(timestamp, mail, mdp, type, device, value, time_from, time_to, priority):
        # Fetching configuration from environment variables
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

    def _predict_and_store(self, device: str, df_window: pd.DataFrame, X: pd.DataFrame, model_key: str, _timestamp: datetime, _newest: datetime, _oldest: datetime) -> None:
        """Loads model from S3, predicts distances, and stores results."""
        try:
            config = self.sql.get_device_config(device, typ="act")
            if config is None:
                print(f"No configuration found for device: {device}")
        except Exception as e:
            print(f"Error fetching configuration for device {device}: {e}")
        
        _n_clusters = config["n_clusters"] if config["n_clusters"] is not None else self.model_clusters
        _sensitivity_min = config["sensitivity_min"] if config["sensitivity_min"] is not None else self.sensitivity_min
        _sensitivity_max = config["sensitivity_max"] if config["sensitivity_max"] is not None else self.sensitivity_max
        _warning = 99 #do testu, oryginal: config["warning"] if config["warning"] is not None else self.warning

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
    # Feature preparation from raw DataFrame
    # ---------------------------
    def _prepare_features(self, df: pd.DataFrame) -> Optional[pd.DataFrame]:
        """Selects feature columns, converts to numeric, drops NaNs."""
        missing = [f for f in self.features if f not in df.columns]
        if missing:
            print(f"Missing feature columns: {missing}")
            return None
        X = df[self.features].apply(pd.to_numeric, errors="coerce").dropna().reset_index(drop=True)
        return X if not X.empty else None

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
