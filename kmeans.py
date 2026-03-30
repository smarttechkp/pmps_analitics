import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
import joblib

class PneumaticAnomalyDetector:
    def __init__(self, n_clusters=1, random_state=42):
        """
        Parametry:
        ----------
        n_clusters  - liczba klastrów w KMeans (ile "trybów" pracy siłownika dopuszczamy, dopuszczamy 1)
        random_state - losowość w KMeans (dla powtarzalności wyników, stosujemy 42)
        """
        self.n_clusters = n_clusters
        self.random_state = random_state

        self.kmeans_ = None
        self.scaler_ = StandardScaler()

    # ------------------------
    #  Uczenie modelu
    # ------------------------
    def fit(self, X: pd.DataFrame, outlier_percent=0):
        X_scaled = self.scaler_.fit_transform(X)

        if outlier_percent > 0:
            temp_kmeans = KMeans(n_clusters=self.n_clusters, random_state=self.random_state)
            temp_kmeans.fit(X_scaled)
            dists = np.min(temp_kmeans.transform(X_scaled), axis=1)
            threshold = np.percentile(dists, 100 - outlier_percent)
            X_scaled = X_scaled[dists <= threshold]

        self.kmeans_ = KMeans(n_clusters=self.n_clusters, random_state=self.random_state)
        self.kmeans_.fit(X_scaled)
        return self

    # ------------------------
    #  Predykcja
    # ------------------------
    def predict(self, X: pd.DataFrame):
        X_scaled = self.scaler_.transform(X)
        dists = np.min(self.kmeans_.transform(X_scaled), axis=1)
        return dists

    # ------------------------
    #  Zapis i odczyt modelu
    # ------------------------
    def save_model(self, filepath: str):
        joblib.dump({
            'kmeans': self.kmeans_,
            'scaler': self.scaler_,
            'n_clusters': self.n_clusters,
            'random_state': self.random_state
        }, filepath)
        print(f"Model zapisany do pliku: {filepath}")

    @classmethod
    def load_model(cls, filepath: str):
        data = joblib.load(filepath)
        obj = cls(
            n_clusters=data['n_clusters'],
            random_state=data['random_state']
        )
        obj.kmeans_ = data['kmeans']
        obj.scaler_ = data['scaler']
        print(f"Model wczytany z pliku: {filepath}")
        return obj
