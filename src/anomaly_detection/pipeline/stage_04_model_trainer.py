import numpy as np
import pandas as pd
import joblib
import shap
import umap
import matplotlib.pyplot as plt
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

class AnomalyDetector:
    def __init__(self, model=None):
        self.model = model if model else IsolationForest(contamination=0.05, random_state=42)
        self.scaler = StandardScaler()
        self.fitted = False
        self.shap_explainer = None
        self.embeddings = None
        self.data = None
        self.scores = None
        self.labels = None

    def fit(self, X: pd.DataFrame):
        """Fit the anomaly detector."""
        self.data = X.copy()
        X_scaled = self.scaler.fit_transform(X)
        self.model.fit(X_scaled)
        self.scores = -self.model.decision_function(X_scaled)  # Higher = more anomalous
        self.labels = np.where(self.model.predict(X_scaled) == -1, 1, 0)  # 1=Anomaly, 0=Normal
        self.fitted = True

        # Prepare SHAP explainer
        self.shap_explainer = shap.Explainer(self.model, X_scaled)

        # Compute embeddings for cluster visualization
        reducer = umap.UMAP(random_state=42)
        self.embeddings = reducer.fit_transform(X_scaled)

    def top_anomalies(self, n=10):
        """Return top N anomalies."""
        if not self.fitted:
            raise ValueError("Model not fitted yet.")
        top_idx = np.argsort(self.scores)[-n:]
        return self.data.iloc[top_idx], self.scores[top_idx], self.labels[top_idx]

    def plot_shap_summary(self, top_n=10):
        """Plot SHAP feature importance for top anomalies."""
        if not self.fitted:
            raise ValueError("Model not fitted yet.")
        top_idx = np.argsort(self.scores)[-top_n:]
        X_scaled = self.scaler.transform(self.data.iloc[top_idx])
        shap_values = self.shap_explainer(X_scaled)
        shap.summary_plot(shap_values, self.data.iloc[top_idx], plot_type="bar")

    def plot_clusters(self):
        """Visualize clusters of anomalies vs normal points."""
        if self.embeddings is None:
            raise ValueError("Run fit() first.")
        plt.figure(figsize=(8, 6))
        plt.scatter(self.embeddings[:, 0], self.embeddings[:, 1],
                    c=self.labels, cmap='coolwarm', alpha=0.6)
        plt.colorbar(label="Anomaly (1) / Normal (0)")
        plt.title("Anomaly Clustering Visualization")
        plt.show()

    def save_model(self, path="anomaly_detector.joblib"):
        """Save model and scaler for deployment."""
        if not self.fitted:
            raise ValueError("Model not fitted yet.")
        joblib.dump({
            "model": self.model,
            "scaler": self.scaler
        }, path)

    def load_model(self, path="anomaly_detector.joblib"):
        """Load a saved model for inference."""
        saved = joblib.load(path)
        self.model = saved["model"]
        self.scaler = saved["scaler"]
        self.fitted = True

