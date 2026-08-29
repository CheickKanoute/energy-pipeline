"""
Training du modèle de prévision de consommation + tracking/registry MLflow.

Volontairement simple sur la partie ML (XGBoost sur features tabulaires) —
l'enjeu du projet est l'industrialisation (tracking, registry, transition de
stage), pas la sophistication du modèle.
"""
import mlflow
import mlflow.xgboost
from xgboost import XGBRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error

CATALOG = "energy"
EXPERIMENT_NAME = "/Shared/energy_consumption_forecast"
MODEL_NAME = f"{CATALOG}.gold.consumption_forecast_model"


def load_training_data(spark):
    df = spark.table(f"{CATALOG}.gold.forecast_features").na.drop(
        subset=["lag_1h", "lag_24h", "rolling_avg_24h"]
    )
    # échantillonnage pour l'entraînement — pas besoin de la totalité du volume
    # pour entraîner un modèle tabulaire raisonnable ; démontrer ce choix explicite
    # en entretien évite l'écueil classique du "tout charger en pandas".
    pdf = df.sample(fraction=0.05, seed=42).toPandas()
    features = ["hour_of_day", "day_of_week", "lag_1h", "lag_24h", "rolling_avg_24h"]
    return pdf[features], pdf["avg_consumption_kw"]


def run(spark):
    mlflow.set_experiment(EXPERIMENT_NAME)
    X, y = load_training_data(spark)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    with mlflow.start_run(run_name="xgboost_baseline") as run:
        params = {"n_estimators": 200, "max_depth": 6, "learning_rate": 0.05}
        mlflow.log_params(params)

        model = XGBRegressor(**params)
        model.fit(X_train, y_train)
        preds = model.predict(X_test)

        mae = mean_absolute_error(y_test, preds)
        rmse = mean_squared_error(y_test, preds, squared=False)
        mlflow.log_metrics({"mae": mae, "rmse": rmse})

        mlflow.xgboost.log_model(model, artifact_path="model", registered_model_name=MODEL_NAME)
        print(f"Run {run.info.run_id} — MAE={mae:.3f} RMSE={rmse:.3f}")

    # Transition vers "Production" via Unity Catalog model aliasing
    client = mlflow.MlflowClient()
    latest = client.get_latest_versions(MODEL_NAME, stages=["None"])[0]
    client.set_registered_model_alias(MODEL_NAME, "champion", latest.version)


if __name__ == "__main__":
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("train_forecast").getOrCreate()
    run(spark)
