import dagster as dg
import numpy as np
import optuna
import polars as pl
import xgboost as xgb
from sklearn.metrics import (
    mean_absolute_error,  # type: ignore[reportUnknownVariableType]
    root_mean_squared_error,  # type: ignore[reportUnknownVariableType]
)
from sklearn.model_selection import (
    train_test_split,  # type: ignore[reportUnknownVariableType]
)

from datascience_as_a_service.defs.resources import S3Resource

n_trials = 50
n_estimators = 200


@dg.asset(group_name="ml", deps=["gold_features"], required_resource_keys={"mlflow"})
def train_xgboost(
    context: dg.AssetExecutionContext,
    s3: S3Resource,
) -> dg.MaterializeResult[None]:
    features_bucket = "gold"
    features_table = "features"

    n_trials = 50
    n_estimators = 200

    # dagster-mlflow resource: already started a run, set tracking URI + env
    mlf = context.resources.mlflow  # type: ignore[reportAttributeAccessIssue]

    opts = s3.delta_storage_options
    features_uri = f"s3://{features_bucket}/{features_table}"

    df = pl.read_delta(features_uri, storage_options=opts)

    # TODO: adjust target column and feature columns to match your data
    target_col = "target"
    feature_cols = [c for c in df.columns if c != target_col]

    X = df.select(feature_cols).to_pandas()
    y = df.select(target_col).to_pandas().squeeze()

    X_train, X_test, y_train, y_test = train_test_split(  # type: ignore[reportUnknownVariableType]
        X, y, test_size=0.2, random_state=42
    )

    # ── Optuna objective ─────────────────────────────────────────────
    def objective(trial: optuna.Trial) -> float:
        model = xgb.XGBRegressor(
            objective="reg:squarederror",
            eval_metric="rmse",
            n_estimators=n_estimators,
            max_depth=trial.suggest_int("max_depth", 3, 10),
            learning_rate=trial.suggest_float("learning_rate", 1e-3, 0.3, log=True),
            subsample=trial.suggest_float("subsample", 0.5, 1.0),
            colsample_bytree=trial.suggest_float("colsample_bytree", 0.5, 1.0),
            min_child_weight=trial.suggest_float("min_child_weight", 0.0, 1.0),
            reg_alpha=trial.suggest_float("reg_alpha", 1e-8, 10.0, log=True),
            reg_lambda=trial.suggest_float("reg_lambda", 1e-8, 10.0, log=True),
            early_stopping_rounds=20,
            verbosity=0,
        )

        model.fit(
            X_train,
            y_train,
            eval_set=[(X_test, y_test)],
            verbose=False,
        )

        preds = model.predict(X_test)
        rmse: float = float(root_mean_squared_error(y_test, preds))  # type: ignore[reportUnknownArgumentType]
        return rmse

    # ── Run Optuna study ─────────────────────────────────────────────
    study = optuna.create_study(
        direction="minimize", pruner=optuna.pruners.MedianPruner()
    )
    study.optimize(objective, n_trials=n_trials, show_progress_bar=True)

    best_params = study.best_params
    context.log.info(f"Best Optuna params: {best_params}")

    # ── Train final model & log to MLflow ────────────────────────────
    mlf.log_params(best_params)

    model = xgb.XGBRegressor(
        objective="reg:squarederror",
        eval_metric="rmse",
        n_estimators=n_estimators,
        **best_params,
    )

    model.fit(
        X_train,
        y_train,
        eval_set=[(X_train, y_train), (X_test, y_test)],
        verbose=False,
    )

    # ── Log metrics ──────────────────────────────────────────────────
    preds_train = model.predict(X_train)  # type: ignore[reportUnknownMemberType]
    preds_test = model.predict(X_test)  # type: ignore[reportUnknownMemberType]

    train_rmse = float(root_mean_squared_error(y_train, preds_train))  # type: ignore[reportUnknownArgumentType]
    test_rmse = float(root_mean_squared_error(y_test, preds_test))  # type: ignore[reportUnknownArgumentType]
    test_mae = float(mean_absolute_error(y_test, preds_test))  # type: ignore[reportUnknownArgumentType]
    test_r2 = float(1 - np.sum((y_test - preds_test) ** 2) / np.sum((y_test - np.mean(y_test)) ** 2))  # type: ignore[reportUnknownArgumentType]

    mlf.log_metrics(
        {
            "train_rmse": train_rmse,
            "test_rmse": test_rmse,
            "test_mae": test_mae,
            "test_r2": test_r2,
        }
    )

    # ── Log model ────────────────────────────────────────────────────
    mlf.xgboost.log_model(  # type: ignore[reportUnknownMemberType]
        xgb_model=model,
        artifact_path="model",
        model_format="json",
        registered_model_name="XGBoostModel",
    )

    run_id: str = mlf.active_run().info.run_id

    context.log.info(f"Trained XGBoost model, run_id={run_id}")

    return dg.MaterializeResult(
        value=None,
        metadata={
            "run_id": dg.MetadataValue.text(run_id),
            "train_rmse": dg.MetadataValue.float(train_rmse),
            "test_rmse": dg.MetadataValue.float(test_rmse),
            "test_mae": dg.MetadataValue.float(test_mae),
            "test_r2": dg.MetadataValue.float(test_r2),
            "best_optuna_rmse": dg.MetadataValue.float(study.best_value),
            "n_trials": dg.MetadataValue.int(n_trials),
            "best_params": dg.MetadataValue.json(best_params),
            "train_rows": dg.MetadataValue.int(len(X_train)),  # type: ignore[reportUnknownArgumentType]
            "test_rows": dg.MetadataValue.int(len(X_test)),  # type: ignore[reportUnknownArgumentType]
        },
    )
