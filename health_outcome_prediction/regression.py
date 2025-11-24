"""
Ridge Regression Forecasting for State-Month Health Outcomes

Notes:
- 'state', 'month', 'year' are ID-like fields (identifiers), not predictive features.
- We only use true numeric environmental / meteorological / urban features as inputs.
"""

import pandas as pd
import numpy as np

from sklearn.linear_model import Ridge
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

# ==============================
# 0. Load data
# ==============================
IN_CSV  = "./Final_Master.csv"
OUT_CSV = "./Final_Master_with_preds.csv"

df = pd.read_csv(IN_CSV)

# Ensure ID columns are strings
df["month"] = df["month"].astype(str)   # format: "YYYY-MM"
df["state"] = df["state"].astype(str)   # full state name

# Recompute year / month_num from 'month' for convenience, but treat them as IDs, not features
dt = pd.to_datetime(df["month"] + "-01")
df["year"] = dt.dt.year.astype(int)
df["month_num"] = dt.dt.month.astype(int)

print("Total rows:", len(df))
print("Columns:", df.columns.tolist())

# ==============================
# 1. Targets and feature set
# ==============================
# Monthly health outcomes (counts per state-month)
target_cols = ["ihd_deaths", "copd_deaths", "asthma_deaths"]

# ID-like columns (NOT used as model features)
id_cols = ["state", "month", "year", "month_num"]

# Automatically select numeric feature columns, excluding:
# - targets
# - ID-like numeric columns: 'year', 'month_num'
num_all = df.select_dtypes(include=[np.number]).columns.tolist()
exclude = set(target_cols + ["year", "month_num"])
num_features = [c for c in num_all if c not in exclude]

print("\nNumber of numeric features used:", len(num_features))
print("Example features:", num_features[:10])

# Initialize prediction columns
for t in target_cols:
    pred_col = t.replace("_deaths", "_pred")
    if pred_col not in df.columns:
        df[pred_col] = np.nan

# ==============================
# 2. Time-based split configuration
# ==============================
TRAIN_END     = "2023-12"
VAL_START     = "2024-01"
VAL_END       = "2025-07"
FUTURE_START  = "2025-08"

# ==============================
# 3. Train / evaluate / forecast per target
# ==============================
for target_col in target_cols:
    print(f"\n================= Target: {target_col} =================")
    pred_col = target_col.replace("_deaths", "_pred")

    # 3-1. Filter to valid observed rows (non-null, non-negative target, complete features)
    df_obs = df[df[target_col].notna()].copy()
    df_obs = df_obs[df_obs[target_col] >= 0]
    df_obs = df_obs.dropna(subset=num_features)

    if df_obs.empty:
        print(f"  -> No valid observed data for {target_col}. Skipping.")
        continue

    # Transform target to log-scale: log(deaths + 1)
    df_obs[target_col + "_log1p"] = np.log1p(df_obs[target_col].values)

    # Time-based train / validation split
    train_mask = df_obs["month"] <= TRAIN_END
    val_mask   = (df_obs["month"] >= VAL_START) & (df_obs["month"] <= VAL_END)

    train = df_obs[train_mask].copy()
    val   = df_obs[val_mask].copy()

    print(f"  Observed samples: {len(df_obs)}")
    print(f"  → Train: {len(train)}, Val: {len(val)}")

    # ----------------------
    # 3-2. Validation (if available)
    # ----------------------
    if len(train) > 0 and len(val) > 0:
        X_train = train[num_features]
        y_train_log = train[target_col + "_log1p"].values

        X_val = val[num_features]
        y_val = val[target_col].values                     # original counts

        model = Ridge(alpha=1.0, random_state=42)
        model.fit(X_train, y_train_log)

        # Predict in log-space and back-transform to counts
        val_pred_log = model.predict(X_val)
        val_pred = np.expm1(val_pred_log)
        val_pred = np.clip(val_pred, 0, None)              # enforce non-negative

        mae  = mean_absolute_error(y_val, val_pred)
        mse  = mean_squared_error(y_val, val_pred)
        rmse = np.sqrt(mse)
        r2   = r2_score(y_val, val_pred)

        print("  [Validation performance (count scale)]")
        print(f"    MAE : {mae:,.3f}")
        print(f"    RMSE: {rmse:,.3f}")
        print(f"    R^2 : {r2:,.4f}")
    else:
        print("  → Not enough train/validation data; skipping formal validation and training on all observed data only.")

    # ----------------------
    # 3-3. Final model: train on all observed data up to 2025-07
    # ----------------------
    full_obs = df_obs[df_obs["month"] <= VAL_END].copy()
    if full_obs.empty:
        print("  → No observed data in the training window. Skipping forecasting.")
        continue

    X_all = full_obs[num_features]
    y_all_log = full_obs[target_col + "_log1p"].values

    final_model = Ridge(alpha=1.0, random_state=42)
    final_model.fit(X_all, y_all_log)

    # ----------------------
    # 3-4. Forecast future window (2025-08+)
    # ----------------------
    df_future = df[df["month"] >= FUTURE_START].copy()
    df_future_feat = df_future.dropna(subset=num_features).copy()

    if df_future_feat.empty:
        print("  → No usable future rows for features. Skipping forecasting.")
        continue

    X_future = df_future_feat[num_features]
    y_future_log_pred = final_model.predict(X_future)
    y_future_pred = np.expm1(y_future_log_pred)
    y_future_pred = np.clip(y_future_pred, 0, None)

    # Write predictions back
    df.loc[df_future_feat.index, pred_col] = y_future_pred

    print(f"  → Future forecast complete: {len(df_future_feat)} rows updated in '{pred_col}'")

# ==============================
# 4. Save results
# ==============================
print("\nSample future forecasts (>= 2030-01):")
print(
    df[df["month"] >= "2030-01"]
      [["state", "month", "ihd_pred", "copd_pred", "asthma_pred"]]
      .head(10)
)

df.to_csv(OUT_CSV, index=False)
print(f"\n>>> Saved with predictions to: {OUT_CSV}")
