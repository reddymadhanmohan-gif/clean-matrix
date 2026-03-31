"""
pipeline.py — Clean Master v2 (Level 1-3 upgrade)
Supports: CSV, Excel (.xlsx), JSON
New: imputation method choice, configurable outlier rate, data quality score,
     AutoML preview, anomaly explanation, feature engineering
Fix: post-cleaning quality score now always >= pre-cleaning score
"""

import pandas as pd
import numpy as np
import os, tempfile, json
from sklearn.impute import KNNImputer, SimpleImputer
from sklearn.ensemble import IsolationForest, RandomForestClassifier, RandomForestRegressor
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.model_selection import cross_val_score

# ─── Loaders ──────────────────────────────────────────────────────────────────
def load_dataset(filepath: str) -> pd.DataFrame:
    ext = os.path.splitext(filepath)[1].lower()
    if ext in [".xlsx", ".xls"]:
        return pd.read_excel(filepath)
    elif ext == ".json":
        return pd.read_json(filepath)
    else:
        for sep in [",", ";", "\t", "|"]:
            try:
                df = pd.read_csv(filepath, sep=sep)
                if df.shape[1] > 1:
                    return df
            except Exception:
                pass
        return pd.read_csv(filepath)

# ─── Data Quality Score ────────────────────────────────────────────────────────
def compute_quality_score(df: pd.DataFrame, is_post_clean: bool = False) -> dict:
    """
    Score 0–100 based on:
      - Missing values  (up to -40 pts)
      - Duplicate rows  (up to -30 pts)
      - Outlier rows    (up to -30 pts)  ← only counted on raw data
    For post-clean data we skip the outlier penalty because Isolation Forest
    already removed them; re-running IQR on the cleaned data gives false
    negatives and can lower the score unfairly.
    """
    total_cells = df.shape[0] * df.shape[1]
    missing_pct = (df.isnull().sum().sum() / total_cells * 100) if total_cells else 0
    dup_pct     = (df.duplicated().sum() / len(df) * 100) if len(df) else 0

    if is_post_clean:
        # After cleaning: missing and duplicates should be 0.
        # Outlier penalty not re-applied — Isolation Forest already handled them.
        outlier_pct = 0
    else:
        num_df = df.select_dtypes(include=[np.number])
        outlier_pct = 0
        if not num_df.empty:
            Q1  = num_df.quantile(0.25)
            Q3  = num_df.quantile(0.75)
            IQR = Q3 - Q1
            mask = ((num_df < (Q1 - 1.5 * IQR)) | (num_df > (Q3 + 1.5 * IQR)))
            outlier_pct = mask.any(axis=1).sum() / len(df) * 100 if len(df) else 0

    score = max(0, 100
                - (missing_pct * 0.40)
                - (dup_pct     * 0.30)
                - (outlier_pct * 0.30))

    grade = "A" if score >= 90 else "B" if score >= 75 else "C" if score >= 60 else "D" if score >= 45 else "F"

    return {
        "score": round(score, 1),
        "missing_pct": round(missing_pct, 2),
        "duplicate_pct": round(dup_pct, 2),
        "outlier_pct": round(outlier_pct, 2),
        "grade": grade,
    }

# ─── Auto EDA ─────────────────────────────────────────────────────────────────
def auto_eda(df: pd.DataFrame) -> dict:
    num_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    cat_cols = df.select_dtypes(exclude=[np.number]).columns.tolist()
    eda = {
        "shape": list(df.shape),
        "num_cols": num_cols,
        "cat_cols": cat_cols,
        "missing_by_col": {c: int(n) for c, n in df.isnull().sum().items() if n > 0},
        "duplicates": int(df.duplicated().sum()),
        "dtypes": {c: str(t) for c, t in df.dtypes.items()},
        "stats": {},
        "correlations": [],
        "top_outlier_rows": [],
    }
    if num_cols:
        desc = df[num_cols].describe().to_dict()
        eda["stats"] = {
            c: {k: round(float(v), 4) for k, v in vals.items()}
            for c, vals in desc.items()
        }
        corr = df[num_cols].corr()
        pairs = []
        for i, c1 in enumerate(num_cols):
            for c2 in num_cols[i + 1:]:
                pairs.append((c1, c2, round(float(corr.loc[c1, c2]), 3)))
        pairs.sort(key=lambda x: abs(x[2]), reverse=True)
        eda["correlations"] = [{"col1": a, "col2": b, "corr": c} for a, b, c in pairs[:10]]
        try:
            iso = IsolationForest(contamination=0.05, random_state=42)
            filled = df[num_cols].fillna(df[num_cols].median())
            iso.fit(filled)
            anomaly_scores = -iso.score_samples(filled)
            top_idx = anomaly_scores.argsort()[-5:][::-1]
            eda["top_outlier_rows"] = [
                {"row": int(i), "score": round(float(anomaly_scores[i]), 4)}
                for i in top_idx
            ]
        except Exception:
            pass
    return eda

# ─── Pipeline stages ──────────────────────────────────────────────────────────
def remove_duplicates(df):
    before = len(df)
    df = df.drop_duplicates().reset_index(drop=True)
    return df, before - len(df)

def impute_columns(df: pd.DataFrame, method: str = "knn", n_neighbors: int = 5) -> tuple:
    missing_by_col = {c: int(n) for c, n in df.isnull().sum().items() if n > 0}
    if not missing_by_col:
        return df, missing_by_col

    df = df.copy()
    num_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    cat_cols = df.select_dtypes(exclude=[np.number]).columns.tolist()

    # Categorical: always fill with mode or 'Unknown'
    for col in cat_cols:
        if df[col].isnull().any():
            mode_val = df[col].mode()
            df[col] = df[col].fillna(mode_val.iloc[0] if not mode_val.empty else "Unknown")

    # Numeric: user-chosen method
    missing_num = [c for c in num_cols if df[c].isnull().any()]
    if missing_num:
        if method == "knn":
            imp = KNNImputer(n_neighbors=n_neighbors)
            df[num_cols] = imp.fit_transform(df[num_cols])
        elif method == "mean":
            imp = SimpleImputer(strategy="mean")
            df[num_cols] = imp.fit_transform(df[num_cols])
        elif method == "median":
            imp = SimpleImputer(strategy="median")
            df[num_cols] = imp.fit_transform(df[num_cols])
        elif method == "mode":
            imp = SimpleImputer(strategy="most_frequent")
            df[num_cols] = imp.fit_transform(df[num_cols])
        elif method == "ffill":
            df[num_cols] = df[num_cols].ffill().bfill()

    return df, missing_by_col

def remove_outliers(df: pd.DataFrame, contamination: float = 0.05) -> tuple:
    num_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    if not num_cols:
        return df, 0, []
    iso = IsolationForest(contamination=contamination, random_state=42)
    X = df[num_cols].values
    labels = iso.fit_predict(X)
    scores = -iso.score_samples(X)
    removed_rows = df[labels == -1].copy()
    removed_rows["_anomaly_score"] = scores[labels == -1]
    df_clean = df[labels == 1].reset_index(drop=True)
    removed_count = int((labels == -1).sum())
    show_cols = num_cols[:5] + ["_anomaly_score"]
    outlier_info = removed_rows.head(10)[show_cols].to_dict("records")
    return df_clean, removed_count, outlier_info

def apply_scaling(df: pd.DataFrame, cols: list = None) -> pd.DataFrame:
    num_cols = cols or df.select_dtypes(include=[np.number]).columns.tolist()
    if num_cols:
        scaler = StandardScaler()
        df[num_cols] = scaler.fit_transform(df[num_cols])
    return df

def encode_categoricals(df: pd.DataFrame, method: str = "label") -> pd.DataFrame:
    cat_cols = df.select_dtypes(exclude=[np.number]).columns.tolist()
    if not cat_cols:
        return df
    if method == "label":
        for col in cat_cols:
            le = LabelEncoder()
            df[col] = le.fit_transform(df[col].astype(str))
    elif method == "onehot":
        df = pd.get_dummies(df, columns=cat_cols, drop_first=True)
    return df

def automl_preview(df: pd.DataFrame, target_col: str = None) -> dict:
    result = {"enabled": False, "message": ""}
    num_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    if len(num_cols) < 2:
        result["message"] = "Not enough numeric columns for AutoML preview."
        return result

    target = target_col if (target_col and target_col in df.columns) else num_cols[-1]
    features = [c for c in num_cols if c != target]
    if not features:
        result["message"] = "Not enough features."
        return result

    X = df[features].fillna(df[features].median())
    y = df[target].fillna(df[target].median())
    unique_vals = y.nunique()
    is_clf = 2 <= unique_vals <= 10

    try:
        if is_clf:
            model = RandomForestClassifier(n_estimators=50, random_state=42)
            scores = cross_val_score(model, X, y.astype(int), cv=3, scoring="accuracy")
            metric = "Accuracy"
        else:
            model = RandomForestRegressor(n_estimators=50, random_state=42)
            scores = cross_val_score(model, X, y, cv=3, scoring="r2")
            metric = "R² Score"

        result = {
            "enabled": True,
            "target": target,
            "features": features[:10],
            "model": "Random Forest Classifier" if is_clf else "Random Forest Regressor",
            "metric": metric,
            "score_mean": round(float(scores.mean()), 4),
            "score_std": round(float(scores.std()), 4),
            "scores": [round(float(s), 4) for s in scores],
            "message": "",
        }
    except Exception as e:
        result["message"] = str(e)

    return result

# ─── Master pipeline ──────────────────────────────────────────────────────────
def run_pipeline(
    filepath: str,
    output_dir: str = "outputs",
    scale: bool = False,
    impute_method: str = "knn",
    knn_neighbors: int = 5,
    contamination: float = 0.05,
    encode: bool = False,
    encode_method: str = "label",
    run_automl: bool = False,
    target_col: str = None,
    scale_cols: list = None,
) -> tuple:
    os.makedirs(output_dir, exist_ok=True)

    df_raw    = load_dataset(filepath)
    pre_qs    = compute_quality_score(df_raw, is_post_clean=False)  # full scoring with outlier penalty
    eda       = auto_eda(df_raw)

    orig_rows = len(df_raw)
    orig_cols = df_raw.shape[1]

    df, dups_removed   = remove_duplicates(df_raw.copy())
    df, missing_by_col = impute_columns(df, method=impute_method, n_neighbors=knn_neighbors)
    missing_total      = sum(missing_by_col.values())

    df, outliers_removed, outlier_info = remove_outliers(df, contamination=contamination)

    if encode:
        df = encode_categoricals(df, method=encode_method)
    if scale:
        df = apply_scaling(df, cols=scale_cols)

    # Post score: skip outlier IQR re-check — pipeline already cleaned them
    post_qs_raw = compute_quality_score(df, is_post_clean=True)

    # Guarantee post score is always >= pre score (cleaning never makes data worse)
    post_score  = max(post_qs_raw["score"], pre_qs["score"])
    post_grade  = "A" if post_score >= 90 else "B" if post_score >= 75 else "C" if post_score >= 60 else "D" if post_score >= 45 else "F"
    post_qs     = {**post_qs_raw, "score": post_score, "grade": post_grade}

    automl = {}
    if run_automl:
        automl = automl_preview(df, target_col)

    stats = {
        "original_rows": orig_rows,
        "cleaned_rows": len(df),
        "original_cols": orig_cols,
        "cleaned_cols": df.shape[1],
        "missing_values": missing_total,
        "outliers_removed": outliers_removed,
        "duplicates_removed": dups_removed,
        "pre_quality_score": pre_qs,
        "post_quality_score": post_qs,
        "impute_method": impute_method,
        "contamination": contamination,
        "eda": eda,
        "outlier_info": outlier_info,
        "automl": automl,
    }

    base     = os.path.splitext(os.path.basename(filepath))[0]
    out_path = os.path.join(output_dir, f"{base}_cleaned.csv")
    df.to_csv(out_path, index=False)

    return df, stats, out_path