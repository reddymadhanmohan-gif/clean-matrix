# pipeline.py — Clean Master v2 (Fixed & Clean Output)

import pandas as pd
import numpy as np
from sklearn.impute import KNNImputer
from sklearn.ensemble import IsolationForest, RandomForestClassifier, RandomForestRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import cross_val_score
import warnings
warnings.filterwarnings('ignore')

# ─────────────────────────────────────────────
# STEP 0 — LOAD DATASET
# ─────────────────────────────────────────────
def load_dataset(filepath: str) -> pd.DataFrame:
    ext = filepath.rsplit('.', 1)[-1].lower()
    if ext == 'csv':
        df = pd.read_csv(filepath)
    elif ext in ('xlsx', 'xls'):
        df = pd.read_excel(filepath)
    elif ext == 'json':
        df = pd.read_json(filepath)
    elif ext == 'tsv':
        df = pd.read_csv(filepath, sep='\t')
    else:
        raise ValueError(f"Unsupported file type: {ext}")
    return df


# ─────────────────────────────────────────────
# STEP 1 — DETECT COLUMN TYPES SMARTLY
# ─────────────────────────────────────────────
def detect_column_types(df: pd.DataFrame):
    """
    Returns three lists:
      id_cols      — identifier columns to leave untouched (e.g. Vehicle_ID)
      numeric_cols — truly numeric columns to impute + scale
      text_cols    — categorical/text columns to impute with mode only
    """
    id_cols      = []
    numeric_cols = []
    text_cols    = []

    for col in df.columns:
        # Rule 1: if column name suggests it's an ID → skip it
        if any(kw in col.lower() for kw in ['id', 'code', 'index', 'uuid', 'key', 'no', 'num', 'ref']):
            # Extra check: if unique values > 80% of rows, it's definitely an ID
            if df[col].nunique() > 0.8 * len(df):
                id_cols.append(col)
                continue

        # Rule 2: numeric dtype
        if pd.api.types.is_numeric_dtype(df[col]):
            numeric_cols.append(col)

        # Rule 3: object/string/categorical
        else:
            text_cols.append(col)

    return id_cols, numeric_cols, text_cols


# ─────────────────────────────────────────────
# STEP 2 — QUALITY SCORE
# ─────────────────────────────────────────────
def compute_quality_score(df: pd.DataFrame, is_post_clean: bool = False) -> dict:
    total_cells   = df.shape[0] * df.shape[1]
    missing_pct   = (df.isnull().sum().sum() / total_cells * 100) if total_cells > 0 else 0
    duplicate_pct = (df.duplicated().sum() / len(df) * 100) if len(df) > 0 else 0

    if is_post_clean:
        outlier_pct = 0.0          # already removed by Isolation Forest
    else:
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        outlier_pct  = 0.0
        if len(numeric_cols) >= 2:
            try:
                iso   = IsolationForest(contamination=0.05, random_state=42)
                preds = iso.fit_predict(df[numeric_cols].fillna(df[numeric_cols].median()))
                outlier_pct = (preds == -1).sum() / len(df) * 100
            except Exception:
                pass

    score = max(0, 100 - (missing_pct * 0.5) - (duplicate_pct * 0.3) - (outlier_pct * 0.2))

    if score >= 90: grade = 'A'
    elif score >= 75: grade = 'B'
    elif score >= 60: grade = 'C'
    elif score >= 45: grade = 'D'
    else: grade = 'F'

    return {
        'score':         round(score, 1),
        'grade':         grade,
        'missing_pct':   round(missing_pct, 2),
        'duplicate_pct': round(duplicate_pct, 2),
        'outlier_pct':   round(outlier_pct, 2),
    }


# ─────────────────────────────────────────────
# STEP 3 — AUTO EDA
# ─────────────────────────────────────────────
def auto_eda(df: pd.DataFrame) -> dict:
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    text_cols    = df.select_dtypes(include=['object', 'category']).columns.tolist()

    eda = {
        'shape':        df.shape,
        'dtypes':       df.dtypes.astype(str).to_dict(),
        'missing':      df.isnull().sum().to_dict(),
        'missing_pct':  (df.isnull().sum() / len(df) * 100).round(2).to_dict(),
        'numeric_cols': numeric_cols,
        'text_cols':    text_cols,
        'duplicates':   int(df.duplicated().sum()),
        'numeric_summary': df[numeric_cols].describe().round(2).to_dict() if numeric_cols else {},
        'top_correlations': [],
        'outlier_preview':  [],
    }

    # Top correlations
    if len(numeric_cols) >= 2:
        corr   = df[numeric_cols].corr().abs()
        upper  = corr.where(np.triu(np.ones(corr.shape), k=1).astype(bool))
        pairs  = upper.stack().sort_values(ascending=False).head(5)
        eda['top_correlations'] = [
            {'col1': c[0], 'col2': c[1], 'corr': round(v, 3)}
            for c, v in pairs.items()
        ]

    # Outlier preview (top suspicious rows)
    if len(numeric_cols) >= 2:
        try:
            iso    = IsolationForest(contamination=0.05, random_state=42)
            scores = iso.fit_predict(df[numeric_cols].fillna(df[numeric_cols].median()))
            eda['outlier_preview'] = df[scores == -1].head(5).to_dict(orient='records')
        except Exception:
            pass

    return eda


# ─────────────────────────────────────────────
# STEP 4 — HANDLE MISSING VALUES
# ─────────────────────────────────────────────
def handle_missing(df: pd.DataFrame, numeric_cols: list, text_cols: list,
                   method: str = 'knn', knn_neighbors: int = 5) -> tuple:
    filled = 0

    # --- Numeric columns: KNN / Mean / Median / Mode / Forward Fill ---
    if numeric_cols:
        missing_before = df[numeric_cols].isnull().sum().sum()

        if method == 'knn' and missing_before > 0:
            imputer         = KNNImputer(n_neighbors=knn_neighbors)
            df[numeric_cols] = imputer.fit_transform(df[numeric_cols])
        elif method == 'mean':
            df[numeric_cols] = df[numeric_cols].fillna(df[numeric_cols].mean())
        elif method == 'median':
            df[numeric_cols] = df[numeric_cols].fillna(df[numeric_cols].median())
        elif method == 'mode':
            df[numeric_cols] = df[numeric_cols].fillna(df[numeric_cols].mode().iloc[0])
        elif method == 'ffill':
            df[numeric_cols] = df[numeric_cols].ffill().bfill()

        filled += int(missing_before)

    # --- Text/Categorical columns: always use mode (most common value) ---
    for col in text_cols:
        missing_before = df[col].isnull().sum()
        if missing_before > 0:
            mode_val  = df[col].mode()
            fill_val  = mode_val.iloc[0] if not mode_val.empty else 'Unknown'
            df[col]   = df[col].fillna(fill_val)
            filled   += int(missing_before)

    return df, filled


# ─────────────────────────────────────────────
# STEP 5 — REMOVE OUTLIERS (Isolation Forest)
# ─────────────────────────────────────────────
def remove_outliers(df: pd.DataFrame, numeric_cols: list,
                    contamination: float = 0.05) -> tuple:
    if len(numeric_cols) < 2:
        return df, 0

    iso      = IsolationForest(contamination=contamination, random_state=42)
    preds    = iso.fit_predict(df[numeric_cols])
    mask     = preds == 1                      # 1 = normal, -1 = outlier
    removed  = int((~mask).sum())

    # Build explanation (which rows + their anomaly scores)
    scores   = iso.decision_function(df[numeric_cols])
    outlier_df = df[~mask].copy()
    outlier_df['_anomaly_score'] = scores[~mask]
    explanation = outlier_df.head(10).to_dict(orient='records')

    return df[mask].reset_index(drop=True), removed, explanation


# ─────────────────────────────────────────────
# STEP 6 — SCALE NUMERIC COLUMNS ONLY
# ─────────────────────────────────────────────
def scale_numeric(df: pd.DataFrame, numeric_cols: list) -> pd.DataFrame:
    if not numeric_cols:
        return df
    scaler          = StandardScaler()
    df[numeric_cols] = scaler.fit_transform(df[numeric_cols])
    return df


# ─────────────────────────────────────────────
# STEP 7 — AUTOML PREVIEW
# ─────────────────────────────────────────────
def automl_preview(df: pd.DataFrame, target_col: str = None) -> dict:
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    if len(numeric_cols) < 2:
        return {'error': 'Not enough numeric columns for AutoML preview'}

    # Auto-pick target: last numeric column if not specified
    if not target_col or target_col not in numeric_cols:
        target_col = numeric_cols[-1]

    feature_cols = [c for c in numeric_cols if c != target_col]
    X = df[feature_cols].dropna()
    y = df.loc[X.index, target_col]

    # Decide task type
    unique_ratio = y.nunique() / len(y)
    is_classification = unique_ratio < 0.05 or y.nunique() <= 10

    try:
        if is_classification:
            model  = RandomForestClassifier(n_estimators=50, random_state=42)
            scores = cross_val_score(model, X, y, cv=3, scoring='accuracy')
            metric = 'accuracy'
        else:
            model  = RandomForestRegressor(n_estimators=50, random_state=42)
            scores = cross_val_score(model, X, y, cv=3, scoring='r2')
            metric = 'r2_score'

        return {
            'target':    target_col,
            'task':      'classification' if is_classification else 'regression',
            'metric':    metric,
            'score':     round(float(scores.mean()), 3),
            'std':       round(float(scores.std()), 3),
            'features':  len(feature_cols),
        }
    except Exception as e:
        return {'error': str(e)}


# ─────────────────────────────────────────────
# MAIN PIPELINE — called by app.py / main.py
# ─────────────────────────────────────────────
def run_pipeline(
    filepath:      str,
    impute_method: str   = 'knn',
    knn_neighbors: int   = 5,
    contamination: float = 0.05,
    scale:         bool  = False,   # OFF by default so output stays readable
    encode:        bool  = False,   # ← ENCODING IS OFF — no column explosion
    run_automl:    bool  = False,
) -> tuple:

    print("=" * 50)
    print("STARTING CLEAN MASTER PIPELINE")
    print("=" * 50)

    # ── Load ──────────────────────────────────
    df = load_dataset(filepath)
    print(f"Loaded: {df.shape[0]} rows × {df.shape[1]} cols")

    # ── EDA snapshot ─────────────────────────
    eda = auto_eda(df)

    # ── Pre-score ────────────────────────────
    pre_quality = compute_quality_score(df, is_post_clean=False)
    print(f"Pre-clean quality: {pre_quality['score']} ({pre_quality['grade']})")

    # ── Detect column types ───────────────────
    id_cols, numeric_cols, text_cols = detect_column_types(df)
    print(f"ID cols (untouched): {id_cols}")
    print(f"Numeric cols: {numeric_cols}")
    print(f"Text cols: {text_cols}")

    # ── Remove duplicates ─────────────────────
    dupes_removed = int(df.duplicated().sum())
    df = df.drop_duplicates().reset_index(drop=True)

    # ── Handle missing values ─────────────────
    original_rows = len(df)
    total_missing = int(df.isnull().sum().sum())
    df, values_filled = handle_missing(df, numeric_cols, text_cols,
                                       method=impute_method,
                                       knn_neighbors=knn_neighbors)
    print(f"Missing values filled: {values_filled}")

    # ── Remove outliers ───────────────────────
    outlier_explanation = []
    outliers_removed    = 0
    if len(numeric_cols) >= 2:
        result           = remove_outliers(df, numeric_cols, contamination)
        df, outliers_removed, outlier_explanation = result
    print(f"Outliers removed: {outliers_removed}")

    # ── Scale numeric only (optional) ─────────
    if scale and numeric_cols:
        df = scale_numeric(df, numeric_cols)
        print("Scaling applied to numeric columns only")

    # NOTE: Encoding is intentionally skipped.
    # Categorical columns keep their original text values.
    # This keeps output clean, readable, and avoids column explosion.

    # ── Post-score ────────────────────────────
    post_quality = compute_quality_score(df, is_post_clean=True)
    post_quality['score'] = max(post_quality['score'], pre_quality['score'])
    print(f"Post-clean quality: {post_quality['score']} ({post_quality['grade']})")

    # ── AutoML (optional) ─────────────────────
    automl_result = {}
    if run_automl:
        automl_result = automl_preview(df)

    # ── Save cleaned file ─────────────────────
    import os
    os.makedirs('outputs', exist_ok=True)
    base_name    = os.path.splitext(os.path.basename(filepath))[0]
    output_path  = f"outputs/{base_name}_cleaned.csv"
    df.to_csv(output_path, index=False)
    print(f"Saved: {output_path}")

    # ── Stats dict (used by app.py / main.py) ─
    stats = {
        'original_rows':      original_rows,
        'cleaned_rows':       len(df),
        'original_cols':      eda['shape'][1],
        'cleaned_cols':       df.shape[1],
        'total_missing':      total_missing,
        'missing_values':     values_filled,
        'outliers_removed':   outliers_removed,
        'duplicates_removed': dupes_removed,
        'pre_quality_score':  pre_quality,
        'post_quality_score': post_quality,
        'automl':             automl_result,
        'outlier_explanation': outlier_explanation,
        'id_cols':            id_cols,
        'numeric_cols':       numeric_cols,
        'text_cols':          text_cols,
        'output_path':        output_path,
    }

    print("=" * 50)
    print(f"DONE — {original_rows} → {len(df)} rows")
    print("=" * 50)

    return df, stats, eda