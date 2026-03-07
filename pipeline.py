# ============================================================
# pipeline.py — Improved Data Cleaning Pipeline v2
# Fixes: text column imputation, better outlier handling,
#        option to keep original scale for readability
# ============================================================

import pandas as pd
import numpy as np
import os
from sklearn.impute import KNNImputer
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler


def load_dataset(filepath):
    """Load a CSV file into a pandas DataFrame."""
    df = pd.read_csv(filepath)
    print(f'Loaded dataset: {df.shape[0]} rows, {df.shape[1]} columns')
    return df


def detect_missing_values(df):
    """Find and report all missing values in the dataset."""
    missing = df.isnull().sum()
    total_missing = int(missing.sum())
    missing_report = missing[missing > 0].to_dict()

    print(f'Total missing values found: {total_missing}')
    if missing_report:
        print('Columns with missing values:')
        for col, count in missing_report.items():
            print(f'  {col}: {count} missing')
    else:
        print('No missing values found!')

    return {'total': total_missing, 'by_column': missing_report}


def fill_text_columns(df):
    """
    Fill missing values in TEXT columns.
    Strategy:
      - If a text column has few unique values (like a category), fill with MODE (most common value)
      - If it has many unique values (like a name/description), fill with 'Unknown'
    """
    df = df.copy()
    text_cols = df.select_dtypes(exclude=[np.number]).columns.tolist()

    for col in text_cols:
        if df[col].isnull().sum() > 0:
            unique_count = df[col].nunique()
            if unique_count <= 20:
                # Fill with most common value (mode)
                fill_val = df[col].mode()[0]
                df[col] = df[col].fillna(fill_val)
                print(f'  Text column "{col}": filled with mode -> "{fill_val}"')
            else:
                # Fill with 'Unknown'
                df[col] = df[col].fillna('Unknown')
                print(f'  Text column "{col}": filled with "Unknown"')

    return df


def fill_numeric_columns(df, n_neighbors=5):
    """
    Fill missing values in NUMERIC columns using KNN Imputation.
    KNN looks at the 5 most similar rows and uses their average.
    """
    df = df.copy()
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()

    if not numeric_cols:
        print('  No numeric columns to impute.')
        return df

    # Check if there are actually missing values
    missing_in_numeric = df[numeric_cols].isnull().sum().sum()
    if missing_in_numeric == 0:
        print('  No missing values in numeric columns.')
        return df

    imputer = KNNImputer(n_neighbors=n_neighbors)
    df[numeric_cols] = imputer.fit_transform(df[numeric_cols])
    print(f'  KNN Imputation applied on {len(numeric_cols)} numeric columns.')
    return df


def detect_and_remove_outliers(df, contamination=0.05):
    """
    Isolation Forest detects and removes outlier rows.
    contamination=0.05 means we expect ~5% of rows to be outliers.
    """
    df = df.copy()
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()

    if len(numeric_cols) < 2:
        print('  Not enough numeric columns for outlier detection. Skipping.')
        return df, 0

    iso_forest = IsolationForest(contamination=contamination, random_state=42)
    predictions = iso_forest.fit_predict(df[numeric_cols])

    outlier_count = int(np.sum(predictions == -1))
    print(f'  Outliers detected and removed: {outlier_count} rows')

    df = df[predictions == 1].reset_index(drop=True)
    return df, outlier_count


def apply_scaling(df):
    """
    StandardScaler rescales numeric columns to mean=0, std=1.
    Only applied if user requests it.
    """
    df = df.copy()
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()

    if not numeric_cols:
        print('  No numeric columns to scale.')
        return df

    scaler = StandardScaler()
    df[numeric_cols] = scaler.fit_transform(df[numeric_cols])
    print(f'  StandardScaler applied on {len(numeric_cols)} columns.')
    return df


def remove_duplicate_rows(df):
    """Remove completely duplicate rows."""
    before = len(df)
    df = df.drop_duplicates().reset_index(drop=True)
    removed = before - len(df)
    if removed > 0:
        print(f'  Duplicate rows removed: {removed}')
    else:
        print('  No duplicate rows found.')
    return df, removed


def run_pipeline(filepath, output_dir='outputs', scale=True):
    """
    Run the full cleaning pipeline on a CSV file.
    Returns: (cleaned_df, stats_dict, output_filepath)
    """
    print('=' * 50)
    print('STARTING DATA CLEANING PIPELINE v2')
    print('=' * 50)

    # Step 1: Load
    df = load_dataset(filepath)
    original_shape = df.shape

    # Step 2: Remove duplicates first
    print('\n[1] Removing Duplicate Rows...')
    df, duplicates_removed = remove_duplicate_rows(df)

    # Step 3: Detect missing values
    print('\n[2] Detecting Missing Values...')
    missing_report = detect_missing_values(df)

    # Step 4: Fill TEXT column missing values
    print('\n[3] Filling Missing Text Column Values...')
    df = fill_text_columns(df)

    # Step 5: Fill NUMERIC column missing values with KNN
    print('\n[4] Applying KNN Imputation on Numeric Columns...')
    df = fill_numeric_columns(df)

    # Step 6: Verify all missing values are gone
    remaining_missing = int(df.isnull().sum().sum())
    print(f'\n[5] Verifying Clean Data...')
    print(f'  Remaining missing values: {remaining_missing}')

    # Step 7: Remove outliers
    print('\n[6] Detecting Outliers (Isolation Forest)...')
    df, outliers_removed = detect_and_remove_outliers(df)

    # Step 8: Scale (optional)
    if scale:
        print('\n[7] Applying StandardScaler...')
        df = apply_scaling(df)
    else:
        print('\n[7] Scaling skipped (user choice).')

    # Step 9: Save cleaned dataset
    os.makedirs(output_dir, exist_ok=True)
    base_name = os.path.basename(filepath)
    base_name = base_name.replace('.csv', '').replace('.CSV', '')
    output_path = os.path.join(output_dir, f'{base_name}_cleaned.csv')
    df.to_csv(output_path, index=False)

    stats = {
        'original_rows':    original_shape[0],
        'original_cols':    original_shape[1],
        'cleaned_rows':     df.shape[0],
        'cleaned_cols':     df.shape[1],
        'missing_values':   missing_report['total'],
        'missing_by_col':   missing_report['by_column'],
        'duplicates_removed': duplicates_removed,
        'outliers_removed': outliers_removed,
        'remaining_missing': remaining_missing,
        'output_path':      output_path
    }

    print('\n' + '=' * 50)
    print('PIPELINE COMPLETE!')
    print(f'Original : {stats["original_rows"]} rows x {stats["original_cols"]} cols')
    print(f'Cleaned  : {stats["cleaned_rows"]} rows x {stats["cleaned_cols"]} cols')
    print(f'Missing values filled : {stats["missing_values"]}')
    print(f'Duplicates removed    : {stats["duplicates_removed"]}')
    print(f'Outliers removed      : {stats["outliers_removed"]}')
    print(f'Saved to : {output_path}')
    print('=' * 50)

    return df, stats, output_path


# ---- Run directly to test ----
if __name__ == '__main__':
    run_pipeline('data/electric_vehicles_spec_2025.csv', scale=False)
