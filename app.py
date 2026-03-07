# ============================================================
# app.py — Clean Matrix Web Application v2
# Run with:  streamlit run app.py
# ============================================================

import streamlit as st
import pandas as pd
import os
import tempfile
from pipeline import run_pipeline
from database import login_user, signup_user, save_dataset_record, save_processed_record

# ---- Page configuration ----
st.set_page_config(
    page_title='Clean Matrix',
    page_icon='🧹',
    layout='wide'
)

# ---- Custom CSS ----
st.markdown('''
<style>
  .main-title {
    font-size: 3rem; font-weight: 900;
    color: #1F4E79; text-align: center; margin-bottom: 0.2rem;
  }
  .sub-title {
    font-size: 1.1rem; color: #555;
    text-align: center; margin-bottom: 2rem;
  }
  .stButton > button { width: 100%; border-radius: 8px; font-size: 1rem; }
  .stat-card {
    background: #f0f7ff; border-radius: 10px;
    padding: 1rem; text-align: center; border: 1px solid #cce0ff;
  }
</style>
''', unsafe_allow_html=True)

# ---- Session State ----
if 'logged_in'       not in st.session_state: st.session_state.logged_in = False
if 'user'            not in st.session_state: st.session_state.user = None
if 'page'            not in st.session_state: st.session_state.page = 'login'
if 'cleaned_result'  not in st.session_state: st.session_state.cleaned_result = None


# ============================================================
# PAGE 1: LOGIN / SIGNUP
# ============================================================
def show_login_page():
    st.markdown('<div class="main-title">🧹 Clean Matrix</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-title">Automated Data Quality & Imputation Pipeline</div>',
                unsafe_allow_html=True)

    # Top-right buttons
    _, c1, c2 = st.columns([6, 1, 1])
    with c1:
        if st.button('Login',  key='top_login'):  st.session_state.page = 'login'
    with c2:
        if st.button('Signup', key='top_signup'): st.session_state.page = 'signup'

    st.divider()
    _, center, _ = st.columns([2, 2, 2])

    with center:
        if st.session_state.page == 'signup':
            st.subheader('📝 Create Account')
            new_user  = st.text_input('Choose a Username', key='su_user')
            new_pass  = st.text_input('Choose a Password', type='password', key='su_pass')
            new_pass2 = st.text_input('Confirm Password',  type='password', key='su_pass2')

            if st.button('Sign Up', type='primary'):
                if not new_user or not new_pass:
                    st.error('Please fill in all fields.')
                elif new_pass != new_pass2:
                    st.error('Passwords do not match!')
                else:
                    result = signup_user(new_user, new_pass)
                    if result['success']:
                        st.success('Account created! Please log in.')
                        st.session_state.page = 'login'
                        st.rerun()
                    else:
                        st.error(result['message'])
        else:
            st.subheader('👋 Welcome Back!')
            username = st.text_input('Username', key='li_user')
            password = st.text_input('Password', type='password', key='li_pass')

            if st.button('Login', type='primary'):
                if not username or not password:
                    st.error('Please enter username and password.')
                else:
                    result = login_user(username, password)
                    if result['success']:
                        st.session_state.logged_in = True
                        st.session_state.user = result['user']
                        st.rerun()
                    else:
                        st.error(result['message'])


# ============================================================
# PAGE 2: UPLOAD & CLEAN
# ============================================================
def show_upload_page():
    user = st.session_state.user

    # Sidebar
    with st.sidebar:
        st.markdown('### 🧹 Clean Matrix')
        st.markdown(f'👤 **{user["username"]}**')
        st.divider()
        if st.button('🏠 Home'):
            st.session_state.cleaned_result = None
            st.rerun()
        st.divider()
        if st.button('🚪 Logout'):
            st.session_state.logged_in = False
            st.session_state.user = None
            st.session_state.page = 'login'
            st.session_state.cleaned_result = None
            st.rerun()

    # Header
    st.markdown('<div class="main-title">🧹 Clean Matrix</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-title">Upload your dataset — we clean it automatically</div>',
                unsafe_allow_html=True)
    st.divider()

    # Upload section
    _, center, _ = st.columns([1, 3, 1])
    with center:
        st.markdown('### 📁 Upload Your Dataset')
        uploaded_file = st.file_uploader(
            label='Drop your CSV file here or click Browse',
            type=['csv'],
            help='Accepted format: CSV files only'
        )

        if uploaded_file is not None:
            df_preview = pd.read_csv(uploaded_file)
            uploaded_file.seek(0)  # reset file pointer after reading

            # Show before-cleaning info
            st.markdown(f'**File:** `{uploaded_file.name}` &nbsp;|&nbsp; '
                        f'**Rows:** {df_preview.shape[0]} &nbsp;|&nbsp; '
                        f'**Columns:** {df_preview.shape[1]}')

            # Show missing values before cleaning
            missing_before = df_preview.isnull().sum()
            missing_cols = missing_before[missing_before > 0]
            if len(missing_cols) > 0:
                st.warning(f'⚠️ Found **{missing_before.sum()}** missing values in '
                           f'**{len(missing_cols)}** columns before cleaning.')
            else:
                st.success('✅ No missing values detected in this dataset.')

            st.markdown('**Preview (first 5 rows — BEFORE cleaning):**')
            st.dataframe(df_preview.head(5), use_container_width=True)

            # Options
            st.markdown('---')
            apply_scaling = st.checkbox(
                '📏 Apply StandardScaler (normalizes numbers — uncheck to keep original values)',
                value=False,
                help='Scaling changes numbers to a -3 to +3 range. Uncheck if you want readable numbers.'
            )

            if st.button('🚀 Run Cleaning Pipeline', type='primary'):
                with st.spinner('🔄 Cleaning your data... please wait...'):
                    # Save to temp file
                    with tempfile.NamedTemporaryFile(delete=False, suffix='.csv', mode='wb') as tmp:
                        tmp.write(uploaded_file.getvalue())
                        tmp_path = tmp.name

                    # Run pipeline
                    df_clean, stats, output_path = run_pipeline(
                        tmp_path,
                        output_dir='outputs',
                        scale=apply_scaling
                    )

                    # Save records to database
                    dataset_id = save_dataset_record(
                        user['id'], uploaded_file.name,
                        df_preview.shape[0], df_preview.shape[1]
                    )
                    if dataset_id:
                        save_processed_record(
                            dataset_id, user['id'],
                            uploaded_file.name,
                            os.path.basename(output_path),
                            stats['missing_values'],
                            stats['outliers_removed']
                        )

                    st.session_state.cleaned_result = {
                        'df':          df_clean,
                        'stats':       stats,
                        'output_path': output_path,
                        'filename':    uploaded_file.name,
                        'df_before':   df_preview
                    }
                    os.unlink(tmp_path)
                st.rerun()

    # ---- Results Section ----
    if st.session_state.cleaned_result:
        result = st.session_state.cleaned_result
        stats  = result['stats']

        st.divider()
        st.markdown('## ✅ Cleaning Complete!')

        # Metrics row
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric('Original Rows',     stats['original_rows'])
        c2.metric('Cleaned Rows',      stats['cleaned_rows'],
                  delta=f'-{stats["original_rows"] - stats["cleaned_rows"]} removed')
        c3.metric('Missing Filled',    stats['missing_values'])
        c4.metric('Duplicates Removed',stats['duplicates_removed'])
        c5.metric('Outliers Removed',  stats['outliers_removed'])

        # Missing values detail
        if stats['missing_by_col']:
            st.markdown('**Missing Values Fixed (by column):**')
            miss_df = pd.DataFrame(
                list(stats['missing_by_col'].items()),
                columns=['Column', 'Missing Values Filled']
            )
            st.dataframe(miss_df, use_container_width=True)

        # Before vs After comparison
        st.markdown('### 🔍 Before vs After Comparison')
        col_before, col_after = st.columns(2)

        with col_before:
            st.markdown('**BEFORE Cleaning (first 5 rows):**')
            st.dataframe(result['df_before'].head(5), use_container_width=True)
            st.caption(f"Missing values: {result['df_before'].isnull().sum().sum()}")

        with col_after:
            st.markdown('**AFTER Cleaning (first 5 rows):**')
            st.dataframe(result['df'].head(5), use_container_width=True)
            st.caption(f"Missing values: {result['df'].isnull().sum().sum()}")

        # Download
        st.divider()
        st.markdown('### ⬇️ Download Your Cleaned Dataset')
        csv_data   = result['df'].to_csv(index=False).encode('utf-8')
        clean_name = result['filename'].replace('.csv', '_cleaned.csv')

        _, dl_col, _ = st.columns([2, 2, 2])
        with dl_col:
            st.download_button(
                label='📥 Download Cleaned CSV',
                data=csv_data,
                file_name=clean_name,
                mime='text/csv',
                type='primary'
            )


# ============================================================
# MAIN ROUTER
# ============================================================
def main():
    if st.session_state.logged_in:
        show_upload_page()
    else:
        show_login_page()

if __name__ == '__main__':
    main()
