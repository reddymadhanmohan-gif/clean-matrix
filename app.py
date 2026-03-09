"""
app.py — Clean Matrix  (Redesigned UI)
Run with:  streamlit run app.py
"""

import streamlit as st
import pandas as pd
import os
import tempfile
from pipeline import run_pipeline
from database import login_user, signup_user, save_dataset_record, save_processed_record

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Clean Matrix | AI Data Quality",
    page_icon="🧹",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Master CSS ─────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@300;400;500;600;700;800&family=JetBrains+Mono:wght@400;600&display=swap');

*,*::before,*::after{box-sizing:border-box;}
html,body,[data-testid="stAppViewContainer"],[data-testid="stMain"],.main{
    font-family:'Plus Jakarta Sans',sans-serif!important;
    background:#0a0e1a!important;color:#e2e8f0!important;}
[data-testid="stSidebar"]{
    background:linear-gradient(180deg,#0d1526 0%,#111827 100%)!important;
    border-right:1px solid rgba(99,102,241,0.2)!important;}
[data-testid="stSidebar"] *{color:#cbd5e1!important;}
#MainMenu,footer,header{visibility:hidden;}
[data-testid="stDecoration"]{display:none;}
.block-container{padding:0!important;max-width:100%!important;}

.cm-navbar{background:rgba(10,14,26,0.95);backdrop-filter:blur(20px);
    border-bottom:1px solid rgba(99,102,241,0.25);padding:0 2.5rem;height:64px;
    display:flex;align-items:center;justify-content:space-between;
    position:sticky;top:0;z-index:999;}
.cm-logo{display:flex;align-items:center;gap:10px;font-size:1.3rem;font-weight:800;
    background:linear-gradient(135deg,#818cf8,#c084fc,#38bdf8);
    -webkit-background-clip:text;-webkit-text-fill-color:transparent;letter-spacing:-0.5px;}

.cm-hero{min-height:88vh;display:flex;flex-direction:column;align-items:center;
    justify-content:center;text-align:center;padding:4rem 2rem;
    background:radial-gradient(ellipse 80% 50% at 50% -10%,rgba(99,102,241,0.18) 0%,transparent 60%),
    radial-gradient(ellipse 60% 40% at 80% 80%,rgba(192,132,252,0.1) 0%,transparent 50%),#0a0e1a;
    position:relative;overflow:hidden;}
.cm-hero::before{content:'';position:absolute;inset:0;
    background-image:linear-gradient(rgba(99,102,241,0.04) 1px,transparent 1px),
    linear-gradient(90deg,rgba(99,102,241,0.04) 1px,transparent 1px);
    background-size:60px 60px;pointer-events:none;}

.cm-badge{display:inline-flex;align-items:center;gap:6px;
    background:rgba(99,102,241,0.12);border:1px solid rgba(99,102,241,0.3);
    border-radius:99px;padding:6px 16px;font-size:0.78rem;font-weight:600;
    color:#a5b4fc;letter-spacing:0.5px;text-transform:uppercase;margin-bottom:1.5rem;}
.cm-hero-title{font-size:clamp(2.8rem,6vw,5rem);font-weight:800;line-height:1.1;
    letter-spacing:-2px;margin:0 0 1.2rem;
    background:linear-gradient(135deg,#ffffff 0%,#c7d2fe 50%,#a78bfa 100%);
    -webkit-background-clip:text;-webkit-text-fill-color:transparent;}
.cm-hero-sub{font-size:1.15rem;color:#94a3b8;max-width:560px;line-height:1.7;margin:0 auto 3rem;}

.cm-pills{display:flex;flex-wrap:wrap;justify-content:center;gap:10px;margin-bottom:2.5rem;}
.cm-pill{background:rgba(15,23,42,0.8);border:1px solid rgba(99,102,241,0.25);
    border-radius:99px;padding:6px 14px;font-size:0.8rem;font-weight:500;color:#94a3b8;}
.cm-pill b{color:#a5b4fc;}

.cm-stats{display:flex;justify-content:center;background:rgba(15,23,42,0.6);
    border:1px solid rgba(99,102,241,0.18);border-radius:16px;overflow:hidden;
    max-width:620px;width:100%;margin-top:1rem;}
.cm-stat{flex:1;padding:1.1rem 1rem;text-align:center;border-right:1px solid rgba(99,102,241,0.15);}
.cm-stat:last-child{border-right:none;}
.cm-stat-num{font-size:1.5rem;font-weight:800;
    background:linear-gradient(135deg,#818cf8,#c084fc);-webkit-background-clip:text;-webkit-text-fill-color:transparent;}
.cm-stat-lbl{font-size:0.72rem;color:#64748b;font-weight:500;text-transform:uppercase;letter-spacing:0.5px;margin-top:2px;}

.cm-auth-card{width:100%;max-width:440px;background:rgba(15,23,42,0.9);
    border:1px solid rgba(99,102,241,0.25);border-radius:20px;padding:2.5rem 2.5rem 2rem;
    box-shadow:0 25px 80px rgba(0,0,0,0.6);backdrop-filter:blur(20px);}
.cm-auth-logo-text{font-size:1.6rem;font-weight:800;
    background:linear-gradient(135deg,#818cf8,#c084fc,#38bdf8);
    -webkit-background-clip:text;-webkit-text-fill-color:transparent;}
.cm-auth-title{text-align:center;font-size:1.4rem;font-weight:700;color:#f1f5f9;margin-bottom:0.3rem;}
.cm-auth-sub{text-align:center;font-size:0.85rem;color:#64748b;margin-bottom:1.8rem;}

.cm-steps{display:flex;justify-content:center;gap:0;flex-wrap:wrap;margin:1.5rem 0;}
.cm-step{display:flex;align-items:center;gap:8px;}
.cm-step-box{display:flex;flex-direction:column;align-items:center;
    background:rgba(15,23,42,0.8);border:1px solid rgba(99,102,241,0.2);
    border-radius:12px;padding:10px 14px;width:110px;transition:all 0.2s;}
.cm-step-box.active{border-color:rgba(99,102,241,0.6);background:rgba(99,102,241,0.12);
    box-shadow:0 0 20px rgba(99,102,241,0.15);}
.cm-step-icon{font-size:1.3rem;}
.cm-step-label{font-size:0.65rem;font-weight:600;color:#94a3b8;text-align:center;
    margin-top:4px;text-transform:uppercase;letter-spacing:0.3px;}
.cm-step-arrow{color:rgba(99,102,241,0.4);font-size:1rem;margin:0 2px;}

.cm-metric-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:1rem;margin:1.5rem 0;}
.cm-metric-card{background:rgba(15,23,42,0.8);border:1px solid rgba(99,102,241,0.2);
    border-radius:14px;padding:1.3rem 1rem;text-align:center;position:relative;overflow:hidden;}
.cm-metric-card::before{content:'';position:absolute;top:0;left:0;right:0;height:3px;
    background:linear-gradient(90deg,#818cf8,#c084fc);}
.cm-metric-icon{font-size:1.6rem;margin-bottom:0.5rem;}
.cm-metric-num{font-size:2rem;font-weight:800;
    background:linear-gradient(135deg,#818cf8,#c084fc);-webkit-background-clip:text;-webkit-text-fill-color:transparent;line-height:1;}
.cm-metric-lbl{font-size:0.75rem;font-weight:600;color:#64748b;text-transform:uppercase;letter-spacing:0.5px;margin-top:4px;}
.cm-metric-card.green::before{background:linear-gradient(90deg,#34d399,#10b981);}
.cm-metric-card.green .cm-metric-num{background:linear-gradient(135deg,#34d399,#10b981);-webkit-background-clip:text;}
.cm-metric-card.red::before{background:linear-gradient(90deg,#f87171,#ef4444);}
.cm-metric-card.red .cm-metric-num{background:linear-gradient(135deg,#f87171,#ef4444);-webkit-background-clip:text;}
.cm-metric-card.amber::before{background:linear-gradient(90deg,#fbbf24,#f59e0b);}
.cm-metric-card.amber .cm-metric-num{background:linear-gradient(135deg,#fbbf24,#f59e0b);-webkit-background-clip:text;}

.cm-section-title{font-size:1rem;font-weight:700;color:#e2e8f0;margin:1.5rem 0 0.8rem;
    display:flex;align-items:center;gap:8px;}
.cm-section-title::after{content:'';flex:1;height:1px;background:rgba(99,102,241,0.2);}

.cm-info{background:rgba(56,189,248,0.07);border:1px solid rgba(56,189,248,0.2);
    border-radius:10px;padding:0.85rem 1rem;font-size:0.85rem;color:#7dd3fc;margin:0.8rem 0;}
.cm-warning{background:rgba(251,191,36,0.07);border:1px solid rgba(251,191,36,0.2);
    border-radius:10px;padding:0.85rem 1rem;font-size:0.85rem;color:#fcd34d;margin:0.8rem 0;}
.cm-success{background:rgba(52,211,153,0.07);border:1px solid rgba(52,211,153,0.2);
    border-radius:10px;padding:0.85rem 1rem;font-size:0.85rem;color:#6ee7b7;margin:0.8rem 0;}

.cm-panel{background:rgba(15,23,42,0.6);border:1px solid rgba(99,102,241,0.15);
    border-radius:16px;padding:1.5rem;margin:1rem 0;}

.cm-fileinfo{display:flex;gap:1rem;flex-wrap:wrap;background:rgba(99,102,241,0.06);
    border:1px solid rgba(99,102,241,0.18);border-radius:10px;padding:0.8rem 1.2rem;
    margin:0.8rem 0;font-size:0.83rem;}
.cm-fileinfo-item{color:#94a3b8;}
.cm-fileinfo-item b{color:#a5b4fc;}

.cm-col-table{width:100%;border-collapse:collapse;font-size:0.83rem;font-family:'JetBrains Mono',monospace;}
.cm-col-table th{background:rgba(99,102,241,0.15);color:#a5b4fc;padding:8px 12px;
    text-align:left;font-weight:600;font-size:0.75rem;text-transform:uppercase;
    letter-spacing:0.5px;border-bottom:1px solid rgba(99,102,241,0.2);}
.cm-col-table td{padding:7px 12px;color:#cbd5e1;border-bottom:1px solid rgba(99,102,241,0.08);}
.cm-col-table tr:hover td{background:rgba(99,102,241,0.05);}
.cm-bar{height:6px;border-radius:99px;background:linear-gradient(90deg,#818cf8,#c084fc);min-width:4px;}

.cm-sidebar-logo{font-size:1.2rem;font-weight:800;
    background:linear-gradient(135deg,#818cf8,#c084fc,#38bdf8);
    -webkit-background-clip:text;-webkit-text-fill-color:transparent;padding:0.5rem 0 1rem;}
.cm-user-chip{background:rgba(99,102,241,0.1);border:1px solid rgba(99,102,241,0.25);
    border-radius:10px;padding:0.7rem;margin-bottom:1rem;font-size:0.82rem;color:#94a3b8!important;}
.cm-user-chip b{color:#a5b4fc!important;}

.cm-download-card{background:linear-gradient(135deg,rgba(99,102,241,0.12),rgba(192,132,252,0.08));
    border:1px solid rgba(99,102,241,0.3);border-radius:16px;padding:1.8rem;
    text-align:center;margin:1rem 0;}
.cm-download-title{font-size:1.1rem;font-weight:700;color:#e2e8f0;margin-bottom:0.4rem;}
.cm-download-sub{font-size:0.83rem;color:#64748b;margin-bottom:1.2rem;}

/* Widget overrides */
[data-testid="stTextInput"] input{
    background:rgba(15,23,42,0.8)!important;border:1px solid rgba(99,102,241,0.3)!important;
    border-radius:10px!important;color:#e2e8f0!important;font-family:'Plus Jakarta Sans',sans-serif!important;}
[data-testid="stTextInput"] input:focus{border-color:rgba(99,102,241,0.7)!important;
    box-shadow:0 0 0 3px rgba(99,102,241,0.12)!important;outline:none!important;}
[data-testid="stTextInput"] label{font-size:0.82rem!important;font-weight:600!important;
    color:#94a3b8!important;text-transform:uppercase!important;letter-spacing:0.5px!important;}

button[data-testid="stBaseButton-primary"],
[data-testid="stButton"] button[kind="primary"]{
    background:linear-gradient(135deg,#6366f1,#8b5cf6)!important;border:none!important;
    border-radius:10px!important;color:white!important;font-weight:700!important;
    font-family:'Plus Jakarta Sans',sans-serif!important;
    box-shadow:0 4px 15px rgba(99,102,241,0.3)!important;transition:all 0.2s!important;}
button[data-testid="stBaseButton-primary"]:hover{transform:translateY(-1px)!important;
    box-shadow:0 6px 20px rgba(99,102,241,0.4)!important;}

button[data-testid="stBaseButton-secondary"],
[data-testid="stButton"] button:not([kind="primary"]){
    background:rgba(15,23,42,0.8)!important;border:1px solid rgba(99,102,241,0.3)!important;
    border-radius:10px!important;color:#a5b4fc!important;font-weight:600!important;
    font-family:'Plus Jakarta Sans',sans-serif!important;transition:all 0.2s!important;}

[data-testid="stDownloadButton"] button{
    background:linear-gradient(135deg,#059669,#10b981)!important;border:none!important;
    border-radius:10px!important;color:white!important;font-weight:700!important;
    font-family:'Plus Jakarta Sans',sans-serif!important;
    box-shadow:0 4px 15px rgba(16,185,129,0.3)!important;
    width:100%!important;padding:0.65rem!important;}

[data-testid="stFileUploader"]{background:rgba(15,23,42,0.6)!important;
    border:2px dashed rgba(99,102,241,0.3)!important;border-radius:14px!important;
    padding:1rem!important;}

[data-testid="stCheckbox"] label{color:#94a3b8!important;font-size:0.85rem!important;}
[data-testid="stDataFrame"]{border:1px solid rgba(99,102,241,0.2)!important;border-radius:10px!important;overflow:hidden!important;}

::-webkit-scrollbar{width:6px;height:6px;}
::-webkit-scrollbar-track{background:rgba(15,23,42,0.5);}
::-webkit-scrollbar-thumb{background:rgba(99,102,241,0.4);border-radius:99px;}
</style>
""", unsafe_allow_html=True)


# ── Session State ──────────────────────────────────────────────────────────────
for k, v in {
    "logged_in": False, "user": None,
    "auth_mode": "login", "cleaned_result": None,
}.items():
    if k not in st.session_state:
        st.session_state[k] = v


# ── Helpers ────────────────────────────────────────────────────────────────────
def navbar(logged_in=False, username=""):
    right = (f'<span style="font-size:0.8rem;color:#64748b;">Signed in as '
             f'<b style="color:#a5b4fc;">{username}</b></span>') if logged_in else ""
    st.markdown(f"""
    <div class="cm-navbar">
      <div class="cm-logo">🧹 Clean Matrix</div>
      {right}
    </div>""", unsafe_allow_html=True)


def pipeline_steps(active=-1):
    steps = [("📤","Upload"),("🔍","Detect"),("✨","Impute"),
             ("🎯","Outliers"),("📏","Scale"),("💾","Export")]
    html = '<div class="cm-steps">'
    for i,(icon,label) in enumerate(steps):
        cls = "cm-step-box active" if i==active else "cm-step-box"
        arrow = '<span class="cm-step-arrow">→</span>' if i<len(steps)-1 else ""
        html += (f'<div class="cm-step">'
                 f'<div class="{cls}"><span class="cm-step-icon">{icon}</span>'
                 f'<span class="cm-step-label">{label}</span></div>{arrow}</div>')
    html += "</div>"
    st.markdown(html, unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
#  AUTH PAGE
# ══════════════════════════════════════════════════════════════════════════════
def show_auth_page():
    navbar()
    is_signup = st.session_state.auth_mode == "signup"

    _, col, _ = st.columns([1, 1.3, 1])
    with col:
        st.markdown("<br><br>", unsafe_allow_html=True)
        st.markdown(f"""
        <div style="text-align:center;margin-bottom:1rem;">
          <span class="cm-auth-logo-text">🧹 Clean Matrix</span>
        </div>
        <div class="cm-auth-title">{"Create your account" if is_signup else "Welcome back"}</div>
        <div class="cm-auth-sub">{"Start cleaning datasets in seconds" if is_signup else "Sign in to continue cleaning data"}</div>
        """, unsafe_allow_html=True)

        c_login, c_signup = st.columns(2)
        with c_login:
            if st.button("Sign In", use_container_width=True,
                         type="primary" if not is_signup else "secondary"):
                st.session_state.auth_mode = "login"; st.rerun()
        with c_signup:
            if st.button("Sign Up", use_container_width=True,
                         type="primary" if is_signup else "secondary"):
                st.session_state.auth_mode = "signup"; st.rerun()

        st.markdown("<br>", unsafe_allow_html=True)

        if is_signup:
            new_user  = st.text_input("Username", placeholder="Choose a username", key="su_user")
            new_pass  = st.text_input("Password", placeholder="Create a password", type="password", key="su_pass")
            new_pass2 = st.text_input("Confirm Password", placeholder="Repeat password", type="password", key="su_pass2")
            if st.button("Create Account →", type="primary", use_container_width=True):
                if not new_user or not new_pass:
                    st.error("Please fill in all fields.")
                elif len(new_pass) < 4:
                    st.error("Password must be at least 4 characters.")
                elif new_pass != new_pass2:
                    st.error("Passwords do not match!")
                else:
                    with st.spinner("Creating account..."):
                        r = signup_user(new_user, new_pass)
                    if r["success"]:
                        st.success("Account created! Please sign in.")
                        st.session_state.auth_mode = "login"; st.rerun()
                    else:
                        st.error(r["message"])
        else:
            username = st.text_input("Username", placeholder="Your username", key="li_user")
            password = st.text_input("Password", placeholder="Your password", type="password", key="li_pass")
            if st.button("Sign In →", type="primary", use_container_width=True):
                if not username or not password:
                    st.error("Please enter your username and password.")
                else:
                    with st.spinner("Signing in..."):
                        r = login_user(username, password)
                    if r["success"]:
                        st.session_state.logged_in = True
                        st.session_state.user = r["user"]
                        st.session_state.cleaned_result = None
                        st.rerun()
                    else:
                        st.error("Invalid username or password.")


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN APP
# ══════════════════════════════════════════════════════════════════════════════
def show_upload_page():
    user     = st.session_state.user
    username = user["username"]
    navbar(logged_in=True, username=username)

    # Sidebar
    with st.sidebar:
        st.markdown(f'<div class="cm-sidebar-logo">🧹 Clean Matrix</div>', unsafe_allow_html=True)
        st.markdown(f'<div class="cm-user-chip">👤 &nbsp;<b>{username}</b></div>', unsafe_allow_html=True)
        st.markdown("**Navigation**")
        if st.button("🏠  Dashboard", use_container_width=True):
            st.session_state.cleaned_result = None; st.rerun()
        if st.button("📂  New Upload", use_container_width=True):
            st.session_state.cleaned_result = None; st.rerun()
        st.markdown("---")
        st.markdown('<p style="font-size:0.78rem;color:#475569;line-height:1.6;">Clean Matrix applies KNN Imputation, Isolation Forest, and StandardScaler to clean your CSV data automatically.</p>', unsafe_allow_html=True)
        st.markdown("---")
        if st.button("🚪  Sign Out", use_container_width=True):
            st.session_state.logged_in = False
            st.session_state.user = None
            st.session_state.cleaned_result = None
            st.session_state.auth_mode = "login"
            st.rerun()

    # Main
    main_col, _ = st.columns([5, 1])
    with main_col:
        st.markdown('<div style="padding:2rem 2rem 0;">', unsafe_allow_html=True)

        # ── Results ──────────────────────────────────────────────────────
        if st.session_state.cleaned_result:
            r             = st.session_state.cleaned_result
            stats         = r["stats"]
            df_raw        = r["df_raw"]
            df_clean      = r["df"]
            filename      = r["filename"]
            missing_by_col = r.get("missing_by_col", {})

            st.markdown('<div class="cm-success">✅ &nbsp;<b>Pipeline complete!</b> &nbsp; Your dataset has been cleaned and is ready to download.</div>', unsafe_allow_html=True)

            # Metrics
            st.markdown('<div class="cm-section-title">📊 Cleaning Summary</div>', unsafe_allow_html=True)
            st.markdown(f"""
            <div class="cm-metric-grid">
              <div class="cm-metric-card">
                <div class="cm-metric-icon">📋</div>
                <div class="cm-metric-num">{stats['original_rows']:,}</div>
                <div class="cm-metric-lbl">Original Rows</div>
              </div>
              <div class="cm-metric-card green">
                <div class="cm-metric-icon">✅</div>
                <div class="cm-metric-num">{stats['cleaned_rows']:,}</div>
                <div class="cm-metric-lbl">Clean Rows</div>
              </div>
              <div class="cm-metric-card amber">
                <div class="cm-metric-icon">🔧</div>
                <div class="cm-metric-num">{stats['missing_values']:,}</div>
                <div class="cm-metric-lbl">Values Filled</div>
              </div>
              <div class="cm-metric-card red">
                <div class="cm-metric-icon">🎯</div>
                <div class="cm-metric-num">{stats['outliers_removed']:,}</div>
                <div class="cm-metric-lbl">Outliers Removed</div>
              </div>
              <div class="cm-metric-card">
                <div class="cm-metric-icon">📐</div>
                <div class="cm-metric-num">{stats['original_cols']}</div>
                <div class="cm-metric-lbl">Columns</div>
              </div>
              <div class="cm-metric-card green">
                <div class="cm-metric-icon">💯</div>
                <div class="cm-metric-num">100%</div>
                <div class="cm-metric-lbl">Completeness</div>
              </div>
            </div>
            """, unsafe_allow_html=True)

            # Missing by column
            if missing_by_col:
                st.markdown('<div class="cm-section-title">🔍 Missing Values — By Column</div>', unsafe_allow_html=True)
                max_m = max(missing_by_col.values())
                rows_html = ""
                for col, cnt in sorted(missing_by_col.items(), key=lambda x: -x[1]):
                    pct = cnt / stats["original_rows"] * 100
                    bw  = int(cnt / max_m * 180)
                    rows_html += (f"<tr><td>{col}</td><td style='color:#fbbf24;'>{cnt:,}</td>"
                                  f"<td>{pct:.1f}%</td>"
                                  f"<td><div class='cm-bar' style='width:{bw}px;'></div></td>"
                                  f"<td style='color:#34d399;'>✓ Filled</td></tr>")
                st.markdown(f"""
                <div class="cm-panel" style="overflow-x:auto;">
                  <table class="cm-col-table">
                    <thead><tr><th>Column</th><th>Missing</th><th>% Rows</th><th>Visual</th><th>Status</th></tr></thead>
                    <tbody>{rows_html}</tbody>
                  </table>
                </div>""", unsafe_allow_html=True)

            # Before / After
            st.markdown('<div class="cm-section-title">🔄 Before vs After Preview</div>', unsafe_allow_html=True)
            c_a, c_b = st.columns(2)
            with c_a:
                st.markdown('<p style="font-size:0.8rem;font-weight:700;color:#f87171;margin-bottom:0.4rem;">⚠️ Raw Dataset</p>', unsafe_allow_html=True)
                st.dataframe(df_raw.head(8), use_container_width=True, height=240)
            with c_b:
                st.markdown('<p style="font-size:0.8rem;font-weight:700;color:#34d399;margin-bottom:0.4rem;">✅ Cleaned Dataset</p>', unsafe_allow_html=True)
                st.dataframe(df_clean.head(8), use_container_width=True, height=240)

            # Download
            st.markdown('<div class="cm-section-title">⬇️ Download</div>', unsafe_allow_html=True)
            csv_data   = df_clean.to_csv(index=False).encode("utf-8")
            clean_name = filename.replace(".csv", "_cleaned.csv")
            file_kb    = round(len(csv_data) / 1024, 1)
            st.markdown(f"""
            <div class="cm-download-card">
              <div class="cm-download-title">🎉 Your cleaned dataset is ready!</div>
              <div class="cm-download-sub">{clean_name} &nbsp;·&nbsp; {stats['cleaned_rows']:,} rows &nbsp;·&nbsp; {file_kb} KB</div>
            </div>""", unsafe_allow_html=True)
            st.download_button("⬇️  Download Cleaned CSV", csv_data, clean_name, "text/csv", use_container_width=True)
            st.markdown("<br>", unsafe_allow_html=True)
            if st.button("🔄  Clean Another Dataset", use_container_width=True):
                st.session_state.cleaned_result = None; st.rerun()

        # ── Upload ───────────────────────────────────────────────────────
        else:
            st.markdown("""
            <div style="margin-bottom:1.5rem;">
              <h2 style="font-size:1.8rem;font-weight:800;color:#e2e8f0;margin:0;letter-spacing:-0.5px;">Upload Your Dataset</h2>
              <p style="color:#64748b;margin:0.3rem 0 0;font-size:0.9rem;">Upload a CSV file and the pipeline will automatically clean it.</p>
            </div>""", unsafe_allow_html=True)

            pipeline_steps(active=0)

            uploaded_file = st.file_uploader(
                "Drop your CSV file here or click Browse",
                type=["csv"],
                help="Supported format: CSV files only",
            )

            if uploaded_file is not None:
                try:
                    df_preview = pd.read_csv(uploaded_file)
                    uploaded_file.seek(0)
                    missing_total = int(df_preview.isnull().sum().sum())
                    missing_cols  = int((df_preview.isnull().sum() > 0).sum())
                    dup_count     = int(df_preview.duplicated().sum())

                    st.markdown(f"""
                    <div class="cm-fileinfo">
                      <span class="cm-fileinfo-item">📄 <b>{uploaded_file.name}</b></span>
                      <span class="cm-fileinfo-item">📊 <b>{df_preview.shape[0]:,}</b> rows</span>
                      <span class="cm-fileinfo-item">🔢 <b>{df_preview.shape[1]}</b> cols</span>
                      <span class="cm-fileinfo-item">⚠️ <b>{missing_total:,}</b> missing</span>
                      <span class="cm-fileinfo-item">🔁 <b>{dup_count}</b> duplicates</span>
                    </div>""", unsafe_allow_html=True)

                    if missing_total > 0:
                        st.markdown(f'<div class="cm-warning">⚠️ Found <b>{missing_total:,} missing values</b> across <b>{missing_cols}</b> column(s). KNN Imputation will fill these automatically.</div>', unsafe_allow_html=True)
                    else:
                        st.markdown('<div class="cm-info">✨ No missing values detected. The pipeline will still check for outliers and duplicates.</div>', unsafe_allow_html=True)

                    st.markdown('<div class="cm-section-title">👁️ Data Preview</div>', unsafe_allow_html=True)
                    st.dataframe(df_preview.head(6), use_container_width=True, height=220)

                    st.markdown('<div class="cm-section-title">⚙️ Pipeline Options</div>', unsafe_allow_html=True)
                    apply_scaling = st.checkbox("Apply StandardScaler — normalise all numeric columns (mean=0, std=1)", value=False)
                    st.markdown("<br>", unsafe_allow_html=True)

                    if st.button("🚀  Run Cleaning Pipeline", type="primary", use_container_width=True):
                        with st.spinner("Running pipeline — this usually takes 5–15 seconds..."):
                            try:
                                with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
                                    tmp.write(uploaded_file.getvalue())
                                    tmp_path = tmp.name

                                df_raw_snap    = pd.read_csv(tmp_path)
                                missing_by_col = {
                                    c: int(n) for c, n in df_raw_snap.isnull().sum().items() if n > 0
                                }

                                df_clean, stats, output_path = run_pipeline(
                                    tmp_path, output_dir="outputs", scale=apply_scaling
                                )

                                try:
                                    dataset_id = save_dataset_record(
                                        user["id"], uploaded_file.name,
                                        df_preview.shape[0], df_preview.shape[1],
                                    )
                                    if dataset_id:
                                        save_processed_record(
                                            dataset_id, user["id"],
                                            uploaded_file.name, os.path.basename(output_path),
                                            stats["missing_values"], stats["outliers_removed"],
                                        )
                                except Exception:
                                    pass

                                st.session_state.cleaned_result = {
                                    "df": df_clean, "df_raw": df_raw_snap,
                                    "stats": stats, "output_path": output_path,
                                    "filename": uploaded_file.name,
                                    "missing_by_col": missing_by_col,
                                }
                                os.unlink(tmp_path)
                                st.rerun()
                            except Exception as e:
                                st.error(f"Pipeline error: {e}")
                                if "tmp_path" in locals() and os.path.exists(tmp_path):
                                    os.unlink(tmp_path)
                except Exception as e:
                    st.error(f"Could not read file: {e}")
            else:
                st.markdown("""
                <div style="background:rgba(15,23,42,0.6);border:2px dashed rgba(99,102,241,0.25);
                    border-radius:16px;padding:3rem;text-align:center;margin:1rem 0;">
                  <div style="font-size:3rem;margin-bottom:0.8rem;">📂</div>
                  <div style="font-size:1rem;font-weight:600;color:#94a3b8;">Drag and drop your CSV file in the uploader above</div>
                  <div style="font-size:0.8rem;color:#475569;margin-top:0.4rem;">Supported format: CSV &nbsp;·&nbsp; Max 200 MB</div>
                </div>""", unsafe_allow_html=True)

                st.markdown('<div class="cm-section-title">🧠 What the Pipeline Does</div>', unsafe_allow_html=True)
                c1, c2, c3 = st.columns(3)
                for col, icon, title, body in [
                    (c1,"✨","KNN Imputation","Fills missing numeric values using the 5 most similar rows in your dataset. Context-aware and accurate."),
                    (c2,"🎯","Isolation Forest","Detects and removes statistical outliers that could skew your analysis or break your ML models."),
                    (c3,"📏","StandardScaler","Normalises all numeric columns to the same scale — mean=0 and std=1 — for better ML performance."),
                ]:
                    with col:
                        st.markdown(f"""
                        <div class="cm-panel" style="text-align:center;padding:1.4rem 1rem;">
                          <div style="font-size:2rem;margin-bottom:0.6rem;">{icon}</div>
                          <div style="font-size:0.9rem;font-weight:700;color:#e2e8f0;margin-bottom:0.4rem;">{title}</div>
                          <div style="font-size:0.78rem;color:#64748b;line-height:1.6;">{body}</div>
                        </div>""", unsafe_allow_html=True)

        st.markdown("</div>", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
#  ROUTER
# ══════════════════════════════════════════════════════════════════════════════
def main():
    if st.session_state.logged_in:
        show_upload_page()
    else:
        show_auth_page()

if __name__ == "__main__":
    main()
