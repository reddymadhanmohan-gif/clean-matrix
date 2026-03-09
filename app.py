"""
app.py — Clean Master (v4 — dark bg + charts + tech icon)
Run with:  streamlit run app.py
"""

import streamlit as st
import pandas as pd
import numpy as np
import os, tempfile
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from pipeline import run_pipeline
from database import login_user, signup_user, save_dataset_record, save_processed_record

st.set_page_config(
    page_title="Clean Master | AI Data Quality",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@300;400;500;600;700;800;900&family=JetBrains+Mono:wght@400;600&display=swap');

/* ═══ FULL-PAGE DARK ANIMATED BACKGROUND ═══ */
html, body { margin:0; padding:0; background:#030711 !important; }

[data-testid="stAppViewContainer"] {
    background:
        radial-gradient(ellipse 100% 70% at 0% 0%,   rgba(99,102,241,0.15) 0%, transparent 50%),
        radial-gradient(ellipse 80%  60% at 100% 10%, rgba(168,85,247,0.12)  0%, transparent 50%),
        radial-gradient(ellipse 70%  50% at 50% 100%, rgba(56,189,248,0.09)  0%, transparent 50%),
        radial-gradient(ellipse 50%  40% at 80% 60%,  rgba(99,102,241,0.07)  0%, transparent 40%),
        #030711 !important;
    background-attachment: fixed !important;
}

/* dot-grid overlay */
[data-testid="stAppViewContainer"]::before {
    content:''; position:fixed; inset:0; z-index:0;
    background-image:
        radial-gradient(circle, rgba(99,102,241,0.18) 1px, transparent 1px);
    background-size: 40px 40px;
    pointer-events:none;
    mask-image: radial-gradient(ellipse 80% 80% at 50% 50%, black 40%, transparent 100%);
}

[data-testid="stMain"], .main {
    font-family:'Plus Jakarta Sans',sans-serif !important;
    color:#e2e8f0 !important;
    background:transparent !important;
    position:relative; z-index:1;
}
.block-container { padding:0 !important; max-width:100% !important; }
#MainMenu,footer,header,[data-testid="stDecoration"] { visibility:hidden; display:none; }

/* ═══ SIDEBAR ═══ */
[data-testid="stSidebar"] {
    background:rgba(3,7,17,0.95) !important;
    border-right:1px solid rgba(99,102,241,0.2) !important;
    backdrop-filter:blur(20px);
}
[data-testid="stSidebar"] * { color:#cbd5e1 !important; }

/* ═══ NAVBAR ═══ */
.cm-navbar {
    background:rgba(3,7,17,0.85);
    backdrop-filter:blur(24px);
    border-bottom:1px solid rgba(99,102,241,0.2);
    padding:0 2.5rem;
    height:66px;
    display:flex; align-items:center; justify-content:space-between;
    position:sticky; top:0; z-index:999;
}
.cm-logo {
    display:flex; align-items:center; gap:12px;
    font-size:1.35rem; font-weight:900; letter-spacing:-0.5px;
    background:linear-gradient(135deg,#a5b4fc 0%,#e879f9 50%,#38bdf8 100%);
    -webkit-background-clip:text; -webkit-text-fill-color:transparent;
}
.cm-logo-icon { font-size:1.6rem; filter:drop-shadow(0 0 8px rgba(99,102,241,0.6)); }
.cm-nav-chip {
    background:rgba(99,102,241,0.1);
    border:1px solid rgba(99,102,241,0.25);
    border-radius:99px; padding:5px 14px;
    font-size:0.78rem; font-weight:600; color:#a5b4fc;
}

/* ═══ AUTH PAGE ═══ */
.cm-auth-wrap {
    min-height:90vh; display:flex;
    flex-direction:column; align-items:center;
    justify-content:center; padding:2rem;
}
.cm-auth-logo {
    font-size:2rem; font-weight:900; letter-spacing:-1px;
    background:linear-gradient(135deg,#a5b4fc,#e879f9,#38bdf8);
    -webkit-background-clip:text; -webkit-text-fill-color:transparent;
    text-align:center; margin-bottom:0.3rem;
}
.cm-auth-title { text-align:center; font-size:1.45rem; font-weight:700; color:#f1f5f9; margin-bottom:0.25rem; }
.cm-auth-sub   { text-align:center; font-size:0.85rem; color:#64748b; margin-bottom:1.8rem; }

/* ═══ PIPELINE STEPS ═══ */
.cm-steps { display:flex; justify-content:center; align-items:center; gap:4px; flex-wrap:wrap; margin:1.5rem 0; }
.cm-step-box {
    display:flex; flex-direction:column; align-items:center;
    background:rgba(15,23,42,0.7); border:1px solid rgba(99,102,241,0.18);
    border-radius:14px; padding:12px 16px; width:100px;
    transition:all 0.25s;
}
.cm-step-box.done {
    border-color:rgba(52,211,153,0.5);
    background:rgba(52,211,153,0.07);
    box-shadow:0 0 16px rgba(52,211,153,0.1);
}
.cm-step-icon  { font-size:1.4rem; }
.cm-step-label { font-size:0.62rem; font-weight:700; color:#94a3b8; text-align:center; margin-top:5px; text-transform:uppercase; letter-spacing:0.4px; }
.cm-step-arrow { color:rgba(99,102,241,0.35); font-size:1.1rem; }

/* ═══ METRICS ═══ */
.cm-metric-grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(155px,1fr)); gap:1rem; margin:1.2rem 0; }
.cm-metric-card {
    background:rgba(10,15,30,0.8);
    border:1px solid rgba(99,102,241,0.2);
    border-radius:16px; padding:1.4rem 1rem; text-align:center;
    position:relative; overflow:hidden;
    backdrop-filter:blur(10px);
    transition:transform 0.2s, box-shadow 0.2s;
}
.cm-metric-card:hover { transform:translateY(-3px); box-shadow:0 12px 32px rgba(0,0,0,0.4); }
.cm-metric-card::before { content:''; position:absolute; top:0; left:0; right:0; height:3px; background:linear-gradient(90deg,#818cf8,#c084fc); }
.cm-metric-card.green::before  { background:linear-gradient(90deg,#34d399,#10b981); }
.cm-metric-card.red::before    { background:linear-gradient(90deg,#f87171,#ef4444); }
.cm-metric-card.amber::before  { background:linear-gradient(90deg,#fbbf24,#f59e0b); }
.cm-metric-card.cyan::before   { background:linear-gradient(90deg,#22d3ee,#0ea5e9); }
.cm-metric-icon { font-size:1.8rem; margin-bottom:0.5rem; }
.cm-metric-num  { font-size:2.1rem; font-weight:900; line-height:1;
    background:linear-gradient(135deg,#818cf8,#c084fc); -webkit-background-clip:text; -webkit-text-fill-color:transparent; }
.cm-metric-card.green .cm-metric-num { background:linear-gradient(135deg,#34d399,#10b981); -webkit-background-clip:text; }
.cm-metric-card.red   .cm-metric-num { background:linear-gradient(135deg,#f87171,#ef4444); -webkit-background-clip:text; }
.cm-metric-card.amber .cm-metric-num { background:linear-gradient(135deg,#fbbf24,#f59e0b); -webkit-background-clip:text; }
.cm-metric-card.cyan  .cm-metric-num { background:linear-gradient(135deg,#22d3ee,#0ea5e9); -webkit-background-clip:text; }
.cm-metric-lbl { font-size:0.72rem; font-weight:700; color:#64748b; text-transform:uppercase; letter-spacing:0.6px; margin-top:5px; }

/* ═══ SECTION TITLES ═══ */
.cm-section-title {
    font-size:1.05rem; font-weight:800; color:#e2e8f0; margin:2rem 0 0.9rem;
    display:flex; align-items:center; gap:10px;
}
.cm-section-title::after { content:''; flex:1; height:1px; background:linear-gradient(90deg,rgba(99,102,241,0.3),transparent); }

/* ═══ ALERTS ═══ */
.cm-success { background:rgba(52,211,153,0.07); border:1px solid rgba(52,211,153,0.25); border-radius:12px; padding:0.9rem 1.2rem; font-size:0.87rem; color:#6ee7b7; margin:0.8rem 0; }
.cm-warning { background:rgba(251,191,36,0.07); border:1px solid rgba(251,191,36,0.25); border-radius:12px; padding:0.9rem 1.2rem; font-size:0.87rem; color:#fcd34d; margin:0.8rem 0; }
.cm-info    { background:rgba(56,189,248,0.07);  border:1px solid rgba(56,189,248,0.25);  border-radius:12px; padding:0.9rem 1.2rem; font-size:0.87rem; color:#7dd3fc; margin:0.8rem 0; }

/* ═══ PANELS ═══ */
.cm-panel {
    background:rgba(10,15,30,0.75);
    border:1px solid rgba(99,102,241,0.15);
    border-radius:16px; padding:1.5rem;
    margin:0.8rem 0; backdrop-filter:blur(12px);
}
.cm-chart-card {
    background:rgba(10,15,30,0.8);
    border:1px solid rgba(99,102,241,0.18);
    border-radius:18px; padding:1.2rem 1.4rem;
    margin:0.5rem 0; backdrop-filter:blur(12px);
}

/* ═══ FILE INFO ═══ */
.cm-fileinfo {
    display:flex; gap:1.2rem; flex-wrap:wrap;
    background:rgba(99,102,241,0.07); border:1px solid rgba(99,102,241,0.18);
    border-radius:12px; padding:0.85rem 1.3rem; margin:0.8rem 0; font-size:0.83rem;
}
.cm-fileinfo-item { color:#94a3b8; }
.cm-fileinfo-item b { color:#a5b4fc; }

/* ═══ DOWNLOAD CARD ═══ */
.cm-download-card {
    background:linear-gradient(135deg,rgba(99,102,241,0.12),rgba(232,121,249,0.08));
    border:1px solid rgba(99,102,241,0.3); border-radius:18px;
    padding:2rem; text-align:center; margin:1rem 0;
}
.cm-download-title { font-size:1.2rem; font-weight:800; color:#e2e8f0; margin-bottom:0.4rem; }
.cm-download-sub   { font-size:0.84rem; color:#64748b; margin-bottom:1.2rem; }

/* ═══ MISSING COLS TABLE ═══ */
.cm-col-table { width:100%; border-collapse:collapse; font-family:'JetBrains Mono',monospace; font-size:0.82rem; }
.cm-col-table th { background:rgba(99,102,241,0.14); color:#a5b4fc; padding:9px 13px; text-align:left; font-weight:700; font-size:0.72rem; text-transform:uppercase; letter-spacing:0.5px; border-bottom:1px solid rgba(99,102,241,0.2); }
.cm-col-table td { padding:8px 13px; color:#cbd5e1; border-bottom:1px solid rgba(99,102,241,0.07); }
.cm-col-table tr:hover td { background:rgba(99,102,241,0.06); }
.cm-bar { height:7px; border-radius:99px; background:linear-gradient(90deg,#818cf8,#e879f9); min-width:4px; }

/* ═══ SIDEBAR ELEMENTS ═══ */
.cm-sidebar-logo { font-size:1.15rem; font-weight:900;
    background:linear-gradient(135deg,#a5b4fc,#e879f9,#38bdf8);
    -webkit-background-clip:text; -webkit-text-fill-color:transparent; padding:0.4rem 0 0.8rem; }
.cm-user-chip { background:rgba(99,102,241,0.1); border:1px solid rgba(99,102,241,0.25); border-radius:10px; padding:0.7rem; margin-bottom:0.8rem; font-size:0.82rem; color:#94a3b8 !important; }
.cm-user-chip b { color:#a5b4fc !important; }

/* ═══ WIDGET OVERRIDES ═══ */
[data-testid="stTextInput"] input {
    background:rgba(10,15,30,0.85) !important; border:1px solid rgba(99,102,241,0.28) !important;
    border-radius:11px !important; color:#e2e8f0 !important;
    font-family:'Plus Jakarta Sans',sans-serif !important; padding:0.6rem 1rem !important;
}
[data-testid="stTextInput"] input:focus {
    border-color:rgba(99,102,241,0.65) !important;
    box-shadow:0 0 0 3px rgba(99,102,241,0.13) !important; outline:none !important;
}
[data-testid="stTextInput"] label {
    font-size:0.8rem !important; font-weight:700 !important;
    color:#64748b !important; text-transform:uppercase !important; letter-spacing:0.5px !important;
}

button[data-testid="stBaseButton-primary"],
[data-testid="stButton"] button[kind="primary"] {
    background:linear-gradient(135deg,#6366f1,#a855f7) !important; border:none !important;
    border-radius:11px !important; color:white !important; font-weight:800 !important;
    font-family:'Plus Jakarta Sans',sans-serif !important;
    box-shadow:0 4px 20px rgba(99,102,241,0.35) !important; transition:all 0.2s !important;
}
button[data-testid="stBaseButton-primary"]:hover { transform:translateY(-2px) !important; box-shadow:0 8px 28px rgba(99,102,241,0.45) !important; }

button[data-testid="stBaseButton-secondary"],
[data-testid="stButton"] button:not([kind="primary"]) {
    background:rgba(10,15,30,0.85) !important; border:1px solid rgba(99,102,241,0.28) !important;
    border-radius:11px !important; color:#a5b4fc !important; font-weight:700 !important;
    font-family:'Plus Jakarta Sans',sans-serif !important; transition:all 0.2s !important;
}
button[data-testid="stBaseButton-secondary"]:hover { background:rgba(99,102,241,0.1) !important; }

[data-testid="stDownloadButton"] button {
    background:linear-gradient(135deg,#059669,#10b981) !important; border:none !important;
    border-radius:11px !important; color:white !important; font-weight:800 !important;
    font-family:'Plus Jakarta Sans',sans-serif !important;
    box-shadow:0 4px 20px rgba(16,185,129,0.35) !important;
    width:100% !important; padding:0.7rem !important;
}

[data-testid="stFileUploader"] {
    background:rgba(10,15,30,0.7) !important;
    border:2px dashed rgba(99,102,241,0.28) !important;
    border-radius:14px !important; padding:1rem !important;
}
[data-testid="stCheckbox"] label { color:#94a3b8 !important; font-size:0.86rem !important; }
[data-testid="stDataFrame"] { border:1px solid rgba(99,102,241,0.18) !important; border-radius:12px !important; overflow:hidden !important; }

::-webkit-scrollbar { width:5px; height:5px; }
::-webkit-scrollbar-track { background:rgba(10,15,30,0.5); }
::-webkit-scrollbar-thumb { background:rgba(99,102,241,0.4); border-radius:99px; }
</style>
""", unsafe_allow_html=True)

# ── session state ──────────────────────────────────────────────────────────────
for k,v in {"logged_in":False,"user":None,"auth_mode":"login","cleaned_result":None}.items():
    if k not in st.session_state: st.session_state[k] = v

# ── helpers ────────────────────────────────────────────────────────────────────
LOGO = "⚡"
APP_NAME = "Clean Master"

def navbar(logged_in=False, username=""):
    right = (f'<span class="cm-nav-chip">👤 {username}</span>') if logged_in else ""
    st.markdown(f"""
    <div class="cm-navbar">
      <div class="cm-logo">
        <span class="cm-logo-icon">{LOGO}</span> {APP_NAME}
      </div>
      {right}
    </div>""", unsafe_allow_html=True)

def pipeline_steps(done=False):
    steps = [("📤","Upload"),("🔍","Detect"),("✨","Impute"),("🎯","Outliers"),("📏","Scale"),("💾","Export")]
    html = '<div class="cm-steps">'
    for i,(icon,label) in enumerate(steps):
        cls = "cm-step-box done" if done else "cm-step-box"
        arrow = '<span class="cm-step-arrow">›</span>' if i<len(steps)-1 else ""
        html += (f'<div style="display:flex;align-items:center;gap:4px;">'
                 f'<div class="{cls}"><span class="cm-step-icon">{icon}</span>'
                 f'<span class="cm-step-label">{label}</span></div>{arrow}</div>')
    html += "</div>"
    st.markdown(html, unsafe_allow_html=True)

# ── Plotly theme ───────────────────────────────────────────────────────────────
PLOT_BG   = "rgba(10,15,30,0)"
GRID_COL  = "rgba(99,102,241,0.12)"
TEXT_COL  = "#94a3b8"
PURPLE    = "#818cf8"
PINK      = "#e879f9"
CYAN      = "#38bdf8"
GREEN     = "#34d399"
AMBER     = "#fbbf24"
RED       = "#f87171"

def plot_layout(fig, title="", height=320):
    fig.update_layout(
        title=dict(text=title, font=dict(color="#e2e8f0", size=13, family="Plus Jakarta Sans"), x=0.02),
        paper_bgcolor=PLOT_BG, plot_bgcolor=PLOT_BG,
        font=dict(family="Plus Jakarta Sans", color=TEXT_COL, size=11),
        height=height, margin=dict(l=40,r=20,t=40,b=40),
        legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(color=TEXT_COL)),
        xaxis=dict(gridcolor=GRID_COL, zerolinecolor=GRID_COL, tickfont=dict(color=TEXT_COL)),
        yaxis=dict(gridcolor=GRID_COL, zerolinecolor=GRID_COL, tickfont=dict(color=TEXT_COL)),
    )
    return fig

# ═══════════════════════════════════════════════════════════════════════════════
#  CHART BUILDERS
# ═══════════════════════════════════════════════════════════════════════════════

def chart_missing_bar(missing_by_col, total_rows):
    if not missing_by_col:
        return None
    cols = list(missing_by_col.keys())
    vals = list(missing_by_col.values())
    pcts = [v/total_rows*100 for v in vals]
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=cols, y=pcts,
        marker=dict(color=AMBER, opacity=0.85,
                    line=dict(color="rgba(251,191,36,0.4)", width=1)),
        text=[f"{p:.1f}%" for p in pcts], textposition="outside",
        textfont=dict(color=AMBER, size=10),
        name="Missing %",
    ))
    fig = plot_layout(fig, "Missing Values per Column (%)", height=300)
    fig.update_layout(showlegend=False)
    fig.update_xaxis(tickangle=-30)
    return fig


def chart_donut_clean(original, cleaned, outliers, missing):
    removed = original - cleaned
    kept    = cleaned
    fig = go.Figure(go.Pie(
        labels=["Clean Rows", "Outliers Removed"],
        values=[kept, removed],
        hole=0.65,
        marker=dict(colors=[GREEN, RED],
                    line=dict(color="rgba(0,0,0,0)", width=0)),
        textinfo="label+percent",
        textfont=dict(color="#e2e8f0", size=11),
        pull=[0, 0.04],
    ))
    fig.add_annotation(
        text=f"<b>{kept:,}</b><br><span style='font-size:10px'>clean rows</span>",
        x=0.5, y=0.5, font=dict(size=14, color="#e2e8f0"), showarrow=False,
    )
    fig = plot_layout(fig, "Dataset Composition After Cleaning", height=300)
    fig.update_layout(showlegend=True, legend=dict(orientation="h", y=-0.05))
    return fig


def chart_distribution_compare(df_raw, df_clean, col):
    """Overlay histogram: raw vs clean for one numeric column."""
    fig = go.Figure()
    fig.add_trace(go.Histogram(
        x=df_raw[col].dropna(), name="Before",
        marker_color=RED, opacity=0.5,
        nbinsx=30, histnorm="probability density",
    ))
    fig.add_trace(go.Histogram(
        x=df_clean[col].dropna(), name="After",
        marker_color=GREEN, opacity=0.65,
        nbinsx=30, histnorm="probability density",
    ))
    fig = plot_layout(fig, f"Distribution: {col}", height=270)
    fig.update_layout(barmode="overlay", showlegend=True)
    return fig


def chart_missing_heatmap(df_raw):
    """Shows which cells are missing as a mini heatmap (sample 100 rows)."""
    sample = df_raw.head(80)
    z = sample.isnull().astype(int).values
    fig = go.Figure(go.Heatmap(
        z=z,
        x=list(sample.columns),
        colorscale=[[0,"rgba(52,211,153,0.15)"],[1,"rgba(248,113,113,0.85)"]],
        showscale=False, hovertemplate="Col: %{x}<br>Row: %{y}<br>Missing: %{z}<extra></extra>",
    ))
    fig = plot_layout(fig, "Missing Value Map (first 80 rows — red = missing)", height=280)
    fig.update_xaxis(tickangle=-40, tickfont=dict(size=9))
    return fig


def chart_bar_before_after(stats):
    cats = ["Original Rows", "Clean Rows", "Values Filled", "Outliers Removed"]
    vals = [stats["original_rows"], stats["cleaned_rows"],
            stats["missing_values"], stats["outliers_removed"]]
    colors = [PURPLE, GREEN, AMBER, RED]
    fig = go.Figure(go.Bar(
        x=cats, y=vals,
        marker=dict(color=colors, opacity=0.85, line=dict(color="rgba(0,0,0,0)",width=0)),
        text=vals, textposition="outside", textfont=dict(size=12, color="#e2e8f0"),
    ))
    fig = plot_layout(fig, "Pipeline Summary — Before vs After", height=300)
    fig.update_layout(showlegend=False)
    return fig


def chart_numeric_boxplots(df_raw, df_clean):
    """Side-by-side box plots for the first 5 numeric columns."""
    num_cols = df_raw.select_dtypes(include=[np.number]).columns.tolist()[:5]
    if not num_cols:
        return None
    fig = make_subplots(rows=1, cols=len(num_cols),
                        subplot_titles=num_cols)
    for i, col in enumerate(num_cols, 1):
        # before
        fig.add_trace(go.Box(
            y=df_raw[col].dropna(), name="Before",
            marker_color=RED, opacity=0.7,
            line=dict(color=RED), showlegend=(i==1),
            boxmean=True,
        ), row=1, col=i)
        # after
        fig.add_trace(go.Box(
            y=df_clean[col].dropna(), name="After",
            marker_color=GREEN, opacity=0.7,
            line=dict(color=GREEN), showlegend=(i==1),
            boxmean=True,
        ), row=1, col=i)

    fig.update_layout(
        paper_bgcolor=PLOT_BG, plot_bgcolor=PLOT_BG,
        font=dict(family="Plus Jakarta Sans", color=TEXT_COL, size=10),
        height=320, margin=dict(l=30,r=20,t=45,b=30),
        legend=dict(bgcolor="rgba(0,0,0,0)"),
        boxmode="group",
    )
    for ann in fig.layout.annotations:
        ann.font.color = TEXT_COL
        ann.font.size  = 10
    for ax in [fig.layout[k] for k in fig.layout if k.startswith("xaxis") or k.startswith("yaxis")]:
        ax.gridcolor  = GRID_COL
        ax.tickfont   = dict(color=TEXT_COL, size=9)
        ax.zerolinecolor = GRID_COL
    return fig


# ═══════════════════════════════════════════════════════════════════════════════
#  AUTH PAGE
# ═══════════════════════════════════════════════════════════════════════════════
def show_auth_page():
    navbar()
    is_signup = st.session_state.auth_mode == "signup"

    st.markdown('<div class="cm-auth-wrap">', unsafe_allow_html=True)
    _, col, _ = st.columns([1, 1.2, 1])
    with col:
        st.markdown(f"""
        <div class="cm-auth-logo">{LOGO} {APP_NAME}</div>
        <div class="cm-auth-title">{"Create your account" if is_signup else "Welcome back"}</div>
        <div class="cm-auth-sub">{"Start cleaning datasets with AI — free forever" if is_signup else "Sign in to continue cleaning data"}</div>
        """, unsafe_allow_html=True)

        c1, c2 = st.columns(2)
        with c1:
            if st.button("Sign In", use_container_width=True,
                         type="primary" if not is_signup else "secondary"):
                st.session_state.auth_mode="login"; st.rerun()
        with c2:
            if st.button("Sign Up", use_container_width=True,
                         type="primary" if is_signup else "secondary"):
                st.session_state.auth_mode="signup"; st.rerun()

        st.markdown("<br>", unsafe_allow_html=True)

        if is_signup:
            nu = st.text_input("Username", placeholder="Choose a username", key="su_u")
            np_ = st.text_input("Password", placeholder="Create a password", type="password", key="su_p")
            np2 = st.text_input("Confirm Password", placeholder="Repeat password", type="password", key="su_p2")
            if st.button("Create Account →", type="primary", use_container_width=True):
                if not nu or not np_: st.error("Please fill in all fields.")
                elif len(np_)<4: st.error("Password must be at least 4 characters.")
                elif np_!=np2: st.error("Passwords do not match!")
                else:
                    with st.spinner("Creating account..."):
                        r = signup_user(nu, np_)
                    if r["success"]:
                        st.success("Account created! Please sign in.")
                        st.session_state.auth_mode="login"; st.rerun()
                    else: st.error(r["message"])
        else:
            u = st.text_input("Username", placeholder="Your username", key="li_u")
            p = st.text_input("Password", placeholder="Your password", type="password", key="li_p")
            if st.button("Sign In →", type="primary", use_container_width=True):
                if not u or not p: st.error("Please enter your username and password.")
                else:
                    with st.spinner("Signing in..."):
                        r = login_user(u, p)
                    if r["success"]:
                        st.session_state.logged_in = True
                        st.session_state.user = r["user"]
                        st.session_state.cleaned_result = None
                        st.rerun()
                    else: st.error("Invalid username or password.")

        st.markdown("""
        <div style="margin-top:2rem;display:flex;justify-content:center;gap:1.5rem;flex-wrap:wrap;">
          <span style="font-size:0.75rem;color:#334155;">⚡ KNN Imputation</span>
          <span style="font-size:0.75rem;color:#334155;">🎯 Isolation Forest</span>
          <span style="font-size:0.75rem;color:#334155;">📏 StandardScaler</span>
          <span style="font-size:0.75rem;color:#334155;">🔒 Secure Login</span>
        </div>""", unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════════
#  MAIN APP
# ═══════════════════════════════════════════════════════════════════════════════
def show_upload_page():
    user     = st.session_state.user
    username = user["username"]
    navbar(logged_in=True, username=username)

    # sidebar
    with st.sidebar:
        st.markdown(f'<div class="cm-sidebar-logo">{LOGO} {APP_NAME}</div>', unsafe_allow_html=True)
        st.markdown(f'<div class="cm-user-chip">👤 &nbsp;<b>{username}</b></div>', unsafe_allow_html=True)
        st.markdown("**Navigation**")
        if st.button("🏠  Dashboard", use_container_width=True):
            st.session_state.cleaned_result=None; st.rerun()
        if st.button("📂  New Upload", use_container_width=True):
            st.session_state.cleaned_result=None; st.rerun()
        st.markdown("---")
        st.markdown(f"""<p style="font-size:0.78rem;color:#334155;line-height:1.7;">
            <b style="color:#64748b;">{APP_NAME}</b> automatically cleans your CSV datasets using
            machine learning — KNN Imputation fills missing values, Isolation Forest removes outliers,
            and StandardScaler normalises features.</p>""", unsafe_allow_html=True)
        st.markdown("---")
        if st.button("🚪  Sign Out", use_container_width=True):
            for k in ["logged_in","user","cleaned_result"]: st.session_state[k]=None if k!="logged_in" else False
            st.session_state.auth_mode="login"; st.rerun()

    # ── main content ──────────────────────────────────────────────────────────
    mc, _ = st.columns([11, 1])
    with mc:
        st.markdown('<div style="padding:2rem 2.5rem 3rem;">', unsafe_allow_html=True)

        # ══ RESULTS VIEW ══════════════════════════════════════════════════════
        if st.session_state.cleaned_result:
            r              = st.session_state.cleaned_result
            stats          = r["stats"]
            df_raw         = r["df_raw"]
            df_clean       = r["df"]
            filename       = r["filename"]
            missing_by_col = r.get("missing_by_col", {})

            st.markdown(f'<div class="cm-success">✅ &nbsp;<b>Pipeline complete!</b> &nbsp; "{filename}" has been cleaned and is ready to download.</div>', unsafe_allow_html=True)

            pipeline_steps(done=True)

            # ── Metric Cards ──────────────────────────────────────────────────
            st.markdown('<div class="cm-section-title">📊 Cleaning Summary</div>', unsafe_allow_html=True)
            rows_saved = stats["original_rows"] - stats["outliers_removed"]
            pct_clean  = round(rows_saved / stats["original_rows"] * 100, 1) if stats["original_rows"] else 100
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
              <div class="cm-metric-card cyan">
                <div class="cm-metric-icon">💯</div>
                <div class="cm-metric-num">{pct_clean}%</div>
                <div class="cm-metric-lbl">Data Retained</div>
              </div>
            </div>""", unsafe_allow_html=True)

            # ── Chart Row 1: summary bar + donut ─────────────────────────────
            st.markdown('<div class="cm-section-title">📈 Visual Analytics</div>', unsafe_allow_html=True)
            col_a, col_b = st.columns(2)
            with col_a:
                st.markdown('<div class="cm-chart-card">', unsafe_allow_html=True)
                fig = chart_bar_before_after(stats)
                st.plotly_chart(fig, use_container_width=True, config={"displayModeBar":False})
                st.markdown("</div>", unsafe_allow_html=True)
            with col_b:
                st.markdown('<div class="cm-chart-card">', unsafe_allow_html=True)
                fig = chart_donut_clean(stats["original_rows"], stats["cleaned_rows"],
                                        stats["outliers_removed"], stats["missing_values"])
                st.plotly_chart(fig, use_container_width=True, config={"displayModeBar":False})
                st.markdown("</div>", unsafe_allow_html=True)

            # ── Chart Row 2: missing bar + heatmap ───────────────────────────
            if missing_by_col:
                col_c, col_d = st.columns(2)
                with col_c:
                    st.markdown('<div class="cm-chart-card">', unsafe_allow_html=True)
                    fig = chart_missing_bar(missing_by_col, stats["original_rows"])
                    if fig: st.plotly_chart(fig, use_container_width=True, config={"displayModeBar":False})
                    st.markdown("</div>", unsafe_allow_html=True)
                with col_d:
                    st.markdown('<div class="cm-chart-card">', unsafe_allow_html=True)
                    fig = chart_missing_heatmap(df_raw)
                    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar":False})
                    st.markdown("</div>", unsafe_allow_html=True)

            # ── Chart Row 3: distribution overlays ───────────────────────────
            num_cols = df_raw.select_dtypes(include=[np.number]).columns.tolist()
            if num_cols:
                st.markdown('<div class="cm-section-title">📉 Distribution Comparison — Before vs After</div>', unsafe_allow_html=True)
                st.markdown('<div class="cm-info">🔴 Red = raw data &nbsp;|&nbsp; 🟢 Green = cleaned data &nbsp;|&nbsp; Showing first 4 numeric columns</div>', unsafe_allow_html=True)
                show_cols = num_cols[:4]
                dist_cols = st.columns(len(show_cols))
                for i, col in enumerate(show_cols):
                    with dist_cols[i]:
                        st.markdown('<div class="cm-chart-card">', unsafe_allow_html=True)
                        fig = chart_distribution_compare(df_raw, df_clean, col)
                        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar":False})
                        st.markdown("</div>", unsafe_allow_html=True)

            # ── Chart Row 4: box plots ────────────────────────────────────────
            st.markdown('<div class="cm-section-title">📦 Outlier Impact — Box Plots</div>', unsafe_allow_html=True)
            st.markdown('<div class="cm-chart-card">', unsafe_allow_html=True)
            fig = chart_numeric_boxplots(df_raw, df_clean)
            if fig: st.plotly_chart(fig, use_container_width=True, config={"displayModeBar":False})
            st.markdown("</div>", unsafe_allow_html=True)

            # ── Missing values table ──────────────────────────────────────────
            if missing_by_col:
                st.markdown('<div class="cm-section-title">🔍 Missing Values Breakdown</div>', unsafe_allow_html=True)
                max_m = max(missing_by_col.values())
                rows_html = ""
                for col, cnt in sorted(missing_by_col.items(), key=lambda x:-x[1]):
                    pct = cnt/stats["original_rows"]*100
                    bw  = int(cnt/max_m*160)
                    rows_html += (f"<tr><td>{col}</td><td style='color:#fbbf24;'>{cnt:,}</td>"
                                  f"<td>{pct:.1f}%</td>"
                                  f"<td><div class='cm-bar' style='width:{bw}px;'></div></td>"
                                  f"<td style='color:#34d399;font-weight:700;'>✓ Filled</td></tr>")
                st.markdown(f"""
                <div class="cm-panel" style="overflow-x:auto;">
                  <table class="cm-col-table">
                    <thead><tr><th>Column</th><th>Missing Count</th><th>% of Rows</th><th>Visual</th><th>Status</th></tr></thead>
                    <tbody>{rows_html}</tbody>
                  </table>
                </div>""", unsafe_allow_html=True)

            # ── Before / After table preview ──────────────────────────────────
            st.markdown('<div class="cm-section-title">🔄 Before vs After — Data Preview</div>', unsafe_allow_html=True)
            ca, cb = st.columns(2)
            with ca:
                st.markdown('<p style="font-size:0.82rem;font-weight:700;color:#f87171;margin-bottom:0.4rem;">⚠️ Raw Dataset (original)</p>', unsafe_allow_html=True)
                st.dataframe(df_raw.head(8), use_container_width=True, height=250)
            with cb:
                st.markdown('<p style="font-size:0.82rem;font-weight:700;color:#34d399;margin-bottom:0.4rem;">✅ Cleaned Dataset (after pipeline)</p>', unsafe_allow_html=True)
                st.dataframe(df_clean.head(8), use_container_width=True, height=250)

            # ── Download ──────────────────────────────────────────────────────
            st.markdown('<div class="cm-section-title">⬇️ Download</div>', unsafe_allow_html=True)
            csv_data   = df_clean.to_csv(index=False).encode("utf-8")
            clean_name = filename.replace(".csv","_cleaned.csv")
            file_kb    = round(len(csv_data)/1024, 1)
            st.markdown(f"""
            <div class="cm-download-card">
              <div class="cm-download-title">🎉 Your cleaned dataset is ready!</div>
              <div class="cm-download-sub">{clean_name} &nbsp;·&nbsp; {stats['cleaned_rows']:,} rows &nbsp;·&nbsp; {file_kb} KB</div>
            </div>""", unsafe_allow_html=True)
            st.download_button("⬇️  Download Cleaned CSV", csv_data, clean_name, "text/csv", use_container_width=True)
            st.markdown("<br>", unsafe_allow_html=True)
            if st.button("🔄  Clean Another Dataset", use_container_width=True):
                st.session_state.cleaned_result=None; st.rerun()

        # ══ UPLOAD VIEW ═══════════════════════════════════════════════════════
        else:
            st.markdown(f"""
            <div style="margin-bottom:1.5rem;">
              <h2 style="font-size:2rem;font-weight:900;color:#e2e8f0;margin:0;letter-spacing:-0.8px;">
                Upload Your Dataset
              </h2>
              <p style="color:#475569;margin:0.4rem 0 0;font-size:0.92rem;">
                Upload any CSV file — {APP_NAME} will automatically clean it using AI.
              </p>
            </div>""", unsafe_allow_html=True)

            pipeline_steps(done=False)

            uploaded_file = st.file_uploader(
                "Drop your CSV file here or click Browse",
                type=["csv"], help="Supported: CSV · Max 200 MB",
            )

            if uploaded_file is not None:
                try:
                    df_preview = pd.read_csv(uploaded_file)
                    uploaded_file.seek(0)
                    missing_total = int(df_preview.isnull().sum().sum())
                    missing_cols  = int((df_preview.isnull().sum()>0).sum())
                    dup_count     = int(df_preview.duplicated().sum())
                    num_count     = len(df_preview.select_dtypes(include=[np.number]).columns)

                    st.markdown(f"""
                    <div class="cm-fileinfo">
                      <span class="cm-fileinfo-item">📄 <b>{uploaded_file.name}</b></span>
                      <span class="cm-fileinfo-item">📊 <b>{df_preview.shape[0]:,}</b> rows</span>
                      <span class="cm-fileinfo-item">🔢 <b>{df_preview.shape[1]}</b> columns</span>
                      <span class="cm-fileinfo-item">🔵 <b>{num_count}</b> numeric cols</span>
                      <span class="cm-fileinfo-item">⚠️ <b>{missing_total:,}</b> missing values</span>
                      <span class="cm-fileinfo-item">🔁 <b>{dup_count}</b> duplicates</span>
                    </div>""", unsafe_allow_html=True)

                    if missing_total > 0:
                        st.markdown(f'<div class="cm-warning">⚠️ Found <b>{missing_total:,} missing values</b> across <b>{missing_cols}</b> column(s). KNN Imputation will fill these automatically.</div>', unsafe_allow_html=True)
                    else:
                        st.markdown('<div class="cm-info">✨ No missing values detected. The pipeline will still remove outliers and duplicates.</div>', unsafe_allow_html=True)

                    st.markdown('<div class="cm-section-title">👁️ Data Preview</div>', unsafe_allow_html=True)
                    st.dataframe(df_preview.head(6), use_container_width=True, height=220)

                    st.markdown('<div class="cm-section-title">⚙️ Pipeline Options</div>', unsafe_allow_html=True)
                    apply_scaling = st.checkbox("Apply StandardScaler — normalise all numeric columns (mean=0, std=1)", value=False)
                    st.markdown("<br>", unsafe_allow_html=True)

                    if st.button("🚀  Run Cleaning Pipeline", type="primary", use_container_width=True):
                        with st.spinner("Running AI pipeline — this usually takes 5–15 seconds..."):
                            try:
                                with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
                                    tmp.write(uploaded_file.getvalue())
                                    tmp_path = tmp.name

                                df_raw_snap    = pd.read_csv(tmp_path)
                                missing_by_col = {c:int(n) for c,n in df_raw_snap.isnull().sum().items() if n>0}

                                df_clean, stats, output_path = run_pipeline(
                                    tmp_path, output_dir="outputs", scale=apply_scaling)

                                try:
                                    did = save_dataset_record(user["id"], uploaded_file.name,
                                                              df_preview.shape[0], df_preview.shape[1])
                                    if did:
                                        save_processed_record(did, user["id"], uploaded_file.name,
                                                              os.path.basename(output_path),
                                                              stats["missing_values"], stats["outliers_removed"])
                                except Exception: pass

                                st.session_state.cleaned_result = {
                                    "df":df_clean, "df_raw":df_raw_snap,
                                    "stats":stats, "output_path":output_path,
                                    "filename":uploaded_file.name,
                                    "missing_by_col":missing_by_col,
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
                <div style="background:rgba(10,15,30,0.65);border:2px dashed rgba(99,102,241,0.22);
                    border-radius:18px;padding:3.5rem;text-align:center;margin:1rem 0;">
                  <div style="font-size:3.5rem;margin-bottom:0.8rem;filter:drop-shadow(0 0 12px rgba(99,102,241,0.4));">📂</div>
                  <div style="font-size:1.05rem;font-weight:700;color:#94a3b8;">Drag and drop your CSV file into the uploader above</div>
                  <div style="font-size:0.8rem;color:#334155;margin-top:0.5rem;">Supported format: CSV &nbsp;·&nbsp; Max 200 MB</div>
                </div>""", unsafe_allow_html=True)

                st.markdown('<div class="cm-section-title">🧠 What the Pipeline Does</div>', unsafe_allow_html=True)
                c1,c2,c3 = st.columns(3)
                for col,icon,title,body,color in [
                    (c1,"✨","KNN Imputation","Fills missing numeric values using the 5 most similar rows. Context-aware, not just mean/median.","rgba(129,140,248,0.06)"),
                    (c2,"🎯","Isolation Forest","Detects and removes statistical outliers — records so different they break your ML models.","rgba(248,113,113,0.06)"),
                    (c3,"📏","StandardScaler","Normalises all numeric columns to mean=0, std=1 so every feature is on the same scale.","rgba(52,211,153,0.06)"),
                ]:
                    with col:
                        st.markdown(f"""
                        <div class="cm-panel" style="text-align:center;background:{color};">
                          <div style="font-size:2.2rem;margin-bottom:0.7rem;">{icon}</div>
                          <div style="font-size:0.95rem;font-weight:800;color:#e2e8f0;margin-bottom:0.5rem;">{title}</div>
                          <div style="font-size:0.78rem;color:#64748b;line-height:1.7;">{body}</div>
                        </div>""", unsafe_allow_html=True)

        st.markdown("</div>", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════════
def main():
    if st.session_state.logged_in: show_upload_page()
    else: show_auth_page()

if __name__ == "__main__":
    main()