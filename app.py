"""
app.py — Clean Master (v6 - Streamlit Cloud compatible)
Run with:  streamlit run app.py
"""

import streamlit as st
import pandas as pd
import numpy as np
import os, tempfile
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pipeline import run_pipeline
from database import login_user, signup_user, save_dataset_record, save_processed_record

st.set_page_config(
    page_title="Clean Master | AI Data Quality",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ─────────────────────────────────────────────────────────────────────────────
# CSS — only background + component styling, never touch visibility/display
# ─────────────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:ital,wght@0,400;0,600;0,700;0,800;0,900&display=swap');

/* ── Background ── */
.stApp {
    background: linear-gradient(135deg, #0a0014 0%, #060b18 40%, #000d1a 100%) !important;
    font-family: 'Plus Jakarta Sans', sans-serif !important;
}

/* purple glow top-left */
.stApp::before {
    content: '';
    position: fixed;
    top: -200px; left: -200px;
    width: 700px; height: 700px;
    background: radial-gradient(circle, rgba(99,102,241,0.22) 0%, transparent 70%);
    pointer-events: none;
    z-index: 0;
}

/* cyan glow bottom-right */
.stApp::after {
    content: '';
    position: fixed;
    bottom: -150px; right: -150px;
    width: 600px; height: 600px;
    background: radial-gradient(circle, rgba(6,182,212,0.15) 0%, transparent 70%);
    pointer-events: none;
    z-index: 0;
}

/* ── Streamlit chrome ── */
[data-testid="stHeader"]          { background: transparent !important; }
[data-testid="stToolbar"]         { display: none !important; }
[data-testid="stDecoration"]      { display: none !important; }
footer                            { visibility: hidden !important; }

/* ── Main area ── */
[data-testid="stMain"]            { background: transparent !important; }
.block-container {
    background: transparent !important;
    padding-top: 1rem !important;
    padding-left: 2rem !important;
    padding-right: 2rem !important;
    max-width: 1400px !important;
}

/* ── Sidebar ── */
[data-testid="stSidebar"] {
    background: rgba(10, 5, 30, 0.95) !important;
    border-right: 1px solid rgba(99,102,241,0.25) !important;
}
[data-testid="stSidebar"] * { color: #cbd5e1 !important; }

/* ── Text inputs ── */
[data-testid="stTextInput"] input {
    background: rgba(15, 10, 40, 0.85) !important;
    border: 1.5px solid rgba(99,102,241,0.4) !important;
    border-radius: 10px !important;
    color: #f1f5f9 !important;
    font-family: 'Plus Jakarta Sans', sans-serif !important;
    font-size: 0.95rem !important;
}
[data-testid="stTextInput"] input:focus {
    border-color: #818cf8 !important;
    box-shadow: 0 0 0 3px rgba(99,102,241,0.2) !important;
}
[data-testid="stTextInput"] label {
    color: #94a3b8 !important;
    font-size: 0.78rem !important;
    font-weight: 700 !important;
    text-transform: uppercase !important;
    letter-spacing: 0.5px !important;
}

/* ── Buttons ── */
button[data-testid="stBaseButton-primary"] {
    background: linear-gradient(135deg, #6366f1 0%, #a855f7 100%) !important;
    border: none !important;
    border-radius: 10px !important;
    color: #ffffff !important;
    font-weight: 800 !important;
    font-size: 0.92rem !important;
    font-family: 'Plus Jakarta Sans', sans-serif !important;
    box-shadow: 0 4px 20px rgba(99,102,241,0.45) !important;
    transition: all 0.2s !important;
}
button[data-testid="stBaseButton-primary"]:hover {
    transform: translateY(-2px) !important;
    box-shadow: 0 8px 28px rgba(99,102,241,0.55) !important;
}
button[data-testid="stBaseButton-secondary"] {
    background: rgba(15, 10, 40, 0.8) !important;
    border: 1.5px solid rgba(99,102,241,0.35) !important;
    border-radius: 10px !important;
    color: #a5b4fc !important;
    font-weight: 700 !important;
    font-family: 'Plus Jakarta Sans', sans-serif !important;
}
button[data-testid="stBaseButton-secondary"]:hover {
    background: rgba(99,102,241,0.15) !important;
    border-color: rgba(99,102,241,0.6) !important;
}
[data-testid="stDownloadButton"] button {
    background: linear-gradient(135deg, #059669 0%, #10b981 100%) !important;
    border: none !important;
    border-radius: 10px !important;
    color: #ffffff !important;
    font-weight: 800 !important;
    font-family: 'Plus Jakarta Sans', sans-serif !important;
    box-shadow: 0 4px 20px rgba(16,185,129,0.4) !important;
    width: 100% !important;
}

/* ── File uploader ── */
[data-testid="stFileUploader"] section {
    background: rgba(15, 10, 40, 0.6) !important;
    border: 2px dashed rgba(99,102,241,0.35) !important;
    border-radius: 14px !important;
}
[data-testid="stFileUploader"] label { color: #94a3b8 !important; }

/* ── Checkbox ── */
[data-testid="stCheckbox"] label { color: #94a3b8 !important; }

/* ── Dataframe ── */
[data-testid="stDataFrame"] {
    border: 1px solid rgba(99,102,241,0.22) !important;
    border-radius: 12px !important;
    overflow: hidden !important;
}

/* ── Scrollbar ── */
::-webkit-scrollbar { width: 5px; height: 5px; }
::-webkit-scrollbar-track { background: rgba(10,5,30,0.5); }
::-webkit-scrollbar-thumb { background: rgba(99,102,241,0.4); border-radius: 99px; }

/* ── Markdown text ── */
.stMarkdown p, .stMarkdown h1, .stMarkdown h2, .stMarkdown h3 {
    color: #e2e8f0 !important;
}
</style>
""", unsafe_allow_html=True)

# ── session state ──────────────────────────────────────────────────────────────
for k, v in {"logged_in": False, "user": None, "auth_mode": "login", "cleaned_result": None}.items():
    if k not in st.session_state:
        st.session_state[k] = v

APP = "⚡ Clean Master"

# ── Plotly dark theme ──────────────────────────────────────────────────────────
def dark_fig(fig, title="", h=300):
    fig.update_layout(
        title=dict(text=title, font=dict(color="#c4c9d4", size=12, family="Plus Jakarta Sans"), x=0.01),
        paper_bgcolor="rgba(12,8,35,0.0)",
        plot_bgcolor ="rgba(12,8,35,0.0)",
        font=dict(family="Plus Jakarta Sans", color="#64748b", size=10),
        height=h, margin=dict(l=44, r=16, t=40, b=36),
        legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(color="#94a3b8", size=10)),
    )
    fig.update_xaxes(gridcolor="rgba(99,102,241,0.1)", zerolinecolor="rgba(99,102,241,0.1)", tickfont=dict(color="#64748b"))
    fig.update_yaxes(gridcolor="rgba(99,102,241,0.1)", zerolinecolor="rgba(99,102,241,0.1)", tickfont=dict(color="#64748b"))
    return fig

def fig_summary_bar(stats):
    vals   = [stats["original_rows"], stats["cleaned_rows"], stats["missing_values"], stats["outliers_removed"]]
    labels = ["Original", "Cleaned", "Values Filled", "Outliers Out"]
    colors = ["#818cf8", "#34d399", "#fbbf24", "#f87171"]
    fig = go.Figure(go.Bar(x=labels, y=vals, marker_color=colors, opacity=0.88,
                           text=vals, textposition="outside",
                           textfont=dict(color="#e2e8f0", size=12), marker_line_width=0))
    return dark_fig(fig, "Pipeline Results", 290)

def fig_donut(original, cleaned):
    fig = go.Figure(go.Pie(
        labels=["Retained", "Removed"], values=[cleaned, original - cleaned],
        hole=0.62, marker_colors=["#34d399", "#f87171"],
        textinfo="label+percent", textfont=dict(color="#e2e8f0", size=11), pull=[0, 0.05]))
    fig.add_annotation(text=f"<b>{cleaned:,}</b><br>clean",
                       x=0.5, y=0.5, font=dict(size=13, color="#e2e8f0"), showarrow=False)
    fig = dark_fig(fig, "Data Retention", 290)
    fig.update_layout(legend=dict(orientation="h", y=-0.08))
    return fig

def fig_missing_bar(missing_by_col, total_rows):
    cols = list(missing_by_col.keys())
    pcts = [v / total_rows * 100 for v in missing_by_col.values()]
    fig = go.Figure(go.Bar(x=cols, y=pcts, marker_color="#fbbf24", opacity=0.85,
                           text=[f"{p:.1f}%" for p in pcts], textposition="outside",
                           textfont=dict(color="#fbbf24", size=10), marker_line_width=0))
    fig.update_xaxes(tickangle=-35, tickfont=dict(size=9))
    return dark_fig(fig, "Missing Values per Column (%)", 280)

def fig_heatmap(df_raw):
    s = df_raw.head(60)
    fig = go.Figure(go.Heatmap(z=s.isnull().astype(int).values, x=list(s.columns),
                                colorscale=[[0,"rgba(52,211,153,0.1)"],[1,"rgba(248,113,113,0.8)"]],
                                showscale=False))
    fig.update_xaxes(tickangle=-45, tickfont=dict(size=8))
    return dark_fig(fig, "Missing Map — Red = Missing (first 60 rows)", 280)

def fig_distribution(df_raw, df_clean, col):
    fig = go.Figure()
    fig.add_trace(go.Histogram(x=df_raw[col].dropna(),   name="Before", marker_color="#f87171", opacity=0.5, nbinsx=28, histnorm="probability density"))
    fig.add_trace(go.Histogram(x=df_clean[col].dropna(), name="After",  marker_color="#34d399", opacity=0.65, nbinsx=28, histnorm="probability density"))
    fig.update_layout(barmode="overlay")
    return dark_fig(fig, col, 250)

def fig_boxplots(df_raw, df_clean):
    ncols = df_raw.select_dtypes(include=[np.number]).columns.tolist()[:5]
    if not ncols: return None
    fig = make_subplots(rows=1, cols=len(ncols), subplot_titles=ncols)
    for i, col in enumerate(ncols, 1):
        fig.add_trace(go.Box(y=df_raw[col].dropna(),   name="Before", marker_color="#f87171", line_color="#f87171", showlegend=(i==1), boxmean=True), row=1, col=i)
        fig.add_trace(go.Box(y=df_clean[col].dropna(), name="After",  marker_color="#34d399", line_color="#34d399", showlegend=(i==1), boxmean=True), row=1, col=i)
    fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                      font=dict(family="Plus Jakarta Sans", color="#64748b", size=9),
                      height=300, margin=dict(l=30,r=16,t=44,b=30),
                      legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(color="#94a3b8")),
                      boxmode="group")
    for ann in fig.layout.annotations:
        ann.font.update(color="#94a3b8", size=9)
    return fig

# ── HTML components ────────────────────────────────────────────────────────────
def card(content, border_color="#6366f1", bg="rgba(15,10,40,0.75)"):
    return f'<div style="background:{bg};border:1px solid {border_color};border-radius:16px;padding:1.3rem;margin:0.4rem 0;backdrop-filter:blur(10px);">{content}</div>'

def metric_card(icon, value, label, color="#818cf8", bg_color="rgba(99,102,241,0.08)"):
    return f"""
    <div style="background:{bg_color};border:1px solid rgba(99,102,241,0.2);border-radius:16px;
         padding:1.3rem 1rem;text-align:center;position:relative;overflow:hidden;">
      <div style="position:absolute;top:0;left:0;right:0;height:3px;background:{color};border-radius:3px 3px 0 0;"></div>
      <div style="font-size:1.7rem;margin-bottom:0.4rem;">{icon}</div>
      <div style="font-size:2rem;font-weight:900;color:{color};line-height:1;">{value}</div>
      <div style="font-size:0.68rem;font-weight:700;color:#475569;text-transform:uppercase;letter-spacing:0.6px;margin-top:5px;">{label}</div>
    </div>"""

def section_title(text):
    st.markdown(f"""
    <div style="display:flex;align-items:center;gap:10px;margin:1.8rem 0 0.8rem;">
      <span style="font-size:1rem;font-weight:800;color:#e2e8f0;">{text}</span>
      <div style="flex:1;height:1px;background:linear-gradient(90deg,rgba(99,102,241,0.4),transparent);"></div>
    </div>""", unsafe_allow_html=True)

def pipeline_steps(done=False):
    steps = [("📤","Upload"),("🔍","Detect"),("✨","Impute"),("🎯","Outliers"),("📏","Scale"),("💾","Export")]
    parts = []
    for i,(icon,label) in enumerate(steps):
        border = "rgba(52,211,153,0.5)" if done else "rgba(99,102,241,0.25)"
        bg     = "rgba(52,211,153,0.07)" if done else "rgba(15,10,40,0.7)"
        arrow  = '<span style="color:rgba(99,102,241,0.4);font-size:1rem;margin:0 3px;">›</span>' if i<len(steps)-1 else ""
        parts.append(f"""
        <div style="display:flex;align-items:center;">
          <div style="display:flex;flex-direction:column;align-items:center;background:{bg};
               border:1px solid {border};border-radius:12px;padding:10px 12px;width:90px;">
            <span style="font-size:1.3rem;">{icon}</span>
            <span style="font-size:0.58rem;font-weight:700;color:#64748b;text-transform:uppercase;
                  letter-spacing:0.4px;margin-top:4px;text-align:center;">{label}</span>
          </div>
          {arrow}
        </div>""")
    st.markdown(f'<div style="display:flex;justify-content:center;align-items:center;flex-wrap:wrap;gap:2px;margin:1.2rem 0 1.8rem;">{"".join(parts)}</div>', unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════════
#  NAVBAR
# ═══════════════════════════════════════════════════════════════════════════════
def navbar(logged_in=False, username=""):
    user_html = f'<span style="font-size:0.8rem;color:#94a3b8;background:rgba(99,102,241,0.12);border:1px solid rgba(99,102,241,0.28);border-radius:99px;padding:5px 14px;">👤 {username}</span>' if logged_in else ""
    st.markdown(f"""
    <div style="background:rgba(10,5,28,0.9);backdrop-filter:blur(20px);
         border-bottom:1px solid rgba(99,102,241,0.22);padding:0 2rem;
         height:62px;display:flex;align-items:center;justify-content:space-between;
         margin-bottom:1.5rem;">
      <div style="font-size:1.3rem;font-weight:900;letter-spacing:-0.5px;
           background:linear-gradient(135deg,#a5b4fc 0%,#e879f9 55%,#38bdf8 100%);
           -webkit-background-clip:text;-webkit-text-fill-color:transparent;">
        {APP}
      </div>
      {user_html}
    </div>""", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════════
#  AUTH PAGE
# ═══════════════════════════════════════════════════════════════════════════════
def show_auth_page():
    navbar()
    is_signup = st.session_state.auth_mode == "signup"

    _, col, _ = st.columns([1, 1.05, 1])
    with col:
        # Logo + heading
        st.markdown(f"""
        <div style="text-align:center;margin-bottom:1.5rem;">
          <div style="font-size:2.2rem;font-weight:900;letter-spacing:-1px;
               background:linear-gradient(135deg,#a5b4fc 0%,#e879f9 55%,#38bdf8 100%);
               -webkit-background-clip:text;-webkit-text-fill-color:transparent;">
            {APP}
          </div>
          <div style="font-size:1.3rem;font-weight:700;color:#e2e8f0;margin-top:0.4rem;">
            {"Create your account" if is_signup else "Welcome back"}
          </div>
          <div style="font-size:0.85rem;color:#475569;margin-top:0.3rem;">
            {"Start cleaning datasets with AI — free forever" if is_signup else "Sign in to continue"}
          </div>
        </div>""", unsafe_allow_html=True)

        # Tab row
        c1, c2 = st.columns(2)
        with c1:
            if st.button("Sign In",  use_container_width=True, type="primary" if not is_signup else "secondary"):
                st.session_state.auth_mode = "login";  st.rerun()
        with c2:
            if st.button("Sign Up",  use_container_width=True, type="primary" if is_signup else "secondary"):
                st.session_state.auth_mode = "signup"; st.rerun()

        st.markdown("<br>", unsafe_allow_html=True)

        if is_signup:
            nu  = st.text_input("Username",         placeholder="Choose a username",  key="su_u")
            np_ = st.text_input("Password",         placeholder="Create a password",  type="password", key="su_p")
            np2 = st.text_input("Confirm Password", placeholder="Repeat password",    type="password", key="su_p2")
            st.markdown("<br>", unsafe_allow_html=True)
            if st.button("Create Account →", type="primary", use_container_width=True):
                if not nu or not np_:      st.error("Please fill in all fields.")
                elif len(np_) < 4:        st.error("Password must be at least 4 characters.")
                elif np_ != np2:          st.error("Passwords do not match!")
                else:
                    with st.spinner("Creating account..."):
                        r = signup_user(nu, np_)
                    if r["success"]:
                        st.success("Account created! Please sign in.")
                        st.session_state.auth_mode = "login"; st.rerun()
                    else:
                        st.error(r["message"])
        else:
            u = st.text_input("Username", placeholder="Your username",  key="li_u")
            p = st.text_input("Password", placeholder="Your password",  type="password", key="li_p")
            st.markdown("<br>", unsafe_allow_html=True)
            if st.button("Sign In →", type="primary", use_container_width=True):
                if not u or not p:
                    st.error("Please enter your credentials.")
                else:
                    with st.spinner("Signing in..."):
                        r = login_user(u, p)
                    if r["success"]:
                        st.session_state.logged_in = True
                        st.session_state.user = r["user"]
                        st.session_state.cleaned_result = None
                        st.rerun()
                    else:
                        st.error("Invalid username or password.")

        # Feature pills
        st.markdown("""
        <div style="margin-top:2rem;display:flex;justify-content:center;gap:1.5rem;flex-wrap:wrap;">
          <span style="font-size:0.74rem;color:#334155;">⚡ KNN Imputation</span>
          <span style="font-size:0.74rem;color:#334155;">🎯 Isolation Forest</span>
          <span style="font-size:0.74rem;color:#334155;">📏 StandardScaler</span>
          <span style="font-size:0.74rem;color:#334155;">🔒 Secure Auth</span>
        </div>""", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════════
#  MAIN APP
# ═══════════════════════════════════════════════════════════════════════════════
def show_upload_page():
    user     = st.session_state.user
    username = user["username"]
    navbar(logged_in=True, username=username)

    # Sidebar
    with st.sidebar:
        st.markdown(f"""
        <div style="font-size:1.1rem;font-weight:900;padding:0.4rem 0 0.8rem;
             background:linear-gradient(135deg,#a5b4fc,#e879f9,#38bdf8);
             -webkit-background-clip:text;-webkit-text-fill-color:transparent;">{APP}</div>
        <div style="background:rgba(99,102,241,0.1);border:1px solid rgba(99,102,241,0.22);
             border-radius:10px;padding:0.65rem;margin-bottom:0.8rem;font-size:0.82rem;color:#94a3b8;">
             👤 &nbsp;<b style="color:#a5b4fc;">{username}</b></div>
        """, unsafe_allow_html=True)
        st.markdown("**Navigation**")
        if st.button("🏠  Dashboard",  use_container_width=True):
            st.session_state.cleaned_result = None; st.rerun()
        if st.button("📂  New Upload", use_container_width=True):
            st.session_state.cleaned_result = None; st.rerun()
        st.markdown("---")
        st.markdown('<p style="font-size:0.78rem;color:#334155;line-height:1.7;">Clean Master uses ML algorithms to automatically prepare your CSV datasets for analysis and modelling.</p>', unsafe_allow_html=True)
        st.markdown("---")
        if st.button("🚪  Sign Out", use_container_width=True):
            st.session_state.logged_in = False; st.session_state.user = None
            st.session_state.cleaned_result = None; st.session_state.auth_mode = "login"
            st.rerun()

    # ══ RESULTS ══════════════════════════════════════════════════════════════
    if st.session_state.cleaned_result:
        r = st.session_state.cleaned_result
        stats = r["stats"]; df_raw = r["df_raw"]; df_clean = r["df"]
        filename = r["filename"]; missing_by_col = r.get("missing_by_col", {})

        st.markdown(f'<div style="background:rgba(52,211,153,0.08);border:1px solid rgba(52,211,153,0.3);border-radius:12px;padding:0.9rem 1.2rem;color:#6ee7b7;font-size:0.88rem;margin-bottom:0.5rem;">✅ &nbsp;<b>Pipeline complete!</b> &nbsp; "{filename}" is cleaned and ready.</div>', unsafe_allow_html=True)
        pipeline_steps(done=True)

        # Metric cards
        section_title("📊 Cleaning Summary")
        pct = round(stats["cleaned_rows"] / stats["original_rows"] * 100, 1) if stats["original_rows"] else 100
        c1,c2,c3,c4,c5,c6 = st.columns(6)
        for col, icon, val, label, color, bg in [
            (c1,"📋", f'{stats["original_rows"]:,}', "Original Rows",    "#818cf8", "rgba(129,140,248,0.08)"),
            (c2,"✅", f'{stats["cleaned_rows"]:,}',  "Clean Rows",       "#34d399", "rgba(52,211,153,0.08)"),
            (c3,"🔧", f'{stats["missing_values"]:,}',"Values Filled",    "#fbbf24", "rgba(251,191,36,0.08)"),
            (c4,"🎯", f'{stats["outliers_removed"]:,}',"Outliers Out",   "#f87171", "rgba(248,113,113,0.08)"),
            (c5,"📐", str(stats["original_cols"]),   "Columns",          "#818cf8", "rgba(129,140,248,0.08)"),
            (c6,"💯", f"{pct}%",                     "Data Retained",    "#22d3ee", "rgba(34,211,238,0.08)"),
        ]:
            with col:
                st.markdown(metric_card(icon, val, label, color, bg), unsafe_allow_html=True)

        # Row 1: bar + donut
        section_title("📈 Visual Analytics")
        ca, cb = st.columns(2)
        with ca:
            st.markdown('<div style="background:rgba(15,10,40,0.7);border:1px solid rgba(99,102,241,0.18);border-radius:16px;padding:0.8rem;">', unsafe_allow_html=True)
            st.plotly_chart(fig_summary_bar(stats), use_container_width=True, config={"displayModeBar": False})
            st.markdown("</div>", unsafe_allow_html=True)
        with cb:
            st.markdown('<div style="background:rgba(15,10,40,0.7);border:1px solid rgba(99,102,241,0.18);border-radius:16px;padding:0.8rem;">', unsafe_allow_html=True)
            st.plotly_chart(fig_donut(stats["original_rows"], stats["cleaned_rows"]), use_container_width=True, config={"displayModeBar": False})
            st.markdown("</div>", unsafe_allow_html=True)

        # Row 2: missing bar + heatmap
        if missing_by_col:
            cc, cd = st.columns(2)
            with cc:
                st.markdown('<div style="background:rgba(15,10,40,0.7);border:1px solid rgba(99,102,241,0.18);border-radius:16px;padding:0.8rem;">', unsafe_allow_html=True)
                st.plotly_chart(fig_missing_bar(missing_by_col, stats["original_rows"]), use_container_width=True, config={"displayModeBar": False})
                st.markdown("</div>", unsafe_allow_html=True)
            with cd:
                st.markdown('<div style="background:rgba(15,10,40,0.7);border:1px solid rgba(99,102,241,0.18);border-radius:16px;padding:0.8rem;">', unsafe_allow_html=True)
                st.plotly_chart(fig_heatmap(df_raw), use_container_width=True, config={"displayModeBar": False})
                st.markdown("</div>", unsafe_allow_html=True)

        # Row 3: distributions
        num_cols = df_raw.select_dtypes(include=[np.number]).columns.tolist()
        if num_cols:
            section_title("📉 Distribution: Before (🔴) vs After (🟢)")
            dcols = st.columns(min(4, len(num_cols)))
            for i, col in enumerate(num_cols[:4]):
                with dcols[i]:
                    st.markdown('<div style="background:rgba(15,10,40,0.7);border:1px solid rgba(99,102,241,0.18);border-radius:16px;padding:0.8rem;">', unsafe_allow_html=True)
                    st.plotly_chart(fig_distribution(df_raw, df_clean, col), use_container_width=True, config={"displayModeBar": False})
                    st.markdown("</div>", unsafe_allow_html=True)

        # Row 4: box plots
        section_title("📦 Box Plots — Outlier Impact")
        st.markdown('<div style="background:rgba(15,10,40,0.7);border:1px solid rgba(99,102,241,0.18);border-radius:16px;padding:0.8rem;">', unsafe_allow_html=True)
        fig = fig_boxplots(df_raw, df_clean)
        if fig: st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
        st.markdown("</div>", unsafe_allow_html=True)

        # Missing table
        if missing_by_col:
            section_title("🔍 Missing Values Breakdown")
            max_m = max(missing_by_col.values())
            rows_html = ""
            for col, cnt in sorted(missing_by_col.items(), key=lambda x: -x[1]):
                p   = cnt / stats["original_rows"] * 100
                bw  = int(cnt / max_m * 140)
                rows_html += f"<tr><td>{col}</td><td style='color:#fbbf24'>{cnt:,}</td><td>{p:.1f}%</td><td><div style='height:7px;width:{bw}px;border-radius:99px;background:linear-gradient(90deg,#818cf8,#e879f9);min-width:4px;'></div></td><td style='color:#34d399;font-weight:700'>✓ Filled</td></tr>"
            st.markdown(f"""
            <div style="background:rgba(15,10,40,0.75);border:1px solid rgba(99,102,241,0.18);border-radius:16px;padding:1.2rem;overflow-x:auto;">
              <table style="width:100%;border-collapse:collapse;font-family:monospace;font-size:0.81rem;">
                <thead><tr>
                  <th style="background:rgba(99,102,241,0.14);color:#a5b4fc;padding:9px 12px;text-align:left;font-size:0.71rem;text-transform:uppercase;letter-spacing:0.5px;border-bottom:1px solid rgba(99,102,241,0.2);">Column</th>
                  <th style="background:rgba(99,102,241,0.14);color:#a5b4fc;padding:9px 12px;text-align:left;font-size:0.71rem;text-transform:uppercase;letter-spacing:0.5px;border-bottom:1px solid rgba(99,102,241,0.2);">Missing</th>
                  <th style="background:rgba(99,102,241,0.14);color:#a5b4fc;padding:9px 12px;text-align:left;font-size:0.71rem;text-transform:uppercase;letter-spacing:0.5px;border-bottom:1px solid rgba(99,102,241,0.2);">% Rows</th>
                  <th style="background:rgba(99,102,241,0.14);color:#a5b4fc;padding:9px 12px;text-align:left;font-size:0.71rem;text-transform:uppercase;letter-spacing:0.5px;border-bottom:1px solid rgba(99,102,241,0.2);">Visual</th>
                  <th style="background:rgba(99,102,241,0.14);color:#a5b4fc;padding:9px 12px;text-align:left;font-size:0.71rem;text-transform:uppercase;letter-spacing:0.5px;border-bottom:1px solid rgba(99,102,241,0.2);">Status</th>
                </tr></thead>
                <tbody style="color:#cbd5e1;">{rows_html}</tbody>
              </table>
            </div>""", unsafe_allow_html=True)

        # Before / after tables
        section_title("🔄 Before vs After — Data Table")
        t1, t2 = st.columns(2)
        with t1:
            st.markdown('<p style="font-size:0.82rem;font-weight:700;color:#f87171;margin-bottom:0.3rem;">⚠️ Raw Dataset</p>', unsafe_allow_html=True)
            st.dataframe(df_raw.head(8), use_container_width=True, height=240)
        with t2:
            st.markdown('<p style="font-size:0.82rem;font-weight:700;color:#34d399;margin-bottom:0.3rem;">✅ Cleaned Dataset</p>', unsafe_allow_html=True)
            st.dataframe(df_clean.head(8), use_container_width=True, height=240)

        # Download
        section_title("⬇️ Download")
        csv_data   = df_clean.to_csv(index=False).encode("utf-8")
        clean_name = filename.replace(".csv", "_cleaned.csv")
        file_kb    = round(len(csv_data) / 1024, 1)
        st.markdown(f"""
        <div style="background:linear-gradient(135deg,rgba(99,102,241,0.12),rgba(168,85,247,0.08));
             border:1px solid rgba(99,102,241,0.3);border-radius:18px;padding:2rem;text-align:center;margin:1rem 0;">
          <div style="font-size:1.15rem;font-weight:800;color:#e2e8f0;margin-bottom:0.4rem;">🎉 Your cleaned dataset is ready!</div>
          <div style="font-size:0.83rem;color:#475569;margin-bottom:1.2rem;">{clean_name} &nbsp;·&nbsp; {stats['cleaned_rows']:,} rows &nbsp;·&nbsp; {file_kb} KB</div>
        </div>""", unsafe_allow_html=True)
        st.download_button("⬇️  Download Cleaned CSV", csv_data, clean_name, "text/csv", use_container_width=True)
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("🔄  Clean Another Dataset", use_container_width=True):
            st.session_state.cleaned_result = None; st.rerun()

    # ══ UPLOAD ═══════════════════════════════════════════════════════════════
    else:
        st.markdown("""
        <h2 style="font-size:1.9rem;font-weight:900;color:#e2e8f0;margin:0 0 0.3rem;letter-spacing:-0.5px;">
          Upload Your Dataset
        </h2>
        <p style="color:#475569;font-size:0.9rem;margin:0 0 1.4rem;">
          Upload any CSV file — Clean Master detects and fixes data quality issues automatically.
        </p>""", unsafe_allow_html=True)

        pipeline_steps(done=False)

        uploaded_file = st.file_uploader(
            "Drop your CSV file here or click Browse",
            type=["csv"], help="Supported: CSV · Max 200 MB")

        if uploaded_file is not None:
            try:
                df_preview    = pd.read_csv(uploaded_file)
                uploaded_file.seek(0)
                missing_total = int(df_preview.isnull().sum().sum())
                missing_cols  = int((df_preview.isnull().sum() > 0).sum())
                dup_count     = int(df_preview.duplicated().sum())
                num_count     = len(df_preview.select_dtypes(include=[np.number]).columns)

                st.markdown(f"""
                <div style="display:flex;gap:1.2rem;flex-wrap:wrap;background:rgba(99,102,241,0.07);
                     border:1px solid rgba(99,102,241,0.2);border-radius:12px;padding:0.8rem 1.2rem;margin:0.8rem 0;font-size:0.83rem;color:#94a3b8;">
                  <span>📄 <b style="color:#a5b4fc">{uploaded_file.name}</b></span>
                  <span>📊 <b style="color:#a5b4fc">{df_preview.shape[0]:,}</b> rows</span>
                  <span>🔢 <b style="color:#a5b4fc">{df_preview.shape[1]}</b> cols</span>
                  <span>🔵 <b style="color:#a5b4fc">{num_count}</b> numeric</span>
                  <span>⚠️ <b style="color:#fbbf24">{missing_total:,}</b> missing</span>
                  <span>🔁 <b style="color:#a5b4fc">{dup_count}</b> duplicates</span>
                </div>""", unsafe_allow_html=True)

                if missing_total > 0:
                    st.markdown(f'<div style="background:rgba(251,191,36,0.08);border:1px solid rgba(251,191,36,0.28);border-radius:12px;padding:0.85rem 1.1rem;color:#fcd34d;font-size:0.86rem;margin:0.6rem 0;">⚠️ <b>{missing_total:,} missing values</b> across <b>{missing_cols}</b> column(s) — KNN Imputation will fill them.</div>', unsafe_allow_html=True)
                else:
                    st.markdown('<div style="background:rgba(56,189,248,0.08);border:1px solid rgba(56,189,248,0.28);border-radius:12px;padding:0.85rem 1.1rem;color:#7dd3fc;font-size:0.86rem;margin:0.6rem 0;">✨ No missing values. Pipeline will remove outliers and duplicates.</div>', unsafe_allow_html=True)

                section_title("👁️ Data Preview")
                st.dataframe(df_preview.head(6), use_container_width=True, height=215)

                section_title("⚙️ Options")
                apply_scaling = st.checkbox("Apply StandardScaler — normalise numeric columns (mean=0, std=1)", value=False)
                st.markdown("<br>", unsafe_allow_html=True)

                if st.button("🚀  Run Cleaning Pipeline", type="primary", use_container_width=True):
                    with st.spinner("Running AI pipeline — usually 5–15 seconds..."):
                        try:
                            with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
                                tmp.write(uploaded_file.getvalue()); tmp_path = tmp.name
                            df_raw_snap    = pd.read_csv(tmp_path)
                            missing_by_col = {c: int(n) for c,n in df_raw_snap.isnull().sum().items() if n > 0}
                            df_clean, stats, output_path = run_pipeline(tmp_path, output_dir="outputs", scale=apply_scaling)
                            try:
                                did = save_dataset_record(user["id"], uploaded_file.name, df_preview.shape[0], df_preview.shape[1])
                                if did: save_processed_record(did, user["id"], uploaded_file.name, os.path.basename(output_path), stats["missing_values"], stats["outliers_removed"])
                            except Exception: pass
                            st.session_state.cleaned_result = {"df": df_clean, "df_raw": df_raw_snap, "stats": stats, "output_path": output_path, "filename": uploaded_file.name, "missing_by_col": missing_by_col}
                            os.unlink(tmp_path); st.rerun()
                        except Exception as e:
                            st.error(f"Pipeline error: {e}")
                            if "tmp_path" in locals() and os.path.exists(tmp_path): os.unlink(tmp_path)
            except Exception as e:
                st.error(f"Could not read file: {e}")
        else:
            st.markdown("""
            <div style="background:rgba(15,10,40,0.6);border:2px dashed rgba(99,102,241,0.25);
                 border-radius:18px;padding:3.5rem;text-align:center;margin:0.8rem 0 1.5rem;">
              <div style="font-size:3.2rem;margin-bottom:0.8rem;">📂</div>
              <div style="font-size:1rem;font-weight:700;color:#94a3b8;">Drag and drop your CSV file into the uploader above</div>
              <div style="font-size:0.78rem;color:#334155;margin-top:0.4rem;">Supported: CSV &nbsp;·&nbsp; Max 200 MB</div>
            </div>""", unsafe_allow_html=True)

            section_title("🧠 What the Pipeline Does")
            for col, icon, title, body in [
                (st.columns(3)[0], "✨", "KNN Imputation",  "Fills missing numeric values using the 5 most similar rows."),
                (st.columns(3)[1], "🎯", "Isolation Forest","Detects and removes outliers that would break your ML models."),
                (st.columns(3)[2], "📏", "StandardScaler",  "Normalises numeric columns to mean=0, std=1 for ML."),
            ]:
                pass

            p1, p2, p3 = st.columns(3)
            for col, icon, title, body, border in [
                (p1,"✨","KNN Imputation",  "Fills missing numeric values using the 5 most similar rows. Context-aware, not just mean/median.","rgba(129,140,248,0.3)"),
                (p2,"🎯","Isolation Forest","Detects and removes statistical outliers that would break your ML models or analysis.","rgba(248,113,113,0.3)"),
                (p3,"📏","StandardScaler",  "Normalises all numeric columns to mean=0, std=1 — so every feature is on the same scale.","rgba(52,211,153,0.3)"),
            ]:
                with col:
                    st.markdown(f"""
                    <div style="background:rgba(15,10,40,0.7);border:1px solid {border};border-radius:16px;
                         padding:1.6rem 1rem;text-align:center;margin-top:0.5rem;">
                      <div style="font-size:2rem;margin-bottom:0.6rem;">{icon}</div>
                      <div style="font-size:0.92rem;font-weight:800;color:#e2e8f0;margin-bottom:0.5rem;">{title}</div>
                      <div style="font-size:0.78rem;color:#475569;line-height:1.7;">{body}</div>
                    </div>""", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════════
def main():
    if st.session_state.logged_in: show_upload_page()
    else: show_auth_page()

if __name__ == "__main__":
    main()