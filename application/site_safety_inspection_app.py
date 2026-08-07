# --------------------------------------------------
# IMPORTS
# --------------------------------------------------
import streamlit as st
import uuid
from io import BytesIO
import re
import json
from collections import Counter
from snowflake.snowpark.context import get_active_session
from datetime import datetime, timezone, timedelta
import base64
import pandas as pd
import altair as alt


# --------------------------------------------------
# SNOWFLAKE SESSION
# --------------------------------------------------
session = get_active_session()
MY_TZ = timezone(timedelta(hours=8))

# --------------------------------------------------
# PAGE CONFIG
# --------------------------------------------------
st.set_page_config(
    page_title="Site Safety Hazard & Risk Inspection",
    layout="wide"
)

# --------------------------------------------------
# DESIGN TOKENS
# --------------------------------------------------
INK = "#161A1F"
AMBER = "#F7A600"
RED = "#D64545"
GREEN = "#3C8F5C"
RULE = "#E3DFD5"
MUTED = "#6B6B63"

SEVERITY_TOKENS = {
    "Low":    {"fg": GREEN},
    "Medium": {"fg": AMBER},
    "High":   {"fg": RED},
}
SEVERITY_ORDER = ["Low", "Medium", "High"]

# --------------------------------------------------
# GLOBAL STYLE
# Palette : ink / safety amber / paper / alert red / green
# Type    : Space Grotesk (display) + Inter (body) + Space Mono (numerals)
# Motif   : diagonal hazard stripe -> scan progress + high-severity cards ONLY
# Severity: segmented indicators (10-block risk meter, 3-block level) —
#           never tinted pills or coloured alert boxes
# --------------------------------------------------
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@500;600;700&family=Inter:wght@400;500;600;700&family=Space+Mono:wght@400;700&display=swap');

:root {
    --ink:#161A1F;
    --amber:#F7A600;
    --paper:#FAF9F5;
    --red:#D64545;
    --green:#3C8F5C;
    --rule:#E3DFD5;
    --muted:#6B6B63;
    --display:'Space Grotesk','Helvetica Neue',Arial,sans-serif;
    --body:'Inter',system-ui,-apple-system,BlinkMacSystemFont,sans-serif;
    --mono:'Space Mono','SFMono-Regular',Consolas,monospace;
}

/* ---------- surface ---------- */
.stApp,
[data-testid="stAppViewContainer"] {
    background:var(--paper);
}
html, body, [data-testid="stAppViewContainer"] * {
    font-family:var(--body);
}
/* keep Streamlit's ligature icon font — the rule above would otherwise
   render icons as their literal names (e.g. "keyboard_arrow_right") */
[data-testid="stIconMaterial"],
span[data-testid="stIconMaterial"],
.material-symbols-rounded,
.material-symbols-outlined {
    font-family:'Material Symbols Rounded','Material Symbols Outlined' !important;
}
code, kbd, pre, samp { font-family:var(--mono); }
[data-testid="stAppViewContainer"] p,
[data-testid="stAppViewContainer"] li,
[data-testid="stAppViewContainer"] label {
    color:var(--ink);
}

/* ---------- hazard stripe motif (2 uses only) ---------- */

/* (a) scan / analysis progress indicator */
[data-testid="stProgressBarTrack"] > div,
.stProgress > div > div > div > div {
    background-image:repeating-linear-gradient(
        45deg,
        #F7A600 0 10px,
        #161A1F 10px 20px
    ) !important;
    background-color:#F7A600 !important;
    background-size:28px 28px;
    animation:stripe-run 0.9s linear infinite;
}
[data-testid="stProgressBarTrack"] {
    background-color:#E3DFD5 !important;
    border-radius:0;
}
@keyframes stripe-run {
    from { background-position:0 0; }
    to   { background-position:28px 0; }
}

/* (b) border accent on high-severity result cards */
.stripe-accent {
    height:6px;
    width:100%;
    margin:0 0 12px 0;
    background-image:repeating-linear-gradient(
        45deg,
        var(--amber) 0 10px,
        var(--ink) 10px 20px
    );
}

/* ---------- header ---------- */
.app-header {
    padding:26px 0 18px 0;
    border-bottom:2px solid var(--ink);
    margin-bottom:26px;
}
.app-eyebrow {
    font-family:var(--mono);
    font-size:12px;
    letter-spacing:0.20em;
    text-transform:uppercase;
    color:var(--muted);
    margin-bottom:10px;
}
/* Space Grotesk is normal-width; sized down from the old condensed face */
.app-title {
    font-family:var(--display);
    font-size:42px;
    font-weight:700;
    line-height:1.05;
    letter-spacing:-0.01em;
    text-transform:uppercase;
    color:var(--ink);
    margin:0 0 10px 0;
}
.app-title .amber { color:var(--amber); }
.app-caption {
    font-size:14px;
    color:var(--muted);
    max-width:680px;
    line-height:1.6;
}

/* ---------- section headings ---------- */
.sec-title {
    font-family:var(--display);
    font-size:22px;
    font-weight:700;
    letter-spacing:0.01em;
    text-transform:uppercase;
    color:var(--ink);
    margin:8px 0 4px 0;
}
.sec-rule {
    height:3px;
    width:56px;
    background:var(--amber);
    margin-bottom:16px;
}
.sec-title-small {
    font-family:var(--display);
    font-size:16px;
    font-weight:600;
    letter-spacing:0.10em;
    text-transform:uppercase;
    color:var(--ink);
    margin:0 0 10px 0;
}
.field-label {
    font-family:var(--display);
    font-size:14px;
    font-weight:600;
    letter-spacing:0.12em;
    text-transform:uppercase;
    color:var(--ink);
    margin:0 0 4px 0;
}

/* ---------- Site ID field ---------- */
.st-key-site_id input,
div[class*="st-key-site_id"] input {
    font-family:var(--display) !important;
    font-size:19px !important;
    font-weight:600 !important;
    letter-spacing:0.06em !important;
    text-transform:uppercase;
    color:var(--ink) !important;
}
[data-testid="stTextInput"] input {
    background:#FFFFFF;
    border-radius:0;
    border:1px solid var(--rule);
}
[data-testid="stTextInput"] input:focus {
    border-color:var(--amber);
    box-shadow:0 0 0 2px rgba(247,166,0,0.25);
}

/* ---------- cards ---------- */
.card {
    background:#FFFFFF;
    border:1px solid var(--rule);
    padding:18px;
    margin-bottom:14px;
}
.card-flat {
    background:#F3F1EA;
    border:1px solid var(--rule);
    padding:16px;
}
.card-plain {
    background:#FFFFFF;
    border:1px solid var(--ink);
    padding:16px;
}

/* ---------- label plate (replaces tinted alert callouts) ----------
   Industrial status plate: solid strip carrying the status word, body on
   white. No tinted fills, no coloured left borders. */
.plate {
    background:#FFFFFF;
    border:1px solid var(--ink);
    margin-bottom:16px;
}
.plate-strip {
    background:var(--ink);
    color:var(--paper);
    font-family:var(--display);
    font-size:13px;
    font-weight:700;
    letter-spacing:0.14em;
    text-transform:uppercase;
    padding:8px 12px;
    display:flex;
    align-items:center;
    gap:8px;
}
.plate-strip .swatch {
    width:8px;
    height:8px;
    display:inline-block;
    flex:0 0 8px;
}
.plate-body {
    padding:14px 16px;
    font-size:15px;
    line-height:1.55;
    color:var(--ink);
}
.plate-body .mono { font-family:var(--mono); font-size:13px; }
.plate--alert .plate-strip { background:var(--red); color:#FFFFFF; }
.plate--ok    .plate-strip { background:var(--green); color:#FFFFFF; }
.plate-metric {
    font-family:var(--mono);
    font-size:26px;
    font-weight:700;
    line-height:1;
    letter-spacing:-0.02em;
    margin-right:8px;
}

/* ---------- segmented severity language ---------- */
/* 10-block risk meter (0–10) */
.seg-track {
    display:flex;
    gap:3px;
    width:100%;
    position:relative;
}
.seg {
    flex:1;
    height:16px;
    background:var(--rule);
    position:relative;
    overflow:hidden;
}
.seg-fill {
    position:absolute;
    top:0; left:0; bottom:0;
}
.seg-threshold {
    position:absolute;
    top:-4px;
    bottom:-4px;
    width:2px;
    background:var(--ink);
}
/* 3-block severity level */
.sev-seg {
    display:flex;
    gap:3px;
}
.sev-seg i {
    display:block;
    flex:1;
    height:8px;
    background:var(--rule);
}
.sev-label {
    font-family:var(--display);
    font-size:16px;
    font-weight:700;
    letter-spacing:0.12em;
    text-transform:uppercase;
    margin-top:8px;
}

/* ---------- numerals ---------- */
.mono { font-family:var(--mono); }
.score-big {
    font-family:var(--mono);
    font-size:34px;
    font-weight:700;
    line-height:1;
    letter-spacing:-0.03em;
}
.score-badge {
    font-family:var(--mono);
    font-size:34px;
    font-weight:700;
    line-height:1;
    padding:8px 0 4px 0;
    display:block;
}
.score-denom {
    font-family:var(--mono);
    font-size:13px;
    font-weight:400;
    color:var(--muted);
    letter-spacing:0.04em;
}
.score-label {
    font-family:var(--display);
    font-size:14px;
    font-weight:600;
    letter-spacing:0.10em;
    text-transform:uppercase;
    color:var(--muted);
}
.img-id {
    font-family:var(--mono);
    font-size:13px;
    font-weight:400;
    letter-spacing:0.02em;
    color:var(--muted);
    word-break:break-all;
}

/* ---------- tags ---------- */
.tag {
    display:inline-block;
    font-size:13px;
    font-weight:500;
    color:var(--ink);
    background:#F3F1EA;
    border:1px solid var(--rule);
    padding:3px 9px;
    margin:0 6px 6px 0;
}
.tag-clear {
    background:#EAF3EC;
    border-color:#C5DFCC;
    color:var(--green);
    font-weight:600;
}

/* ---------- lists ---------- */
.clean-list { margin:0; padding-left:18px; line-height:1.7; font-size:15px; }
.clean-list li { margin-bottom:6px; }
.rank-list { margin:0; padding-left:0; list-style:none; counter-reset:rank; }
.rank-list li {
    counter-increment:rank;
    position:relative;
    padding-left:34px;
    margin-bottom:12px;
    font-size:15px;
    line-height:1.55;
}
.rank-list li::before {
    content:counter(rank);
    position:absolute;
    left:0; top:0;
    width:24px; height:24px;
    background:var(--amber);
    color:var(--ink);
    font-family:var(--mono);
    font-size:12px;
    font-weight:700;
    display:flex;
    align-items:center;
    justify-content:center;
}

/* ---------- buttons ---------- */
.stButton > button,
.stDownloadButton > button {
    font-family:var(--display);
    font-size:14px;
    font-weight:600;
    letter-spacing:0.12em;
    text-transform:uppercase;
    border-radius:0;
    border:1px solid var(--ink);
    background:#FFFFFF;
    color:var(--ink);
    transition:transform 0.06s ease;
}
.stButton > button:hover,
.stDownloadButton > button:hover {
    border-color:var(--amber);
    color:var(--ink);
    background:#FDF2DC;
}
.stButton > button[kind="primary"],
.stButton > button[data-testid="stBaseButton-primary"] {
    background:var(--amber);
    color:var(--ink);
    border:1px solid var(--ink);
}
.stButton > button[kind="primary"]:hover,
.stButton > button[data-testid="stBaseButton-primary"]:hover {
    background:var(--ink);
    color:var(--amber);
}
.stButton > button:active,
.stDownloadButton > button:active { transform:translateY(1px); }
/* button label lives in an inner <p>; make it follow the button's colour
   so hover/disabled states never go text-on-same-colour */
.stButton > button p,
.stDownloadButton > button p {
    color:inherit !important;
    font-family:inherit !important;
    font-size:inherit !important;
    letter-spacing:inherit !important;
    font-weight:inherit !important;
}

.dl-link {
    display:block;
    width:100%;
    text-align:center;
    background:var(--amber);
    color:var(--ink) !important;
    border:1px solid var(--ink);
    padding:11px;
    font-family:var(--display);
    font-size:14px;
    font-weight:600;
    letter-spacing:0.12em;
    text-transform:uppercase;
    text-decoration:none !important;
    cursor:pointer;
}
.dl-link:hover { background:var(--ink); color:var(--amber) !important; }

/* ---------- expanders ---------- */
[data-testid="stExpander"] {
    border:none;
    background:transparent;
}
[data-testid="stExpander"] details {
    border:1px solid var(--rule);
    border-radius:0;
    background:#FFFFFF;
}
[data-testid="stExpander"] summary {
    border-radius:0;
    font-family:var(--display);
    font-size:13px;
    font-weight:600;
    letter-spacing:0.12em;
    text-transform:uppercase;
}

/* ---------- containers ---------- */
[data-testid="stVerticalBlockBorderWrapper"],
[data-testid="stVerticalBlock"] {
    border-radius:0 !important;
    border-color:var(--rule) !important;
}

/* ---------- sidebar : ink panel ---------- */
[data-testid="stSidebar"] {
    background:var(--ink);
    border-right:3px solid var(--amber);
}
[data-testid="stSidebar"] *,
[data-testid="stSidebar"] p,
[data-testid="stSidebar"] li,
[data-testid="stSidebar"] label,
[data-testid="stSidebar"] h1,
[data-testid="stSidebar"] h2,
[data-testid="stSidebar"] h3 {
    color:var(--paper);
}
[data-testid="stSidebar"] [data-testid="stExpander"] details {
    background:#1F242B;
    border:1px solid #2E353E;
}
/* Streamlit paints <summary> with a translucent light overlay, which reads as
   a pale strip on the dark panel and drops the icon to ~2.3:1 contrast */
[data-testid="stSidebar"] [data-testid="stExpander"] summary {
    background:#1F242B !important;
    color:var(--amber) !important;
    font-size:13px;
}
[data-testid="stSidebar"] [data-testid="stExpander"] summary:hover {
    background:#2E353E !important;
}
[data-testid="stSidebar"] [data-testid="stIconMaterial"] {
    color:var(--paper) !important;
}
[data-testid="stSidebar"] blockquote {
    border-left:3px solid var(--amber);
    padding-left:10px;
    color:#CFCBC1;
}
[data-testid="stSidebar"] code {
    background:#2E353E;
    color:var(--amber);
    font-size:13px;
}
[data-testid="stSidebar"] [data-testid="stTab"] {
    font-family:var(--display);
    font-size:13px;
    letter-spacing:0.08em;
    text-transform:uppercase;
    color:var(--paper);
}
[data-testid="stSidebar"] [role="tablist"] {
    border-bottom:1px solid #2E353E;
    /* four tabs overflow the narrow rail; wrap instead of scrolling so no
       pale scrollbar sits on the dark panel */
    flex-wrap:wrap;
    overflow-x:visible;
    row-gap:2px;
}
[data-testid="stSidebar"] [role="tablist"]::-webkit-scrollbar {
    height:4px;
    background:#1F242B;
}
[data-testid="stSidebar"] [role="tablist"]::-webkit-scrollbar-thumb {
    background:#2E353E;
}
/* active-tab indicator ships as Streamlit red (#FF4B4B); bring it on-palette */
[data-testid="stTab"] > div:not([data-testid]) {
    background-color:var(--amber) !important;
}
.side-brand {
    font-family:var(--display);
    font-size:18px;
    font-weight:700;
    letter-spacing:0.12em;
    text-transform:uppercase;
    color:var(--paper);
    margin-bottom:2px;
}
.side-sub {
    font-family:var(--mono);
    font-size:12px;
    letter-spacing:0.16em;
    text-transform:uppercase;
    color:#8A8F97;
    margin-bottom:14px;
}

/* ---------- scan progress block ---------- */
.scan-label {
    font-family:var(--display);
    font-size:16px;
    font-weight:600;
    letter-spacing:0.12em;
    text-transform:uppercase;
    color:var(--ink);
    margin-bottom:2px;
}
.scan-status {
    font-family:var(--mono);
    font-size:13px;
    letter-spacing:0.01em;
    color:var(--muted);
}
.scan-status b { color:var(--amber); font-weight:700; }

/* ---------- Vega/Altair tooltip ---------- */
#vg-tooltip-element {
    background:var(--ink) !important;
    color:var(--paper) !important;
    border:1px solid var(--ink) !important;
    border-radius:0 !important;
    box-shadow:none !important;
    font-family:var(--body) !important;
    font-size:12px !important;
    padding:8px 10px !important;
}
#vg-tooltip-element .key,
#vg-tooltip-element td.key {
    color:#A9A79E !important;
    font-weight:400 !important;
    text-transform:uppercase;
    letter-spacing:0.08em;
    font-size:10px !important;
}
#vg-tooltip-element .value,
#vg-tooltip-element td.value {
    color:var(--paper) !important;
    font-family:var(--mono) !important;
    font-weight:700 !important;
}

/* ---------- misc ---------- */
[data-testid="stDataFrame"] { border:1px solid var(--rule); border-radius:0; }
hr { border-color:var(--rule); }
</style>
""", unsafe_allow_html=True)


# --------------------------------------------------
# SECTION: SIDE-BAR
# Single icon-triggered panel, collapsed by default.
# (Streamlit forbids nested expanders -> tabs inside.)
# --------------------------------------------------
with st.sidebar:
    st.markdown(
        """
        <div class="side-brand">Site Safety</div>
        <div class="side-sub">AISQL Inspection</div>
        """,
        unsafe_allow_html=True
    )

    with st.expander("ℹ️", expanded=False):
        tab_model, tab_limits, tab_fn, tab_sev = st.tabs(
            ["Model", "Limits", "AI SQL", "Severity"]
        )

        # ------------------------------
        # MODEL USED
        # ------------------------------
        with tab_model:
            st.markdown("""
            **Claude Sonnet 4.0**

            - Vision capable large language model
            - Optimized for structured reasoning and safety analysis
            - Strong performance on image understanding + text generation
            """)

            st.caption("🔒 Model fixed to Claude Sonnet 4.0 for consistency and auditability")

        # ------------------------------
        # MODEL LIMITATIONS
        # ------------------------------
        with tab_limits:
            st.markdown("""
            All models available in **Snowflake Cortex** have limitations related to
            their **context window**, input size, and output capacity.

            ### Context Window
            - **Claude Sonnet 4.0** supports a **200,000 token context window**
            - Tokens include:
                - Image content
                - Prompt instructions
                - Generated output
            - Inputs exceeding this limit may result in **errors**
            - Outputs exceeding the limit may be **truncated**

            ### Image Processing Limits
            - Supported file types: `.jpg`, `.jpeg`, `.png`, `.webp`, `.gif`
            - **Maximum file size per image:** ~**3.75 MB**
            - **Maximum images per prompt:** up to **20**
            - Images must be **smaller than 8000 × 8000 pixels**
            - Limits apply **per individual image**

            ### Model Behavior
            - Image understanding is **probabilistic**, not deterministic
            - Results depend on:
                - Image clarity
                - Lighting conditions
                - Camera angle and occlusion
            - The model **cannot verify information outside the visible image**
            - Risk scores and severity levels are **estimates**, not compliance decisions

            > This AI system is designed to **assist** safety assessments
            > and should **not replace certified safety inspections**.
            """)

        # ------------------------------
        # AISQL FUNCTIONS USED
        # ------------------------------
        with tab_fn:
            st.markdown("""
            This application uses **Snowflake Cortex AI SQL** for end to end image based safety analysis:

            **`AI_FILTER()`**
            - Early screening to detect whether an image contains potential safety hazards
            - Filters out non actionable images to **avoid unnecessary AI processing**
            - Improves performance and cost efficiency at scale

            **`AI_CLASSIFY()`**
            - Multi label **hazard category classification** per image
            - Identifies real world site hazards (e.g., fall risk, PPE, electrical, trip hazards)
            - Produces structured outputs for aggregation and analytics

            **`AI_COMPLETE()`**
            - Risk score calculation (0–10)
            - Hazard explanations based on visible conditions
            - Corrective action recommendations
            - Site-wide **Top 3 prioritized corrective actions**
            """)

        # ------------------------------
        # RISK SEVERITY LOGIC
        # ------------------------------
        with tab_sev:
            st.markdown("""
            Risk **severity levels** are derived deterministically from the
            AI generated **risk score (0–10)** to ensure consistency and transparency.

            ### Severity Mapping Logic
            - **Low Risk:** score **< 4**
            - **Medium Risk:** score **≥ 4 and < 7**
            - **High Risk:** score **≥ 7**
            """)


# --------------------------------------------------
# SECTION: HEADER
# --------------------------------------------------
st.markdown("""
<div class="app-header">
    <div class="app-eyebrow">Snowflake Cortex AISQL</div>
    <div class="app-title">
        Site Safety<br/>Hazard &amp; <span class="amber">Risk</span> Inspection
    </div>
    <div class="app-caption">
        Detect safety hazards, calculate risk, and recommend corrective actions
        from site inspection imagery.
    </div>
</div>
""", unsafe_allow_html=True)

# --------------------------------------------------
# SECTION: INPUT
# --------------------------------------------------
st.markdown(
    '<div class="sec-title">Site Information</div><div class="sec-rule"></div>',
    unsafe_allow_html=True
)

in_col_id, in_col_files = st.columns([1, 2])

with in_col_id:
    st.markdown('<div class="field-label">Site ID</div>', unsafe_allow_html=True)
    site_id = st.text_input(
        "Site ID",
        value="SITE_A",
        key="site_id",
        label_visibility="collapsed"
    )

with in_col_files:
    st.markdown('<div class="field-label">Inspection Images</div>', unsafe_allow_html=True)
    uploaded_files = st.file_uploader(
        "Upload site inspection images",
        type=["jpg", "jpeg", "png"],
        accept_multiple_files=True,
        label_visibility="collapsed"
    )

analyze_btn = st.button(
    "Analyze Site",
    use_container_width=True,
    type="primary"
)

st.markdown("<div style='height:10px;'></div>", unsafe_allow_html=True)

# --------------------------------------------------
# SESSION STATE – SITE HISTORY
# --------------------------------------------------

if "analysis_results" not in st.session_state:
    st.session_state.analysis_results = []

if "email_status" not in st.session_state:
    st.session_state.email_status = None

if "email_status_type" not in st.session_state:
    st.session_state.email_status_type = None

# --------------------------------------------------
# HELPERS
# --------------------------------------------------
def upload_image_to_stage(uploaded_file):
    image_id = f"IMG_{uuid.uuid4().hex[:8]}"
    ext = uploaded_file.name.split(".")[-1].lower()
    file_name = f"{image_id}.{ext}"

    stage_path = f"@SYNOGIZE_DB.AISQL_SITE_SAFETY.SAFETY_IMG_STG/{file_name}"

    session.file.put_stream(
        BytesIO(uploaded_file.getvalue()),
        stage_path,
        auto_compress=False,
        overwrite=True
    )

    return file_name


def extract_labels(value):
    if isinstance(value, dict):
        return value.get("labels", [])
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, dict):
                return parsed.get("labels", [])
        except:
            return [value]
    return []

def severity_from_score(score):
    if score < 4:
        return "Low"
    elif score < 7:
        return "Medium"
    else:
        return "High"


def severity_color(sev):
    return SEVERITY_TOKENS.get(sev, {}).get("fg", INK)


def severity_segments(sev, width="84px"):
    """
    3-block severity indicator (Low | Medium | High). The block matching the
    level is filled with the status colour; the rest stay neutral. Replaces
    tinted pill badges so severity is read positionally, not by chip colour.
    """
    active = SEVERITY_ORDER.index(sev) if sev in SEVERITY_ORDER else -1
    blocks = "".join(
        f'<i style="background:{severity_color(sev) if i == active else RULE};"></i>'
        for i in range(len(SEVERITY_ORDER))
    )
    return (
        f'<div class="sev-seg" style="width:{width};">{blocks}</div>'
        f'<div class="sev-label" style="color:{severity_color(sev)};">{sev}</div>'
    )


def risk_meter(score, threshold=7.0):
    """
    10-block instrument meter for a 0-10 score, with a threshold rule.
    Flat and square - the same visual language as severity_segments().
    """
    colour = severity_color(severity_from_score(score))
    blocks = ""
    for i in range(10):
        fill = max(0.0, min(1.0, score - i)) * 100
        blocks += (
            '<span class="seg">'
            f'<span class="seg-fill" style="width:{fill:.0f}%; background:{colour};"></span>'
            "</span>"
        )
    marker = f'<span class="seg-threshold" style="left:{threshold * 10:.0f}%;"></span>'
    return f'<div class="seg-track">{blocks}{marker}</div>'


def hazard_bar_chart(df, cat_col, val_col, cat_title, val_title, colour=None):
    """
    Horizontal bar chart in the app's visual language: transparent ground so
    the paper background shows through, square amber bars, no gridlines, no
    axis furniture, and the value set at the bar end in the mono face so the
    chart reads at rest rather than only on hover.

    Category names are drawn as text marks inside the plot area rather than as
    y-axis labels. Vega sizes the axis-label gutter from its own text
    measurement, which happens before the webfonts finish loading, so long
    hazard names were being clipped. Marks inside the plot cannot be clipped
    that way.
    """
    colour = colour or AMBER
    height = max(150, 46 * len(df))
    # headroom so the value label at the bar end is never clipped
    vmax = float(df[val_col].max()) if len(df) else 1.0
    domain_max = vmax * 1.12 if vmax > 0 else 1.0

    # explicit order: sort="-x" cannot resolve on the label layer, whose x is a
    # constant rather than a field
    cat_order = list(df.sort_values(val_col, ascending=False)[cat_col])

    base = alt.Chart(df).encode(
        y=alt.Y(f"{cat_col}:N", sort=cat_order, title=None, axis=None),
        tooltip=[
            alt.Tooltip(f"{cat_col}:N", title=cat_title),
            alt.Tooltip(f"{val_col}:Q", title=val_title)
        ]
    )

    x_enc = alt.X(
        f"{val_col}:Q",
        title=None,
        scale=alt.Scale(domain=[0, domain_max], nice=False),
        axis=None
    )

    bars = base.mark_bar(color=colour, height=13, yOffset=9).encode(x=x_enc)

    names = base.mark_text(
        align="left",
        baseline="middle",
        dy=-9,
        font="Inter",
        fontSize=13,
        color=INK
    ).encode(x=alt.value(0), text=alt.Text(f"{cat_col}:N"))

    values = base.mark_text(
        align="left",
        baseline="middle",
        dx=6,
        dy=9,
        font="Space Mono",
        fontSize=13,
        fontWeight=700,
        color=INK
    ).encode(x=x_enc, text=alt.Text(f"{val_col}:Q"))

    return (
        (bars + names + values)
        .properties(height=height, background="transparent")
        .configure_view(strokeWidth=0)
        .configure_axis(labelFont="Inter", titleFont="Inter")
    )


def plate(title, body_html, kind="ink", swatch=None):
    """
    Industrial status plate: solid strip carrying the status word, body on
    white. Replaces the tinted left-border alert boxes.
    kind: ink | alert | ok
    """
    modifier = {"ink": "", "alert": " plate--alert", "ok": " plate--ok"}.get(kind, "")
    dot = f'<span class="swatch" style="background:{swatch};"></span>' if swatch else ""
    return (
        f'<div class="plate{modifier}">'
        f'<div class="plate-strip">{dot}{title}</div>'
        f'<div class="plate-body">{body_html}</div>'
        "</div>"
    )

def bullets_to_html(text):
    """
    Convert AI-generated bullet text into clean plain text
    suitable for emails (no markdown, no HTML).
    """
    cleaned = (
        str(text)
        .replace("\\n", "\n")
        .strip()
        .strip('"')
        .strip("'")
    )

    cleaned = re.sub(r"<[^>]+>", "", cleaned)

    lines = []
    for line in cleaned.splitlines():
        line = line.strip()
        if not line:
            continue

        line = re.sub(r"\*\*(.*?)\*\*", r"\1", line)
        line = line.lstrip("-•0123456789. ").strip()
        lines.append(f"- {line}")

    return "\n".join(lines) if lines else "- No actions identified."

def parse_bullet_lines(text):
    """
    Normalize AI bullet output into display lines,
    preserving bold markers as <strong>.
    """
    raw = str(text).replace("\\n", "\n").strip().strip('"').strip("'")

    lines = []
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        line = re.sub(r"\*\*(.*?)\*\*", r"<strong>\1</strong>", line)
        line = line.lstrip("-•0123456789. ").strip()
        lines.append(line)

    return lines

def build_corrective_actions_checklist(results):
    """
    Build a deduplicated corrective actions checklist
    from all image-level recommended actions.
    """
    checklist_rows = []

    for item in results:
        if item["hazard_categories"] == ["No Visible Hazard"]:
            continue

        raw_actions = str(item["recommended_actions"])
        raw_actions = raw_actions.replace("\\n", "\n").strip().strip('"').strip("'")

        for line in raw_actions.splitlines():
            line = line.strip()
            if not line:
                continue

            line = re.sub(r"\*\*(.*?)\*\*", r"\1", line)
            line = line.lstrip("-•0123456789. ").strip()

            checklist_rows.append({
                "Corrective Action": line,
                "Completed (Yes/No)": "",
                "Responsible Person": "",
                "Target Date": "",
                "Remarks": ""
            })

    if not checklist_rows:
        return pd.DataFrame()

    df = pd.DataFrame(checklist_rows)

    df = df.drop_duplicates(subset=["Corrective Action"]).reset_index(drop=True)

    return df

HAZARD_EMOJI = {
    "Missing PPE": "🦺",
    "Fall Risk": "⬇️",
    "Fire or Explosion Hazard": "🔥",
    "Electrical Hazard": "⚡",
    "Trip or Slip Hazard": "⚠️",
    "Equipment Safety Issue": "🛠️",
    "Improper Storage": "📦",
    "Poor Housekeeping": "🧹",
    "Inadequate Ventilation": "🌬️",
    "Chemical Exposure": "☣️",
    "Structural Hazard": "🏗️",
    "Poor Lighting": "💡",
    "Ergonomic Hazard": "🪑",
    "Struck-by Hazard": "💥",
    "Caught-in or Between Hazard": "🪤",
    "No Visible Hazard": "✅"
}

# --------------------------------------------------
# MAIN ANALYSIS LOGIC
# --------------------------------------------------
results = st.session_state.analysis_results

if analyze_btn:
    if not site_id.strip():
        st.error("❌ Site ID is required.")
        st.stop()

    if not uploaded_files:
        st.error("❌ Please upload at least one site inspection image.")
        st.stop()

    results.clear()

    # ------------------------------------------------------------------
    # PER-IMAGE PROGRESS
    # Three observable steps per image: stage upload, AI_FILTER, then the
    # combined AI_CLASSIFY + AI_COMPLETE statement.
    # ------------------------------------------------------------------
    total_images = len(uploaded_files)
    total_steps = total_images * 3
    step = 0

    scan_box = st.empty()
    with scan_box.container():
        st.markdown('<div class="scan-label">Scanning Site</div>', unsafe_allow_html=True)
        status_slot = st.empty()
        progress_bar = st.progress(0.0)

    def set_status(image_index, file_label, message):
        status_slot.markdown(
            f'<div class="scan-status">IMAGE {image_index}/{total_images} · '
            f'{file_label} — <b>{message}</b></div>',
            unsafe_allow_html=True
        )

    for image_index, uploaded_file in enumerate(uploaded_files, start=1):
        set_status(image_index, uploaded_file.name, "Uploading to SAFETY_IMG_STG")

        file_name = upload_image_to_stage(uploaded_file)

        step += 1
        progress_bar.progress(step / total_steps)

        # --------------------------------------------------
        # AI_FILTER – Pre-check
        # --------------------------------------------------
        set_status(image_index, uploaded_file.name, "AI_FILTER — screening for unsafe conditions")

        filter_query = f"""
        SELECT
            AI_FILTER(
                'Does this image show any unsafe condition, safety hazard, or situation that could pose a risk to people or property?',
                TO_FILE('@SYNOGIZE_DB.AISQL_SITE_SAFETY.SAFETY_IMG_STG','{file_name}')
            ) AS has_potential_hazard
        """

        filter_row = session.sql(filter_query).collect()[0]
        has_potential_hazard = filter_row["HAS_POTENTIAL_HAZARD"]

        step += 1
        progress_bar.progress(step / total_steps)

        # --------------------------------------------------
        # SHORT-CIRCUIT = NO HAZARDS DETECTED
        # --------------------------------------------------
        if not has_potential_hazard:
            set_status(
                image_index,
                uploaded_file.name,
                "AI_FILTER — no hazard found, deep analysis skipped"
            )

            results.append({
                "image_name": uploaded_file.name,
                "image_bytes": uploaded_file.getvalue(),
                "score": 0,
                "severity": "Low",
                "hazard_categories": ["No Visible Hazard"],
                "detected_hazards": None,
                "recommended_actions": None,
                "risk_explanation": (
                    "This image was automatically classified as non actionable by the AI safety filter. "
                    "No unsafe conditions or hazards were detected."
                ),
                "has_potential_hazard": False
            })

            step += 1
            progress_bar.progress(step / total_steps)
            continue

        set_status(
            image_index,
            uploaded_file.name,
            "AI_CLASSIFY + AI_COMPLETE — hazard categories, risk score, actions, explanation"
        )

        query = f"""
SELECT
    AI_COMPLETE(
        'claude-4-sonnet',
        'Return ONLY a single integer risk score from 0 to 10.',
        TO_FILE('@SYNOGIZE_DB.AISQL_SITE_SAFETY.SAFETY_IMG_STG','{file_name}')
    ) AS risk_score,

    AI_CLASSIFY(
        TO_FILE('@SYNOGIZE_DB.AISQL_SITE_SAFETY.SAFETY_IMG_STG','{file_name}'),
        [
            'Missing PPE','Fall Risk','Fire or Explosion Hazard',
            'Electrical Hazard','Trip or Slip Hazard',
            'Equipment Safety Issue','Improper Storage',
            'Poor Housekeeping','Inadequate Ventilation',
            'Chemical Exposure','Structural Hazard','No Visible Hazard',
            'Poor Lighting', 'Ergonomic Hazard', 'Struck-by Hazard',
            'Caught-in or Between Hazard', 'Vehicle or Mobile Equipment Hazard'
        ],
        OBJECT_CONSTRUCT(
            'task_description','Identify all applicable hazard categories.',
            'output_mode','multi'
        )
    ) AS hazard_categories,

    AI_COMPLETE(
        'claude-4-sonnet',
        'List all specific safety hazards visible in this image. Use this exact format:
- [Hazard 1]
- [Hazard 2]
- [Hazard 3]

Do not include any introductory text. Bold keywords. Start directly with the first dash.',
        TO_FILE('@SYNOGIZE_DB.AISQL_SITE_SAFETY.SAFETY_IMG_STG','{file_name}')
    ) AS detected_hazards,

    AI_COMPLETE(
        'claude-4-sonnet',
        'Provide specific corrective actions for the hazards in this image. Use this exact format:
- [Action 1]
- [Action 2]
- [Action 3]

Do not include any introductory text. Bold keywords. Start directly with the first dash.',
        TO_FILE('@SYNOGIZE_DB.AISQL_SITE_SAFETY.SAFETY_IMG_STG','{file_name}')
    ) AS recommended_actions,

    AI_COMPLETE(
        'claude-4-sonnet',
        'Explain concisely why this image received its risk score.
Reference specific visible conditions and explain how they contribute
to the level of risk. Keep the explanation factual, neutral, and
appropriate for a safety inspection report. Limit to a short 1–2 sentences.',
        TO_FILE('@SYNOGIZE_DB.AISQL_SITE_SAFETY.SAFETY_IMG_STG','{file_name}')
    ) AS risk_explanation
"""

        row = session.sql(query).collect()[0]

        score = int(re.search(r"\d+", str(row["RISK_SCORE"])).group())
        severity = severity_from_score(score)

        results.append({
            "image_name": uploaded_file.name,
            "image_bytes": uploaded_file.getvalue(),
            "score": score,
            "severity": severity,
            "hazard_categories": extract_labels(row["HAZARD_CATEGORIES"]),
            "detected_hazards": row["DETECTED_HAZARDS"],
            "recommended_actions": row["RECOMMENDED_ACTIONS"],
            "risk_explanation": (
                str(row["RISK_EXPLANATION"])
                .replace("\\n", " ")
                .replace('"', "")
                .replace("'", "")
                .strip()
            ),
            "has_potential_hazard": True
        })

        step += 1
        progress_bar.progress(step / total_steps)

    progress_bar.progress(1.0)
    scan_box.empty()

    st.session_state.analysis_results = results


# --------------------------------------------------
# SECTION: RESULTS
# --------------------------------------------------

if results:
    st.markdown(
        plate(
            "Analysis complete",
            f'<span class="plate-metric">{len(results)}</span>'
            f'image(s) processed for site <strong class="mono">{site_id}</strong>',
            kind="ink",
            swatch=AMBER
        ),
        unsafe_allow_html=True
    )

    hazard_counter = Counter()

    # --------------------------------------------------
    # BUILD HAZARD COUNTS
    # --------------------------------------------------
    for item in results:
        hazard_counter.update(item["hazard_categories"])

    filtered_hazards = [
        (h, c)
        for h, c in hazard_counter.most_common()
        if h != "No Visible Hazard"
    ]

    site_has_hazards = any(
        r.get("has_potential_hazard") for r in results
    )

    # --------------------------------------------------
    # PERSIST SITE HAZARD HISTORY
    # --------------------------------------------------
    inspection_ts = datetime.now(MY_TZ).strftime("%Y-%m-%d %H:%M:%S")

    for hazard, count in hazard_counter.items():
        if hazard == "No Visible Hazard":
            continue

        session.sql(f"""
            INSERT INTO SYNOGIZE_DB.AISQL_SITE_SAFETY.SITE_HAZARD_HISTORY
            (
                SITE_ID,
                INSPECTION_TS,
                HAZARD_CATEGORY,
                HAZARD_COUNT
            )
            VALUES (
                '{site_id}',
                '{inspection_ts}',
                '{hazard}',
                {count}
            )
        """).collect()

    # --------------------------------------------------
    # SITE-WIDE AGGREGATES
    # --------------------------------------------------
    weights = {"Low": 1, "Medium": 2, "High": 3}
    weighted_score = sum(
        item["score"] * weights[item["severity"]] for item in results
    ) / sum(weights[item["severity"]] for item in results)

    site_severity = severity_from_score(weighted_score)


    # --------------------------------------------------
    # AUTO EMAIL ALERT – SAFETY MANAGER
    # --------------------------------------------------
    SAFETY_MANAGER_NAME = "Rafi Hidayat"
    SAFETY_MANAGER_EMAIL = "rafi.hidayat@synogize.io"

    # Prevent duplicate emails per run
    if "auto_email_sent" not in st.session_state:
        st.session_state.auto_email_sent = False

    if site_severity == "High" and not st.session_state.auto_email_sent:

        auto_email_body = f"""
    ⚠️ HIGH SITE RISK ALERT
    
    Site ID: {site_id}
    
    Weighted Site Risk Score: {round(weighted_score, 2)}
    Site Severity: {site_severity}
    
    Most Frequent Hazards:
    {chr(10).join(
        f"- {h}: {c} images"
        for h, c in hazard_counter.most_common()
        if h != "No Visible Hazard"
    )}
    
    Assessment Time (MYT):
    {datetime.now(MY_TZ).strftime("%Y-%m-%d %H:%M:%S")}
    
    This alert was automatically generated due to high site risk.
    Immediate review and mitigation is recommended.
    """

        safe_auto_email_body = auto_email_body.replace("'", "''")

        try:
            session.sql(
                f"""
                CALL SYSTEM$SEND_EMAIL(
                    'SITE_EMAIL_INT',
                    '{SAFETY_MANAGER_EMAIL}',
                    '⚠️ High Site Risk Alert – {site_id}',
                    '{safe_auto_email_body}'
                )
                """
            ).collect()

            st.session_state.auto_email_sent = True

            # UI feedback
            st.markdown(
                plate(
                    "High site risk detected",
                    "Notification automatically sent to <strong>Safety Manager</strong><br/>"
                    f'<strong class="mono">{SAFETY_MANAGER_NAME}</strong>'
                    f'<span class="mono"> · {SAFETY_MANAGER_EMAIL}</span>',
                    kind="alert"
                ),
                unsafe_allow_html=True
            )

        except Exception as e:
            st.error("❌ Failed to send automated safety alert.")
            st.exception(e)

    # --------------------------------------------------
    # PRIORITIZED CORRECTIVE ACTIONS
    # --------------------------------------------------

    # Determine site-level hazard presence using AI_FILTER output (single source of truth)
    site_has_hazards = any(
        r.get("has_potential_hazard") for r in results
    )

    prioritized_actions = None

    if site_has_hazards:

        # Build hazard frequency summary
        hazard_summary = ", ".join(
            f"{hazard} ({count})"
            for hazard, count in filtered_hazards
        )

        # Collect ONLY recommended actions from actionable images
        all_actions_text = "\n".join(
            str(r["recommended_actions"])
            for r in results
            if r.get("has_potential_hazard") is True
            and r.get("recommended_actions")
        )

        # Only call AI if we actually have content
        if all_actions_text.strip():

            prioritized_actions_query = f"""
            SELECT AI_COMPLETE(
                'claude-4-sonnet',
                'You are a site safety expert.
    Based on the following site-wide hazards and observations, generate a prioritized list
    of the TOP 3 corrective actions.
    Rank them from highest to lowest priority.
    Focus on actions that reduce the most risk.
    
    Hazard frequency summary:
    {hazard_summary}
    
    Observed corrective actions:
    {all_actions_text}
    
    Use this exact format:
    - [Action]
    - [Action]
    - [Action]
    
    Do not include any introductory text. Start directly with the first dash.'
            ) AS prioritized_actions
            """

            prioritized_actions = session.sql(
                prioritized_actions_query
            ).collect()[0]["PRIORITIZED_ACTIONS"]

        else:
            prioritized_actions = None

    # --------------------------------------------------
    # SECTION: SITE RISK SUMMARY  (above per-image results)
    # --------------------------------------------------
    st.markdown(
        '<div class="sec-title">Site Risk Summary</div><div class="sec-rule"></div>',
        unsafe_allow_html=True
    )

    col1, col2, col3 = st.columns([1, 1.5, 1.5])

    risk_color = severity_color(site_severity)

    risk_note_map = {
    "High": "This site requires immediate mitigation actions.",
    "Medium": "Mitigation actions should be planned and closely monitored.",
    "Low": "Site risk is currently within acceptable limits."
    }

    risk_note = risk_note_map.get(site_severity, "")


    with col1:
        st.markdown(
            "<div class='sec-title-small'>Overall Site Risk</div>",
            unsafe_allow_html=True
        )
        with st.container(border=True):
            st.markdown(
                f"""
<div>
<div style="display:flex; align-items:baseline; gap:10px;">
<span class="score-big" style="color:{risk_color};">{round(weighted_score, 1)}</span>
<span class="score-denom">/ 10</span>
</div>
<div class="sec-title-small" style="margin:6px 0 16px 0; color:{MUTED};">Weighted Site Risk Score</div>
{risk_meter(weighted_score)}
<div style="display:flex; justify-content:space-between; margin-top:8px;
font-family:'Space Mono',monospace; font-size:12px; letter-spacing:0.08em; color:{MUTED};">
<span>0</span><span>10</span>
</div>
<div style="margin-top:4px; font-family:'Space Mono',monospace; font-size:12px;
letter-spacing:0.06em; color:{MUTED};">HIGH RISK THRESHOLD 7.0</div>
<div style="margin-top:18px; padding-top:14px; border-top:1px solid {RULE};">
{severity_segments(site_severity, width="100%")}
<div style="font-size:14px; line-height:1.55; color:{INK}; margin-top:10px;">{risk_note}</div>
</div>
</div>
""",
                unsafe_allow_html=True
            )

    with col2:
        st.markdown(
            "<div class='sec-title-small'>Hazards In This Inspection</div>",
            unsafe_allow_html=True
        )

        if not filtered_hazards:
            st.markdown(
                """
                <div class="plate plate--ok">
                    <div class="plate-strip">No recurring hazards</div>
                    <div class="plate-body">
                        No hazards were identified in this site inspection.
                    </div>
                </div>
                """,
                unsafe_allow_html=True
            )
        else:
            hazard_df = pd.DataFrame(
                filtered_hazards, columns=["HAZARD_CATEGORY", "IMAGE_COUNT"]
            )

            hazard_chart = hazard_bar_chart(
                hazard_df, "HAZARD_CATEGORY", "IMAGE_COUNT", "Hazard", "Images"
            )

            # same bordered container as the sibling columns so the three read
            # as one group rather than two cards and a floating chart
            with st.container(border=True):
                # theme=None keeps Streamlit's default chart theme from
                # overriding the fonts and colours configured above
                st.altair_chart(hazard_chart, use_container_width=True, theme=None)

    with col3:
        st.markdown(
            "<div class='sec-title-small'>Top 3 Prioritized Actions</div>",
            unsafe_allow_html=True
        )

        if not site_has_hazards:
            st.markdown(
                """
                <div class="plate plate--ok">
                    <div class="plate-strip">No corrective actions required</div>
                    <div class="plate-body">
                        No safety hazards were identified across submitted images.
                    </div>
                </div>
                """,
                unsafe_allow_html=True
            )
        else:
            action_lines = parse_bullet_lines(prioritized_actions)
            actions_html = "".join(f"<li>{line}</li>" for line in action_lines)

            with st.container(border=True):
                st.markdown(
                    f"""
                    <ul class="rank-list">
                        {actions_html}
                    </ul>
                    """,
                    unsafe_allow_html=True
                )

                # ----------------------------------------------
                # CORRECTIVE ACTIONS CHECKLIST EXPORT (CSV)
                # ----------------------------------------------
                checklist_df = build_corrective_actions_checklist(results)

                if not checklist_df.empty:
                    csv_data = checklist_df.to_csv(index=False)

                    st.download_button(
                        label="Corrective Actions Checklist (CSV)",
                        data=csv_data,
                        file_name=f"{site_id}_corrective_actions_checklist.csv",
                        mime="text/csv",
                        use_container_width=True
                    )
                else:
                    st.info("No corrective actions available to generate a checklist.")

    # --------------------------------------------------
    # SECTION: PER-IMAGE RESULT CARDS
    # --------------------------------------------------
    st.markdown("<div style='height:14px;'></div>", unsafe_allow_html=True)
    st.markdown(
        '<div class="sec-title">Image Findings</div><div class="sec-rule"></div>',
        unsafe_allow_html=True
    )

    for idx, item in enumerate(results, start=1):
        sev = item["severity"]
        sev_fg = severity_color(sev)

        with st.container(border=True):

            # Hazard-stripe border accent : high-severity cards only
            if sev == "High":
                st.markdown('<div class="stripe-accent"></div>', unsafe_allow_html=True)

            c_img, c_meta, c_score = st.columns([1, 2.6, 1])

            with c_img:
                st.image(item["image_bytes"], width=150)

            with c_meta:
                if not item["has_potential_hazard"]:
                    tags_html = '<span class="tag tag-clear">✅ No Visible Hazard</span>'
                else:
                    tags_html = "".join(
                        f'<span class="tag">{HAZARD_EMOJI.get(c, "⚠️")} {c}</span>'
                        for c in item["hazard_categories"]
                    )

                st.markdown(
                    f"""
                    <div class="img-id">IMG {idx:02d} · {item['image_name']}</div>
                    <div style="margin:10px 0 12px 0; max-width:120px;">
                        {severity_segments(sev, width="120px")}
                    </div>
                    <div>{tags_html}</div>
                    """,
                    unsafe_allow_html=True
                )

            with c_score:
                st.markdown(
                    f"""
                    <div style="text-align:right;">
                        <span class="score-label">Risk Score</span>
                        <span class="score-badge" style="color:{sev_fg};">
                            {item['score']}<span class="score-denom">/10</span>
                        </span>
                    </div>
                    """,
                    unsafe_allow_html=True
                )

            # --------------------------------------------------
            # COLLAPSED DETAIL
            # --------------------------------------------------
            with st.expander("Detail", expanded=False):

                # ----- WHY THIS SCORE -----
                st.markdown(
                    f"""
                    <div class="card-flat" style="margin-bottom:14px;">
                        <div class="sec-title-small">Why this risk score</div>
                        <div style="font-size:13.5px; line-height:1.65; color:#161A1F;">
                            {item['risk_explanation']}
                        </div>
                    </div>
                    """,
                    unsafe_allow_html=True
                )

                d_haz, d_act = st.columns(2)

                # ----- DETECTED HAZARDS -----
                with d_haz:
                    st.markdown(
                        "<div class='sec-title-small'>Detected Hazards</div>",
                        unsafe_allow_html=True
                    )

                    if not item["has_potential_hazard"]:
                        st.markdown(
                            """
                            <div class="plate plate--ok">
                                <div class="plate-strip">Clear</div>
                                <div class="plate-body">No hazards detected in this image.</div>
                            </div>
                            """,
                            unsafe_allow_html=True
                        )
                    else:
                        hazard_lines = parse_bullet_lines(item["detected_hazards"])
                        hazards_html = "".join(f"<li>{line}</li>" for line in hazard_lines)

                        st.markdown(
                            f"""
                            <div class="card">
                                <ul class="clean-list">
                                    {hazards_html}
                                </ul>
                            </div>
                            """,
                            unsafe_allow_html=True
                        )

                # ----- RECOMMENDED ACTIONS -----
                with d_act:
                    st.markdown(
                        "<div class='sec-title-small'>Recommended Actions</div>",
                        unsafe_allow_html=True
                    )

                    if not item["has_potential_hazard"]:
                        st.markdown(
                            """
                            <div class="plate plate--ok">
                                <div class="plate-strip">Clear</div>
                                <div class="plate-body">No corrective actions required.</div>
                            </div>
                            """,
                            unsafe_allow_html=True
                        )
                    else:
                        action_lines = parse_bullet_lines(item["recommended_actions"])
                        actions_html = "".join(f"<li>{line}</li>" for line in action_lines)

                        st.markdown(
                            f"""
                            <div class="card">
                                <ul class="clean-list">
                                    {actions_html}
                                </ul>
                            </div>
                            """,
                            unsafe_allow_html=True
                        )

    # --------------------------------------------------
    # SECTION: SITE RISK HISTORY
    # --------------------------------------------------
    highest_image_score = max(item["score"] for item in results)

    session.sql(f"""
    INSERT INTO SYNOGIZE_DB.AISQL_SITE_SAFETY.SITE_RISK_HISTORY
    (
        SITE_ID,
        INSPECTION_TS,
        IMAGE_COUNT,
        WEIGHTED_SITE_RISK_SCORE,
        SITE_SEVERITY,
        HIGHEST_IMAGE_SCORE
    )
    VALUES (
        '{site_id}',
        CONVERT_TIMEZONE('America/Los_Angeles', 'Asia/Kuala_Lumpur', CURRENT_TIMESTAMP()),
        {len(results)},
        {round(weighted_score, 2)},
        '{site_severity}',
        {highest_image_score}
    )
    """).collect()

    st.markdown("<div style='height:14px;'></div>", unsafe_allow_html=True)
    st.markdown(
        '<div class="sec-title">Site Risk History</div><div class="sec-rule"></div>',
        unsafe_allow_html=True
    )

    history_df = session.sql(f"""
    SELECT
        INSPECTION_TS AS "Date & Time",
        SITE_ID AS "Site ID",
        IMAGE_COUNT AS "Images",
        WEIGHTED_SITE_RISK_SCORE AS "Weighted Score",
        SITE_SEVERITY AS "Severity",
        HIGHEST_IMAGE_SCORE AS "Highest Image Score"
    FROM SYNOGIZE_DB.AISQL_SITE_SAFETY.SITE_RISK_HISTORY
    WHERE SITE_ID = '{site_id}'
    ORDER BY INSPECTION_TS DESC
    """).to_pandas()


    st.dataframe(history_df, use_container_width=True)


    # MOVING AVERAGE (LAST 3 INSPECTIONS)
    avg_query = f"""
    SELECT
        ROUND(AVG(WEIGHTED_SITE_RISK_SCORE), 2) AS AVG_SCORE
    FROM (
        SELECT WEIGHTED_SITE_RISK_SCORE
        FROM SYNOGIZE_DB.AISQL_SITE_SAFETY.SITE_RISK_HISTORY
        WHERE SITE_ID = '{site_id}'
        ORDER BY INSPECTION_TS DESC
        LIMIT 3
    )
    """

    avg_result = session.sql(avg_query).collect()
    avg_score = avg_result[0]["AVG_SCORE"] if avg_result else None
    avg_severity = severity_from_score(avg_score) if avg_score is not None else "N/A"


    if len(history_df) >= 2:
        curr = history_df.iloc[0]
        prev = history_df.iloc[1]

        diff = round(curr["Weighted Score"] - prev["Weighted Score"], 1)

        col_prev, col_avg, col_curr = st.columns(3)

        with col_prev:
            st.markdown(
                f"""
                <div class="card">
                    <div class="score-denom" style="text-transform:uppercase; letter-spacing:0.14em;">
                        Previous Inspection
                    </div>
                    <div class="score-big" style="margin:8px 0 12px 0; color:{severity_color(prev["Severity"])};">
                        {prev["Weighted Score"]}<span class="score-denom">/10</span>
                    </div>
                    {severity_segments(prev["Severity"])}
                    <div class="img-id" style="margin-top:12px;">
                        {prev["Date & Time"]}
                    </div>
                </div>
                """,
                unsafe_allow_html=True
            )

        with col_avg:
            st.markdown(
                f"""
                <div class="card-flat">
                    <div class="score-denom" style="text-transform:uppercase; letter-spacing:0.14em;">
                        Recent Average
                    </div>
                    <div class="score-big" style="margin:8px 0 12px 0; color:{severity_color(avg_severity)};">
                        {avg_score}<span class="score-denom">/10</span>
                    </div>
                    {severity_segments(avg_severity)}
                    <div class="img-id" style="margin-top:12px;">
                        Last 3 inspections
                    </div>
                </div>
                """,
                unsafe_allow_html=True
            )


        with col_curr:
            st.markdown(
                f"""
                <div class="card" style="border:2px solid {INK};">
                    <div class="score-denom" style="text-transform:uppercase; letter-spacing:0.14em; color:{INK};">
                        Current Inspection
                    </div>
                    <div class="score-big" style="margin:8px 0 12px 0; color:{severity_color(curr["Severity"])};">
                        {curr["Weighted Score"]}<span class="score-denom">/10</span>
                    </div>
                    {severity_segments(curr["Severity"])}
                    <div class="img-id" style="margin-top:12px;">
                        {curr["Date & Time"]}
                    </div>
                </div>
                """,
                unsafe_allow_html=True
            )

        st.markdown("<div style='height:10px;'></div>", unsafe_allow_html=True)

        if diff > 0:
            trend_html = plate(
                "Site risk increased",
                f'<span class="plate-metric" style="color:{RED};">&#9650; {diff}</span>'
                'points versus the previous inspection.',
                kind="alert"
            )
        elif diff < 0:
            trend_html = plate(
                "Site risk decreased",
                f'<span class="plate-metric" style="color:{GREEN};">&#9660; {abs(diff)}</span>'
                'points versus the previous inspection.',
                kind="ok"
            )
        else:
            trend_html = plate(
                "Site risk unchanged",
                '<span class="plate-metric">0.0</span>'
                'points versus the previous inspection.',
                kind="ink"
            )

        st.markdown(trend_html, unsafe_allow_html=True)

        # --------------------------------------------------
        # SECTION: HAZARD FREQUENCY TREND (LAST 10 INSPECTIONS)
        # --------------------------------------------------
        hazard_trend_df = session.sql(f"""
        WITH last_10_inspections AS (
            SELECT DISTINCT INSPECTION_TS
            FROM SYNOGIZE_DB.AISQL_SITE_SAFETY.SITE_HAZARD_HISTORY
            WHERE SITE_ID = '{site_id}'
            ORDER BY INSPECTION_TS DESC
            LIMIT 10
        )
        SELECT
            HAZARD_CATEGORY,
            SUM(HAZARD_COUNT) AS TOTAL_COUNT
        FROM SYNOGIZE_DB.AISQL_SITE_SAFETY.SITE_HAZARD_HISTORY
        WHERE SITE_ID = '{site_id}'
          AND INSPECTION_TS IN (SELECT INSPECTION_TS FROM last_10_inspections)
        GROUP BY HAZARD_CATEGORY
        ORDER BY TOTAL_COUNT DESC
        """).to_pandas()

        st.markdown("<div style='height:14px;'></div>", unsafe_allow_html=True)
        st.markdown(
            "<div class='sec-title-small'>Most Recurring Hazards · Last 10 Inspections</div>",
            unsafe_allow_html=True
        )

        if hazard_trend_df.empty:
            st.markdown(
                plate("No history", "No historical hazard data available yet.", kind="ink"),
                unsafe_allow_html=True
            )
        else:
            # Ensure correct ordering
            hazard_trend_df = hazard_trend_df.sort_values(
                "TOTAL_COUNT", ascending=False
            )

            chart = hazard_bar_chart(
                hazard_trend_df, "HAZARD_CATEGORY", "TOTAL_COUNT",
                "Hazard", "Occurrences"
            )

            st.altair_chart(chart, use_container_width=True, theme=None)


    # --------------------------------------------------
    # SECTION: SHARE & EXPORT ASSESSMENT
    # --------------------------------------------------

    html = f"""
    <html>
    <head>
        <title>Site Safety Report</title>
        <style>
            body {{
                font-family: Arial, sans-serif;
                line-height: 1.6;
                padding: 24px;
            }}
            h1 {{ margin-bottom: 4px; }}
            h2 {{ margin-top: 32px; }}
            table {{
                border-collapse: collapse;
                width: 100%;
                margin-top: 12px;
            }}
            th, td {{
                border: 1px solid #ccc;
                padding: 8px;
                vertical-align: top;
            }}
            th {{
                background: #f3f4f6;
            }}
            .meta {{
                margin-bottom: 16px;
            }}
            .severity {{
                font-weight: bold;
            }}
        </style>
    </head>
    
    <body>
    
    <h1>Site Safety Report</h1>
    
    <div class="meta">
        <p><b>Site ID:</b> {site_id}</p>
        <p><b>Assessment Time (MYT):</b> {datetime.now(MY_TZ).strftime("%Y-%m-%d %H:%M:%S")}</p>
        <p><b>Weighted Site Risk Score:</b> {round(weighted_score, 2)}</p>
        <p><b>Site Severity:</b> <span class="severity">{site_severity}</span></p>
    </div>
    
    <h2>Image-Level Summary</h2>
    
    <table>
        <tr>
            <th>Image</th>
            <th>Risk</th>
            <th>Severity</th>
            <th>Hazards</th>
        </tr>
    """

    for r in results:
        img = base64.b64encode(r["image_bytes"]).decode()
        html += f"""
        <tr>
            <td>
                <img src="data:image/jpeg;base64,{img}" width="160"/><br/>
                {r['image_name']}
            </td>
            <td>{r['score']}</td>
            <td>{r['severity']}</td>
            <td>{", ".join(r['hazard_categories'])}</td>
        </tr>
        """

    html += """
    </table>

    <h2>Most Frequent Hazards</h2>
    <ul>
    """

    for h, c in hazard_counter.items():
        if h != "No Visible Hazard":
            html += f"<li>{h}: {c} images</li>"

    html += "</ul>"

    html += "<h2>Top 3 Prioritized Corrective Actions</h2>"

    if prioritized_actions:
        raw_actions = str(prioritized_actions).replace("\\n", "\n").strip().strip('"').strip("'")
        html += "<ul>"
        for line in raw_actions.splitlines():
            line = line.strip().lstrip("-•0123456789. ").strip()
            if line:
                html += f"<li>{line}</li>"
        html += "</ul>"
    else:
        html += "<p>No prioritized corrective actions generated.</p>"

    html += """
    <p style="margin-top:32px; font-size:12px; color:#666;">
        This report was automatically generated using Snowflake Cortex AISQL.
        Results are based on visible site conditions and are intended to assist safety inspections.
    </p>

    </body>
    </html>
    """


    html_bytes = html.encode("utf-8")
    b64 = base64.b64encode(html_bytes).decode()

    st.markdown("<div style='height:14px;'></div>", unsafe_allow_html=True)
    st.markdown(
        '<div class="sec-title">Share &amp; Export</div><div class="sec-rule"></div>',
        unsafe_allow_html=True
    )

    col_dl, col_email = st.columns([1, 1])

    # --------------------------------------------------
    # DOWNLOAD REPORT
    # --------------------------------------------------
    with col_dl:
        with st.container(border=True):
            st.markdown(
                """
                <div class="sec-title-small">Download Report</div>
                <p style="color:#6B6B63; font-size:13.5px; line-height:1.6; margin-bottom:14px;">
                    Export the full site safety assessment as an HTML report
                    for offline review or audit documentation.
                </p>
                """,
                unsafe_allow_html=True
            )

            st.markdown(
                f"""
                <a class="dl-link"
                   href="data:text/html;base64,{b64}"
                   download="site_safety_report_{site_id}.html">
                    ⬇ Site Safety Report (HTML)
                </a>
                """,
                unsafe_allow_html=True
            )

    # --------------------------------------------------
    # SEND VIA EMAIL
    # --------------------------------------------------
    with col_email:
        with st.container(border=True):
            st.markdown(
                """
                <div class="sec-title-small">Send via Email</div>
                <p style="color:#6B6B63; font-size:13.5px; line-height:1.6; margin-bottom:14px;">
                    Send the assessment summary and prioritized actions
                    directly to stakeholders.
                </p>
                """,
                unsafe_allow_html=True
            )

            recipient_email = st.text_input(
                "Recipient Email Address",
                placeholder="Enter recipient email address here",
                label_visibility="collapsed"
            )

            send_email_btn = st.button(
                "Send Site Risk Assessment",
                type="primary",
                use_container_width=True
            )

            if send_email_btn:
                if not recipient_email:
                    st.error("Please enter a valid email address.")
                else:
                    email_body = f"""
        ⚠️ SITE SAFETY RISK ASSESSMENT
        
        SITE ID: {site_id}
        ASSESSMENT TIME (MYT): {datetime.now(MY_TZ).strftime("%Y-%m-%d %H:%M:%S")}
        
        WEIGHTED SITE RISK SCORE: {round(weighted_score, 2)}
        SITE SEVERITY: {site_severity}
        
        MOST FREQUENT HAZARDS:
        {chr(10).join(
            f"- {h}: {c} images"
            for h, c in hazard_counter.items()
            if h != "No Visible Hazard"
        )}
        
        TOP 3 PRIORITIZED CORRECTIVE ACTIONS:
        {bullets_to_html(prioritized_actions)}
        
        This assessment was generated automatically using Snowflake Cortex AISQL.
        """


                    safe_email_body = email_body.replace("'", "''")

                    try:
                        session.sql(
                            f"""
                            CALL SYSTEM$SEND_EMAIL(
                                'SITE_EMAIL_INT',
                                '{recipient_email}',
                                'Site Safety Risk Assessment – {site_id}',
                                '{safe_email_body}'
                            )
                            """
                        ).collect()

                        st.success(f"✅ Assessment successfully sent to {recipient_email}")

                    except Exception as e:
                        st.error("❌ Failed to send email.")
                        st.exception(e)
