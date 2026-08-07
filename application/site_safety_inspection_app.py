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
import streamlit.components.v1 as components
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
# DESIGN TOKENS — IBM Carbon Design System (g10 theme)
# https://carbondesignsystem.com
# --------------------------------------------------
CDS_BACKGROUND       = "#f4f4f4"   # $background        Gray 10
CDS_LAYER_01         = "#ffffff"   # $layer-01
CDS_LAYER_ACCENT     = "#e0e0e0"   # $layer-accent-01
CDS_BORDER_SUBTLE    = "#e0e0e0"   # $border-subtle-01  Gray 20
CDS_BORDER_STRONG    = "#8d8d8d"   # $border-strong-01  Gray 50
CDS_TEXT_PRIMARY     = "#161616"   # $text-primary      Gray 100
CDS_TEXT_SECONDARY   = "#525252"   # $text-secondary    Gray 70
CDS_TEXT_HELPER      = "#6f6f6f"   # $text-helper       Gray 60
CDS_SUPPORT_ERROR    = "#da1e28"   # $support-error     Red 60
CDS_SUPPORT_WARNING  = "#F7A600"   # brand amber (in place of Carbon Yellow 30)
CDS_SUPPORT_SUCCESS  = "#24a148"   # $support-success   Green 50
CDS_BLUE_60          = "#0f62fe"   # $button-primary / $focus

# Status colour is reserved for status: severity indicators, meter fill,
# and inline-notification borders. Never a surface fill.
SEVERITY_TOKENS = {
    "Low":    {"fg": CDS_SUPPORT_SUCCESS},
    "Medium": {"fg": CDS_SUPPORT_WARNING},
    "High":   {"fg": CDS_SUPPORT_ERROR},
}

# --------------------------------------------------
# GLOBAL STYLE — Carbon tokens injected as CSS
# Type  : IBM Plex Sans (UI) + IBM Plex Mono (numerals, IDs, timestamps)
# Shape : square corners, 1px subtle borders, no shadows
# --------------------------------------------------
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;600&family=IBM+Plex+Mono:wght@400;500&display=swap');

:root {
    --cds-background:#f4f4f4;
    --cds-layer:#ffffff;
    --cds-layer-accent:#e0e0e0;
    --cds-border-subtle:#e0e0e0;
    --cds-border-strong:#8d8d8d;
    --cds-text-primary:#161616;
    --cds-text-secondary:#525252;
    --cds-text-helper:#6f6f6f;
    --cds-text-on-color:#ffffff;
    --cds-support-error:#da1e28;
    --cds-support-warning:#F7A600;
    --cds-support-success:#24a148;
    --cds-blue-60:#0f62fe;
    --cds-blue-hover:#0353e9;
    --cds-blue-active:#002d9c;
    --cds-gray-80:#393939;
    --cds-gray-70-hover:#4c4c4c;
    --sans:'IBM Plex Sans','Helvetica Neue',Arial,sans-serif;
    --mono:'IBM Plex Mono','SFMono-Regular',Consolas,monospace;
}

/* ---------- surface ---------- */
.stApp,
[data-testid="stAppViewContainer"] {
    background:var(--cds-background);
}
html, body, [data-testid="stAppViewContainer"] * {
    font-family:var(--sans);
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

/* ---------- Carbon type scale ---------- */
[data-testid="stAppViewContainer"] p,
[data-testid="stAppViewContainer"] li,
[data-testid="stAppViewContainer"] label {
    color:var(--cds-text-primary);
    font-size:14px;
    line-height:20px;
    letter-spacing:0.16px;
}

/* label-01 */
.cds-label {
    font-size:12px;
    line-height:16px;
    letter-spacing:0.32px;
    font-weight:400;
    color:var(--cds-text-secondary);
    margin:0 0 8px 0;
}
/* label-01, uppercase — section + eyebrow only */
.cds-section-label {
    font-size:12px;
    line-height:16px;
    letter-spacing:0.32px;
    font-weight:400;
    text-transform:uppercase;
    color:var(--cds-text-secondary);
    margin:0;
}
/* section divider: a neutral rule, never a coloured one */
.cds-section {
    border-top:1px solid var(--cds-border-subtle);
    margin:32px 0 16px 0;
    padding-top:12px;
}
/* the header already draws a rule; the first section must not double it */
.cds-section--flush {
    border-top:none;
    margin-top:16px;
    padding-top:0;
}
/* heading-compact-01 */
.cds-heading {
    font-size:14px;
    line-height:18px;
    font-weight:600;
    letter-spacing:0.16px;
    color:var(--cds-text-primary);
    margin:0 0 8px 0;
}
/* body-01 */
.cds-body {
    font-size:14px;
    line-height:20px;
    letter-spacing:0.16px;
    color:var(--cds-text-primary);
}
/* helper-text-01 */
.cds-helper {
    font-size:12px;
    line-height:16px;
    color:var(--cds-text-helper);
}

/* ---------- header bar ---------- */
.cds-header {
    border-bottom:1px solid var(--cds-border-subtle);
    padding:4px 0 16px 0;
    margin-bottom:4px;
}
.cds-eyebrow {
    font-size:12px;
    line-height:16px;
    letter-spacing:0.32px;
    text-transform:uppercase;
    color:var(--cds-text-secondary);
    margin-bottom:8px;
}
/* heading-04 — normal weight, no display face */
.cds-title {
    font-size:28px;
    line-height:36px;
    font-weight:400;
    letter-spacing:0;
    color:var(--cds-text-primary);
    margin:0;
}

/* ---------- tile ---------- */
.cds-tile {
    background:var(--cds-layer);
    border:1px solid var(--cds-border-subtle);
    border-radius:0;
    padding:16px;
}

/* ---------- numerals ---------- */
.cds-mono { font-family:var(--mono); }
.cds-score-lg {
    font-family:var(--mono);
    font-size:32px;
    line-height:40px;
    font-weight:400;
    color:var(--cds-text-primary);
}
.cds-score-md {
    font-family:var(--mono);
    font-size:20px;
    line-height:28px;
    font-weight:400;
    color:var(--cds-text-primary);
}
.cds-denom {
    font-family:var(--mono);
    font-size:12px;
    line-height:16px;
    color:var(--cds-text-helper);
}
.cds-id {
    font-family:var(--mono);
    font-size:12px;
    line-height:16px;
    color:var(--cds-text-secondary);
    word-break:break-all;
}

/* ---------- status indicator (colour lives here) ---------- */
.cds-status {
    display:inline-flex;
    align-items:center;
    gap:8px;
    font-size:14px;
    line-height:18px;
    letter-spacing:0.16px;
}
.cds-status-swatch {
    width:8px;
    height:8px;
    flex:0 0 8px;
    display:inline-block;
}

/* ---------- tag (outline, no fill) ---------- */
.cds-tag {
    display:inline-block;
    font-size:12px;
    line-height:16px;
    color:var(--cds-text-secondary);
    background:var(--cds-layer);
    border:1px solid var(--cds-border-subtle);
    border-radius:0;
    padding:3px 8px;
    margin:0 4px 4px 0;
}

/* ---------- inline notification ---------- */
.cds-notification {
    background:var(--cds-layer);
    border:1px solid var(--cds-border-subtle);
    border-left:3px solid var(--cds-blue-60);
    border-radius:0;
    padding:14px 16px;
}
.cds-notification .cds-n-title {
    font-size:14px;
    line-height:18px;
    font-weight:600;
    color:var(--cds-text-primary);
}
.cds-notification .cds-n-body {
    font-size:14px;
    line-height:18px;
    letter-spacing:0.16px;
    color:var(--cds-text-secondary);
    margin-top:4px;
}
.cds-notification--error   { border-left-color:var(--cds-support-error); }
.cds-notification--warning { border-left-color:var(--cds-support-warning); }
.cds-notification--success { border-left-color:var(--cds-support-success); }
.cds-notification--neutral { border-left-color:var(--cds-gray-80); }

/* ---------- lists ---------- */
.cds-list { margin:0; padding-left:18px; }
.cds-list li {
    font-size:14px;
    line-height:20px;
    letter-spacing:0.16px;
    color:var(--cds-text-primary);
    margin-bottom:6px;
}
.cds-ordered { margin:0; padding-left:0; list-style:none; counter-reset:cds-rank; }
.cds-ordered li {
    counter-increment:cds-rank;
    position:relative;
    padding-left:28px;
    margin-bottom:12px;
    font-size:14px;
    line-height:20px;
    letter-spacing:0.16px;
    color:var(--cds-text-primary);
}
.cds-ordered li::before {
    content:counter(cds-rank);
    position:absolute;
    left:0; top:1px;
    font-family:var(--mono);
    font-size:12px;
    line-height:18px;
    color:var(--cds-text-secondary);
}

/* ---------- data table (Carbon, sm row height) ---------- */
.cds-table-wrap {
    border:1px solid var(--cds-border-subtle);
    background:var(--cds-layer);
    overflow-x:auto;
}
.cds-table {
    width:100%;
    border-collapse:collapse;
    font-family:var(--sans);
}
.cds-table th {
    background:var(--cds-layer-accent);
    color:var(--cds-text-primary);
    font-size:14px;
    font-weight:600;
    line-height:18px;
    text-align:left;
    height:32px;
    padding:0 16px;
    white-space:nowrap;
    border-bottom:1px solid var(--cds-border-subtle);
}
.cds-table td {
    height:32px;
    padding:0 16px;
    font-size:14px;
    line-height:18px;
    letter-spacing:0.16px;
    color:var(--cds-text-primary);
    white-space:nowrap;
    border-bottom:1px solid var(--cds-border-subtle);
}
.cds-table tr:last-child td { border-bottom:none; }
.cds-table td.cds-num,
.cds-table td.cds-ts { font-family:var(--mono); }
.cds-table td.cds-num { text-align:right; }
.cds-table th.cds-num-h { text-align:right; }

/* ---------- buttons ---------- */
.stButton > button,
.stDownloadButton > button {
    font-family:var(--sans);
    font-size:14px;
    line-height:18px;
    letter-spacing:0.16px;
    font-weight:400;
    text-transform:none;
    border-radius:0;
    min-height:48px;
    padding:13px 60px 13px 15px;
    border:1px solid transparent;
    background:var(--cds-blue-60);
    color:var(--cds-text-on-color);
    display:flex;
    align-items:center;
    justify-content:flex-start;
    text-align:left;
    transition:background 70ms cubic-bezier(0,0,0.38,0.9);
}
.stButton > button:hover,
.stDownloadButton > button:hover {
    background:var(--cds-blue-hover);
    color:var(--cds-text-on-color);
    border-color:transparent;
}
.stButton > button:active,
.stDownloadButton > button:active { background:var(--cds-blue-active); }
.stButton > button:focus,
.stDownloadButton > button:focus {
    outline:2px solid var(--cds-blue-60);
    outline-offset:-2px;
    box-shadow:none;
}
/* secondary = Carbon Gray 80 */
.stDownloadButton > button {
    background:var(--cds-gray-80);
}
.stDownloadButton > button:hover { background:var(--cds-gray-70-hover); }
/* Streamlit nests the label in a centring div; Carbon buttons are left-aligned */
.stButton > button > div,
.stDownloadButton > button > div {
    justify-content:flex-start !important;
    width:100%;
}
/* button label lives in an inner <p>; make it follow the button's colour
   so hover/disabled states never go text-on-same-colour */
.stButton > button p,
.stDownloadButton > button p {
    color:inherit !important;
    font-family:inherit !important;
    font-size:inherit !important;
    line-height:inherit !important;
    letter-spacing:inherit !important;
    font-weight:inherit !important;
    text-align:left;
    width:100%;
}

/* anchor styled as a Carbon primary button */
.cds-btn-link {
    display:flex;
    align-items:center;
    justify-content:flex-start;
    width:100%;
    min-height:48px;
    padding:13px 60px 13px 15px;
    background:var(--cds-blue-60);
    color:var(--cds-text-on-color) !important;
    border:1px solid transparent;
    border-radius:0;
    font-family:var(--sans);
    font-size:14px;
    line-height:18px;
    letter-spacing:0.16px;
    text-decoration:none !important;
    cursor:pointer;
}
.cds-btn-link:hover {
    background:var(--cds-blue-hover);
    text-decoration:none !important;
}

/* ---------- text input (Carbon field: bottom rule only) ---------- */
[data-testid="stTextInput"] div[data-baseweb="input"],
[data-testid="stTextInput"] div[data-baseweb="base-input"] {
    background:var(--cds-layer);
    border:none;
    border-radius:0;
    box-shadow:none;
}
[data-testid="stTextInput"] div[data-baseweb="input"] {
    border-bottom:1px solid var(--cds-border-strong);
}
[data-testid="stTextInput"] div[data-baseweb="input"]:focus-within {
    outline:2px solid var(--cds-blue-60);
    outline-offset:-2px;
    border-bottom-color:transparent;
}
[data-testid="stTextInput"] input {
    background:transparent;
    font-family:var(--sans);
    font-size:14px;
    letter-spacing:0.16px;
    color:var(--cds-text-primary);
    height:40px;
    padding:0 16px;
    border-radius:0;
}
[data-testid="stTextInput"] input::placeholder { color:var(--cds-text-helper); }

/* ---------- file uploader ---------- */
[data-testid="stFileUploaderDropzone"] {
    background:var(--cds-layer);
    border:1px dashed var(--cds-border-strong);
    border-radius:0;
}

/* ---------- progress bar ---------- */
[data-testid="stProgressBarTrack"] {
    background-color:var(--cds-border-subtle) !important;
    border-radius:0 !important;
    height:8px !important;
}
[data-testid="stProgressBarTrack"] > div,
.stProgress > div > div > div > div {
    background-image:none !important;
    background-color:var(--cds-blue-60) !important;
    border-radius:0 !important;
    animation:none !important;
}

/* ---------- accordion (Streamlit expander) ---------- */
[data-testid="stExpander"] {
    background:transparent;
    border:none;
    border-radius:0;
    box-shadow:none;
}
[data-testid="stExpander"] details {
    background:transparent;
    border:none;
    border-top:1px solid var(--cds-border-subtle);
    border-bottom:1px solid var(--cds-border-subtle);
    border-radius:0;
    box-shadow:none;
}
[data-testid="stExpander"] summary {
    border-radius:0;
    font-family:var(--sans);
    font-size:14px;
    font-weight:400;
    letter-spacing:0.16px;
    text-transform:none;
    color:var(--cds-text-primary);
}
[data-testid="stExpander"] summary:hover { background:var(--cds-background); }

/* ---------- bordered container = Carbon tile ----------
   st.container(border=True) draws its 1px border on the stVerticalBlock
   itself in this Streamlit version, with an 8px radius. Setting radius +
   border-colour broadly is safe: blocks with border-style:none show neither. */
[data-testid="stVerticalBlockBorderWrapper"],
[data-testid="stVerticalBlock"] {
    border-radius:0 !important;
    border-color:var(--cds-border-subtle) !important;
}

/* ---------- tabs ---------- */
[data-baseweb="tab-list"] {
    gap:0;
    border-bottom:1px solid var(--cds-border-subtle);
    background:transparent;
}
[data-baseweb="tab"] {
    font-family:var(--sans);
    font-size:14px;
    letter-spacing:0.16px;
    text-transform:none;
    color:var(--cds-text-secondary);
}
[data-baseweb="tab"][aria-selected="true"] {
    color:var(--cds-text-primary);
    font-weight:600;
}
[data-baseweb="tab-highlight"] { background-color:var(--cds-blue-60); }

/* ---------- sidebar = Carbon SideNav ---------- */
[data-testid="stSidebar"] {
    background:var(--cds-layer);
    border-right:1px solid var(--cds-border-subtle);
}
[data-testid="stSidebar"] *,
[data-testid="stSidebar"] p,
[data-testid="stSidebar"] li,
[data-testid="stSidebar"] label {
    color:var(--cds-text-primary);
}
[data-testid="stSidebar"] [data-testid="stExpander"] {
    background:transparent;
    border-top:1px solid var(--cds-border-subtle);
    border-bottom:1px solid var(--cds-border-subtle);
}
[data-testid="stSidebar"] blockquote {
    border-left:3px solid var(--cds-border-subtle);
    padding-left:12px;
    color:var(--cds-text-secondary);
}
[data-testid="stSidebar"] code {
    background:var(--cds-background);
    color:var(--cds-text-primary);
    font-size:12px;
}
.cds-product {
    font-size:14px;
    line-height:18px;
    font-weight:600;
    color:var(--cds-text-primary);
    margin-bottom:2px;
}
.cds-product-sub {
    font-size:12px;
    line-height:16px;
    letter-spacing:0.32px;
    text-transform:uppercase;
    color:var(--cds-text-secondary);
    margin-bottom:16px;
}

/* ---------- scan progress ---------- */
.cds-scan-label {
    font-size:14px;
    line-height:18px;
    font-weight:600;
    color:var(--cds-text-primary);
    margin-bottom:4px;
}
.cds-scan-status {
    font-family:var(--mono);
    font-size:12px;
    line-height:16px;
    color:var(--cds-text-secondary);
}

/* ---------- Streamlit alerts (st.exception fallback) ---------- */
[data-testid="stAlert"] {
    border-radius:0;
}

/* ---------- misc ---------- */
[data-testid="stDataFrame"] {
    border:1px solid var(--cds-border-subtle);
    border-radius:0;
}
hr { border-color:var(--cds-border-subtle); }
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
        <div class="cds-product">Site Safety Inspection</div>
        <div class="cds-product-sub">Snowflake Cortex AISQL</div>
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

            - Vision-capable large language model
            - Optimized for structured reasoning and safety analysis
            - Strong performance on image understanding + text generation
            """)

            st.caption("Model fixed to Claude Sonnet 4.0 for consistency and auditability")

        # ------------------------------
        # MODEL LIMITATIONS
        # ------------------------------
        with tab_limits:
            st.markdown("""
            All models available in **Snowflake Cortex** have limitations related to
            their **context window**, input size, and output capacity.

            ### Context Window
            - **Claude Sonnet 4.0** supports a **200,000-token context window**
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
            This application uses **Snowflake Cortex AI SQL** for end-to-end image-based safety analysis:

            **`AI_FILTER()`**
            - Early screening to detect whether an image contains potential safety hazards
            - Filters out non-actionable images to **avoid unnecessary AI processing**
            - Improves performance and cost efficiency at scale

            **`AI_CLASSIFY()`**
            - Multi-label **hazard category classification** per image
            - Identifies real-world site hazards (e.g., fall risk, PPE, electrical, trip hazards)
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
            AI-generated **risk score (0–10)** to ensure consistency and transparency.

            ### Severity Mapping Logic
            - **Low Risk:** score **< 4**
            - **Medium Risk:** score **≥ 4 and < 7**
            - **High Risk:** score **≥ 7**
            """)


# --------------------------------------------------
# SECTION: HEADER
# --------------------------------------------------
st.markdown("""
<div class="cds-header">
    <div class="cds-eyebrow">Snowflake Cortex AISQL</div>
    <div class="cds-title">Site Safety Hazard &amp; Risk Inspection</div>
</div>
""", unsafe_allow_html=True)

# --------------------------------------------------
# SECTION: INPUT
# --------------------------------------------------
st.markdown(
    '<div class="cds-section cds-section--flush">'
    '<div class="cds-section-label">Site Information</div></div>',
    unsafe_allow_html=True
)

in_col_id, in_col_files = st.columns([1, 2])

with in_col_id:
    st.markdown('<div class="cds-label">Site ID</div>', unsafe_allow_html=True)
    site_id = st.text_input(
        "Site ID",
        value="SITE_A",
        key="site_id",
        label_visibility="collapsed"
    )

with in_col_files:
    st.markdown('<div class="cds-label">Inspection images</div>', unsafe_allow_html=True)
    uploaded_files = st.file_uploader(
        "Upload site inspection images",
        type=["jpg", "jpeg", "png"],
        accept_multiple_files=True,
        label_visibility="collapsed"
    )

analyze_btn = st.button(
    "Analyze site",
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
    return SEVERITY_TOKENS.get(sev, {}).get("fg", CDS_TEXT_SECONDARY)

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

def notify(title, body=None, kind="neutral"):
    """Carbon inline notification. kind: error | warning | success | neutral."""
    body_html = f'<div class="cds-n-body">{body}</div>' if body else ""
    st.markdown(
        f'<div class="cds-notification cds-notification--{kind}">'
        f'<div class="cds-n-title">{title}</div>{body_html}</div>',
        unsafe_allow_html=True
    )


def render_history_table(df):
    """
    Render the inspection history as a Carbon data table (sm row height,
    monospace numerals, no zebra striping). Replaces st.dataframe, whose
    cells are canvas-rendered and therefore not styleable.
    """
    num_cols = {"Images", "Weighted Score", "Highest Image Score"}
    ts_cols = {"Date & Time"}

    head = "".join(
        f'<th class="{"cds-num-h" if c in num_cols else ""}">{c}</th>'
        for c in df.columns
    )

    body = ""
    for _, row in df.iterrows():
        cells = ""
        for c in df.columns:
            value = row[c]
            if c == "Severity":
                colour = severity_color(str(value))
                cells += (
                    '<td><span class="cds-status">'
                    f'<span class="cds-status-swatch" style="background:{colour};"></span>'
                    f'<span style="color:{colour};">{value}</span>'
                    "</span></td>"
                )
            elif c in num_cols:
                cells += f'<td class="cds-num">{value}</td>'
            elif c in ts_cols:
                cells += f'<td class="cds-ts">{value}</td>'
            else:
                cells += f"<td>{value}</td>"
        body += f"<tr>{cells}</tr>"

    st.markdown(
        f'<div class="cds-table-wrap"><table class="cds-table">'
        f"<thead><tr>{head}</tr></thead><tbody>{body}</tbody></table></div>",
        unsafe_allow_html=True
    )


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
        notify("Site ID is required", kind="error")
        st.stop()

    if not uploaded_files:
        notify("At least one site inspection image is required", kind="error")
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
        st.markdown('<div class="cds-scan-label">Scanning site</div>', unsafe_allow_html=True)
        status_slot = st.empty()
        progress_bar = st.progress(0.0)

    def set_status(image_index, file_label, message):
        status_slot.markdown(
            f'<div class="cds-scan-status">IMAGE {image_index}/{total_images} · '
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
                    "This image was automatically classified as non-actionable by the AI safety filter. "
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
        f"""
        <div class="cds-notification cds-notification--neutral" style="margin-bottom:24px;">
            <div class="cds-n-title">Analysis complete</div>
            <div class="cds-n-body">
                {len(results)} image(s) processed for site
                <span class="cds-mono">{site_id}</span>
            </div>
        </div>
        """,
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
                f"""
                <div class="cds-notification cds-notification--error" style="margin-bottom:24px;">
                    <div class="cds-n-title">High site risk detected</div>
                    <div class="cds-n-body">
                        Notification automatically sent to safety manager
                        ({SAFETY_MANAGER_NAME} · {SAFETY_MANAGER_EMAIL})
                    </div>
                </div>
                """,
                unsafe_allow_html=True
            )

        except Exception as e:
            notify("Failed to send automated safety alert", kind="error")
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
        '<div class="cds-section"><div class="cds-section-label">Site Risk Summary</div></div>',
        unsafe_allow_html=True
    )

    col1, col2, col3 = st.columns([1, 1.5, 1.5])

    risk_pct = min(max(int((weighted_score / 10) * 100), 0), 100)

    risk_color = severity_color(site_severity)

    threshold_pct = 70  # 7.0 / 10 threshold

    risk_note_map = {
    "High": "This site requires immediate mitigation actions.",
    "Medium": "Mitigation actions should be planned and closely monitored.",
    "Low": "Site risk is currently within acceptable limits."
    }

    risk_note = risk_note_map.get(site_severity, "")


    with col1:
        st.markdown(
            "<div class='cds-heading'>Overall site risk</div>",
            unsafe_allow_html=True
        )
        components.html(
            f"""
            <!-- non-blocking font load: a hanging/blocked font host must never
                 delay first paint of this iframe (fallback stacks below) -->
            <link rel="stylesheet" media="print" onload="this.media='all'"
                  href="https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;600&family=IBM+Plex+Mono:wght@400;500&display=swap"/>
            <style>
                * {{ box-sizing:border-box; }}
                body {{ margin:0; }}
                .cds-meter-tile {{
                    background:#ffffff;
                    border:1px solid #e0e0e0;
                    padding:16px;
                    font-family:'IBM Plex Sans','Helvetica Neue',Arial,sans-serif;
                }}
                .cds-meter-status {{
                    display:flex;
                    align-items:center;
                    gap:8px;
                    font-size:14px;
                    line-height:18px;
                    letter-spacing:0.16px;
                    color:{risk_color};
                }}
                .cds-meter-swatch {{
                    width:8px; height:8px;
                    flex:0 0 8px;
                    background:{risk_color};
                    display:inline-block;
                }}
                .cds-meter-score {{
                    font-family:'IBM Plex Mono','SFMono-Regular',Consolas,monospace;
                    font-size:32px;
                    line-height:40px;
                    font-weight:400;
                    color:#161616;
                    margin:12px 0 0 0;
                }}
                .cds-meter-denom {{
                    font-family:'IBM Plex Mono','SFMono-Regular',Consolas,monospace;
                    font-size:12px;
                    line-height:16px;
                    color:#6f6f6f;
                }}
                .cds-meter-label {{
                    font-size:12px;
                    line-height:16px;
                    letter-spacing:0.32px;
                    color:#525252;
                    margin:0;
                }}
                /* Carbon Meter: flat track, solid status fill, 1px threshold rule */
                .cds-meter-track {{
                    position:relative;
                    height:8px;
                    background:#e0e0e0;
                    margin:20px 0 8px 0;
                }}
                .cds-meter-fill {{
                    position:absolute;
                    top:0; left:0;
                    height:8px;
                    width:{risk_pct}%;
                    background:{risk_color};
                }}
                .cds-meter-threshold {{
                    position:absolute;
                    top:-4px;
                    left:{threshold_pct}%;
                    width:1px;
                    height:16px;
                    background:#161616;
                }}
                .cds-meter-help {{
                    font-size:12px;
                    line-height:16px;
                    color:#6f6f6f;
                    margin:0;
                }}
                .cds-meter-note {{
                    font-size:14px;
                    line-height:20px;
                    letter-spacing:0.16px;
                    color:#525252;
                    margin:16px 0 0 0;
                }}
            </style>

            <div class="cds-meter-tile">
                <div class="cds-meter-status">
                    <span class="cds-meter-swatch"></span>
                    <span>{site_severity} risk</span>
                </div>

                <div class="cds-meter-score">
                    {round(weighted_score, 1)}<span class="cds-meter-denom">/10</span>
                </div>
                <p class="cds-meter-label">Weighted site risk score</p>

                <div class="cds-meter-track">
                    <div class="cds-meter-fill"></div>
                    <div class="cds-meter-threshold"></div>
                </div>
                <p class="cds-meter-help">High-risk threshold 7.0</p>

                <p class="cds-meter-note">{risk_note}</p>
            </div>
            """,
            height=300
        )

    with col2:
        st.markdown(
            "<div class='cds-heading'>Hazards this inspection</div>",
            unsafe_allow_html=True
        )

        if not filtered_hazards:
            st.markdown(
                """
                <div class="cds-notification cds-notification--success">
                    <div class="cds-n-title">No recurring hazards detected</div>
                    <div class="cds-n-body">
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

            hazard_chart = (
                alt.Chart(hazard_df)
                .mark_bar(color=CDS_BLUE_60, height=16)
                .encode(
                    y=alt.Y(
                        "HAZARD_CATEGORY:N",
                        sort="-x",
                        title=None,
                        axis=alt.Axis(
                            labelColor=CDS_TEXT_SECONDARY,
                            labelFont="IBM Plex Sans",
                            labelFontSize=12,
                            labelLimit=220,
                            domainColor=CDS_BORDER_SUBTLE,
                            ticks=False
                        )
                    ),
                    x=alt.X(
                        "IMAGE_COUNT:Q",
                        title="Images",
                        axis=alt.Axis(
                            tickMinStep=1,
                            labelColor=CDS_TEXT_SECONDARY,
                            labelFont="IBM Plex Mono",
                            titleColor=CDS_TEXT_SECONDARY,
                            titleFont="IBM Plex Sans",
                            titleFontSize=12,
                            grid=False,
                            domainColor=CDS_BORDER_SUBTLE
                        )
                    ),
                    tooltip=[
                        alt.Tooltip("HAZARD_CATEGORY:N", title="Hazard"),
                        alt.Tooltip("IMAGE_COUNT:Q", title="Images")
                    ]
                )
                .properties(height=max(140, 30 * len(filtered_hazards)))
                .configure_view(strokeWidth=0)
                .configure_axis(labelFont="IBM Plex Sans", titleFont="IBM Plex Sans")
            )

            st.altair_chart(hazard_chart, use_container_width=True)

    with col3:
        st.markdown(
            "<div class='cds-heading'>Top 3 prioritized actions</div>",
            unsafe_allow_html=True
        )

        if not site_has_hazards:
            st.markdown(
                """
                <div class="cds-notification cds-notification--success">
                    <div class="cds-n-title">No corrective actions required</div>
                    <div class="cds-n-body">
                        No safety hazards were identified across submitted images.
                    </div>
                </div>
                """,
                unsafe_allow_html=True
            )
        else:
            action_lines = parse_bullet_lines(prioritized_actions)
            actions_html = "".join(f"<li>{line}</li>" for line in action_lines)

            st.markdown(
                f"""
                <div class="cds-tile">
                    <ol class="cds-ordered">
                        {actions_html}
                    </ol>
                </div>
                """,
                unsafe_allow_html=True
            )

            # --------------------------------------------------
            # CORRECTIVE ACTIONS CHECKLIST EXPORT (CSV)
            # --------------------------------------------------
            checklist_df = build_corrective_actions_checklist(results)

            if not checklist_df.empty:
                csv_data = checklist_df.to_csv(index=False)

                st.download_button(
                    label="Download corrective actions checklist (CSV)",
                    data=csv_data,
                    file_name=f"{site_id}_corrective_actions_checklist.csv",
                    mime="text/csv",
                    use_container_width=True
                )
            else:
                notify("No corrective actions available to generate a checklist.")

    # --------------------------------------------------
    # SECTION: PER-IMAGE RESULT CARDS
    # --------------------------------------------------
    st.markdown("<div style='height:14px;'></div>", unsafe_allow_html=True)
    st.markdown(
        '<div class="cds-section"><div class="cds-section-label">Image Findings</div></div>',
        unsafe_allow_html=True
    )

    for idx, item in enumerate(results, start=1):
        sev = item["severity"]
        sev_fg = severity_color(sev)

        with st.container(border=True):

            c_img, c_meta, c_score = st.columns([1, 2.6, 1])

            with c_img:
                st.image(item["image_bytes"], width=150)

            with c_meta:
                tags_html = "".join(
                    f'<span class="cds-tag">{c}</span>'
                    for c in item["hazard_categories"]
                )

                st.markdown(
                    f"""
                    <div class="cds-id">IMG {idx:02d} · {item['image_name']}</div>
                    <div class="cds-status" style="margin:8px 0 10px 0;">
                        <span class="cds-status-swatch" style="background:{sev_fg};"></span>
                        <span style="color:{sev_fg};">{sev} risk</span>
                    </div>
                    <div>{tags_html}</div>
                    """,
                    unsafe_allow_html=True
                )

            with c_score:
                st.markdown(
                    f"""
                    <div style="text-align:right;">
                        <div class="cds-denom">Risk score</div>
                        <div class="cds-score-md">{item['score']}<span class="cds-denom">/10</span></div>
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
                    <div class="cds-tile" style="margin-bottom:16px;">
                        <div class="cds-heading">Why this risk score</div>
                        <div class="cds-body">
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
                        "<div class='cds-heading'>Detected hazards</div>",
                        unsafe_allow_html=True
                    )

                    if not item["has_potential_hazard"]:
                        st.markdown(
                            """
                            <div class="cds-notification cds-notification--success">
                                <div class="cds-n-title">No hazards detected in this image</div>
                            </div>
                            """,
                            unsafe_allow_html=True
                        )
                    else:
                        hazard_lines = parse_bullet_lines(item["detected_hazards"])
                        hazards_html = "".join(f"<li>{line}</li>" for line in hazard_lines)

                        st.markdown(
                            f"""
                            <div class="cds-tile">
                                <ul class="cds-list">
                                    {hazards_html}
                                </ul>
                            </div>
                            """,
                            unsafe_allow_html=True
                        )

                # ----- RECOMMENDED ACTIONS -----
                with d_act:
                    st.markdown(
                        "<div class='cds-heading'>Recommended actions</div>",
                        unsafe_allow_html=True
                    )

                    if not item["has_potential_hazard"]:
                        st.markdown(
                            """
                            <div class="cds-notification cds-notification--success">
                                <div class="cds-n-title">No corrective actions required</div>
                            </div>
                            """,
                            unsafe_allow_html=True
                        )
                    else:
                        action_lines = parse_bullet_lines(item["recommended_actions"])
                        actions_html = "".join(f"<li>{line}</li>" for line in action_lines)

                        st.markdown(
                            f"""
                            <div class="cds-tile">
                                <ul class="cds-list">
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
        '<div class="cds-section"><div class="cds-section-label">Site Risk History</div></div>',
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


    render_history_table(history_df)


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
                <div class="cds-tile">
                    <div class="cds-label">Previous inspection</div>
                    <div class="cds-score-lg">{prev["Weighted Score"]}<span class="cds-denom">/10</span></div>
                    <div class="cds-status" style="margin-top:8px;">
                        <span class="cds-status-swatch" style="background:{severity_color(prev["Severity"])};"></span>
                        <span style="color:{severity_color(prev["Severity"])};">{prev["Severity"]}</span>
                    </div>
                    <div class="cds-id" style="margin-top:12px;">{prev["Date & Time"]}</div>
                </div>
                """,
                unsafe_allow_html=True
            )

        with col_avg:
            st.markdown(
                f"""
                <div class="cds-tile">
                    <div class="cds-label">Recent average</div>
                    <div class="cds-score-lg">{avg_score}<span class="cds-denom">/10</span></div>
                    <div class="cds-status" style="margin-top:8px;">
                        <span class="cds-status-swatch" style="background:{severity_color(avg_severity)};"></span>
                        <span style="color:{severity_color(avg_severity)};">{avg_severity}</span>
                    </div>
                    <div class="cds-id" style="margin-top:12px;">Last 3 inspections</div>
                </div>
                """,
                unsafe_allow_html=True
            )


        with col_curr:
            st.markdown(
                f"""
                <div class="cds-tile" style="border-left:3px solid {CDS_BLUE_60};">
                    <div class="cds-label">Current inspection</div>
                    <div class="cds-score-lg">{curr["Weighted Score"]}<span class="cds-denom">/10</span></div>
                    <div class="cds-status" style="margin-top:8px;">
                        <span class="cds-status-swatch" style="background:{severity_color(curr["Severity"])};"></span>
                        <span style="color:{severity_color(curr["Severity"])};">{curr["Severity"]}</span>
                    </div>
                    <div class="cds-id" style="margin-top:12px;">{curr["Date & Time"]}</div>
                </div>
                """,
                unsafe_allow_html=True
            )

        st.markdown("<div style='height:10px;'></div>", unsafe_allow_html=True)

        if diff > 0:
            trend_html = (
                f'<div class="cds-notification cds-notification--error">'
                f'<div class="cds-n-title">Site risk increased by {diff} points</div>'
                f'<div class="cds-n-body">Compared to the previous inspection.</div></div>'
            )
        elif diff < 0:
            trend_html = (
                f'<div class="cds-notification cds-notification--success">'
                f'<div class="cds-n-title">Site risk decreased by {abs(diff)} points</div>'
                f'<div class="cds-n-body">Compared to the previous inspection.</div></div>'
            )
        else:
            trend_html = (
                '<div class="cds-notification cds-notification--neutral">'
                '<div class="cds-n-title">Site risk unchanged</div>'
                '<div class="cds-n-body">Compared to the previous inspection.</div></div>'
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
            "<div class='cds-heading'>Most recurring hazards · last 10 inspections</div>",
            unsafe_allow_html=True
        )

        if hazard_trend_df.empty:
            st.markdown(
                '<div class="cds-notification cds-notification--neutral">'
                '<div class="cds-n-body">No historical hazard data available yet.</div></div>',
                unsafe_allow_html=True
            )
        else:
            # Ensure correct ordering
            hazard_trend_df = hazard_trend_df.sort_values(
                "TOTAL_COUNT", ascending=False
            )

            chart = (
                alt.Chart(hazard_trend_df)
                .mark_bar(color=CDS_BLUE_60)
                .encode(
                    x=alt.X(
                        "HAZARD_CATEGORY:N",
                        sort="-y",
                        title="Hazard Category",
                        axis=alt.Axis(
                            labelAngle=-30,
                            labelColor=CDS_TEXT_SECONDARY,
                            labelFont="IBM Plex Sans",
                            titleColor=CDS_TEXT_SECONDARY,
                            titleFont="IBM Plex Sans",
                            titleFontSize=12,
                            domainColor=CDS_BORDER_SUBTLE,
                            ticks=False
                        )
                    ),
                    y=alt.Y(
                        "TOTAL_COUNT:Q",
                        title="Total Occurrences",
                        axis=alt.Axis(
                            tickMinStep=1,
                            labelColor=CDS_TEXT_SECONDARY,
                            labelFont="IBM Plex Mono",
                            titleColor=CDS_TEXT_SECONDARY,
                            titleFont="IBM Plex Sans",
                            titleFontSize=12,
                            gridColor=CDS_BORDER_SUBTLE,
                            domainColor=CDS_BORDER_SUBTLE
                        )
                    ),
                    tooltip=[
                        alt.Tooltip("HAZARD_CATEGORY:N", title="Hazard"),
                        alt.Tooltip("TOTAL_COUNT:Q", title="Occurrences")
                    ]
                )
                .properties(height=320)
                .configure_view(strokeWidth=0)
                .configure_axis(labelFont="IBM Plex Sans", titleFont="IBM Plex Sans")
            )

            st.altair_chart(chart, use_container_width=True)


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
        '<div class="cds-section"><div class="cds-section-label">Share &amp; Export</div></div>',
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
                <div class="cds-heading">Download report</div>
                <p class="cds-helper" style="margin-bottom:16px;">
                    Export the full site safety assessment as an HTML report
                    for offline review or audit documentation.
                </p>
                """,
                unsafe_allow_html=True
            )

            st.markdown(
                f"""
                <a class="cds-btn-link"
                   href="data:text/html;base64,{b64}"
                   download="site_safety_report_{site_id}.html">
                    Download site safety report (HTML)
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
                <div class="cds-heading">Send via email</div>
                <p class="cds-helper" style="margin-bottom:16px;">
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
                "Send site risk assessment",
                type="primary",
                use_container_width=True
            )

            if send_email_btn:
                if not recipient_email:
                    notify("Enter a valid email address", kind="error")
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

                        notify("Assessment sent", f"Delivered to {recipient_email}", kind="success")

                    except Exception as e:
                        notify("Failed to send email", kind="error")
                        st.exception(e)
