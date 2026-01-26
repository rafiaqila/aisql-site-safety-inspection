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

st.markdown("""
<style>
/* App Header Wrapper */
.app-header {
    text-align: center;
    margin: 32px 0 24px 0;
}

/* App Title */
.app-title {
    font-size: 50px;
    font-weight: 800;
    letter-spacing: -0.02em;
    color: #111827;
    margin-bottom: 8px;
}

/* App Caption */
.app-caption {
    font-size: 14px;
    color: #6b7280;
    max-width: 720px;
    margin: 0 auto;
    line-height: 1.6;
}

/* Small section headings (Risk Severity, Risk Score, Hazard Categories) */
.section-title-small {
    font-size: 20px;
    font-weight: 700;
    margin: 12px 0 8px 0;
}

/* Medium section headings (Detected Hazards, Recommended Actions) */
.section-title-medium {
    font-size: 26px;
    font-weight: 700;
    margin: 20px 0 12px 0;
}
</style>
""", unsafe_allow_html=True)

SECTION_CARD_GREY = """
    background:#f9fafb;
    padding:14px;
    border-radius:12px;
    border:1px solid #e5e7eb;
    margin-bottom:12px;
"""

SECTION_CARD_BLUE = """
    background:#f0f6ff;
    padding:18px;
    border-radius:14px;
    border:1px solid #dbeafe;
    margin-bottom:16px;
"""

# --------------------------------------------------
# SECTION: SIDE-BAR
# --------------------------------------------------
with st.sidebar:
    st.markdown("## ⚙️ Model & System Info")

    with st.expander("🤖 Model Used", expanded=True):
            st.markdown("""
            **Claude Sonnet 4.0**
    
            - Vision-capable large language model
            - Optimized for structured reasoning and safety analysis
            - Strong performance on image understanding + text generation
            """)
    
    st.caption("🔒 Model fixed to Claude Sonnet 4.0 for consistency and auditability")

    # ------------------------------
    # MODEL LIMITATIONS
    # ------------------------------
    with st.expander("⚠️ Model Limitations"):
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
    with st.expander("🧠 AI SQL Functions Used"):
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
    with st.expander("⚖️ Risk Severity Determination"):
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
<div class="app-header">
    <div class="app-title">
        🦺 Site Safety Hazard & Risk Inspection
    </div>
    <div class="app-caption">
        Detect safety hazards, calculate risk, and recommend corrective actions using Snowflake Cortex AISQL
    </div>
</div>
""", unsafe_allow_html=True)

st.divider()

# --------------------------------------------------
# SECTION: INPUT
# --------------------------------------------------
st.subheader("Site Information")

site_id = st.text_input("Site ID", value="SITE_A")

uploaded_files = st.file_uploader(
    "Upload site inspection images",
    type=["jpg", "jpeg", "png"],
    accept_multiple_files=True
)

analyze_btn = st.button(
    "🔍 Analyze Site",
    use_container_width=True,
    type="primary"
)

st.divider()

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

    stage_path = f"@SNOWFLAKE_LEARNING_DB.PUBLIC.SAFETY_IMG_STG/{file_name}"

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


def severity_style(severity):
    if severity == "High":
        return "🔴", "#fdecea"
    elif severity == "Medium":
        return "🟡", "#fff8db"
    else:
        return "🟢", "#edf7ed"

def severity_color(sev):
    return {
        "Low": "#16a34a",
        "Medium": "#f59e0b",
        "High": "#dc2626"
    }.get(sev, "#6b7280")

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

    with st.spinner("Analyzing site images..."):
        for uploaded_file in uploaded_files:
            file_name = upload_image_to_stage(uploaded_file)

            # --------------------------------------------------
            # AI_FILTER – Pre-check 
            # --------------------------------------------------
            filter_query = f"""
            SELECT
                AI_FILTER(
                    'Does this image show any unsafe condition, safety hazard, or situation that could pose a risk to people or property?',
                    TO_FILE('@SNOWFLAKE_LEARNING_DB.PUBLIC.SAFETY_IMG_STG','{file_name}')
                ) AS has_potential_hazard
            """

            filter_row = session.sql(filter_query).collect()[0]
            has_potential_hazard = filter_row["HAS_POTENTIAL_HAZARD"]

            # --------------------------------------------------
            # SHORT-CIRCUIT = NO HAZARDS DETECTED
            # --------------------------------------------------
            if not has_potential_hazard:
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
                continue


            query = f"""
SELECT
    AI_COMPLETE(
        'claude-4-sonnet',
        'Return ONLY a single integer risk score from 0 to 10.',
        TO_FILE('@SNOWFLAKE_LEARNING_DB.PUBLIC.SAFETY_IMG_STG','{file_name}')
    ) AS risk_score,

    AI_CLASSIFY(
        TO_FILE('@SNOWFLAKE_LEARNING_DB.PUBLIC.SAFETY_IMG_STG','{file_name}'),
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
        TO_FILE('@SNOWFLAKE_LEARNING_DB.PUBLIC.SAFETY_IMG_STG','{file_name}')
    ) AS detected_hazards,

    AI_COMPLETE(
        'claude-4-sonnet',
        'Provide specific corrective actions for the hazards in this image. Use this exact format:
- [Action 1]
- [Action 2]
- [Action 3]

Do not include any introductory text. Bold keywords. Start directly with the first dash.',
        TO_FILE('@SNOWFLAKE_LEARNING_DB.PUBLIC.SAFETY_IMG_STG','{file_name}')
    ) AS recommended_actions,

    AI_COMPLETE(
        'claude-4-sonnet',
        'Explain concisely why this image received its risk score.
Reference specific visible conditions and explain how they contribute
to the level of risk. Keep the explanation factual, neutral, and
appropriate for a safety inspection report. Limit to a short 1–2 sentences.',
        TO_FILE('@SNOWFLAKE_LEARNING_DB.PUBLIC.SAFETY_IMG_STG','{file_name}')
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

    st.session_state.analysis_results = results


# --------------------------------------------------
# SECTION: DISPLAY IMAGE ANALYSIS RESULTS
# --------------------------------------------------

if results:
    st.success(f"🎉 Site analysis complete – {len(results)} images processed")

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
    # PERSIST SITE RISK HISTORY
    # --------------------------------------------------
    inspection_ts = datetime.now(MY_TZ).strftime("%Y-%m-%d %H:%M:%S")
    
    for hazard, count in hazard_counter.items():
        if hazard == "No Visible Hazard":
            continue
    
        session.sql(f"""
            INSERT INTO SNOWFLAKE_LEARNING_DB.PUBLIC.SITE_HAZARD_HISTORY
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


    for idx, item in enumerate(results, start=1):
        hazard_counter.update(item["hazard_categories"])

        with st.expander(f"📸 Image {idx}: {item['image_name']}", expanded=True):

            img_col, info_col = st.columns([2, 1])

            with img_col:
                st.image(item["image_bytes"], width=720)

            with info_col:
                icon, bg = severity_style(item["severity"])

            
                # --------------------------------------------------
                # RISK SEVERITY
                # --------------------------------------------------
                st.markdown(
                    f"""
                    <div style="{SECTION_CARD_GREY}">
                        <div class="section-title-small">Risk Severity</div>
                        <div style="background:{bg}; padding:14px; border-radius:10px; font-weight:600;">
                            {icon} {item['severity']}
                        </div>
                    </div>
                    """,
                    unsafe_allow_html=True
                )

                # --------------------------------------------------
                # RISK SCORE + EXPLANATION
                # --------------------------------------------------
                if not item["has_potential_hazard"]:
                    risk_html = f"""
                    <div style="background:#f9fafb; padding:16px; border-radius:12px; margin-bottom:16px;">
                        <div style="font-size:15px; font-weight:700; margin-bottom:12px; color:#111827;">
                            Risk Score (0–10)
                        </div>
                        <div style="background:#f0fdf4; padding:14px; border-radius:10px; font-weight:600; margin-bottom:8px;">
                            🟢 {item['score']} / 10
                        </div>
                        <div style="padding-top:8px; border-top:1px solid #e5e7eb;">
                            <div style="font-size:13px; font-weight:700; margin-bottom:4px; color:#166534;">
                                🧠 Why this risk score?
                            </div>
                            <div style="font-size:13px; color:#374151; line-height:1.6;">
                                {item['risk_explanation']}
                            </div>
                        </div>
                    </div>
                    """
                else:
                    risk_html = f"""
                    <div style="background:#f9fafb; padding:16px; border-radius:12px; margin-bottom:16px;">
                        <div style="font-size:15px; font-weight:700; margin-bottom:12px; color:#111827;">
                            Risk Score (0–10)
                        </div>
                        <div style="background:#fff8db; padding:14px; border-radius:10px; font-weight:600; margin-bottom:8px;">
                            🟡 {item['score']} / 10
                        </div>
                        <div style="padding-top:8px; border-top:1px solid #e5e7eb;">
                            <div style="font-size:13px; font-weight:700; margin-bottom:4px; color:#4338ca;">
                                🧠 Why this risk score?
                            </div>
                            <div style="font-size:13px; color:#374151; line-height:1.6;">
                                {item['risk_explanation']}
                            </div>
                        </div>
                    </div>
                    """
                st.markdown(risk_html, unsafe_allow_html=True)

                # --------------------------------------------------
                # HAZARD CATEGORIES
                # --------------------------------------------------
                if not item["has_potential_hazard"]:
                    hazard_html = (
                        "<li style='color:#16a34a; font-weight:600;'>"
                        "✅ No hazards detected"
                        "</li>"
                    )
                else:
                    hazard_html = ""
                    for c in item["hazard_categories"]:
                        emoji = HAZARD_EMOJI.get(c, "⚠️")
                        hazard_html += f"<li>{emoji} {c}</li>"
                
                st.markdown(
                    f"""
                    <div style="{SECTION_CARD_GREY}">
                        <div class="section-title-small">Hazard Categories</div>
                        <ul style="
                            margin:0;
                            padding-left:0;
                            list-style:none;
                            margin-top:8px;
                        ">
                            {hazard_html}
                        </ul>
                    </div>
                    """,
                    unsafe_allow_html=True
                )

            # --------------------------------------------------
            # DETECTED HAZARDS
            # --------------------------------------------------
            if not item["has_potential_hazard"]:
                st.markdown(
                    f"""
                    <div style="{SECTION_CARD_BLUE}">
                        <div class="section-title-medium">Detected Hazards</div>
                        <div style="
                            background:#f0fdf4;
                            border:1px solid #bbf7d0;
                            border-radius:10px;
                            padding:14px;
                            color:#166534;
                            font-weight:600;
                        ">
                            ✅ No hazards detected in this image
                        </div>
                    </div>
                    """,
                    unsafe_allow_html=True
                )
            else:
                raw_hazards = str(item["detected_hazards"])
                raw_hazards = raw_hazards.replace("\\n", "\n").strip().strip('"').strip("'")
            
                hazard_lines = []
                for line in raw_hazards.splitlines():
                    line = line.strip()
                    if not line:
                        continue
                    line = re.sub(r"\*\*(.*?)\*\*", r"<strong>\1</strong>", line)
                    line = line.lstrip("-• ").strip()
                    hazard_lines.append(line)
            
                hazards_html = "".join(f"<li>{line}</li>" for line in hazard_lines)
            
                st.markdown(
                    f"""
                    <div style="{SECTION_CARD_BLUE}">
                        <div class="section-title-medium">Detected Hazards</div>
                        <ul style="margin:0; padding-left:18px; line-height:1.6;">
                            {hazards_html}
                        </ul>
                    </div>
                    """,
                    unsafe_allow_html=True
                )


            # --------------------------------------------------
            # RECOMMENDED ACTIONS
            # --------------------------------------------------
            if not item["has_potential_hazard"]:
                st.markdown(
                    f"""
                    <div style="{SECTION_CARD_BLUE}">
                        <div class="section-title-medium">Recommended Actions</div>
                        <div style="
                            background:#f0fdf4;
                            border:1px solid #bbf7d0;
                            border-radius:10px;
                            padding:14px;
                            color:#166534;
                            font-weight:600;
                        ">
                            ✅ No corrective actions required for this image
                        </div>
                    </div>
                    """,
                    unsafe_allow_html=True
                )
            else:
                raw_actions = str(item["recommended_actions"])
                raw_actions = raw_actions.replace("\\n", "\n").strip().strip('"').strip("'")
            
                action_lines = []
                for line in raw_actions.splitlines():
                    line = line.strip()
                    if not line:
                        continue
                    line = re.sub(r"\*\*(.*?)\*\*", r"<strong>\1</strong>", line)
                    line = line.lstrip("-•0123456789. ").strip()
                    action_lines.append(line)
            
                actions_html = "".join(f"<li>{line}</li>" for line in action_lines)
            
                st.markdown(
                    f"""
                    <div style="{SECTION_CARD_BLUE}">
                        <div class="section-title-medium">Recommended Actions</div>
                        <ul style="margin:0; padding-left:18px; line-height:1.6;">
                            {actions_html}
                        </ul>
                    </div>
                    """,
                    unsafe_allow_html=True
                )

    # --------------------------------------------------
    # SECTION: SITE RISK SUMMARY
    # --------------------------------------------------
    st.divider()
    st.header("🏗️ Site Risk Summary")

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
            st.warning(
                f"⚠️ **High risk detected** – notification automatically sent to "
                f"**Safety Manager ({SAFETY_MANAGER_NAME}: {SAFETY_MANAGER_EMAIL})**"
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
    # SITE RISK SUMMARY 3-COLUMN VISUAL LAYOUT
    # --------------------------------------------------
    col1, col2, col3 = st.columns([1, 1.5, 1.5])

    card_style = """
        background-color:#f6f7f9;
        padding:20px;
        border-radius:12px;
        height:100%;
    """

    risk_pct = min(max(int((weighted_score / 10) * 100), 0), 100)
    
    if site_severity == "High":
        risk_color = "#dc2626"   # red
        card_bg = "#fef2f2"      # soft red
    elif site_severity == "Medium":
        risk_color = "#f59e0b"   # amber
        card_bg = "#fffbeb"      # soft amber
    else:
        risk_color = "#16a34a"   # green
        card_bg = "#f0fdf4"      # soft green
    
    threshold_pct = 70  # 7.0 / 10 threshold

    risk_note_map = {
    "High": "This site requires immediate mitigation actions.",
    "Medium": "Mitigation actions should be planned and closely monitored.",
    "Low": "Site risk is currently within acceptable limits."
    }
    
    risk_note = risk_note_map.get(site_severity, "")

    
    with col1:
        st.markdown(f"<div style='{card_style}'>", unsafe_allow_html=True)
        st.markdown(
            "<div class='section-title-small' style='text-align:center;'>Overall Site Risk</div>",
            unsafe_allow_html=True
        )
        components.html(
            f"""
            <div style="
                background:{card_bg};
                padding:20px;
                border-radius:14px;
                height:100%;
                font-family: system-ui, -apple-system, BlinkMacSystemFont, sans-serif;
            ">
                <div style="
                    display:flex;
                    align-items:center;
                    gap:10px;
                    margin:6px 0;
                ">
                    <div style="
                        width:35px;
                        height:35px;
                        background:{risk_color};
                        border-radius:50%;
                        flex-shrink:0;
                    "></div>
                
                    <div style="display:flex; align-items:center; gap:10px;">
                        <span style="
                            width:14px;
                            height:14px;
                            background:{risk_color};
                            border-radius:50%;
                            display:inline-block;
                        "></span>
                    
                        <h2 class="severity-label" style="margin:0;">
                            {site_severity} Risk
                        </h2>
                    </div>

                </div>
    
                <div style="
                    margin:14px 0 16px 0;
                    text-align:center;
                    font-size:54px;
                    font-weight:900;
                    line-height:1.05;
                    letter-spacing:-0.02em;
                    color:{risk_color};
                ">
                    {round(weighted_score, 1)}
                    <span style="
                        font-size:24px;
                        font-weight:600;
                        color:{risk_color};
                        opacity:0.7;
                    ">
                        / 10
                    </span>
                </div>

                <p style="
                    margin-top:-6px;
                    text-align:center;
                    font-size:12px;
                    font-weight:600;
                    color:#6b7280;
                ">
                    Weighted Site Risk Score
                </p>

                <!-- Gauge -->
                <div style="position:relative; margin-top:16px;">
                    <div style="
                        height:14px;
                        background:linear-gradient(
                            to right,
                            #16a34a 0%,
                            #f59e0b 50%,
                            #dc2626 100%
                        );
                        border-radius:999px;
                    "></div>
    
                    <!-- Animated Indicator -->
                    <div style="
                        position:absolute;
                        top:-4px;
                        left:{risk_pct}%;
                        width:22px;
                        height:22px;
                        background:#111;
                        border-radius:50%;
                        transform:translateX(-50%);
                        transition:left 0.6s ease;
                    "></div>
    
                    <!-- Threshold Marker -->
                    <div style="
                        position:absolute;
                        top:-6px;
                        left:{threshold_pct}%;
                        width:2px;
                        height:26px;
                        background:#111;
                    "></div>
                </div>
    
                <p style="font-size:12px; margin-top:12px; color:#666;">
                    High-risk threshold: 7.0
                </p>

                <p style="
                    margin-top:14px;
                    margin-bottom:20px;
                    font-size:14px;
                    font-weight:500;
                    color:#374151;
                ">
                    {risk_note}
                </p>
            
            </div>
            """,
            height=340
        )


    
    with col2:
        st.markdown(f"<div style='{card_style}'>", unsafe_allow_html=True)
    
        st.markdown(
            "<div class='section-title-small' style='text-align:center;'>Most Frequent Hazards</div>",
            unsafe_allow_html=True
        )
    
        if not filtered_hazards:
            # ✅ Empty state
            st.markdown(
                f"""
                <div style="
                    background:#f0fdf4;
                    border:1px solid #bbf7d0;
                    border-radius:12px;
                    padding:18px;
                    text-align:center;
                    color:#166534;
                    font-weight:600;
                ">
                    ✅ No recurring hazards detected<br/>
                    <span style="font-size:13px; font-weight:400; color:#15803d;">
                        No hazards were identified in this site inspection
                    </span>
                </div>
                """,
                unsafe_allow_html=True
            )
        else:
            hazards_html = "".join(
                f"<li>{HAZARD_EMOJI.get(h, '⚠️')} <strong>{h}</strong>: {c} images</li>"
                for h, c in filtered_hazards
            )

            st.markdown(
                f"""
                <div style="{SECTION_CARD_BLUE}">
                    <ul style="margin:0; padding-left:0; list-style:none;">
                        {hazards_html}
                    </ul>
                </div>
                """,
                unsafe_allow_html=True
            )


    with col3:
        st.markdown(f"<div style='{card_style}'>", unsafe_allow_html=True)
    
        st.markdown(
            "<div class='section-title-small' style='text-align:center'>TOP 3 Prioritized Corrective Actions</div>",
            unsafe_allow_html=True
        )
    
        if not site_has_hazards:
            st.markdown(
                f"""
                <div style="
                    background:#f0fdf4;
                    border:1px solid #bbf7d0;
                    border-radius:12px;
                    padding:18px;
                    text-align:center;
                    color:#166534;
                    font-weight:600;
                ">
                    ✅ No corrective actions required<br/>
                    <span style="font-size:13px; font-weight:400; color:#15803d;">
                        No safety hazards were identified across submitted images
                    </span>
                </div>
                """,
                unsafe_allow_html=True
            )
        else:
            raw_actions = str(prioritized_actions)
            raw_actions = raw_actions.replace("\\n", "\n").strip().strip('"').strip("'")
        
            action_lines = []
            for line in raw_actions.splitlines():
                line = line.strip()
                if not line:
                    continue
        
                line = re.sub(r"\*\*(.*?)\*\*", r"<strong>\1</strong>", line)
                line = line.lstrip("-•0123456789. ").strip()
                action_lines.append(line)
        
            actions_html = "".join(f"<li>{line}</li>" for line in action_lines)
        
            st.markdown(
                f"""
                <div style="{SECTION_CARD_BLUE}">
                    <ul style="margin:0; padding-left:18px; line-height:1.6;">
                        {actions_html}
                    </ul>
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
                
                st.markdown('<div style="margin-top:12px;">', unsafe_allow_html=True)
                st.download_button(
                    label="📥 Download Corrective Actions Checklist (CSV)",
                    data=csv_data,
                    file_name=f"{site_id}_corrective_actions_checklist.csv",
                    mime="text/csv",
                    use_container_width=True
                )
                st.markdown('</div>', unsafe_allow_html=True)
            else:
                st.info("No corrective actions available to generate a checklist.")



    # --------------------------------------------------
    # SECTION: SITE RISK HISTORY
    # --------------------------------------------------
    highest_image_score = max(item["score"] for item in results)
    
    session.sql(f"""
    INSERT INTO SNOWFLAKE_LEARNING_DB.PUBLIC.SITE_RISK_HISTORY
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
    st.divider()
    st.header("📈 Site Risk History")
    
    history_df = session.sql(f"""
    SELECT
        INSPECTION_TS AS "Date & Time",
        SITE_ID AS "Site ID",
        IMAGE_COUNT AS "Images",
        WEIGHTED_SITE_RISK_SCORE AS "Weighted Score",
        SITE_SEVERITY AS "Severity",
        HIGHEST_IMAGE_SCORE AS "Highest Image Score"
    FROM SNOWFLAKE_LEARNING_DB.PUBLIC.SITE_RISK_HISTORY
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
        FROM SNOWFLAKE_LEARNING_DB.PUBLIC.SITE_RISK_HISTORY
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
                <div style="
                    background:#f9fafb;
                    padding:18px;
                    border-radius:14px;
                    border:1px solid #e5e7eb;
                ">
                    <p style="margin:0; font-size:13px; color:#6b7280;">
                        Previous Inspection
                    </p>
                    <h2 style="margin:6px 0;">
                        {prev["Weighted Score"]}
                        <span style="font-size:14px; color:#6b7280;">/ 10</span>
                    </h2>
                    <p style="margin:0; font-weight:600; color:{severity_color(prev["Severity"])};">
                        {prev["Severity"]}
                    </p>
                    <p style="margin-top:6px; font-size:12px; color:#9ca3af;">
                        {prev["Date & Time"]}
                    </p>
                </div>
                """,
                unsafe_allow_html=True
            )

        with col_avg:
            st.markdown(
                f"""
                <div style="
                    background:#f4f7ff;
                    padding:18px;
                    border-radius:14px;
                    border:1px solid #c7d2fe;
                    box-shadow:0 4px 14px rgba(99,102,241,0.10);
                ">
                    <p style="margin:0; font-size:13px; color:#4338ca; font-weight:600;">
                        Recent Average
                    </p>
                    <h2 style="margin:6px 0;">
                        {avg_score}
                        <span style="font-size:14px; color:#4338ca;">/ 10</span>
                    </h2>
                    <p style="margin:0; font-weight:600; color:{severity_color(avg_severity)};">
                        {avg_severity}
                    </p>
                    <p style="margin-top:6px; font-size:12px; color:#4338ca;">
                        Last 3 inspections
                    </p>
                </div>
                """,
                unsafe_allow_html=True
            )


        with col_curr:
            st.markdown(
                f"""
                <div style="
                    background:#f0f6ff;
                    padding:18px;
                    border-radius:14px;
                    border:1.5px solid #60a5fa;
                    box-shadow:0 8px 22px rgba(37,99,235,0.18);
                ">
                    <p style="margin:0; font-size:13px; color:#1d4ed8; font-weight:700;">
                        Current Inspection
                    </p>
                    <h2 style="margin:6px 0;">
                        {curr["Weighted Score"]}
                        <span style="font-size:14px; color:#1d4ed8;">/ 10</span>
                    </h2>
                    <p style="margin:0; font-weight:700; color:{severity_color(curr["Severity"])};">
                        {curr["Severity"]}
                    </p>
                    <p style="margin-top:6px; font-size:12px; color:#1d4ed8;">
                        {curr["Date & Time"]}
                    </p>
                </div>
                """,
                unsafe_allow_html=True
            )

        st.markdown("<div style='height:8px;'></div>", unsafe_allow_html=True)
    
        if diff > 0:
            st.warning(f"⬆️ **Site risk increased by {diff} points compared to the previous inspection**")
        elif diff < 0:
            st.success(f"⬇️ **Site risk decreased by {abs(diff)} points compared to the previous inspection**")
        else:
            st.info("➖ **Site risk remains unchanged compared to the previous inspection**")

        # --------------------------------------------------
        # SECTION: HAZARD FREQUENCY TREND (LAST 10 INSPECTIONS)
        # --------------------------------------------------
        hazard_trend_df = session.sql(f"""
        WITH last_10_inspections AS (
            SELECT DISTINCT INSPECTION_TS
            FROM SNOWFLAKE_LEARNING_DB.PUBLIC.SITE_HAZARD_HISTORY
            WHERE SITE_ID = '{site_id}'
            ORDER BY INSPECTION_TS DESC
            LIMIT 10
        )
        SELECT
            HAZARD_CATEGORY,
            SUM(HAZARD_COUNT) AS TOTAL_COUNT
        FROM SNOWFLAKE_LEARNING_DB.PUBLIC.SITE_HAZARD_HISTORY
        WHERE SITE_ID = '{site_id}'
          AND INSPECTION_TS IN (SELECT INSPECTION_TS FROM last_10_inspections)
        GROUP BY HAZARD_CATEGORY
        ORDER BY TOTAL_COUNT DESC
        """).to_pandas()

        st.subheader("📊 Most Recurring Safety Hazards (Last 10 Inspections)")
        
        if hazard_trend_df.empty:
            st.info("No historical hazard data available yet.")
        else:
            # Ensure correct ordering
            hazard_trend_df = hazard_trend_df.sort_values(
                "TOTAL_COUNT", ascending=False
            )
        
            chart = (
                alt.Chart(hazard_trend_df)
                .mark_bar(radius=6)
                .encode(
                    x=alt.X(
                        "HAZARD_CATEGORY:N",
                        sort="-y",
                        title="Hazard Category",
                        axis=alt.Axis(labelAngle=-30)
                    ),
                    y=alt.Y(
                        "TOTAL_COUNT:Q",
                        title="Total Occurrences"
                    ),
                    tooltip=[
                        alt.Tooltip("HAZARD_CATEGORY:N", title="Hazard"),
                        alt.Tooltip("TOTAL_COUNT:Q", title="Occurrences")
                    ]
                )
                .properties(height=320)
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

    st.divider()
    st.header("📤 Share & Export Assessment")
    
    col_dl, col_email = st.columns([1, 1])
    
    # --------------------------------------------------
    # DOWNLOAD REPORT
    # --------------------------------------------------
    with col_dl:
        st.markdown(
            """
            <div style="
                background:#f6f7f9;
                padding:18px;
                border-radius:12px;
            ">
                <h4 style="margin-top:0;">📄 Download Report</h4>
                <p style="color:#555; font-size:14px; margin-bottom:16px;">
                    Export the full site safety assessment as an HTML report
                    for offline review or audit documentation.
                </p>
            """,
            unsafe_allow_html=True
        )

        st.markdown("<div style='height:8px;'></div>", unsafe_allow_html=True)
        
        st.markdown(
            f"""
            <a href="data:text/html;base64,{b64}" download="site_safety_report_{site_id}.html"
               style="
                display:block;
                width:100%;
                text-align:center;
                background:#1f6feb;
                color:white;
                padding:0.4rem;
                border-radius:0.4rem;
                font-weight:600;
                text-decoration:none;
                cursor:pointer;
               ">
                ⬇️ Download Site Safety Report (HTML)
            </a>
            """,
            unsafe_allow_html=True
        )

    
        st.markdown("</div>", unsafe_allow_html=True)
    
    # --------------------------------------------------
    # SEND VIA EMAIL
    # --------------------------------------------------
    with col_email:
        st.markdown(
            """
            <div style="
                background:#f6f7f9;
                padding:18px;
                border-radius:12px;
            ">
                <h4 style="margin-top:0;">✉️ Send via Email</h4>
                <p style="color:#555; font-size:14px; margin-bottom:12px;">
                    Send the assessment summary and prioritized actions
                    directly to stakeholders.
                </p>
            """,
            unsafe_allow_html=True
        )

        st.markdown("<div style='height:8px;'></div>", unsafe_allow_html=True)
    
        recipient_email = st.text_input(
            "Recipient Email Address",
            placeholder="Enter receipient email address here",
            label_visibility="collapsed"
        )
    
        send_email_btn = st.button(
            "📤 Send Site Risk Assessment",
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
