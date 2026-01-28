# AISQL Site Safety Hazard & Risk Inspection

An end-to-end **AI-powered site safety inspection application** built using  
**Snowflake AI SQL** and **Streamlit**.

This application analyzes site inspection images to:
- Detect safety hazards
- Classify hazard types
- Generate risk scores and explanations
- Recommend corrective actions
- Aggregate site-level risk insights
- Export reports and send email notifications

---

## 🎥 Demo Video
▶️ **Application Demo (YouTube):**  
https://youtu.be/CPCsGUesCik

---

## 🏗️ Architecture Overview

**Technology Stack**
- Snowflake AI SQL (`AI_FILTER`, `AI_CLASSIFY`, `AI_COMPLETE`)
- Streamlit (UI)
- Snowflake Email Notification Integration

**High-Level Flow**
1. Images are uploaded to a Snowflake internal stage
2. `AI_FILTER` screens out non-actionable images
3. `AI_CLASSIFY` identifies hazard categories
4. `AI_COMPLETE` generates:
   - Risk score (0–10)
   - Hazard descriptions
   - Corrective actions
   - Site-wide prioritized actions
5. Results are aggregated into site-level insights
6. Reports can be downloaded or emailed

---

## 📂 Repository Structure

### `ai_sql_functions/`
Contains isolated AISQL queries used by the application.

| File | Description |
|-----|------------|
| `01_ai_filter.sql` | Pre-check to detect potential hazards |
| `02_ai_classify.sql` | Multi-label hazard classification |
| `03_ai_complete_detected_hazards.sql` | Detailed hazard detection |
| `04_ai_complete_prioritized_actions.sql` | Site-wide top 3 actions |
| `05_ai_complete_recommended_actions.sql` | Image-level corrective actions |
| `06_ai_complete_risk_explanation.sql` | Risk explanation |
| `07_ai_complete_risk_score.sql` | Risk score generation |

---

### `application/`
Streamlit application source code.

- `site_safety_inspection_app.py`  
  Main application that:
  - Uploads images
  - Executes AISQL queries
  - Displays results
  - Exports reports
  - Sends email notifications

---

### `setup_scripts/`
Snowflake object creation scripts.

| Script | Purpose |
|------|--------|
| `01_stage_setup.sql` | Create image upload stage |
| `02_tables_setup.sql` | Create history tables |
| `03_notification_setup.sql` | Email notification integration |
| `04_permissions.sql` | Role & privilege grants |

Run these scripts **in order** during setup.

---

## ⚙️ Setup Instructions

1. **Run setup scripts (ACCOUNTADMIN)**  
   Execute all files in `setup_scripts/` in sequence.

2. **Verify Snowflake Cortex is enabled**
   - Required for `AI_FILTER`, `AI_CLASSIFY`, `AI_COMPLETE`

3. **Deploy Streamlit App**
   - Run via Snowflake Native Streamlit or locally with Snowpark

---

## ⚠️ Model Notes

- **Model used:** Claude Sonnet 4.0
- Fixed model selection ensures:
  - Consistent risk scoring
  - Comparable outputs
  - Auditability
- Supports **up to 20 images per prompt**

---

## 📤 Outputs & Exports

- Site Safety Report (HTML)
- Corrective Actions Checklist (CSV)
- Email notifications for high-risk sites
- Historical risk trend tracking

---

## 📸 Sample Inputs

### `input_images/`
Contains sample site inspection images used for:
- Local testing
- Demo recordings
- Reproducible analysis

---
## 📊 Presentation & Demo Assets

### `presentation/`
Contains presentation materials submitted with this project.

- **PowerPoint slides**: 📁 `presentation/Rafi AISQL Challenge_Site Safety Hazard & Risk Inspection.pptx`
- **Demo screenshots** used in the presentation

---

## 👤 Author

**Rafi Aqila Hidayat**  
Synogize Snowflake AISQL Innovation Challenge
