SELECT
    AI_COMPLETE(
        'claude-4-sonnet',
        'Explain concisely why this image received its risk score.
Reference specific visible conditions and explain how they contribute
to the level of risk. Keep the explanation factual, neutral, and
appropriate for a safety inspection report. Limit to a short 1–2 sentences.',
        TO_FILE('@SNOWFLAKE_LEARNING_DB.PUBLIC.SAFETY_IMG_STG','{file_name}')
    ) AS risk_explanation;