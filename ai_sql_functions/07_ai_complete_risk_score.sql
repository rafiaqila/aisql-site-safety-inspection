SELECT
    AI_COMPLETE(
        'claude-4-sonnet',
        'Return ONLY a single integer risk score from 0 to 10.',
        TO_FILE('@SNOWFLAKE_LEARNING_DB.PUBLIC.SAFETY_IMG_STG','{file_name}')
    ) AS risk_score;