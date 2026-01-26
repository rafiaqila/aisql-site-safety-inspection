SELECT
    AI_COMPLETE(
        'claude-4-sonnet',
        'Provide specific corrective actions for the hazards in this image. Use this exact format:
- [Action 1]
- [Action 2]
- [Action 3]

Do not include any introductory text. Bold keywords. Start directly with the first dash.',
        TO_FILE('@SNOWFLAKE_LEARNING_DB.PUBLIC.SAFETY_IMG_STG','{file_name}')
    ) AS recommended_actions;