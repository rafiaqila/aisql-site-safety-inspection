SELECT
    AI_COMPLETE(
        'claude-4-sonnet',
        'List all specific safety hazards visible in this image. Use this exact format:
- [Hazard 1]
- [Hazard 2]
- [Hazard 3]

Do not include any introductory text. Bold keywords. Start directly with the first dash.',
        TO_FILE('@SNOWFLAKE_LEARNING_DB.PUBLIC.SAFETY_IMG_STG','{file_name}')
    ) AS detected_hazards;