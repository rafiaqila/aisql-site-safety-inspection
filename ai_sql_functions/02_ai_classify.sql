SELECT
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
    ) AS hazard_categories;