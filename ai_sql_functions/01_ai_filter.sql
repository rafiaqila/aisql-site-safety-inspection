SELECT
	AI_FILTER(
        	'Does this image show any unsafe condition, safety hazard, or situation that could pose a risk to people or property?',
        	TO_FILE('@SNOWFLAKE_LEARNING_DB.PUBLIC.SAFETY_IMG_STG','{file_name}')
        ) AS has_potential_hazard;
