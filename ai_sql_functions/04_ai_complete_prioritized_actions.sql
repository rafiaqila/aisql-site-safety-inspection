SELECT 
	AI_COMPLETE(
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
            ) AS prioritized_actions;