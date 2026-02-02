



select
    1
from DE_LEARNING.DBT_DEV_gold.user_activity_summary

where not(total_posts total_posts >= 0)

