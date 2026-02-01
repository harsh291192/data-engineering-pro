-- Singular test to verify that total_posts is never negative
select *
from {{ ref('user_activity_summary') }}
where total_posts < 0
