{{ config(
    materialized='table',
    schema='gold'
) }}

with silver_posts as (
    select * from {{ ref('stg_posts') }}
),

user_summary as (
    select
        user_id,
        count(post_id) as total_posts,
        avg(length(title)) as avg_title_length
    from silver_posts
    group by user_id
)

select * from user_summary
