with source as (
    select * from {{ source('snowflake_raw', 'RAW_POSTS') }}
),

renamed as (
    select
        json_data:id::integer as post_id,
        json_data:userId::integer as user_id,
        json_data:title::string as title,
        json_data:body::string as body,
        ingested_at
    from source
)

select * from renamed
