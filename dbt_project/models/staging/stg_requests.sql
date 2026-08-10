-- Staging model: 1:1 mirror of the Raw layer, with light cleanup
-- (type casting, column renaming). No business logic here.

with source as (

    select *
    from read_parquet(
        's3://data-lake/raw/requests/*/*/*/*.parquet',
        hive_partitioning = true
    )

),

renamed as (

    select
        id                 as request_id,
        method              as http_method,
        path                as request_path,
        status_code,
        duration_ms,
        response_size,
        upstream,
        client_ip,
        user_agent,
        created_at::timestamp as requested_at

    from source

)

select * from renamed
