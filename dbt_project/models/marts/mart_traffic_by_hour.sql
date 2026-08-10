-- Traffic volume aggregated by hour.

with stg as (
    select * from {{ ref('stg_requests') }}
),

aggregated as (
    select
        date_trunc('hour', requested_at) as hour,
        count(*) as total_requests,
        sum(response_size) as total_response_bytes
    from stg
    group by 1
)

select * from aggregated
order by hour
