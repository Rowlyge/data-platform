-- Request volume and average latency broken down by upstream server.

with stg as (
    select * from {{ ref('stg_requests') }}
),

aggregated as (
    select
        upstream,
        count(*) as total_requests,
        round(avg(duration_ms), 2) as avg_duration_ms,
        count(*) filter (where status_code >= 400) as error_requests
    from stg
    group by 1
)

select * from aggregated
order by total_requests desc
