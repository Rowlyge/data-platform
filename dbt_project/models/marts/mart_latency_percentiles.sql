-- Latency percentiles (p50, p95, p99) aggregated by day.

with stg as (
    select * from {{ ref('stg_requests') }}
),

aggregated as (
    select
        date_trunc('day', requested_at) as day,
        count(*) as total_requests,
        percentile_cont(0.50) within group (order by duration_ms) as p50_duration_ms,
        percentile_cont(0.95) within group (order by duration_ms) as p95_duration_ms,
        percentile_cont(0.99) within group (order by duration_ms) as p99_duration_ms
    from stg
    group by 1
)

select * from aggregated
order by day
