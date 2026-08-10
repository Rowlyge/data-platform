-- Error rate (share of non-2xx responses) aggregated by day.

with stg as (
    select * from {{ ref('stg_requests') }}
),

aggregated as (
    select
        date_trunc('day', requested_at) as day,
        count(*) as total_requests,
        count(*) filter (where status_code >= 400) as error_requests,
        round(
            count(*) filter (where status_code >= 400)::float
            / nullif(count(*), 0),
            4
        ) as error_rate
    from stg
    group by 1
)

select * from aggregated
order by day
