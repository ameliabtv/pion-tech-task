with customers as (
select * from {{ ref('stg_customers') }}
),

subscriptions as (
select * from {{ ref('int_subscriptions_enriched') }}
),

mrr as (
select * from {{ ref('fct_mrr_monthly') }}
),

ltv as (
select
customer_id,
sum(mrr) as lifetime_value
from mrr
group by customer_id
),

current_status as (
select
customer_id,
case when max(end_date) is null then 'active'
when max(end_date) >= current_date then 'active'
else 'churned' end as current_subscription_status
from subscriptions
group by customer_id
),

subscription_agg as (
select
customer_id,
min(start_date) as first_subscription_start_date,
count(distinct subscription_id) as total_subscription_count,
sum(case when end_date is null
or end_date >= current_date then 1
else 0 end) as active_subscription_count

from subscriptions
group by customer_id

),

final as (
select
c.customer_id,
c.customer_name,
lower(c.customer_email) as customer_email,
coalesce(c.region_raw, 'unknown') as region,
coalesce(c.marketing_source_raw, 'unknown') as marketing_source,
c.signup_at,
coalesce(l.lifetime_value, 0) as lifetime_value,
s.current_subscription_status,

sa.first_subscription_start_date,
coalesce(sa.total_subscription_count, 0) as total_subscription_count,
coalesce(sa.active_subscription_count, 0) as active_subscription_count

from customers c
left join ltv l on c.customer_id = l.customer_id
left join current_status s on c.customer_id = s.customer_id
left join subscription_agg sa on c.customer_id = sa.customer_id 
  )

select * from final
order by customer_id
