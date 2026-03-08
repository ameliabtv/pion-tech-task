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
)

select
c.customer_id,
c.customer_name,
lower(c.customer_email) as customer_email,
coalesce(c.region_raw, 'unknown') as region,
coalesce(c.marketing_source_raw, 'unknown') as marketing_source,
c.signup_at,
coalesce(l.lifetime_value, 0) as lifetime_value,
s.current_subscription_status

from customers c
left join ltv l on c.customer_id = l.customer_id
left join current_status s on c.customer_id = s.customer_id
