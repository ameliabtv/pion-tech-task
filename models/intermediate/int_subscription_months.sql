{{ config(materialized='view') }}
  
with subscriptions_enriched as (
  select * from {{ ref('int_subscriptions_enriched') }}
),

date_spine as ( 
{{ 
dbt_utils.date_spine( datepart="month", 
start_date="(select min(start_date) from subscriptions_enriched)", 
end_date="(select current_date)" ) 
}}
), 

expanded as (
select
s.customer_id,
s.subscription_id,
s.plan_id,
s.plan_name,
s.monthly_cost,
date_trunc('month', d.date_month) as revenue_month,
s.subscription_start_date,
s.subscription_end_date

from subscriptions_enriched s
cross join date_spine d 
where date_trunc('month', d.date_month) >= date_trunc('month', s.subscription_start_date)
and (s.subscription_end_date is null or date_trunc('month', d.date_month) <= date_trunc('month', s.subscription_end_date))

)

select *
from expanded
order by customer_id, revenue_month
