with months as (
select * from {{ ref('int_subscription_months') }}
),

mrr as (
select
customer_id,
revenue_month,
sum(monthly_cost) as mrr
from months
group by
customer_id,
revenue_month
)

select * from mrr
