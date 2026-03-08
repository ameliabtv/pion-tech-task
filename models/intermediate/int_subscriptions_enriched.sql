with subscriptions as (
  select * from {{ ref('stg_subscriptions') }}
), 

plan as (
  select * from {{ ref('stg_plans') }}
  
),

joined as (
  select s.subscription_id, s.customer_id, s.plan_id, s.status_code, s.start_date, s.end_date, p.plan_name, p.monthly_cost
  from subscriptions s
  left join plan p on s.plan_id = p.plan_id
)

select * from joined
