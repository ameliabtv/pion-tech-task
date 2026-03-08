# pion-tech-task

## Overview

This repository contains my solution for the PetPulse Analytics Engineering Task.

The objective was to build the **Intermediate** and **Marts** layers on top of the provided staging models in order to deliver two analytics-ready outputs:

- `dim_customers` — one row per customer
- `fct_mrr_monthly` — one row per customer per month

The solution is designed to make the data easier to use for self-service reporting by the Marketing and Finance teams.



## Data Model Architecture

The provided staging models are:

- `stg_customers`
- `stg_subscriptions`
- `stg_plans`

I built the following downstream models:

```text
stg_customers
stg_subscriptions + stg_plans
        ↓
int_subscriptions_enriched
        ↓
int_subscription_months
        ↓
fct_mrr_monthly
        ↓
dim_customers
