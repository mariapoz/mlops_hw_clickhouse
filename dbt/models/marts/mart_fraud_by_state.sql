select
    state,
    count() as transaction_count,
    countIf(is_fraud = 1) as fraud_count,
    100.0 * countIf(is_fraud = 1) / count() as fraud_rate,
    sum(amount) as total_amount,
    sumIf(amount, is_fraud = 1) as fraud_amount,
    avg(amount) as avg_amount,
    uniqExact(customer_id) as unique_customers,
    uniqExact(merchant) as unique_merchants
from {{ ref('stg_transactions') }}
group by state
