select
    merchant,
    count() as transaction_count,
    countIf(is_fraud = 1) as fraud_count,
    100.0 * countIf(is_fraud = 1) / count() as fraud_rate,
    sum(amount) as total_amount,
    sumIf(amount, is_fraud = 1) as fraud_amount,
    avg(amount) as avg_amount,
    uniqExact(customer_id) as unique_customers,
    (count() >= 20 and 100.0 * countIf(is_fraud = 1) / count() >= 5) as is_suspicious
from {{ ref('stg_transactions') }}
group by merchant
