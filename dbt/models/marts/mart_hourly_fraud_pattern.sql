select
    day_of_week, transaction_hour,
    count() as transaction_count,
    countIf(is_fraud = 1) as fraud_count,
    100.0 * countIf(is_fraud = 1) / count() as fraud_rate,
    sum(amount) as total_amount,
    sumIf(amount, is_fraud = 1) as fraud_amount,
    avg(amount) as avg_amount
from {{ ref('stg_transactions') }}
group by day_of_week, transaction_hour
