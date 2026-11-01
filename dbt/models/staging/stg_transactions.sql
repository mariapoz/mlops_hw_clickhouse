with normalized as (
    select
        toDateTime(transaction_time) as transaction_time,
        trimBoth(toString(merch)) as merchant,
        lowerUTF8(trimBoth(toString(cat_id))) as category,
        toFloat64(amount) as amount,
        upperUTF8(trimBoth(toString(us_state))) as state,
        upperUTF8(trimBoth(toString(gender))) as gender,
        toUInt8(target) as is_fraud,
        toFloat64(lat) as lat,
        toFloat64(lon) as lon,
        toFloat64(merchant_lat) as merchant_lat,
        toFloat64(merchant_lon) as merchant_lon,
        {{ dbt_utils.generate_surrogate_key([
            "lowerUTF8(trimBoth(toString(name_1)))",
            "lowerUTF8(trimBoth(toString(name_2)))",
            "toString(lat)", "toString(lon)"
        ]) }} as customer_id
    from {{ source('raw', 'transactions') }}
)
select
    *,
    toDate(transaction_time) as transaction_date,
    toHour(transaction_time) as transaction_hour,
    {{ dbt_date.day_of_week('transaction_time') }} as day_of_week,
    {{ amount_bucket('amount') }} as amount_bucket
from normalized
