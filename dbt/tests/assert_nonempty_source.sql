select 1 as empty_source
where (select count() from {{ source('raw', 'transactions') }}) = 0
