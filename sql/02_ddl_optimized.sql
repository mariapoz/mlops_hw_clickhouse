-- =============================================================================
-- П.4. Оптимизированное хранение (изменённый DDL таблицы хранения из п.2).
--
-- Источник тот же: fraud.transactions_queue (Kafka) из 01_ddl.sql.
-- К одной Kafka-таблице можно подключить несколько materialized view,
-- поэтому каждое сообщение попадает и в transactions_basic (п.2),
-- и в transactions (п.4) — так их удобно сравнивать на одинаковых данных.
--
-- Что оптимизировано и зачем:
--  1. LowCardinality(String) для всех текстовых колонок. У них от 2 до ~1000
--     уникальных значений: ClickHouse хранит словарь + номера вместо строк,
--     колонки меньше на диске, GROUP BY по ним быстрее.
--  2. ORDER BY (us_state, cat_id, transaction_time). Данные физически
--     отсортированы по штату: строки одного штата лежат рядом, лучше
--     сжимаются, а фильтр/группировка по us_state читают меньше гранул
--     (разреженный первичный индекс).
--  3. Кодеки сжатия: Delta+ZSTD для времени (соседние значения близки),
--     ZSTD для чисел. Меньше байт с диска — быстрее чтение.
--  4. Компактные типы: UInt8 для target, UInt32 для population_city.
--  5. PROJECTION с предагрегацией по штату. ClickHouse при вставке
--     сразу считает max(amount) и argMax(cat_id, amount) по каждому штату
--     и хранит рядом с данными. Запрос из 03_top_category_by_state.sql
--     автоматически читает проекцию (~51 строку на кусок данных),
--     а не все сотни тысяч транзакций. Текст запроса менять не нужно.
--
-- PARTITION BY сознательно не используется: партиции в ClickHouse нужны
-- для управления жизненным циклом данных (удалить/перенести месяц),
-- а не для ускорения. На ~800 тыс. строк помесячные партиции дают 15 мелких
-- кусков вместо одного, и запрос без проекции становится медленнее
-- (замер: 26 мс против 20 мс).
-- =============================================================================

CREATE TABLE IF NOT EXISTS fraud.transactions
(
    transaction_time DateTime                  CODEC(Delta, ZSTD(1)),
    merch            LowCardinality(String),
    cat_id           LowCardinality(String),
    amount           Float64                   CODEC(ZSTD(1)),
    name_1           LowCardinality(String),
    name_2           LowCardinality(String),
    gender           LowCardinality(String),
    street           LowCardinality(String),
    one_city         LowCardinality(String),
    us_state         LowCardinality(String),
    post_code        LowCardinality(String),
    lat              Float64                   CODEC(ZSTD(1)),
    lon              Float64                   CODEC(ZSTD(1)),
    population_city  UInt32                    CODEC(ZSTD(1)),
    jobs             LowCardinality(String),
    merchant_lat     Float64                   CODEC(ZSTD(1)),
    merchant_lon     Float64                   CODEC(ZSTD(1)),
    target           UInt8,

    -- Предагрегация для запроса "категория наибольшей транзакции по штату"
    PROJECTION max_amount_by_state
    (
        SELECT
            us_state,
            argMax(cat_id, amount),
            max(amount)
        GROUP BY us_state
    )
)
ENGINE = MergeTree
ORDER BY (us_state, cat_id, transaction_time);

CREATE MATERIALIZED VIEW IF NOT EXISTS fraud.transactions_mv
TO fraud.transactions
AS
SELECT
    parseDateTimeBestEffort(transaction_time) AS transaction_time,
    merch, cat_id, amount, name_1, name_2, gender, street, one_city, us_state,
    post_code, lat, lon, population_city, jobs, merchant_lat, merchant_lon, target
FROM fraud.transactions_queue;
