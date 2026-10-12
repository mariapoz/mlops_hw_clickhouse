-- =============================================================================
-- П.2. Таблицы ClickHouse, получающие данные из топика Kafka `transactions`.
--
-- Схема:  Kafka topic ──► transactions_queue (ENGINE = Kafka)
--                              │  materialized view transactions_basic_mv
--                              ▼
--                         transactions_basic (ENGINE = MergeTree)
--
-- Таблица с движком Kafka сама ничего не хранит: это "окно" в топик.
-- Materialized view срабатывает на каждый прочитанный блок сообщений
-- и вставляет его в обычную MergeTree-таблицу, где данные и хранятся.
-- =============================================================================

CREATE DATABASE IF NOT EXISTS fraud;

-- 1. Чтение топика. Продюсер шлёт значения строками "как в CSV",
--    ClickHouse сам приводит "148.04" -> Float64 и т.п.
--    transaction_time оставляем строкой (в данных формат 'YYYY-MM-DD HH:MM'
--    без секунд) и парсим в materialized view.
CREATE TABLE IF NOT EXISTS fraud.transactions_queue
(
    transaction_time String,
    merch            String,
    cat_id           String,
    amount           Float64,
    name_1           String,
    name_2           String,
    gender           String,
    street           String,
    one_city         String,
    us_state         String,
    post_code        String,
    lat              Float64,
    lon              Float64,
    population_city  UInt32,
    jobs             String,
    merchant_lat     Float64,
    merchant_lon     Float64,
    target           UInt8
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'transactions',
    kafka_group_name = 'clickhouse_transactions',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_skip_broken_messages = 100;   -- битые сообщения пропускаем, а не останавливаем чтение

-- 2. Хранение: "наивная" таблица без оптимизаций.
--    Обычные String, без сортировки (ORDER BY tuple()), без партиций и кодеков.
CREATE TABLE IF NOT EXISTS fraud.transactions_basic
(
    transaction_time DateTime,
    merch            String,
    cat_id           String,
    amount           Float64,
    name_1           String,
    name_2           String,
    gender           String,
    street           String,
    one_city         String,
    us_state         String,
    post_code        String,
    lat              Float64,
    lon              Float64,
    population_city  UInt32,
    jobs             String,
    merchant_lat     Float64,
    merchant_lon     Float64,
    target           UInt8
)
ENGINE = MergeTree
ORDER BY tuple();

-- 3. Перекладка из топика в таблицу хранения
CREATE MATERIALIZED VIEW IF NOT EXISTS fraud.transactions_basic_mv
TO fraud.transactions_basic
AS
SELECT
    parseDateTimeBestEffort(transaction_time) AS transaction_time,
    merch, cat_id, amount, name_1, name_2, gender, street, one_city, us_state,
    post_code, lat, lon, population_city, jobs, merchant_lat, merchant_lon, target
FROM fraud.transactions_queue;
