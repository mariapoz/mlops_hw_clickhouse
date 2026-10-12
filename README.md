# Kafka → ClickHouse: транзакции по штатам

Домашнее задание №2 курса MLOps МТС ШАД 2025. Транзакции из CSV загружаются в Kafka, ClickHouse читает топик и складывает данные в таблицы. По этим таблицам считается категория наибольшей транзакции в каждом штате, и хранение оптимизировано под этот запрос.

Данные: `train.csv` из соревнования [teta-ml-1-2025](https://www.kaggle.com/competitions/teta-ml-1-2025), 786 431 транзакция.

## Архитектура

```
 data/train.csv
      │  producer/load_to_kafka.py      (п.1)
      ▼
 Kafka: топик transactions  (JSON, одна строка CSV = одно сообщение)
      │
      ▼
 ClickHouse
   fraud.transactions_queue   ENGINE = Kafka        ← читает топик        (п.2)
      ├─ MV transactions_basic_mv ─► fraud.transactions_basic   без оптимизаций (п.2)
      └─ MV transactions_mv       ─► fraud.transactions         оптимизированная (п.4)
                                          │
                                          ▼
                    sql/03_top_category_by_state.sql  →  results/top_category_by_state.csv  (п.3)
```

Таблица с движком Kafka сама ничего не хранит: это «окно» в топик. Materialized view срабатывает на каждый прочитанный блок сообщений и вставляет его в MergeTree-таблицу, где данные и хранятся. К одной Kafka-таблице подключены два MV, поэтому каждое сообщение попадает в обе таблицы, и их удобно сравнивать на одинаковых данных.

## Что где лежит

| Пункт задания | Файл |
|---|---|
| 1. Загрузка CSV в Kafka | `producer/load_to_kafka.py` |
| 2. DDL таблиц, получающих данные из топика | `sql/01_ddl.sql` |
| 3. Запрос: категория наибольшей транзакции по штату | `sql/03_top_category_by_state.sql` |
| 3. Результат запроса | `results/top_category_by_state.csv` |
| 4. Оптимизированный DDL | `sql/02_ddl_optimized.sql` |

Прочие файлы:
- `docker-compose.yml`: Zookeeper, Kafka, создание топика, Kafka UI, ClickHouse и продюсер.
- `clickhouse/kafka.xml`: настройка `auto_offset_reset = earliest`, чтобы ClickHouse читал топик с начала.
- `scripts/run_query.sh`: выполняет запрос п.3 и сохраняет CSV.
- `requirements.txt`: зависимости продюсера.

## Запуск

Требования: Docker 20.10+ и Docker Compose v2. На хосте должны быть свободны порты 8080, 8123, 9000 и 9095.

**1. Клонируйте репозиторий и положите данные.**
```bash
git clone https://github.com/mariapoz/mlops_hw_clickhouse.git
cd mlops_hw_clickhouse
```
Скачайте `train.csv` с [Kaggle](https://www.kaggle.com/competitions/teta-ml-1-2025/data) и положите в папку `data/`, чтобы получилось `data/train.csv`.

**2. Поднимите инфраструктуру.**
```bash
docker compose up -d
docker compose ps -a
```
Когда всё поднимется, `kafka-setup` будет в статусе `Exited (0)` (создал топик и завершился), `clickhouse` в статусе `healthy`, остальные сервисы `running`. При первом старте ClickHouse сам выполняет `sql/01_ddl.sql` и `sql/02_ddl_optimized.sql`.

**3. Загрузите CSV в Kafka (п.1).**
```bash
docker compose run --rm producer
```
Загрузка `train.csv` занимает около минуты. В конце будет строка `Done: 786431 rows read, 786431 delivered, 0 failed`.

Другой файл можно загрузить так: `docker compose run --rm -e CSV_PATH=/data/test.csv producer`. Часть файла: `docker compose run --rm producer --limit 1000`.

Скрипт можно запустить и с хоста, без Docker:
```bash
pip install -r requirements.txt
python producer/load_to_kafka.py --csv data/train.csv --bootstrap localhost:9095
```

**4. Убедитесь, что данные дошли до ClickHouse (п.2).**
```bash
docker compose exec clickhouse clickhouse-client -q "
  SELECT 'basic' AS t, count() FROM fraud.transactions_basic
  UNION ALL
  SELECT 'optimized', count() FROM fraud.transactions"
```
В обеих таблицах должно быть 786431 строк. ClickHouse сбрасывает данные из Kafka блоками, поэтому сразу после загрузки число может расти ещё несколько секунд. Сообщения в топике можно посмотреть в Kafka UI: http://localhost:8080.

**5. Выполните запрос и сохраните результат (п.3).**
```bash
./scripts/run_query.sh
```
Результат запишется в `results/top_category_by_state.csv`: 51 строка (50 штатов и DC).

## П.3. Запрос

```sql
SELECT
    us_state,
    argMax(cat_id, amount) AS top_category,
    max(amount)            AS max_amount
FROM fraud.transactions
GROUP BY us_state
ORDER BY us_state
```

`argMax(cat_id, amount)` возвращает `cat_id` из строки с максимальным `amount` внутри группы, то есть штата. Это один проход по данным без подзапросов и JOIN-ов. Совпадающих максимумов в данных нет, поэтому ответ однозначный. Результат сверен с независимым расчётом в pandas.

Фрагмент результата:
```
us_state,top_category,max_amount
AK,travel,3178.51
AL,travel,10525.07
AZ,shopping_pos,1929.43
NY,travel,27390.12
...
```
Итог: в 35 штатах самая крупная транзакция в категории `travel`, в 8 штатах в `shopping_pos`, ещё в 8 в `shopping_net`.

## П.4. Оптимизация хранения

Базовая таблица `transactions_basic` из п.2: обычные `String`, `ORDER BY tuple()` без сортировки, без кодеков.

Оптимизированная `transactions` из `sql/02_ddl_optimized.sql`:

1. **`LowCardinality(String)`** для всех текстовых колонок. У них от 2 до ~1000 уникальных значений. ClickHouse хранит словарь и номера вместо самих строк: колонки меньше, а `GROUP BY` по ним быстрее.
2. **`ORDER BY (us_state, cat_id, transaction_time)`**. Строки одного штата физически лежат рядом, поэтому лучше сжимаются. Фильтры и группировки по `us_state` используют разреженный первичный индекс.
3. **Кодеки**: `Delta + ZSTD` для времени (соседние значения близки) и `ZSTD` для чисел.
4. **Компактные типы**: `UInt8` для `target`, `UInt32` для `population_city`.
5. **`PROJECTION`** с предагрегацией по штату. При вставке ClickHouse сразу считает `max(amount)` и `argMax(cat_id, amount)` по каждому штату и хранит результат рядом с данными. Запрос п.3 автоматически читает проекцию, а не все строки. Текст запроса при этом не меняется.

`PARTITION BY` здесь сознательно не используется. Партиции в ClickHouse нужны для управления жизненным циклом данных (удалить или перенести месяц целиком), а не для ускорения запросов. На ~800 тыс. строк помесячные партиции дают 15 мелких кусков вместо одного, и запрос без проекции замедляется с 20 до 26 мс.

### Замеры

ClickHouse 24.8, полный `train.csv`, медиана из 100 запусков (`clickhouse-benchmark`):

| Таблица | Время запроса | Прочитано строк | Размер на диске |
|---|---|---|---|
| `transactions_basic` (п.2) | 31 мс | 786 431 | 56 МБ |
| `transactions` без проекции | 17 мс | 786 431 | 24 МБ |
| `transactions` (п.4) | **4 мс** | **51** | 24 МБ |

Отдельные колонки после оптимизации: `us_state` занимала 1.2 МБ, стала 4 КБ; `cat_id` 2.1 МБ → 8 КБ; `merch` 6.2 МБ → 1.5 МБ.

### Как проверить самому

```bash
# Размер таблиц
docker compose exec clickhouse clickhouse-client -q "
  SELECT table, sum(rows) AS rows, formatReadableSize(sum(data_compressed_bytes)) AS on_disk
  FROM system.parts WHERE database = 'fraud' AND active GROUP BY table"

# Запрос читает проекцию: в плане есть ReadFromMergeTree (max_amount_by_state)
docker compose exec clickhouse clickhouse-client -q "
  EXPLAIN SELECT us_state, argMax(cat_id, amount), max(amount)
  FROM fraud.transactions GROUP BY us_state"

# Скорость: базовая таблица против оптимизированной
docker compose exec clickhouse clickhouse benchmark -i 50 --query "
  SELECT us_state, argMax(cat_id, amount), max(amount) FROM fraud.transactions_basic GROUP BY us_state"
docker compose exec clickhouse clickhouse benchmark -i 50 --query "
  SELECT us_state, argMax(cat_id, amount), max(amount) FROM fraud.transactions GROUP BY us_state"
```

## Остановка

```bash
docker compose down       # остановить контейнеры
docker compose down -v    # остановить и удалить данные ClickHouse
```

Если нужно пересоздать таблицы после изменения DDL, сначала выполните `docker compose down -v`. DDL написан с `IF NOT EXISTS`, поэтому уже существующие таблицы при повторном запуске не пересоздаются.
