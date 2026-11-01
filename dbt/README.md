# Домашняя работа: dbt + ClickHouse

Проект рассчитан на критерии 10 баллов: staging, шесть витрин, собственный макрос,
три пакета, описания и тесты, два unit-теста, sqlfluff, pre-commit и Makefile.
Для сдачи нужно выполнить полный запуск на реальных данных и приложить его лог и скриншот DAG.

## Как это устроено

`source('raw', 'transactions')` → `stg_transactions` → шесть `mart_*`.
Staging — view, витрины — таблицы MergeTree, пересоздаваемые при dbt run/build.
Исходная таблица читается без изменений. Инкрементальная загрузка здесь не требуется.

В исходном репозитории данные находятся в `fraud.transactions`, поэтому это значение по умолчанию.
Если преподаватель требует именно `transactions_db.transactions`, укажите
`export DBT_SOURCE_SCHEMA=transactions_db` и предварительно обеспечьте наличие таблицы с данными в этой базе.
В ClickHouse параметр dbt `schema` означает базу данных.
Результаты создаются в отдельной базе `fraud_analytics`.

## Запуск из существующего репозитория

Распакуйте каталог `dbt/` в корень `mlops_hw_clickhouse`.
Сначала выполните загрузку исходных данных по инструкции в корневом README:

```bash
docker compose up -d
# Только если train.csv ещё не загружен (повторная загрузка создаёт дубликаты):
docker compose run --rm producer

docker compose exec clickhouse clickhouse-client -q 'SELECT count() FROM fraud.transactions'

python3 -m venv .venv
source .venv/bin/activate
pip install -r dbt/requirements.txt
cd dbt
make deps
make debug
mkdir -p artifacts
# bash: pipefail сохраняет ошибочный статус dbt при использовании tee.
set -o pipefail
dbt build --profiles-dir . 2>&1 | tee artifacts/dbt_build.log
dbt test --profiles-dir . 2>&1 | tee artifacts/dbt_test.log
make lint
make docs
```

Docs откроются на http://localhost:8081 (8080 занят Kafka UI).
На странице модели нажмите кнопку DAG в правом нижнем углу и сделайте скриншот.
Сохраните, например, в `dbt/artifacts/dbt_dag.png` и приложите к PR.
Для остановки docs используйте Ctrl+C.

`dbt build` запускает unit-тесты, строит модели и выполняет data tests в порядке зависимостей.
При первом запуске исходная таблица уже должна существовать.
Если нужна раздельная последовательность: `dbt run --profiles-dir .` и `dbt test --profiles-dir .`.
Seeds в решении не используются: справочник штатов не нужен для выбранных расчётов.
Пакеты установлены в `packages.yml` и зафиксированы в `package-lock.yml`.

Переменные окружения (не записывайте пароли в Git):

| Переменная | По умолчанию | Назначение |
|---|---|---|
| DBT_HOST | localhost | ClickHouse HTTP host |
| DBT_USER | default | Пользователь |
| DBT_PASSWORD | пусто | Пароль |
| DBT_SOURCE_SCHEMA | fraud | База исходных данных |
| DBT_SCHEMA | fraud_analytics | База результатов |

Профиль рассчитан на локальный учебный Docker, HTTP 8123 без TLS.
В Docker-контейнере localhost не указывает на другой контейнер; для такой установки нужен DBT_HOST=clickhouse и общая сеть.

## Витрины и определения

| Модель | Одна строка | Метрики |
|---|---|---|
| mart_daily_state_metrics | дата × штат | число, сумма, средний чек, точный P95, процент крупных транзакций |
| mart_fraud_by_category | категория | число транзакций и фродов, fraud_rate, суммы, средний чек |
| mart_fraud_by_state | штат | fraud_rate, число уникальных клиентов и мерчантов, суммы |
| mart_customer_risk_profile | прокси клиента | число, fraud_rate, средний чек, первая/последняя транзакция, риск |
| mart_hourly_fraud_pattern | ISO день недели × час | число, fraud_rate и суммы для сравнения временных окон |
| mart_merchant_analytics | мерчант | оборот, fraud_rate, число клиентов, флаг подозрительности |

- Все доли выражены в процентах 0–100; `countIf(is_fraud = 1)` исключает неоднозначность суммирования метки.
- `amount_bucket`: SMALL <100; MEDIUM от 100 включительно до 1000; LARGE >=1000.
- Риск клиента: HIGH при fraud_rate >=5%; MEDIUM при наличии фрода ниже 5%; LOW без фрода.
- Мерчант подозрителен при >=20 транзакциях и fraud_rate >=5%. Это учебные эвристики, не обученная модель.
- `customer_id` — surrogate key из нормализованных имени, фамилии и исходных координат.
  Настоящего ID клиента нет: совпадение этих полей может объединить людей, смена координат — разделить одного человека.
- `transaction_id` не придуман. Дедупликация без исходного ключа могла бы удалить настоящие повторные покупки.
- Время сохраняет семантику исходного DateTime; локальный час клиента по координатам не вычисляется.
- DC сохраняется. Данные с ошибочной суммой не скрываются фильтром: их выявляет тест.
- `dbt_utils` используется для ключа клиента и уникальности составных ключей.
- `dbt_date.day_of_week` используется в staging. Dispatch в `macros/clickhouse_day_of_week.sql`
  реализует ClickHouse-совместимый вариант с ISO-нумерацией 1–7.
- `dbt_expectations` используется для проверки неотрицательной суммы.

## Тесты и инструменты

84 data tests: not_null, unique, accepted_values, уникальность составных ключей,
проверка диапазона суммы через dbt_expectations и четыре singular tests:
отрицательные/нечисловые суммы, границы fraud_rate, сверка количества транзакций
каждой витрины со staging, непустой источник.
Два native dbt unit-теста проверяют границы 100/1000 и риск 0%/1%/5%, а также нормализацию и ISO-воскресенье.
Все описания моделей и колонок — в `models/schema.yml`; определения unit-тестов — в `models/unit_tests.yml`.

Установка Git hook выполняется из корня Git-репозитория при активном venv:

```bash
pre-commit install --config dbt/.pre-commit-config.yaml
pre-commit run --config dbt/.pre-commit-config.yaml --all-files
```

SQLFluff использует dbt templater, поэтому для `make lint` требуется доступный ClickHouse.
`make fix` применяет исправления форматирования; после него повторите `make lint` и `make build`.

## Что проверено при подготовке

- dbt-core 1.10.23; dbt-clickhouse 1.9.8; SQLFluff 3.5.0.
- Успешны `dbt deps`, `dbt parse`, компиляция моделей и data tests без обращения к БД.
- Все 7 скомпилированных моделей и 84 SQL-запроса data tests выполнены на синтетических данных в chDB (ClickHouse 26.9.2.1).
- Обе SQL-фикстуры unit-тестов отдельно выполнены с реальным скомпилированным SQL моделей; результаты совпали с expect.
- Лог: `artifacts/offline_check.txt`. Это НЕ лог успешного `dbt test` на исходном датасете.
- В этой среде нет работающего сервера исходного проекта. Не проверены dbt materializations,
  штатный runner unit-тестов, исходный датасет и совместимость выполнения с ClickHouse 24.8.
- SQLFluff с dbt templater остановился из-за недоступности localhost:8123; успешный lint не заявляется.
- Docs с пустым каталогом сгенерированы для проверки сборки документации; перед сдачей выполните обычный `make docs`.

Повторение дополнительной проверки (не требуется для обычного запуска):

```bash
pip install chdb==4.4.0
dbt compile --profiles-dir . --no-populate-cache --no-introspect --select resource_type:model resource_type:test
python scripts/verify_offline.py
```

## Сдача

Создайте отдельную ветку, добавьте `dbt/` и откройте PR. Основа текста — `PR_DESCRIPTION.md`.
Замените поля с результатами фактическими логами, приложите скриншот dbt Docs.
Не добавляйте в Git `target/`, `dbt_packages/`, пароли или сам датасет.
