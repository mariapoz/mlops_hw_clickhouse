# dbt: staging и шесть витрин анализа транзакций и фрода

Добавлен проект `dbt/` поверх существующей `fraud.transactions` с настраиваемой базой источника.
Staging нормализует типы и текст, добавляет дату/час/день недели, сегмент суммы и прокси ID клиента.
Шесть витрин: daily_state_metrics, fraud_by_category, fraud_by_state,
customer_risk_profile, hourly_fraud_pattern, merchant_analytics (все с префиксом mart_).

Версии: dbt-core 1.10.23, dbt-clickhouse 1.9.8; ClickHouse исходного Compose — 24.8.
Пакеты: dbt_utils 1.3.0, dbt_date 0.10.1, dbt_expectations 0.10.4.

Проверки: 84 data tests (базовые, составные ключи, expectations, четыре singular tests),
два native dbt unit-теста. Добавлены `.sqlfluff`, `.pre-commit-config.yaml`, `Makefile`.

При подготовке прошли parse/compile и исполнение скомпилированного SQL на синтетических данных;
лог — `dbt/artifacts/offline_check.txt`. Это отдельная проверка, не запуск dbt test на исходном датасете.

Перед отправкой PR заполнить:

- Результат `dbt build` на исходных данных: [вставить фактический результат].
- Лог успешного `dbt test`: [приложить artifacts/dbt_test.log].
- Результат `make lint`: [вставить фактический результат].
- Скриншот DAG из dbt Docs: [приложить изображение].

Порог риска 5% и флаг мерчанта — учебные эвристики.
ID клиента построен по имени и координатам и не гарантирует идентичность человека.
