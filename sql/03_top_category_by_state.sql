-- П.3. Категория наибольшей транзакции по каждому штату.
--
-- argMax(cat_id, amount) возвращает значение cat_id из строки,
-- в которой amount максимален (в пределах группы — штата).
-- Это один проход по данным без подзапросов и JOIN-ов.
--
-- Запрос читает оптимизированную таблицу из п.4; на таблице из п.2
-- результат тот же (замените fraud.transactions на fraud.transactions_basic).
SELECT
    us_state,
    argMax(cat_id, amount) AS top_category,
    max(amount)            AS max_amount
FROM fraud.transactions
GROUP BY us_state
ORDER BY us_state
