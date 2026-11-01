.PHONY: deps debug build test docs lint fix check versions
deps:
	dbt deps --profiles-dir .
debug:
	dbt debug --profiles-dir .
build:
	dbt build --profiles-dir .
test:
	dbt test --profiles-dir .
docs:
	dbt docs generate --profiles-dir .
	dbt docs serve --profiles-dir . --port 8081
lint:
	sqlfluff lint models macros tests
fix:
	sqlfluff fix models macros tests
check: deps build lint
versions:
	dbt --version
