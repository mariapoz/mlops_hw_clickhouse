"""Execute dbt-compiled SQL using an embedded ClickHouse and synthetic fixtures.
Optional dependency: pip install chdb==4.4.0. Run dbt compile first (see README).
This does not replace dbt build on the real source.
"""
from pathlib import Path
import json
import tempfile
import yaml
from chdb import session

root = Path(__file__).resolve().parents[1]
compiled = root / 'target/compiled/fraud_analytics'
with tempfile.TemporaryDirectory() as directory:
    db = session.Session(directory)
    def query(sql):
        return json.loads(str(db.query(sql, 'JSON')))['data']

    db.query('CREATE DATABASE fraud')
    db.query('CREATE DATABASE fraud_analytics')
    db.query('''CREATE TABLE fraud.transactions ENGINE=Memory AS SELECT
        toDateTime('2024-01-07 00:00:00') + toIntervalHour(number % 24) AS transaction_time,
        if(number % 2 = 0, ' Shop ', ' Store ') AS merch,
        if(number % 2 = 0, ' FOOD ', ' TRAVEL ') AS cat_id,
        arrayElement([0., 99.99, 100., 999.99, 1000.], number % 5 + 1) AS amount,
        concat('name', toString(intDiv(number, 100))) AS name_1,
        ' Smith ' AS name_2, ' f ' AS gender,
        if(number % 2 = 0, ' dc ', ' ca ') AS us_state,
        38.9 AS lat, -77.0 AS lon, 39.0 AS merchant_lat, -77.1 AS merchant_lon,
        toUInt8(number < 5 OR number = 100) AS target
        FROM numbers(201)''')
    models = [compiled / 'models/staging/stg_transactions.sql'] + sorted((compiled / 'models/marts').glob('*.sql'))
    for f in models:
        db.query(f'CREATE TABLE fraud_analytics.{f.stem} ENGINE=Memory AS {f.read_text()}')
    manifest = json.loads((root / 'target/manifest.json').read_text())
    tests = [n for n in manifest['nodes'].values() if n['resource_type'] == 'test' and n['package_name'] == 'fraud_analytics']
    for test in tests:
        sql = test.get('compiled_code')
        assert sql, f"Not compiled: {test['name']}"
        rows = query(sql)
        assert not rows, (test['name'], rows[:3])
    print(f'PASS: {len(models)} compiled models and {len(tests)} data-test queries on synthetic data')
    # Execute the actual model SQL against each native dbt unit-test SQL fixture.
    unit_tests = yaml.safe_load((root / 'models/unit_tests.yml').read_text())['unit_tests']
    for unit in unit_tests:
        fixture = unit['given'][0]
        db.query('DROP TABLE IF EXISTS fraud_analytics.unit_input')
        db.query('CREATE TABLE fraud_analytics.unit_input ENGINE=Memory AS ' + fixture['rows'])
        model = next(f for f in models if f.stem == unit['model'])
        original = '`fraud`.`transactions`' if unit['model'] == 'stg_transactions' else '`fraud_analytics`.`stg_transactions`'
        sql = model.read_text().replace(original, 'fraud_analytics.unit_input')
        actual = query(sql)
        expected = unit['expect']['rows']
        columns = list(expected[0])
        normalize = lambda rows: sorted((tuple(r[c] for c in columns) for r in rows), key=repr)
        assert normalize(actual) == normalize(expected), (unit['name'], actual, expected)
        print('PASS: fixture logic ' + unit['name'])
    print('Engine: ' + query('SELECT version() AS version')[0]['version'])
    print('Not executed: dbt materializations/unit-test runner, real dataset, dbt Docs catalog.')
