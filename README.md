## POC of DBT integration

Compiles provided preql script to one dbt model per persist statement.

Executes via DBT CLI.

## How to run

```bash
python preqlt/scripts/main.py C:\Users\ethan\coding_projects\pypreql-etl\jaffle_shop\models\example\customer.preql C:\Users\ethan\coding_projects\pypreql-etl\jaffle_shop bigquery
```